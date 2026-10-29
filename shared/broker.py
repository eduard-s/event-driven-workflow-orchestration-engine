# Redis Streams carry task inputs and worker reports. PostgreSQL holds workflow state.

from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Literal
from uuid import UUID

from redis.asyncio import Redis
from redis.exceptions import RedisError, ResponseError
from tenacity import (
    AsyncRetrying,
    RetryCallState,
    retry_if_exception,
    stop_after_attempt,
    wait_none,
)

from shared.config import Settings

Kind = Literal["task", "event"]
MAX_ENVELOPE_BYTES = 33_558_528
LOG = logging.getLogger(__name__)

_REMOVE_IDLE_CONSUMER = """
local pending = redis.call('XPENDING', KEYS[1], ARGV[1], '-', '+', 1, ARGV[2])
if #pending == 0 then
    redis.call('XGROUP', 'DELCONSUMER', KEYS[1], ARGV[1], ARGV[2])
    return 1
end
return 0
"""


def task_identity(envelope: Mapping[str, Any]) -> bool:
    # Reject malformed task identities before Redis key access or database work.
    # Args: envelope contains event, execution, node, and try identifiers.
    # Returns: True for UUID identities, a nonempty node ID, and a positive integer try.
    try:
        UUID(envelope["event_id"])
        UUID(envelope["execution_id"])
        return (
            isinstance(envelope.get("node_id"), str)
            and bool(envelope["node_id"])
            and type(envelope.get("try_number")) is int
            and envelope["try_number"] > 0
        )
    except ValueError, TypeError, KeyError, AttributeError:
        return False


def task_claim_key(settings: Settings, envelope: Mapping[str, Any]) -> str:
    # Scope Redis ownership to one execution, node, and try.
    # Args: settings selects the task stream, and envelope identifies the logical try.
    # Returns: An unambiguous Redis key shared by workers and orchestrators.
    identity = [str(envelope[key]) for key in ("execution_id", "node_id", "try_number")]
    return settings.task_stream + ":claim:" + json.dumps(identity, separators=(",", ":"))


@dataclass(frozen=True, slots=True)
class StreamMessage:
    # Keep a delivery identity and its decoded envelope together for safe consumer handling.
    #
    # Args:
    #     entry_id: Redis stream entry ID as text.
    #     envelope: Decoded envelope dictionary, or None for an invalid record.
    #     error: Rejection code for an invalid record, or None for a decoded envelope.
    #
    # Returns:
    #     An immutable message that consumers can acknowledge or apply.
    entry_id: str
    envelope: dict[str, Any] | None
    error: str | None = None


def _invalid_number(value: str) -> None:
    # Reject non-finite JSON constants so malformed broker records cannot enter state transitions.
    #
    # Args:
    #     value: Invalid JSON constant supplied by the decoder.
    #
    # Returns:
    #     No value. This decoder callback always raises.
    #
    # Raises:
    #     ValueError: The JSON constant is not finite.
    raise ValueError("Non-finite JSON number")


class Broker:
    # Share bounded Redis consumption and acknowledgments between workers and orchestrators.
    #
    # Attributes:
    #     settings: Connection URL and stream names.
    #     client: Redis client used for stream commands.
    #
    # Returns:
    #     A consumer adapter. The constructor does not create stream groups.
    def __init__(self, settings: Settings, *, client: Any | None = None) -> None:
        # Create the Redis adapter and read cursors so each consumer can recover pending deliveries.
        #
        # Args:
        #     settings: Redis URL and task and event stream names.
        #     client: Existing Redis-compatible client, or None to create a bounded client.
        #
        # Returns:
        #     None. Stream groups are created when start runs.
        self.settings = settings
        self.client = (
            client
            if client is not None
            else Redis.from_url(
                settings.redis_url,
                decode_responses=False,
                socket_connect_timeout=3,
                socket_timeout=3,
                max_connections=16,
                health_check_interval=15,
            )
        )
        self._cursors: dict[tuple[Kind, str], str] = {}

    def _stream(self, kind: Kind) -> str:
        # Select the configured stream so task and control deliveries use separate channels.
        #
        # Args:
        #     kind: Task or event channel.
        #
        # Returns:
        #     The configured Redis stream name.
        #
        # Raises:
        #     ValueError: The kind is unsupported.
        if kind == "task":
            return self.settings.task_stream
        if kind == "event":
            return self.settings.event_stream
        raise ValueError("Unknown stream kind")

    @staticmethod
    def _group(kind: Kind) -> str:
        # Select the consumer group so workers and orchestrators retain independent delivery progress.
        #
        # Args:
        #     kind: Task or event channel.
        #
        # Returns:
        #     The workers group for tasks, or the orchestrators group for events.
        #
        # Raises:
        #     ValueError: The kind is unsupported.
        if kind == "task":
            return "workers"
        if kind == "event":
            return "orchestrators"
        raise ValueError("Unknown stream kind")

    async def _ensure_group(self, kind: Kind) -> None:
        # Create a stream group from its first entry so startup and broker repair can consume existing work.
        #
        # Args:
        #     kind: Channel whose stream and consumer group must exist.
        #
        # Returns:
        #     None when the group exists. An existing group needs no change.
        #
        # Raises:
        #     RedisError: Group creation fails for a reason other than an existing group.
        try:
            await self.client.xgroup_create(
                self._stream(kind), self._group(kind), id="0-0", mkstream=True
            )
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    async def start(self) -> None:
        # Create both consumer groups so task and control readers can start independently.
        #
        # No arguments.
        #
        # Returns:
        #     None after both groups exist.
        #
        # Raises:
        #     RedisError: A group cannot be created.
        await self._ensure_group("task")
        await self._ensure_group("event")

    async def close(self) -> None:
        # Remove empty consumers and close Redis so stopped readers do not leave unnecessary metadata.
        #
        # Call this method after all readers stop. Consumers with pending work remain available for recovery.
        #
        # No arguments.
        #
        # Returns:
        #     None after local cursors reset and the client closes.
        #
        # Raises:
        #     Exception: Client cleanup fails. Consumer-removal errors only produce a log record.
        try:
            for kind, consumer in self._cursors:
                try:
                    await self._remove_idle_consumer(kind, consumer)
                except RedisError as exc:
                    if not isinstance(exc, ResponseError) or "NOGROUP" not in str(exc):
                        LOG.warning("consumer_cleanup_deferred kind=%s", kind)
        finally:
            self._cursors.clear()
            await self.client.aclose()

    async def _remove_idle_consumer(self, kind: Kind, consumer: str | bytes) -> int:
        # Atomically remove a consumer only when it has no pending deliveries so cleanup cannot discard unacknowledged work.
        #
        # Args:
        #     kind: Task or event channel.
        #     consumer: Redis consumer name as text or bytes.
        #
        # Returns:
        #     One when the script requests deletion. Zero when pending work prevents deletion.
        #
        # Raises:
        #     RedisError: The atomic consumer-removal command fails.
        return await self.client.eval(
            _REMOVE_IDLE_CONSUMER, 1, self._stream(kind), self._group(kind), consumer
        )

    @staticmethod
    def _text(value: bytes | str) -> str:
        # Convert stream IDs to text so consumers compare one representation.
        #
        # Args:
        #     value: ASCII Redis ID as bytes or existing text.
        #
        # Returns:
        #     The ID as text.
        #
        # Raises:
        #     UnicodeDecodeError: A byte value contains non-ASCII data.
        return value.decode("ascii") if isinstance(value, bytes) else value

    @classmethod
    def _message(cls, entry: Any) -> StreamMessage:
        # Decode a bounded envelope so consumers can reject malformed records without applying them.
        #
        # Args:
        #     entry: Redis entry containing its ID and field mapping.
        #
        # Returns:
        #     A StreamMessage with an envelope, or a rejection code and no envelope.
        #
        # Raises:
        #     ValueError: The outer entry shape or entry ID is invalid.
        entry_id, fields = entry
        entry_text = cls._text(entry_id)
        raw = fields.get(b"envelope", fields.get("envelope")) if fields else None
        if not isinstance(raw, (bytes, str)):
            return StreamMessage(entry_text, None, "MISSING_ENVELOPE")
        try:
            encoded = raw.encode("utf-8") if isinstance(raw, str) else raw
            if len(encoded) > MAX_ENVELOPE_BYTES:
                return StreamMessage(entry_text, None, "ENVELOPE_TOO_LARGE")
            value = json.loads(encoded.decode("utf-8"), parse_constant=_invalid_number)
        except UnicodeError, ValueError, RecursionError:
            return StreamMessage(entry_text, None, "INVALID_ENVELOPE_JSON")
        if not isinstance(value, dict):
            return StreamMessage(entry_text, None, "INVALID_ENVELOPE_TYPE")
        return StreamMessage(entry_text, value)

    async def read(
        self,
        kind: Kind,
        consumer: str,
        count: int = 1,
        block_ms: int = 500,
    ) -> list[StreamMessage]:
        # Read a bounded batch and repair one missing group so broker loss does not permanently stop a reader.
        #
        # Args:
        #     kind: Task or event channel.
        #     consumer: Unique name for this reader process.
        #     count: Maximum deliveries, from one to 100.
        #     block_ms: Maximum wait for fresh data, from zero to 1,000 milliseconds.
        #
        # Returns:
        #     Decoded deliveries, with rejection records for invalid envelopes. An idle read can return an empty list.
        #
        # Raises:
        #     ValueError: The batch or wait bound is invalid.
        #     RedisError: Reading fails after the one missing-group repair retry.
        if not 1 <= count <= 100:
            raise ValueError("Broker batch size must be between 1 and 100")
        if not 0 <= block_ms <= 1000:
            raise ValueError("Broker wait must be between 0 and 1000 milliseconds")

        async def restore_group(_state: RetryCallState) -> None:
            # Restore a lost group and cursor before the single read retry so pending recovery can start again.
            #
            # Args:
            #     _state: Tenacity retry metadata. This callback does not read it.
            #
            # Returns:
            #     None after group creation and cursor reset.
            await self._ensure_group(kind)
            self._cursors[(kind, consumer)] = "0-0"

        messages: list[StreamMessage] = await AsyncRetrying(
            retry=retry_if_exception(
                lambda exc: isinstance(exc, ResponseError) and "NOGROUP" in str(exc)
            ),
            stop=stop_after_attempt(2),
            wait=wait_none(),
            before_sleep=restore_group,
            reraise=True,
        )(self._read, kind, consumer, count, block_ms)
        return messages

    async def _read(
        self,
        kind: Kind,
        consumer: str,
        count: int,
        block_ms: int,
    ) -> list[StreamMessage]:
        # Recover idle pending deliveries before fresh messages so consumer loss does not strand tasks.
        #
        # Args:
        #     kind: Task or event channel.
        #     consumer: Reader name whose recovery cursor is saved locally.
        #     count: Maximum deliveries in this pass.
        #     block_ms: Fresh-data wait limit when no recovered entries are available.
        #
        # Returns:
        #     Up to count decoded deliveries. Redis can return an empty batch.
        #
        # Raises:
        #     RedisError: A pending-claim or fresh-read command fails.
        key = (kind, consumer)
        # Register before I/O: Redis can create a consumer even when its reply
        # does not reach this process.
        self._cursors.setdefault(key, "0-0")
        claimed = await self.client.xautoclaim(
            self._stream(kind),
            self._group(kind),
            consumer,
            min_idle_time=3000,
            start_id=self._cursors[key],
            count=count,
        )
        self._cursors[key] = self._text(claimed[0])
        entries = list(claimed[1])
        remaining = count - len(entries)
        if remaining:
            options: dict[str, Any] = {"count": remaining}
            # BLOCK 0 means an infinite wait in Redis, so omit the argument.
            if not entries and block_ms:
                options["block"] = block_ms
            response: Any = await self.client.xreadgroup(
                self._group(kind),
                consumer,
                {self._stream(kind): ">"},
                **options,
            )
            for _stream, fresh in response:
                entries.extend(fresh)
        return [self._message(entry) for entry in entries]

    async def ack(self, kind: Kind, entry_id: str) -> None:
        # Acknowledge and delete a consumed entry so finished delivery metadata stays bounded.
        #
        # Args:
        #     kind: Task or event channel.
        #     entry_id: Redis stream entry ID to acknowledge.
        #
        # Returns:
        #     None. A missing group is recreated for later deliveries.
        #
        # Raises:
        #     RedisError: Acknowledgment or group repair fails.
        try:
            await self.client.xackdel(
                self._stream(kind), self._group(kind), entry_id, ref_policy="ACKED"
            )
        except ResponseError as exc:
            if "NOGROUP" not in str(exc):
                raise
            # Recreate the group after broker loss. Surviving messages can be delivered again.
            await self._ensure_group(kind)
