# Publish durable events and repair broker metadata for the orchestrator.

import json
from collections.abc import Mapping
from itertools import islice
from typing import Any

from redis.exceptions import ResponseError

from shared.broker import MAX_ENVELOPE_BYTES, Kind, task_claim_key
from shared.broker import Broker as ConsumerBroker

CONSUMER_IDLE_MS = 60_000
CONSUMER_CLEANUP_BATCH = 100


class Broker(ConsumerBroker):
    # Add publication and broker repair to the common consumer adapter. Only the orchestrator owns these operations.
    #
    # Args:
    #     settings: Connection settings and task/event stream names for the inherited constructor.
    #     client: Optional Redis-compatible client for the inherited constructor.
    #
    # Returns:
    #     An orchestrator broker with common read, acknowledgment, and lifecycle methods.

    async def publish(self, kind: Kind, envelope: Mapping[str, Any]) -> str:
        # Append a bounded notification to Redis. Stable event identity permits safe repeated delivery.
        #
        # Args:
        #     kind: The task or event stream selector.
        #     envelope: JSON-compatible notification with the stored event identity.
        #
        # Returns:
        #     The new Redis stream entry ID as text.
        #
        # Raises:
        #     ValueError: The envelope exceeds its size limit or contains invalid JSON numbers.
        #     TypeError: The envelope contains a value that JSON cannot encode.
        #     RedisError: Redis cannot accept the publication.
        encoded = json.dumps(
            envelope,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
            allow_nan=False,
        ).encode("utf-8")
        if len(encoded) > MAX_ENVELOPE_BYTES:
            raise ValueError("Broker envelope exceeds its byte limit")
        result = await self.client.xadd(self._stream(kind), {"envelope": encoded})
        return self._text(result)

    async def exists(self, kind: Kind, entry_id: str, event_id: str) -> bool:
        # Examine a publication receipt before repair so the relay restores only missing logical notifications.
        #
        # Args:
        #     kind: The task or event stream selector.
        #     entry_id: Redis stream entry ID from the stored publication receipt.
        #     event_id: Expected stable event ID inside that entry.
        #
        # Returns:
        #     True if the entry contains the expected event ID. Otherwise, False.
        #
        # Raises:
        #     RedisError: The stream lookup fails.
        response = await self.client.xrange(
            self._stream(kind),
            min=entry_id,
            max=entry_id,
            count=1,
        )
        if not response:
            return False
        message = self._message(response[0])
        return bool(message.envelope and message.envelope.get("event_id") == event_id)

    async def prune_consumers(self) -> int:
        # Remove idle consumers with no pending deliveries to bound broker metadata growth.
        #
        # No arguments.
        #
        # Returns:
        #     The sum of successful removal-script results across both stream groups.
        #
        # Raises:
        #     RedisError: Redis fails for a reason other than a missing stream or group.
        removed = 0
        for kind in ("task", "event"):
            try:
                consumers = await self.client.xinfo_consumers(self._stream(kind), self._group(kind))
                idle = (
                    consumer
                    for consumer in consumers
                    if consumer["idle"] >= CONSUMER_IDLE_MS and consumer["pending"] == 0
                )
                for consumer in islice(idle, CONSUMER_CLEANUP_BATCH):
                    # Recheck pending deliveries atomically after the metadata read.
                    removed += await self._remove_idle_consumer(kind, consumer["name"])
            except ResponseError as exc:
                if "NOGROUP" not in str(exc) and "no such key" not in str(exc).lower():
                    raise
        return removed

    async def worker_report(
        self, envelope: Mapping[str, Any], *, include_start: bool = False
    ) -> dict[str, Any] | None:
        # Read a saved worker report before event or publication repair.
        # Args: envelope identifies a try, and include_start permits a start report when no result exists.
        # Returns: The saved result or permitted start report. Missing reports return None.
        fields = ["outcome", "started"] if include_start else ["outcome"]
        values = await self.client.hmget(task_claim_key(self.settings, envelope), fields)
        raw = next((value for value in values if value is not None), None)
        return self._message(("0-0", {"envelope": raw})).envelope if raw is not None else None

    async def expired_report(
        self, envelope: Mapping[str, Any]
    ) -> tuple[bool, dict[str, Any] | None]:
        # Examine Redis time and saved output atomically before the database declares a timeout.
        # Args: envelope identifies a database claim whose lease appears expired.
        # Returns: Whether timeout recovery can proceed, plus a saved result if one exists.
        ready, raw = await self.client.eval(
            """local result = redis.call('HGET', KEYS[1], 'outcome')
               if result then return {1, result} end
               local lease = redis.call('HGET', KEYS[1], 'lease_ms')
               local now = redis.call('TIME')
               local ms = tonumber(now[1])*1000 + math.floor(tonumber(now[2])/1000)
               if lease and tonumber(lease)>ms then return {0, ''} end
               return {1, ''}""",
            1,
            task_claim_key(self.settings, envelope),
        )
        return bool(ready), self._message(("0-0", {"envelope": raw})).envelope if raw else None

    async def forget_outcome(self, envelope: Mapping[str, Any]) -> None:
        # Remove the cached result after PostgreSQL accepts or rejects its event. Keep the duplicate-task marker.
        # Args: envelope identifies the claim and its owner token.
        # Returns: None after the conditional cleanup.
        await self.client.eval(
            "if redis.call('HGET',KEYS[1],'token')==ARGV[1] then return redis.call('HDEL',KEYS[1],'outcome','started') end return 0",
            1,
            task_claim_key(self.settings, envelope),
            envelope["token"],
        )
