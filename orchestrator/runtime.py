import asyncio
import logging
import os
from collections.abc import Awaitable, Callable
from uuid import uuid4

import psycopg
from psycopg_pool import PoolTimeout
from redis.exceptions import RedisError
from tenacity import AsyncRetrying, retry_if_exception_type, stop_after_attempt, wait_fixed

from orchestrator import outbox, storage
from orchestrator.broker import Broker
from orchestrator.config import Settings
from orchestrator.storage import apply_event
from shared.broker import StreamMessage, task_identity
from shared.context import Context

LOG = logging.getLogger(__name__)
DEPENDENCY_ERRORS = (psycopg.Error, PoolTimeout, RedisError, TimeoutError, OSError)


class Runtime:
    # Supervise independent relay, event, and maintenance coroutines so I/O waits can overlap across workflows.
    #
    # Attributes:
    #     context: Database access and orchestrator settings.
    #     broker: Broker for event consumption, publication, and metadata repair.
    #     settings: Local orchestrator settings from context.
    #     stop: Event that asks all loops to stop.
    #     consumer: Unique broker consumer name for this runtime.
    #     failure: Name of a failed loop, or None before an unrecovered failure.
    #     tasks: Background tasks created by start.
    #
    # Returns:
    #     A runtime instance. Call start to create its background tasks.

    def __init__(self, context: Context[Settings], broker: Broker) -> None:
        # Create runtime state without database or broker I/O so application startup controls resource lifetime.
        #
        # Args:
        #     context: Database access and orchestrator settings.
        #     broker: Orchestrator broker for events and publication.
        #
        # Returns:
        #     None.
        self.context = context
        self.broker = broker
        self.settings = context.settings
        self.stop = asyncio.Event()
        self.consumer = f"orchestrator-{os.getpid()}-{uuid4().hex}"
        self.failure: str | None = None
        self.tasks: list[asyncio.Task[None]] = []
        self._consumer_cleanup_at = 0.0

    async def start(self) -> None:
        # Create separate relay, event, and maintenance tasks so each role can progress during other I/O waits.
        #
        # No arguments.
        #
        # Returns:
        #     None. The task list contains the three new background tasks.
        self.tasks = [
            asyncio.create_task(self._loop("publication", self.publish, 0.05)),
            asyncio.create_task(self._loop("events", self.events, 0.01)),
            asyncio.create_task(
                self._loop("maintenance", self.maintain, self.settings.maintenance_interval)
            ),
        ]

    async def close(self) -> None:
        # Signal stop and cancel all loop tasks so resource cleanup can close the broker and database safely.
        #
        # No arguments.
        #
        # Returns:
        #     None after the loop tasks finish cancellation.
        self.stop.set()
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)

    async def _loop(
        self, name: str, operation: Callable[[], Awaitable[None]], interval: float
    ) -> None:
        # Repeat one operation with bounded dependency retries. An unrecovered error records the loop name and stops the runtime.
        #
        # Args:
        #     name: Loop name for diagnostics and failure reports.
        #     operation: Asynchronous operation for one pass.
        #     interval: Seconds between successful passes. Retry waits also include this interval.
        #
        # Returns:
        #     None when stop is set or an operation fails beyond recovery.
        #
        # Raises:
        #     CancelledError: Runtime shutdown cancels this loop.
        retry = AsyncRetrying(
            retry=retry_if_exception_type(DEPENDENCY_ERRORS),
            stop=stop_after_attempt(self.settings.orchestrator_retries + 1),
            wait=wait_fixed(0.5 + interval),
            sleep=asyncio.sleep,
            reraise=True,
            before_sleep=lambda attempt: LOG.warning(
                "dependency_unavailable loop=%s retry=%s", name, attempt.attempt_number
            ),
        )
        while not self.stop.is_set():
            try:
                await retry(operation)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.failure = name
                LOG.error("loop_failed loop=%s type=%s", name, type(exc).__name__)
                self.stop.set()
                return
            await asyncio.sleep(interval)

    async def publish(self) -> None:
        # Run one bounded outbox relay pass so committed tasks and control events reach the broker.
        #
        # No arguments.
        #
        # Returns:
        #     None after the pass.
        #
        # Raises:
        #     RedisError: The broker cannot accept a publication.
        #     psycopg.Error: An outbox operation fails.
        await outbox.publish_once(self.context, self.broker)

    async def events(self) -> None:
        # Apply up to 16 control notifications with bounded concurrency. Complete the batch before an error leaves this method.
        #
        # No arguments.
        #
        # Returns:
        #     None after all messages in the batch finish.
        #
        # Raises:
        #     Exception: A read, event, or acknowledgment fails. Unexpected failures take priority over dependency failures.
        messages = await self.broker.read("event", self.consumer, count=16)
        # Leave pool headroom for publication and maintenance with the default pool.
        concurrency = min(self.settings.event_concurrency, max(1, self.settings.pool_size - 2))
        slots = asyncio.Semaphore(concurrency)

        async def process(message: StreamMessage) -> None:
            # Apply one batch message inside a semaphore slot so event work leaves database capacity for other loops.
            #
            # Args:
            #     message: Decoded broker notification with its entry ID and optional envelope.
            #
            # Returns:
            #     None after application and acknowledgment.
            #
            # Raises:
            #     Exception: The message fails. The failure is logged and propagated to the batch.
            async with slots:
                try:
                    await self._process_event(message)
                except Exception as exc:
                    LOG.error(
                        "event_failed entry_id=%s type=%s", message.entry_id, type(exc).__name__
                    )
                    raise

        results = await asyncio.gather(
            *(process(message) for message in messages), return_exceptions=True
        )
        # Finish the batch before retrying or stopping. Only failed entries remain unacknowledged.
        failures = [result for result in results if isinstance(result, BaseException)]
        if failures:
            # An invariant or version failure must not be hidden by a transient dependency failure.
            raise next(
                (failure for failure in failures if not isinstance(failure, DEPENDENCY_ERRORS)),
                failures[0],
            )

    async def _process_event(self, message: StreamMessage) -> None:
        # Reject malformed messages or apply valid events, then acknowledge the delivery to prevent needless repeats.
        #
        # Args:
        #     message: Broker entry with decoded envelope or a decode-error code.
        #
        # Returns:
        #     None after acknowledgment.
        #
        # Raises:
        #     RuntimeError: The envelope uses an unsupported schema version.
        #     Exception: Event application or broker acknowledgment fails.
        envelope = message.envelope
        if envelope is None:
            LOG.warning("event_rejected code=%s", message.error)
        elif type(envelope.get("schema_version")) is not int:
            LOG.warning("event_rejected code=INVALID_SCHEMA_VERSION")
        elif envelope["schema_version"] != 1:
            raise RuntimeError("Unsupported event schema")
        else:
            if envelope.get("kind") in ("started", "outcome"):
                if not task_identity(envelope) or not isinstance(envelope.get("token"), str):
                    LOG.warning("event_rejected code=INVALID_WORKER_IDENTITY")
                elif envelope["kind"] == "started":
                    await storage.apply_worker_event(self.context, envelope)
                else:
                    outcome = await self.broker.worker_report(envelope)
                    if outcome is not None:
                        await storage.apply_worker_event(self.context, outcome)
                        await self.broker.forget_outcome(outcome)
            else:
                await apply_event(self.context, envelope)
            LOG.info("event_processed entry_id=%s", message.entry_id)
        await self.broker.ack("event", message.entry_id)

    async def maintain(self) -> None:
        # Run due database work, optional publication repair, and periodic consumer cleanup so recovery remains bounded.
        #
        # No arguments.
        #
        # Returns:
        #     None after the maintenance pass.
        #
        # Raises:
        #     RedisError: Broker repair or consumer cleanup fails.
        #     psycopg.Error: Database maintenance fails.
        expired = await storage.expired_worker_claims(self.context)
        checked = []
        for claim in expired:
            ready, outcome = await self.broker.expired_report(claim)
            if not ready:
                continue
            if outcome is not None:
                await storage.apply_worker_event(self.context, outcome)
                await self.broker.forget_outcome(outcome)
            checked.append(str(claim["event_id"]))
        await storage.maintain_once(self.context, checked_claims=checked)
        if self.settings.enable_repair:
            await outbox.repair_publications(self.context, self.broker)
        if asyncio.get_running_loop().time() >= self._consumer_cleanup_at:
            await self.broker.prune_consumers()
            self._consumer_cleanup_at = asyncio.get_running_loop().time() + 60
