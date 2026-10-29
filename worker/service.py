# The stateless worker claims tasks and reports outcomes through Redis.

from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import signal
import socket
import time
import uuid
from contextlib import AsyncExitStack

from redis.exceptions import RedisError
from tenacity import (
    AsyncRetrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
    wait_fixed,
)

from shared.broker import StreamMessage, task_identity
from shared.config import configure_logging
from shared.cpu import BoundedProcessPool
from shared.output import PreparedOutput, prepare_result
from worker.broker import Broker, outcome_envelope
from worker.config import Settings
from worker.execution import run_handler
from worker.messages import Claim
from worker.runner import HandlerResult

LOG = logging.getLogger(__name__)
REPORT_ERRORS = (RedisError, TimeoutError, OSError)
SHUTDOWN_SECONDS = 20


class UnsupportedSchema(RuntimeError):
    # Stop task consumption when the worker cannot interpret a message version.
    #
    # Args:
    #     *args: Optional exception details inherited from RuntimeError.
    #
    # Returns:
    #     An exception that prevents acknowledgment of the unsupported task.

    pass


class Worker:
    # Coordinate bounded task consumption, Redis claims, handlers, and result reports.
    #
    # The Redis claim protects each logical try from duplicate handler calls.
    #
    # Attributes:
    #     settings: Worker capacity, handler mode, and test fixtures.
    #     broker: Shared task consumer.
    #     stop: Event that stops new reads and starts task drain.
    #     consumer: Unique broker consumer identity for this process.
    #
    # Constructor arguments are documented by __init__.
    #
    # Returns:
    #     A Worker instance with no active broker reads or handler tasks.

    def __init__(self, settings: Settings, broker: Broker) -> None:
        # Create worker state. Broker reads and handler tasks start later.
        #
        # Args:
        #     settings: Worker capacity, handler mode, and test fixtures.
        #     broker: Task consumer used for reads and acknowledgments.
        #
        # Returns:
        #     None.
        self.broker = broker
        self.settings = settings
        self.output_pool = BoundedProcessPool()
        self.stop = asyncio.Event()
        self.consumer = f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:8]}"
        self._tasks: set[asyncio.Task[None]] = set()

    def _failpoint(self, name: str) -> None:
        # Exit at a named test point to examine recovery after abrupt worker loss.
        #
        # Args:
        #     name: Current named point in the worker lifecycle.
        #
        # Returns:
        #     None if the point is disabled. An enabled point exits the process immediately.
        if self.settings.enable_test_fixtures and self.settings.worker_failpoint == name:
            os._exit(86)

    @staticmethod
    def _envelope_error(message: StreamMessage) -> str | None:
        # Reject malformed task identities before Redis access or handler work.
        #
        # Args:
        #     message: Broker entry with its decoded envelope or transport error.
        #
        # Returns:
        #     A rejection code, or None for a supported task shape.
        #
        # Raises:
        #     UnsupportedSchema: The integer schema version is unsupported.
        if message.error or message.envelope is None:
            return message.error or "MISSING_ENVELOPE"
        envelope = message.envelope
        version = envelope.get("schema_version")
        if type(version) is not int:
            return "INVALID_SCHEMA_VERSION"
        if version != 1:
            raise UnsupportedSchema("Unsupported task schema version")
        if envelope.get("kind") != "task":
            return "INVALID_EVENT_KIND"
        for name in ("event_id", "execution_id", "node_id"):
            if not isinstance(envelope.get(name), str) or not envelope[name]:
                return "INVALID_TASK_IDENTITY"
        if type(envelope.get("try_number")) is not int or envelope["try_number"] < 1:
            return "INVALID_TRY_NUMBER"
        if not task_identity(envelope):
            return "INVALID_TASK_IDENTITY"

        if not isinstance(envelope.get("input"), dict) or not isinstance(
            envelope["input"].get("handler"), str
        ):
            return "INVALID_TASK_INPUT"
        for name in ("deadline_ms", "max_output_bytes", "max_depth"):
            if type(envelope.get(name)) is not int or envelope[name] <= 0:
                return "INVALID_TASK_LIMIT"
        timeout = envelope.get("handler_timeout")
        if (
            not isinstance(timeout, (int, float))
            or isinstance(timeout, bool)
            or not math.isfinite(timeout)
            or timeout <= 0
        ):
            return "INVALID_TASK_LIMIT"
        return None

    async def _acknowledge(self, entry_id: str) -> None:
        # Acknowledge a task while its durable claim survives a lost broker reply.
        #
        # Args:
        #     entry_id: Redis task entry ID.
        #
        # Returns:
        #     None. Broker errors produce a warning and leave orchestrator recovery in control.
        try:
            await self.broker.ack("task", entry_id)
        except Exception:
            # A lost broker acknowledgment does not cancel the Redis claim.
            LOG.warning("worker_ack_failed entry=%s", entry_id)

    def _fixture(self, node_id: str, try_number: int) -> dict:
        # Select optional handler behavior for the test profile.
        # Args: node_id selects a fixture, and try_number limits configured failures.
        # Returns: The active fixture, or an empty dictionary outside the test profile.
        if not self.settings.enable_test_fixtures:
            return {}

        fixture = dict(json.loads(self.settings.test_fixtures_json).get(node_id, {}))
        limit = fixture.pop("fail_until_try", None)
        if limit is not None and try_number > limit:
            fixture.pop("fail", None)
            fixture.pop("crash", None)
        return fixture

    async def process_message(self, message: StreamMessage) -> None:
        # Claim and run one task only while its Redis lease permits work.
        #
        # Args:
        #     message: Broker task entry to examine, claim, and acknowledge.
        #
        # Returns:
        #     None after rejection of an invalid or duplicate task, or an outcome write within the deadline.
        #
        # Raises:
        #     UnsupportedSchema: The task version is unsupported and must remain unacknowledged.
        #     Exception: Handler dispatch or unexpected report errors escape to the task wrapper.
        error = self._envelope_error(message)
        if error:
            LOG.warning("worker_message_rejected entry=%s code=%s", message.entry_id, error)
            await self._acknowledge(message.entry_id)
            return
        assert message.envelope is not None
        try:
            claim = await self.broker.claim(message.envelope)
        except Exception:
            LOG.warning("worker_claim_broker_failure entry=%s", message.entry_id)
            return
        if claim is None:
            await self._acknowledge(message.entry_id)
            return
        deadline = claim["deadline"]
        self._failpoint("after_claim_before_ack")
        await self._acknowledge(message.entry_id)
        if deadline <= time.monotonic():
            return
        self._failpoint("after_claim_before_call")
        LOG.info(
            "worker_task_started execution_id=%s node_id=%s try=%s",
            message.envelope["execution_id"],
            message.envelope["node_id"],
            message.envelope["try_number"],
        )
        request = claim["input"].copy()
        request["fixture"] = self._fixture(
            message.envelope["node_id"], message.envelope["try_number"]
        )
        execution = await run_handler(
            request,
            pool=self.output_pool,
            mode=self.settings.handler_mode,
            remaining_seconds=deadline - time.monotonic(),
            max_output_bytes=claim["max_output_bytes"],
            max_depth=claim["max_depth"],
        )
        await self._persist_result(
            message, claim, execution.result, deadline, prepared=execution.prepared
        )

    async def _persist_result(
        self,
        message: StreamMessage,
        claim: Claim,
        result: HandlerResult,
        deadline: float,
        *,
        prepared: PreparedOutput | None = None,
    ) -> None:
        # Retry the same prepared result within its original claim deadline.
        #
        # Args:
        #     message: Claimed task entry with its trusted identity.
        #     claim: Owner, Redis timestamps, and local deadline from the atomic claim.
        #     result: Native handler success, value or error, and runtime metadata.
        #     deadline: Absolute monotonic deadline shared with handler execution.
        #     prepared: Existing immutable output, or None to prepare it once before writes.
        #
        # Returns:
        #     None after a write, deadline expiry, stale-result rejection, or exhausted Redis retries.
        #
        # Raises:
        #     Exception: Unexpected errors escape. Classified Redis errors are logged and suppressed.
        success, _value, _error, runtime = result
        assert message.envelope is not None
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return
        try:
            async with asyncio.timeout(remaining):
                if prepared is None:
                    prepared = await prepare_result(result, pool=self.output_pool)
                encoded = (
                    await self.output_pool.run(
                        outcome_envelope, message.envelope, claim, prepared, runtime
                    )
                    if len(prepared.payload or "") > 16_384
                    else outcome_envelope(message.envelope, claim, prepared, runtime)
                )
                stored: bool = await AsyncRetrying(
                    retry=retry_if_exception_type(REPORT_ERRORS),
                    stop=stop_after_attempt(3),
                    wait=wait_exponential(multiplier=0.1, max=0.2),
                    reraise=True,
                    before_sleep=lambda _: LOG.warning(
                        "worker_outcome_broker_failure entry=%s", message.entry_id
                    ),
                )(
                    self.broker.complete,
                    message.envelope,
                    claim["token"],
                    encoded,
                )
        except REPORT_ERRORS:
            LOG.warning("worker_outcome_broker_exhausted entry=%s", message.entry_id)
            return
        if stored:
            self._failpoint("after_result_publish")
        LOG.info(
            "worker_task_outcome execution_id=%s node_id=%s try=%s success=%s stored=%s",
            message.envelope["execution_id"],
            message.envelope["node_id"],
            message.envelope["try_number"],
            success,
            stored,
        )

    async def _wait_for_capacity(self) -> None:
        # Wait for a worker slot or stop event so full capacity cannot hide shutdown.
        #
        # No arguments beyond this worker instance.
        #
        # Returns:
        #     None when a task finishes or the stop event becomes set.
        stopped = asyncio.create_task(self.stop.wait())
        try:
            await asyncio.wait({*self._tasks, stopped}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            stopped.cancel()
            await asyncio.gather(stopped, return_exceptions=True)

    async def _run_message(self, message: StreamMessage) -> None:
        # Contain one task failure so unrelated active handlers can continue.
        #
        # Args:
        #     message: Broker entry to process.
        #
        # Returns:
        #     None. Unexpected task errors produce a log record.
        #
        # Raises:
        #     CancelledError: Shutdown cancels this task.
        try:
            await self.process_message(message)
        except asyncio.CancelledError:
            raise
        except Exception:
            LOG.exception("worker_message_failed entry=%s", message.entry_id)

    async def _read_messages(self, available: int) -> list[StreamMessage]:
        # Retry broker reads within the number of free worker slots.
        #
        # Args:
        #     available: Number of free worker slots to fill.
        #
        # Returns:
        #     A bounded message list, or an empty list after shutdown stops retries.
        #
        # Raises:
        #     CancelledError: Shutdown cancels the read or its retry wait.
        async for attempt in AsyncRetrying(
            retry=retry_if_exception_type(Exception),
            wait=wait_fixed(1),
            sleep=self._wait_before_read,
            stop=lambda _: self.stop.is_set(),
            after=lambda _: LOG.warning("worker_broker_unavailable"),
            retry_error_callback=lambda _: None,
        ):
            with attempt:
                if self.stop.is_set():
                    return []
                return await self.broker.read("task", self.consumer, count=available)
        return []

    async def _wait_before_read(self, delay: float) -> None:
        # Wait between failed reads until the delay ends or shutdown starts.
        #
        # Args:
        #     delay: Retry delay in seconds.
        #
        # Returns:
        #     None after the delay or stop event.
        try:
            await asyncio.wait_for(self.stop.wait(), timeout=delay)
        except TimeoutError:
            pass

    async def _drain_tasks(self) -> None:
        # Allow active work to finish, then cancel tasks after the shutdown grace.
        #
        # No arguments beyond this worker instance.
        #
        # Returns:
        #     None after all remaining worker tasks finish or cancel.
        if self._tasks:
            _done, pending = await asyncio.wait(self._tasks, timeout=SHUTDOWN_SECONDS)
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)

    async def run(self) -> None:
        # Read tasks only within capacity and drain active work during shutdown.
        #
        # No arguments beyond this worker instance.
        #
        # Returns:
        #     None after the stop event and task drain.
        #
        # Raises:
        #     UnsupportedSchema: An unsupported task version stops new reads.
        #     RedisError: Broker startup fails.
        #     CancelledError: The caller cancels the worker. Active tasks still drain.
        concurrency = self.settings.worker_concurrency
        await self.broker.start()
        LOG.info("worker_ready consumer=%s", self.consumer)
        try:
            while not self.stop.is_set():
                available = concurrency - len(self._tasks)
                if not available:
                    await self._wait_for_capacity()
                    continue
                messages = await self._read_messages(available)
                for message in messages:
                    if self.stop.is_set():
                        break
                    # A version mismatch stops new reads. The message stays available for a compatible worker.
                    self._envelope_error(message)
                    task = asyncio.create_task(self._run_message(message))
                    self._tasks.add(task)
                    task.add_done_callback(self._tasks.discard)
        finally:
            await self._drain_tasks()
            await self.output_pool.close()


async def main() -> None:
    # Open worker resources and connect process signals to graceful shutdown.
    #
    # No arguments. Settings come from WF_ environment variables.
    #
    # Returns:
    #     None after shutdown and resource cleanup.
    #
    # Raises:
    #     Exception: Startup or cleanup fails. Registered cleanup callbacks still run.
    settings = Settings()
    logging_controller = configure_logging(settings)
    broker = Broker(settings)
    worker = Worker(settings, broker)
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, worker.stop.set)
    try:
        await worker.run()
    finally:
        async with AsyncExitStack() as cleanup:
            cleanup.push_async_callback(asyncio.to_thread, logging_controller.close)
            cleanup.push_async_callback(worker.output_pool.close)
            cleanup.push_async_callback(broker.close)
