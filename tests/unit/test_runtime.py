import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import psycopg
import pytest
from redis.exceptions import RedisError

from orchestrator import runtime as runtime_module
from orchestrator.runtime import Runtime
from shared.broker import StreamMessage
from tests.settings import Settings


async def test_fatal_loop_failure_stops_runtime() -> None:
    runtime = Runtime(Mock(settings=Settings()), Mock())
    operation = AsyncMock(side_effect=ValueError("invariant failed"))
    await runtime._loop("test", operation, 0)
    assert runtime.failure == "test"
    assert runtime.stop.is_set()


async def test_dependency_failure_retries_without_false_fatal_state() -> None:
    runtime = Runtime(Mock(settings=Settings()), Mock())

    async def operation() -> None:
        if not runtime.stop.is_set():
            runtime.stop.set()
            raise TimeoutError

    await runtime._loop("dependency", operation, 0)
    assert runtime.failure is None


@pytest.mark.parametrize("retries", [0, 1, 3])
@pytest.mark.parametrize("failure", [psycopg.OperationalError, RedisError, TimeoutError])
async def test_dependency_failures_stop_after_configured_retries(
    monkeypatch: pytest.MonkeyPatch, retries: int, failure: type[Exception]
) -> None:
    monkeypatch.setattr(runtime_module.asyncio, "sleep", AsyncMock())
    runtime = Runtime(Mock(settings=Settings(orchestrator_retries=retries)), Mock())
    operation = AsyncMock(side_effect=failure("unavailable"))

    await runtime._loop("events", operation, 0)

    assert operation.await_count == retries + 1
    assert runtime.failure == "events"
    assert runtime.stop.is_set()


async def test_success_resets_consecutive_dependency_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(runtime_module.asyncio, "sleep", AsyncMock())
    runtime = Runtime(Mock(settings=Settings(orchestrator_retries=1)), Mock())
    calls = 0

    async def operation() -> None:
        nonlocal calls
        calls += 1
        if calls in (1, 3):
            raise TimeoutError
        if calls == 4:
            runtime.stop.set()

    await runtime._loop("events", operation, 0)

    assert calls == 4
    assert runtime.failure is None


async def test_cancellation_does_not_consume_retry_budget() -> None:
    runtime = Runtime(Mock(settings=Settings(orchestrator_retries=3)), Mock())
    operation = AsyncMock(side_effect=runtime_module.asyncio.CancelledError)

    with pytest.raises(runtime_module.asyncio.CancelledError):
        await runtime._loop("events", operation, 0)

    operation.assert_awaited_once()
    assert runtime.failure is None


async def test_malformed_version_is_rejected_but_future_version_stops_consumption(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    broker = SimpleNamespace(
        read=AsyncMock(return_value=[StreamMessage("1-0", {"schema_version": True})]),
        ack=AsyncMock(),
    )
    apply_event = AsyncMock()
    monkeypatch.setattr(runtime_module, "apply_event", apply_event)
    runtime = Runtime(Mock(settings=Settings()), broker)
    await runtime.events()
    apply_event.assert_not_awaited()
    broker.ack.assert_awaited_once_with("event", "1-0")
    broker.read.return_value = [StreamMessage("2-0", {"schema_version": 2})]
    with pytest.raises(RuntimeError):
        await runtime.events()
    assert broker.ack.await_count == 1
    apply_event.assert_not_awaited()


@pytest.mark.parametrize(
    ("event_concurrency", "pool_size", "expected"), [(2, 5, 2), (10, 5, 3), (10, 1, 1)]
)
async def test_event_batch_runs_concurrently_with_bounded_pool_use(
    monkeypatch: pytest.MonkeyPatch, event_concurrency: int, pool_size: int, expected: int
) -> None:
    messages = [StreamMessage(f"{i}-0", {"schema_version": 1}) for i in range(6)]
    broker = SimpleNamespace(read=AsyncMock(return_value=messages), ack=AsyncMock())
    active = peak = 0
    full = asyncio.Event()
    release = asyncio.Event()

    async def apply(_context, _envelope) -> None:
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        if active == expected:
            full.set()
        try:
            await release.wait()
        finally:
            active -= 1

    apply_event = AsyncMock(side_effect=apply)
    monkeypatch.setattr(runtime_module, "apply_event", apply_event)
    runtime = Runtime(
        Mock(settings=Settings(event_concurrency=event_concurrency, pool_size=pool_size)), broker
    )
    batch = asyncio.create_task(runtime.events())
    try:
        await asyncio.wait_for(full.wait(), 2)
        assert apply_event.await_count == expected
        assert peak == expected
        broker.ack.assert_not_awaited()
    finally:
        release.set()
        await asyncio.wait_for(batch, 2)
    assert active == 0
    assert peak == expected
    assert apply_event.await_count == len(messages)
    assert broker.ack.await_count == len(messages)


@pytest.mark.parametrize("failure", [TimeoutError, RuntimeError])
async def test_failed_event_leaves_only_itself_unacknowledged_and_drains_batch(
    monkeypatch: pytest.MonkeyPatch, failure: type[Exception]
) -> None:
    messages = [StreamMessage(f"{i}-0", {"schema_version": 1, "id": i}) for i in range(5)]
    broker = SimpleNamespace(read=AsyncMock(return_value=messages), ack=AsyncMock())

    async def apply(_context, envelope) -> None:
        if envelope["id"] == 1:
            raise failure("failed entry")

    apply_event = AsyncMock(side_effect=apply)
    monkeypatch.setattr(runtime_module, "apply_event", apply_event)
    runtime = Runtime(Mock(settings=Settings(event_concurrency=1)), broker)
    with pytest.raises(failure, match="failed entry"):
        await runtime.events()
    assert apply_event.await_count == len(messages)
    assert [call.args for call in broker.ack.await_args_list] == [
        ("event", f"{i}-0") for i in (0, 2, 3, 4)
    ]


async def test_batch_invariant_failure_takes_precedence_over_dependency_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    broker = SimpleNamespace(
        read=AsyncMock(
            return_value=[StreamMessage(f"{i}-0", {"schema_version": 1}) for i in range(3)]
        ),
        ack=AsyncMock(),
    )
    monkeypatch.setattr(
        runtime_module, "apply_event", AsyncMock(side_effect=[TimeoutError, ValueError, None])
    )
    runtime = Runtime(Mock(settings=Settings(event_concurrency=1)), broker)
    with pytest.raises(ValueError):
        await runtime.events()
    broker.ack.assert_awaited_once_with("event", "2-0")


async def test_cancelling_event_batch_cancels_and_awaits_inflight_work(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    broker = SimpleNamespace(
        read=AsyncMock(
            return_value=[StreamMessage(f"{i}-0", {"schema_version": 1}) for i in range(5)]
        ),
        ack=AsyncMock(),
    )
    active = 0
    full = asyncio.Event()

    async def apply(_context, _envelope) -> None:
        nonlocal active
        active += 1
        if active == 2:
            full.set()
        try:
            await asyncio.Event().wait()
        finally:
            active -= 1

    monkeypatch.setattr(runtime_module, "apply_event", apply)
    runtime = Runtime(Mock(settings=Settings(event_concurrency=2)), broker)
    batch = asyncio.create_task(runtime.events())
    try:
        await asyncio.wait_for(full.wait(), 2)
    finally:
        batch.cancel()
        with pytest.raises(asyncio.CancelledError):
            await batch
    assert active == 0
    broker.ack.assert_not_awaited()


@pytest.mark.parametrize("kind", ["started", "outcome"])
@pytest.mark.parametrize(
    "identity", [{}, {"event_id": "bad", "execution_id": "bad", "node_id": "a", "try_number": 1}]
)
async def test_malformed_worker_reports_do_not_stop_consumption(kind, identity, monkeypatch):
    broker = SimpleNamespace(ack=AsyncMock(), worker_report=AsyncMock(), forget_outcome=AsyncMock())
    apply = AsyncMock()
    monkeypatch.setattr(runtime_module.storage, "apply_worker_event", apply)
    runtime = Runtime(Mock(settings=Settings()), broker)
    await runtime._process_event(
        StreamMessage("1-0", {"schema_version": 1, "kind": kind, "token": "bad", **identity})
    )
    broker.ack.assert_awaited_once()
    broker.worker_report.assert_not_awaited()
    apply.assert_not_awaited()


async def test_database_error_keeps_the_redis_result_and_event_for_replay(monkeypatch):
    envelope = {
        "schema_version": 1,
        "kind": "outcome",
        "event_id": "00000000-0000-0000-0000-000000000001",
        "execution_id": "00000000-0000-0000-0000-000000000002",
        "node_id": "a",
        "try_number": 1,
        "token": "owner",
    }
    report = {**envelope, "payload": "7"}
    broker = SimpleNamespace(
        ack=AsyncMock(), worker_report=AsyncMock(return_value=report), forget_outcome=AsyncMock()
    )
    apply = AsyncMock(side_effect=psycopg.OperationalError("offline"))
    monkeypatch.setattr(runtime_module.storage, "apply_worker_event", apply)
    runtime = Runtime(Mock(settings=Settings()), broker)
    with pytest.raises(psycopg.OperationalError):
        await runtime._process_event(StreamMessage("1-0", envelope))
    broker.ack.assert_not_awaited()
    broker.forget_outcome.assert_not_awaited()
