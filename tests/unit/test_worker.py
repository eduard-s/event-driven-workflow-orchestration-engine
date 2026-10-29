import asyncio
import json
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from redis.exceptions import ConnectionError as OperationalError

import worker.runner as runner_module
import worker.service as worker_module
from shared.broker import StreamMessage
from shared.output import prepare_value
from tests.settings import Settings
from worker.execution import HandlerExecution
from worker.runner import HandlerResult, run_handler_process
from worker.service import UnsupportedSchema, Worker


def execution_result(success, value=None, error=None, runtime=None) -> HandlerExecution:
    return HandlerExecution(
        HandlerResult(success, value, error, runtime), prepare_value(success, value, error)
    )


def message(version: int = 1) -> StreamMessage:
    return StreamMessage(
        "1-0",
        {
            "schema_version": version,
            "kind": "task",
            "event_id": "00000000-0000-0000-0000-000000000001",
            "execution_id": "00000000-0000-0000-0000-000000000002",
            "node_id": "input",
            "try_number": 1,
            "input": {"handler": "input", "data": None, "config": {}},
            "deadline_ms": int((time.time() + 60) * 1000),
            "handler_timeout": 5,
            "max_output_bytes": 1024,
            "max_depth": 32,
        },
    )


def make_worker(monkeypatch: pytest.MonkeyPatch) -> tuple[Worker, SimpleNamespace, AsyncMock]:
    storage = SimpleNamespace(claim=AsyncMock(), store_outcome=AsyncMock(return_value=True))
    storage.claim.return_value = {
        "token": "claim-token",
        "input": {"handler": "input", "data": None, "config": {}},
        "deadline": worker_module.time.monotonic() + 5,
        "max_output_bytes": 1024,
        "max_depth": 32,
        "started_ms": 1000,
        "lease_ms": 6000,
    }
    broker = AsyncMock()
    broker.claim = storage.claim
    broker.complete = storage.store_outcome
    settings = Settings(enable_test_fixtures=False, worker_failpoint="", worker_concurrency=4)
    return Worker(settings, broker), storage, broker


async def test_subprocess_preserves_json_and_records_actual_runtime() -> None:
    success, value, error, runtime = await run_handler_process(
        {"handler": "input", "data": {"values": [None, True, 3, "é"]}},
        remaining_seconds=3,
        max_output_bytes=1024,
        max_depth=32,
    )
    assert success and error is None
    assert value == {"values": [None, True, 3, "é"]}
    assert runtime and runtime["pid"] > 0
    assert runtime["finished_at"] >= runtime["started_at"]


async def test_protocol_metadata_does_not_consume_output_budget() -> None:
    success, value, error, _runtime = await run_handler_process(
        {"handler": "input", "data": "é"},
        remaining_seconds=3,
        max_output_bytes=4,
        max_depth=32,
    )
    assert success and value == "é" and error is None


async def test_oversized_child_output_does_not_hang_on_full_pipe() -> None:
    async with asyncio.timeout(5):
        success, _, error, _runtime = await run_handler_process(
            {"handler": "input", "data": "x" * 500000},
            remaining_seconds=3,
            max_output_bytes=128,
            max_depth=32,
        )
    assert not success
    assert error and error["code"] == "OUTPUT_TOO_LARGE" and error["retryable"] is False


async def test_timeout_reaps_child_before_return(monkeypatch: pytest.MonkeyPatch) -> None:
    original = asyncio.create_subprocess_exec
    children = []

    async def capture(*args, **kwargs):
        process = await original(*args, **kwargs)
        children.append(process)
        return process

    monkeypatch.setattr(runner_module.asyncio, "create_subprocess_exec", capture)
    success, _, error, _runtime = await run_handler_process(
        {"handler": "input", "data": None, "fixture": {"delay": 30}},
        remaining_seconds=0.2,
        max_output_bytes=1024,
        max_depth=32,
    )
    assert not success and error and error["code"] == "HANDLER_TIMEOUT"
    assert len(children) == 1 and children[0].returncode is not None


async def test_cancellation_reaps_child(monkeypatch: pytest.MonkeyPatch) -> None:
    original = asyncio.create_subprocess_exec
    created = asyncio.Event()
    children = []

    async def capture(*args, **kwargs):
        process = await original(*args, **kwargs)
        children.append(process)
        created.set()
        return process

    monkeypatch.setattr(runner_module.asyncio, "create_subprocess_exec", capture)
    task = asyncio.create_task(
        run_handler_process(
            {"handler": "input", "data": None, "fixture": {"delay": 30}},
            remaining_seconds=60,
            max_output_bytes=1024,
            max_depth=32,
        )
    )
    await asyncio.wait_for(created.wait(), 3)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert children[0].returncode is not None


async def test_duplicate_acknowledges_without_handler(monkeypatch: pytest.MonkeyPatch) -> None:
    instance, storage, broker = make_worker(monkeypatch)
    storage.claim.return_value = None
    handler = AsyncMock()
    monkeypatch.setattr(worker_module, "run_handler", handler)
    await instance.process_message(message())
    broker.ack.assert_awaited_once_with("task", "1-0")
    handler.assert_not_awaited()
    storage.store_outcome.assert_not_awaited()


async def test_claim_failure_never_acknowledges_or_calls_handler(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    instance, storage, broker = make_worker(monkeypatch)
    storage.claim.side_effect = RuntimeError("database unavailable")
    handler = AsyncMock()
    monkeypatch.setattr(worker_module, "run_handler", handler)
    await instance.process_message(message())
    broker.ack.assert_not_awaited()
    handler.assert_not_awaited()


async def test_delayed_claim_keeps_fresh_budget_and_ack_consumes_it_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    instance, storage, broker = make_worker(monkeypatch)
    clock = [100.0]
    monkeypatch.setattr(worker_module, "time", SimpleNamespace(monotonic=lambda: clock[0]))

    async def delayed_claim(_envelope):
        # A lock/pool wait happens before the database creates the five-second lease.
        clock[0] += 8
        storage.claim.return_value["deadline"] = clock[0] + 5
        return storage.claim.return_value

    async def delayed_ack(_kind, _entry_id):
        clock[0] += 0.2

    storage.claim.side_effect = delayed_claim
    broker.ack.side_effect = delayed_ack
    handler = AsyncMock(return_value=execution_result(True, "completed"))
    monkeypatch.setattr(worker_module, "run_handler", handler)

    await instance.process_message(message())

    assert handler.await_args.kwargs["remaining_seconds"] == pytest.approx(4.8)
    storage.store_outcome.assert_awaited_once()
    envelope, token, encoded = storage.store_outcome.await_args.args
    assert token == "claim-token"
    report = json.loads(encoded)
    assert report["success"] is True and json.loads(report["payload"]) == "completed"


async def test_ack_failure_preserves_authorized_call(monkeypatch: pytest.MonkeyPatch) -> None:
    instance, storage, broker = make_worker(monkeypatch)
    broker.ack.side_effect = RuntimeError("Redis unavailable")
    runtime = {"pid": 123, "started_at": 1.0, "finished_at": 2.0}
    handler = AsyncMock(return_value=execution_result(True, runtime=runtime))
    monkeypatch.setattr(worker_module, "run_handler", handler)
    await instance.process_message(message())
    handler.assert_awaited_once()
    storage.store_outcome.assert_awaited_once()
    report = json.loads(storage.store_outcome.await_args.args[2])
    assert report["runtime"] == runtime
    assert report["success"] is True and report["payload"] == "null"


async def test_outcome_failure_does_not_repeat_handler(monkeypatch: pytest.MonkeyPatch) -> None:
    instance, storage, _broker = make_worker(monkeypatch)
    storage.store_outcome.side_effect = OperationalError("uncertain commit")
    handler = AsyncMock(return_value=execution_result(True, "result"))
    monkeypatch.setattr(worker_module, "run_handler", handler)
    await instance.process_message(message())
    handler.assert_awaited_once()


async def test_unknown_schema_stops_without_acknowledgment(monkeypatch: pytest.MonkeyPatch) -> None:
    instance, storage, broker = make_worker(monkeypatch)
    with pytest.raises(UnsupportedSchema):
        await instance.process_message(message(version=2))
    storage.claim.assert_not_awaited()
    broker.ack.assert_not_awaited()


async def test_unknown_schema_stops_acquisition_loop(monkeypatch: pytest.MonkeyPatch) -> None:
    instance, storage, broker = make_worker(monkeypatch)
    broker.read.return_value = [message(version=2)]
    with pytest.raises(UnsupportedSchema):
        await instance.run()
    broker.read.assert_awaited_once()
    broker.ack.assert_not_awaited()
    storage.claim.assert_not_awaited()


async def test_full_worker_capacity_stops_message_acquisition(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    instance, _storage, broker = make_worker(monkeypatch)
    instance.settings = instance.settings.model_copy(update={"worker_concurrency": 2})
    broker.read.return_value = [message(), message()]
    started = asyncio.Event()
    release = asyncio.Event()
    calls = 0

    async def handler(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 2:
            started.set()
        await release.wait()
        return execution_result(True)

    monkeypatch.setattr(worker_module, "run_handler", handler)
    coordinator = asyncio.create_task(instance.run())
    await asyncio.wait_for(started.wait(), 3)
    assert broker.read.await_count == 1
    assert broker.read.await_args.kwargs["count"] == 2
    instance.stop.set()
    release.set()
    await coordinator


async def test_outcome_retry_reuses_result_without_another_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    instance, storage, _broker = make_worker(monkeypatch)
    storage.store_outcome.side_effect = [OperationalError("uncertain commit"), True]
    handler = AsyncMock(return_value=execution_result(True, {"value": 1}))
    monkeypatch.setattr(worker_module, "run_handler", handler)
    await instance.process_message(message())
    handler.assert_awaited_once()
    assert storage.store_outcome.await_count == 2
    first, second = storage.store_outcome.await_args_list
    assert first == second


async def test_depth_limit_applies_to_value_not_protocol_wrapper() -> None:
    success, _, error, _runtime = await run_handler_process(
        {"handler": "input", "data": {"one": {"two": []}}},
        remaining_seconds=3,
        max_output_bytes=1024,
        max_depth=2,
    )
    assert not success and error and error["code"] == "OUTPUT_DEPTH_EXCEEDED"


@pytest.mark.parametrize(
    ("stdout", "limit", "depth", "error"),
    [
        (b'{"value":{"items":[]},"runtime":{"nested":{"metadata":[]}}}', 12, 2, None),
        (b'{"value":{"items":[]},"runtime":{}}', 12, 1, "OUTPUT_DEPTH_EXCEEDED"),
        (b'{"value":"\\u00e9","runtime":{}}', 4, 1, None),
        (b'{"value":"\\u00e9","runtime":{}}', 3, 1, "OUTPUT_TOO_LARGE"),
        (b'{"value":NaN,"runtime":{}}', 32, 2, "INVALID_HANDLER_OUTPUT"),
        (b'{"value":1e999,"runtime":{}}', 32, 2, "INVALID_HANDLER_OUTPUT"),
        (b'{"value":"\\ud800","runtime":{}}', 32, 2, "INVALID_HANDLER_OUTPUT"),
    ],
)
def test_shared_json_validation_preserves_worker_output_contract(
    stdout: bytes, limit: int, depth: int, error: str | None
) -> None:
    result = runner_module._decode_output(stdout, limit, depth)
    assert result.success is (error is None)
    if error is not None:
        assert result.error and result.error["code"] == error
