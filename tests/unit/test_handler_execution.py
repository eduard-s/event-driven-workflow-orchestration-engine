"""Handler dispatch preserves parallel waits, budgets, and process isolation."""

import asyncio
import os
from unittest.mock import AsyncMock

import pytest

import worker.execution as execution_module
from shared.cpu import BoundedProcessPool
from shared.output import prepare_value
from worker.execution import run_handler
from worker.runner import HandlerResult


async def test_trusted_transform_avoids_child_and_preserves_json(monkeypatch) -> None:
    process = AsyncMock()
    monkeypatch.setattr(execution_module, "run_prepared_handler_process", process)
    value = {"items": [None, True, "é", 3]}
    execution = await run_handler(
        {"handler": "input", "data": value},
        pool=BoundedProcessPool(),
        remaining_seconds=2,
        max_output_bytes=1024,
        max_depth=32,
    )
    assert execution.result.success and execution.result.value == value
    assert execution.result.runtime["pid"] == os.getpid()
    assert execution.prepared.payload == '{"items":[null,true,"é",3]}'
    process.assert_not_awaited()


async def test_remote_mock_waits_overlap_without_blocking_loop() -> None:
    async def execute(url):
        return await run_handler(
            {"handler": "call_external_service", "config": {"url": url}, "data": 7},
            pool=BoundedProcessPool(),
            remaining_seconds=4,
            max_output_bytes=1024,
            max_depth=32,
        )

    first, second = await asyncio.gather(execute("http://first"), execute("http://second"))
    runtimes = [execution.result.runtime for execution in (first, second)]
    assert all(execution.result.success for execution in (first, second))
    assert max(runtime["started_at"] for runtime in runtimes) < min(
        runtime["finished_at"] for runtime in runtimes
    )
    assert first.result.value == {"url": "http://first", "data": 7}
    assert second.result.value == {"url": "http://second", "data": 7}


@pytest.mark.parametrize(("mode", "fixture"), [("process", {}), ("auto", {"delay": 30})])
async def test_forced_mode_and_fixtures_keep_process_isolation(monkeypatch, mode, fixture) -> None:
    result = HandlerResult(True, 42)
    prepared = prepare_value(True, 42, None)
    process = AsyncMock(return_value=(result, prepared))
    monkeypatch.setattr(execution_module, "run_prepared_handler_process", process)
    request = {"handler": "input", "data": 42, "fixture": fixture}
    execution = await run_handler(
        request,
        pool=BoundedProcessPool(),
        remaining_seconds=0.3,
        max_output_bytes=1024,
        max_depth=32,
        mode=mode,
    )
    assert execution.result is result and execution.prepared is prepared
    assert process.await_args.args == (request,)
    assert process.await_args.kwargs["remaining_seconds"] == 0.3


@pytest.mark.parametrize(
    ("data", "remaining", "limit", "depth", "code"),
    [
        (None, 0, 1024, 32, "HANDLER_TIMEOUT"),
        ("é", 2, 3, 32, "OUTPUT_TOO_LARGE"),
        ({"one": []}, 2, 1024, 1, "OUTPUT_DEPTH_EXCEEDED"),
        (float("nan"), 2, 1024, 32, "INVALID_HANDLER_OUTPUT"),
    ],
)
async def test_trusted_dispatch_preserves_output_limits(data, remaining, limit, depth, code):
    execution = await run_handler(
        {"handler": "input", "data": data},
        pool=BoundedProcessPool(),
        remaining_seconds=remaining,
        max_output_bytes=limit,
        max_depth=depth,
    )
    assert not execution.result.success
    assert execution.result.error["code"] == code
    assert not execution.prepared.success


async def test_coroutine_timeout_uses_original_budget():
    execution = await run_handler(
        {"handler": "call_external_service", "config": {"url": "http://mock"}},
        pool=BoundedProcessPool(),
        remaining_seconds=0.01,
        max_output_bytes=1024,
        max_depth=32,
    )
    assert not execution.result.success
    assert execution.result.error["code"] == "HANDLER_TIMEOUT"
