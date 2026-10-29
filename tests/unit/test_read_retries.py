"""Read retries preserve group recovery, delivery bounds, and worker shutdown."""

import asyncio
from unittest.mock import AsyncMock, call

import pytest
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import ResponseError

from tests.unit.test_broker import make_broker
from tests.unit.test_worker import make_worker, message


async def test_missing_group_retries_once_and_preserves_read_timeouts() -> None:
    broker, client = make_broker()
    broker._cursors[("task", "worker")] = "90-0"
    client.xautoclaim.side_effect = [ResponseError("NOGROUP missing"), [b"0-0", [], []]]
    client.xreadgroup.return_value = []
    assert await broker.read("task", "worker", count=2, block_ms=700) == []
    reads = client.xautoclaim.await_args_list
    assert [read.kwargs["start_id"] for read in reads] == ["90-0", "0-0"]
    assert all(read.kwargs["min_idle_time"] == 3000 for read in reads)
    assert client.xreadgroup.await_args.kwargs == {"count": 2, "block": 700}
    client.xgroup_create.assert_awaited_once()


async def test_repeated_group_loss_stops_after_two_reads() -> None:
    broker, client = make_broker()
    client.xautoclaim.side_effect = ResponseError("NOGROUP still missing")
    with pytest.raises(ResponseError, match="still missing"):
        await broker.read("task", "worker")
    assert client.xautoclaim.await_count == 2
    client.xgroup_create.assert_awaited_once()


@pytest.mark.parametrize(
    "failure", [ResponseError("WRONGTYPE"), RedisConnectionError("offline"), ValueError("bug")]
)
async def test_other_read_errors_do_not_recreate_groups(failure: Exception) -> None:
    broker, client = make_broker()
    client.xautoclaim.side_effect = failure
    with pytest.raises(type(failure), match=str(failure)):
        await broker.read("task", "worker")
    client.xautoclaim.assert_awaited_once()
    client.xgroup_create.assert_not_awaited()


async def test_group_repair_failure_propagates_without_another_read() -> None:
    broker, client = make_broker()
    client.xautoclaim.side_effect = ResponseError("NOGROUP missing")
    client.xgroup_create.side_effect = RedisConnectionError("repair failed")
    with pytest.raises(RedisConnectionError, match="repair failed"):
        await broker.read("task", "worker")
    client.xautoclaim.assert_awaited_once()
    client.xgroup_create.assert_awaited_once()


@pytest.mark.parametrize("failure", [RedisConnectionError, RuntimeError])
async def test_worker_read_errors_keep_the_one_second_retry_wait(
    monkeypatch: pytest.MonkeyPatch, failure: type[Exception]
) -> None:
    worker, _storage, broker = make_worker(monkeypatch)
    batch = [message()]
    broker.read.side_effect = [failure("offline"), failure("offline"), batch]
    pause = AsyncMock()
    monkeypatch.setattr(worker, "_wait_before_read", pause)
    assert await worker._read_messages(2) == batch
    assert pause.await_args_list == [call(1), call(1)]
    assert broker.read.await_args_list == [call("task", worker.consumer, count=2)] * 3


async def test_stopped_worker_never_starts_a_read(monkeypatch: pytest.MonkeyPatch) -> None:
    worker, _storage, broker = make_worker(monkeypatch)
    worker.stop.set()
    assert await worker._read_messages(1) == []
    broker.read.assert_not_awaited()


async def test_stop_after_failure_skips_another_read(monkeypatch: pytest.MonkeyPatch) -> None:
    worker, _storage, broker = make_worker(monkeypatch)

    async def failed(*args, **kwargs):
        worker.stop.set()
        raise RedisConnectionError("offline")

    broker.read.side_effect = failed
    pause = AsyncMock()
    monkeypatch.setattr(worker, "_wait_before_read", pause)
    assert await worker._read_messages(1) == []
    broker.read.assert_awaited_once()
    pause.assert_not_awaited()


async def test_stop_interrupts_the_retry_wait(monkeypatch: pytest.MonkeyPatch) -> None:
    worker, _storage, broker = make_worker(monkeypatch)
    broker.read.side_effect = RedisConnectionError("offline")
    waiting = asyncio.Event()
    original = worker._wait_before_read

    async def pause(delay):
        waiting.set()
        await original(delay)

    monkeypatch.setattr(worker, "_wait_before_read", pause)
    task = asyncio.create_task(worker._read_messages(1))
    try:
        await asyncio.wait_for(waiting.wait(), 1)
        worker.stop.set()
        assert await asyncio.wait_for(task, 1) == []
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    broker.read.assert_awaited_once()


@pytest.mark.parametrize("phase", ["read", "wait"])
async def test_cancellation_stops_read_retries(monkeypatch: pytest.MonkeyPatch, phase: str) -> None:
    worker, _storage, broker = make_worker(monkeypatch)
    entered = asyncio.Event()

    async def blocked(*args, **kwargs):
        entered.set()
        await asyncio.Event().wait()

    if phase == "read":
        broker.read.side_effect = blocked
    else:
        broker.read.side_effect = RedisConnectionError("offline")
        monkeypatch.setattr(worker, "_wait_before_read", blocked)
    task = asyncio.create_task(worker._read_messages(1))
    try:
        await asyncio.wait_for(entered.wait(), 1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    broker.read.assert_awaited_once()
