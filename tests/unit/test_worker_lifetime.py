"""Claim replies, persistence, and shutdown obey their original deadlines."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from redis.exceptions import ConnectionError as OperationalError

import worker.broker as storage_module
import worker.service as worker_module
from tests.unit.test_worker import make_worker, message
from worker.runner import HandlerResult


async def test_claim_deadline_includes_redis_reply_latency(monkeypatch: pytest.MonkeyPatch) -> None:
    clock = [100.0]
    monkeypatch.setattr(storage_module, "time", SimpleNamespace(monotonic=lambda: clock[0]))

    async def delayed_eval(*args):
        clock[0] += 2
        return [1000, 11000]

    from tests.settings import Settings

    broker = storage_module.Broker(
        Settings(), client=SimpleNamespace(eval=AsyncMock(side_effect=delayed_eval))
    )
    claim = await broker.claim(message().envelope)
    assert claim["deadline"] == 110
    assert claim["deadline"] - clock[0] == 8


async def test_expired_claim_reply_does_not_start_a_handler(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    worker, storage, broker = make_worker(monkeypatch)
    clock = [100.0]
    monkeypatch.setattr(worker_module, "time", SimpleNamespace(monotonic=lambda: clock[0]))
    storage.claim.return_value["deadline"] = 105

    async def delayed_reply(*_args):
        clock[0] += 6
        return storage.claim.return_value

    storage.claim.side_effect = delayed_reply
    handler = AsyncMock()
    monkeypatch.setattr(worker_module, "run_handler", handler)
    await worker.process_message(message())
    broker.ack.assert_awaited_once()
    handler.assert_not_awaited()
    storage.store_outcome.assert_not_awaited()


async def test_persistence_deadline_interrupts_retry_backoff(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    worker, storage, _broker = make_worker(monkeypatch)
    storage.store_outcome.side_effect = OperationalError("offline")
    deadline = worker_module.time.monotonic() + 0.02
    async with asyncio.timeout(1):
        await worker._persist_result(
            message(), storage.claim.return_value, HandlerResult(True, 1), deadline
        )
    storage.store_outcome.assert_awaited_once()


async def test_persistence_deadline_cancels_an_inflight_write(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    worker, storage, _broker = make_worker(monkeypatch)
    cancelled = asyncio.Event()

    async def blocked(*args, **kwargs):
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    storage.store_outcome.side_effect = blocked
    async with asyncio.timeout(1):
        await worker._persist_result(
            message(),
            storage.claim.return_value,
            HandlerResult(True, 1),
            worker_module.time.monotonic() + 0.02,
        )
    assert cancelled.is_set()
    storage.store_outcome.assert_awaited_once()


async def test_unexpected_persistence_error_propagates_without_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    worker, storage, _broker = make_worker(monkeypatch)
    storage.store_outcome.side_effect = ValueError("bug")
    with pytest.raises(ValueError, match="bug"):
        await worker._persist_result(
            message(),
            storage.claim.return_value,
            HandlerResult(True, 1),
            worker_module.time.monotonic() + 5,
        )
    storage.store_outcome.assert_awaited_once()


async def test_full_worker_observes_stop_before_its_handler_finishes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    worker, _storage, broker = make_worker(monkeypatch)
    worker.settings = worker.settings.model_copy(update={"worker_concurrency": 1})
    broker.read.return_value = [message()]
    started, drained, cancelled = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def handler(*_args):
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    original_drain = worker._drain_tasks

    async def drain():
        drained.set()
        await original_drain()

    monkeypatch.setattr(worker, "_run_message", handler)
    monkeypatch.setattr(worker, "_drain_tasks", drain)
    monkeypatch.setattr(worker_module, "SHUTDOWN_SECONDS", 0.01)
    task = asyncio.create_task(worker.run())
    try:
        await asyncio.wait_for(started.wait(), 1)
        worker.stop.set()
        await asyncio.wait_for(drained.wait(), 1)
        await asyncio.wait_for(task, 1)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    assert cancelled.is_set()
    broker.read.assert_awaited_once()
