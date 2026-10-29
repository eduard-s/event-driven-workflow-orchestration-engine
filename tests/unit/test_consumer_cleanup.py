"""Consumer cleanup is bounded and never removes pending deliveries."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from redis.exceptions import ResponseError

from api import storage
from orchestrator import runtime as runtime_module
from orchestrator.broker import CONSUMER_CLEANUP_BATCH
from orchestrator.runtime import Runtime
from tests.settings import Settings
from tests.unit.test_orchestrator_broker import make_broker


async def test_consumer_cleanup_keeps_active_and_pending_consumers() -> None:
    broker, client = make_broker()
    client.xinfo_consumers.side_effect = [
        [
            {"name": b"old", "idle": 60_000, "pending": 0},
            {"name": b"active", "idle": 100, "pending": 0},
            {"name": b"pending", "idle": 100_000, "pending": 1},
        ],
        [],
    ]
    client.eval.return_value = 1
    assert await broker.prune_consumers() == 1
    client.eval.assert_awaited_once()
    assert client.eval.await_args.args[1:] == (1, "test.tasks", "workers", b"old")
    client.xgroup_delconsumer.assert_not_awaited()


async def test_consumer_cleanup_bounds_each_group_batch() -> None:
    broker, client = make_broker()
    client.xinfo_consumers.return_value = [
        {"name": f"old-{index}", "idle": 60_000, "pending": 0}
        for index in range(CONSUMER_CLEANUP_BATCH + 1)
    ]
    client.eval.return_value = 1
    assert await broker.prune_consumers() == 2 * CONSUMER_CLEANUP_BATCH
    assert client.eval.await_count == 2 * CONSUMER_CLEANUP_BATCH


@pytest.mark.parametrize("error", ["NOGROUP missing", "ERR no such key"])
async def test_consumer_cleanup_tolerates_missing_groups(error: str) -> None:
    broker, client = make_broker()
    client.xinfo_consumers.side_effect = ResponseError(error)
    assert await broker.prune_consumers() == 0
    client.eval.assert_not_awaited()


async def test_consumer_cleanup_tolerates_group_loss_after_metadata_read() -> None:
    broker, client = make_broker()
    client.xinfo_consumers.return_value = [{"name": b"old", "idle": 60_000, "pending": 0}]
    client.eval.side_effect = ResponseError("NOGROUP missing")
    assert await broker.prune_consumers() == 0


async def test_consumer_cleanup_propagates_other_redis_errors() -> None:
    broker, client = make_broker()
    client.xinfo_consumers.side_effect = ResponseError("WRONGTYPE invalid stream")
    with pytest.raises(ResponseError, match="WRONGTYPE"):
        await broker.prune_consumers()


async def test_maintenance_limits_consumer_cleanup_frequency(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    work = AsyncMock()
    broker = SimpleNamespace(prune_consumers=AsyncMock())
    runtime = Runtime(Mock(settings=Settings(enable_repair=False)), broker)
    monkeypatch.setattr(runtime_module.storage, "maintain_once", work)
    monkeypatch.setattr(runtime_module.storage, "expired_worker_claims", AsyncMock(return_value=[]))
    await runtime.maintain()
    await runtime.maintain()
    broker.prune_consumers.assert_awaited_once()
    runtime._consumer_cleanup_at = 0
    await runtime.maintain()
    assert broker.prune_consumers.await_count == 2
    assert work.await_count == 3


async def test_full_audit_batches_drain_promptly_with_a_bounded_delay(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    batch = storage.config.AUDIT_PRUNE_BATCH
    prune = AsyncMock(side_effect=[batch, batch, batch - 1])
    sleep = AsyncMock(side_effect=[None, None, asyncio.CancelledError])
    monkeypatch.setattr(storage, "prune_audit", prune)
    monkeypatch.setattr(storage.asyncio, "sleep", sleep)
    with pytest.raises(asyncio.CancelledError):
        await storage._prune_audit(Mock())
    assert [call.args[0] for call in sleep.await_args_list] == [
        1,
        1,
        storage.config.AUDIT_PRUNE_INTERVAL_SECONDS,
    ]
