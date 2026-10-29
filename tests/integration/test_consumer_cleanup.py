"""The Redis cleanup guard preserves deliveries that arrive during a prune."""

import pytest

from orchestrator.broker import Broker
from tests.settings import Settings

pytestmark = pytest.mark.integration


async def test_cleanup_preserves_pending_and_active_consumers(
    settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    broker = Broker(settings)
    await broker.start()
    try:
        for name in ("old", "active", "pending"):
            await broker.client.xgroup_createconsumer(settings.task_stream, "workers", name)
        await broker.publish("task", {"schema_version": 1})
        await broker.client.xreadgroup("workers", "pending", {settings.task_stream: ">"})
        original = broker.client.xinfo_consumers

        async def aged(stream, group):
            consumers = await original(stream, group)
            for consumer in consumers:
                if consumer["name"] != b"active":
                    consumer["idle"] = 60_000
            return consumers

        monkeypatch.setattr(broker.client, "xinfo_consumers", aged)
        assert await broker.prune_consumers() == 1
        consumers = await original(settings.task_stream, "workers")
        assert {consumer["name"] for consumer in consumers} == {b"active", b"pending"}
        assert await broker.client.xlen(settings.task_stream) == 1
    finally:
        await broker.client.delete(settings.task_stream, settings.event_stream)
        await broker.close()


async def test_cleanup_rechecks_deliveries_after_the_metadata_read(
    settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    broker = Broker(settings)
    await broker.start()
    try:
        await broker.client.xgroup_createconsumer(settings.task_stream, "workers", "racing")
        original_info, original_eval = broker.client.xinfo_consumers, broker.client.eval

        async def aged(stream, group):
            consumers = await original_info(stream, group)
            for consumer in consumers:
                consumer["idle"] = 60_000
            return consumers

        async def deliver(*args):
            await broker.publish("task", {"schema_version": 1})
            await broker.client.xreadgroup("workers", "racing", {settings.task_stream: ">"})
            return await original_eval(*args)

        monkeypatch.setattr(broker.client, "xinfo_consumers", aged)
        monkeypatch.setattr(broker.client, "eval", deliver)
        assert await broker.prune_consumers() == 0
        consumers = await original_info(settings.task_stream, "workers")
        assert len(consumers) == 1 and consumers[0]["pending"] == 1
    finally:
        await broker.client.delete(settings.task_stream, settings.event_stream)
        await broker.close()
