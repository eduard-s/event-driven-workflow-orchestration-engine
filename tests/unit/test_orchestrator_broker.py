from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from orchestrator.broker import Broker
from shared.broker import MAX_ENVELOPE_BYTES
from tests.unit.test_broker import entry


def make_broker() -> tuple[Broker, AsyncMock]:
    client = AsyncMock()
    settings = SimpleNamespace(task_stream="test.tasks", event_stream="test.events")
    return Broker(settings, client=client), client


async def test_exists_compares_logical_identity_not_only_stream_id() -> None:
    broker, client = make_broker()
    client.xrange.return_value = [entry("1-0", "different")]
    assert not await broker.exists("task", "1-0", "expected")
    assert await broker.exists("task", "1-0", "different")


async def test_oversize_publication_never_reaches_redis() -> None:
    broker, client = make_broker()
    with pytest.raises(ValueError, match="byte limit"):
        await broker.publish("task", {"event_id": "x" * MAX_ENVELOPE_BYTES})
    client.xadd.assert_not_awaited()
