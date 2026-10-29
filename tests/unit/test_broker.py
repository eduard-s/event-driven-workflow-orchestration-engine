import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import ResponseError

from shared.broker import MAX_ENVELOPE_BYTES, Broker


def make_broker() -> tuple[Broker, AsyncMock]:
    client = AsyncMock()
    settings = SimpleNamespace(task_stream="test.tasks", event_stream="test.events")
    return Broker(settings, client=client), client


def entry(entry_id: str, event_id: str) -> tuple[bytes, dict[bytes, bytes]]:
    return entry_id.encode(), {b"envelope": json.dumps({"event_id": event_id}).encode()}


async def test_pending_reclaim_precedes_new_messages_and_obeys_capacity() -> None:
    broker, client = make_broker()
    client.xautoclaim.return_value = [b"12-0", [entry("1-0", "old")], []]
    client.xreadgroup.return_value = [(b"test.tasks", [entry("2-0", "new")])]

    messages = await broker.read("task", "worker-a", count=2)

    assert [message.envelope for message in messages] == [{"event_id": "old"}, {"event_id": "new"}]
    assert client.xreadgroup.await_args.kwargs == {"count": 1}
    await broker.read("task", "worker-a", count=2)
    assert client.xautoclaim.await_args.kwargs["start_id"] == "12-0"


async def test_full_pending_batch_does_not_acquire_more_messages() -> None:
    broker, client = make_broker()
    client.xautoclaim.return_value = [b"0-0", [entry("1-0", "old")], []]
    assert len(await broker.read("task", "worker-a")) == 1
    client.xreadgroup.assert_not_awaited()


async def test_group_loss_recreates_from_start_and_preserves_delivery() -> None:
    broker, client = make_broker()
    client.xautoclaim.side_effect = [
        ResponseError("NOGROUP missing"),
        [b"0-0", [], []],
    ]
    client.xreadgroup.return_value = [(b"test.tasks", [entry("1-0", "saved")])]

    result = await broker.read("task", "worker-a")

    client.xgroup_create.assert_awaited_once_with("test.tasks", "workers", id="0-0", mkstream=True)
    assert result[0].envelope == {"event_id": "saved"}


@pytest.mark.parametrize("raw", [b"\xff", b"NaN", b"[]", b"{", b"x" * (MAX_ENVELOPE_BYTES + 1)])
def test_malformed_message_is_a_rejectable_record(raw: bytes) -> None:
    message = Broker._message((b"1-0", {b"envelope": raw}))
    assert message.entry_id == "1-0"
    assert message.envelope is None
    assert message.error


async def test_group_creation_ignores_only_existing_group() -> None:
    broker, client = make_broker()
    client.xgroup_create.side_effect = ResponseError("WRONGTYPE wrong key")
    with pytest.raises(ResponseError, match="WRONGTYPE"):
        await broker.start()


async def test_zero_block_omits_infinite_redis_wait() -> None:
    broker, client = make_broker()
    client.xautoclaim.return_value = [b"0-0", [], []]
    client.xreadgroup.return_value = []
    assert await broker.read("event", "app-a", block_ms=0) == []
    assert "block" not in client.xreadgroup.await_args.kwargs


async def test_close_cleans_only_consumers_used_by_this_broker() -> None:
    broker, client = make_broker()
    client.xautoclaim.return_value = [b"0-0", [], []]
    client.xreadgroup.return_value = []
    await broker.read("task", "own-worker", block_ms=0)
    await broker.read("event", "own-app", block_ms=0)

    await broker.close()

    calls = client.eval.await_args_list
    assert [call.args[1:] for call in calls] == [
        (1, "test.tasks", "workers", "own-worker"),
        (1, "test.events", "orchestrators", "own-app"),
    ]
    client.xgroup_delconsumer.assert_not_awaited()
    client.aclose.assert_awaited_once()


@pytest.mark.parametrize(
    "failure",
    [ResponseError("NOGROUP No such key or consumer group"), RedisConnectionError("offline")],
)
async def test_close_still_closes_client_after_cleanup_failure(failure: Exception) -> None:
    broker, client = make_broker()
    client.xautoclaim.side_effect = RedisConnectionError("lost response")
    with pytest.raises(RedisConnectionError):
        await broker.read("task", "own-worker")
    client.eval.side_effect = failure

    await broker.close()

    assert client.eval.await_args.args[1:] == (1, "test.tasks", "workers", "own-worker")
    client.aclose.assert_awaited_once()


async def test_close_without_readers_does_not_remove_consumers() -> None:
    broker, client = make_broker()
    await broker.close()
    client.eval.assert_not_awaited()
    client.aclose.assert_awaited_once()
