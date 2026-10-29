"""Repeated task delivery cannot repeat the handler for one logical try."""

import asyncio
from unittest.mock import AsyncMock

import pytest
from redis.exceptions import ConnectionError as RedisConnectionError

import worker.service as worker_module
from api import storage
from api.context import Context
from orchestrator.broker import Broker as OrchestratorBroker
from orchestrator.messages import event_envelope
from orchestrator.runtime import Runtime
from orchestrator.storage import apply_event
from shared.broker import StreamMessage
from shared.database import Database
from tests.integration.test_engine import accepted, graph, node
from tests.integration.test_protocol import events
from tests.integration.worker_events import task_payload
from tests.settings import Settings
from worker.broker import Broker
from worker.service import Worker

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("same_entry", [False, True])
@pytest.mark.parametrize("lost_ack", [False, True])
async def test_two_workers_call_handler_once_for_repeated_task_delivery(
    db: Database,
    settings: Settings,
    monkeypatch: pytest.MonkeyPatch,
    same_entry: bool,
    lost_ack: bool,
) -> None:
    settings = settings.model_copy(update={"handler_timeout": 5})
    second_db = Database(settings)
    await second_db.open()
    try:
        contexts = (Context(db, settings), Context(second_db, settings))
        identifier = await accepted(contexts[0], graph(node("a")), {"input": {"value": 42}})
        await apply_event(contexts[0], event_envelope((await events(db, identifier, "start"))[0]))
        envelope = await task_payload(
            contexts[0], event_envelope((await events(db, identifier, "task"))[0])
        )
        brokers = (Broker(settings), Broker(settings))
        for broker in brokers:
            await broker.start()
        if lost_ack:
            brokers[0].ack = AsyncMock(
                side_effect=[RedisConnectionError("lost acknowledgment")] + [None] * 30
            )
        workers = tuple(Worker(settings, broker) for broker in brokers)
        handler = await deliver_duplicates(workers, envelope, monkeypatch, same_entry)

        # Redis holds the result while PostgreSQL still awaits the orchestrator.
        await workers[1].process_message(StreamMessage("1-0", envelope))
        handler.assert_awaited_once()
        orchestrator = OrchestratorBroker(settings, client=brokers[0].client)
        await Runtime(contexts[1], orchestrator).events()
        await workers[0].process_message(StreamMessage("1-0", envelope))
        handler.assert_awaited_once()
        status = await storage.status(contexts[0], identifier)
        assert status["state"] == "COMPLETED"
        assert status["nodes"][0]["starts"] == 1
        assert len(status["nodes"][0]["tries"]) == 1
        assert (await storage.results(contexts[0], identifier))["outputs"] == {"a": {"value": 42}}
    finally:
        for worker in locals().get("workers", ()):
            await worker.output_pool.close()
        for broker in locals().get("brokers", ()):
            await broker.close()
        await second_db.close()


async def deliver_duplicates(
    workers: tuple[Worker, ...],
    envelope: dict,
    monkeypatch: pytest.MonkeyPatch,
    same_entry: bool,
) -> AsyncMock:
    entered, release = asyncio.Event(), asyncio.Event()
    original = worker_module.run_handler

    async def handler(*args, **kwargs):
        entered.set()
        await release.wait()
        return await original(*args, **kwargs)

    counted = AsyncMock(side_effect=handler)
    monkeypatch.setattr(worker_module, "run_handler", counted)
    original_delivery = asyncio.create_task(
        workers[0].process_message(StreamMessage("1-0", envelope))
    )
    try:
        await asyncio.wait_for(entered.wait(), timeout=2)
        duplicates = [
            workers[index % 2].process_message(
                StreamMessage("1-0" if same_entry else f"{index + 2}-0", dict(envelope))
            )
            for index in range(20)
        ]
        await asyncio.wait_for(asyncio.gather(*duplicates), timeout=2)
        counted.assert_awaited_once()
    finally:
        release.set()
        await asyncio.wait_for(original_delivery, timeout=5)
    return counted
