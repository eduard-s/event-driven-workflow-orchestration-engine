"""Transaction invariants under independent consumers and shuffled DAGs."""

import asyncio
import random
from typing import Any

import pytest

from api import storage
from api.context import Context
from orchestrator.broker import Broker
from orchestrator.messages import event_envelope
from orchestrator.storage import apply_event
from shared.database import Database
from tests.integration import worker_events as worker_storage
from tests.integration.test_engine import accepted, graph, node, running, terminal
from tests.settings import Settings

pytestmark = pytest.mark.integration


async def test_ack_preserves_unread_entries_and_other_consumer_groups(settings: Settings) -> None:
    broker = Broker(settings)
    await broker.start()
    try:
        entry = await broker.publish("task", {"schema_version": 1})
        await broker.ack("task", entry)
        assert await broker.client.xlen(settings.task_stream) == 1

        await broker.client.xgroup_create(settings.task_stream, "observer", id="0-0")
        assert (await broker.read("task", "worker", block_ms=0))[0].entry_id == entry
        await broker.ack("task", entry)
        assert await broker.client.xlen(settings.task_stream) == 1

        assert await broker.client.xreadgroup("observer", "reader", {settings.task_stream: ">"})
        await broker.client.xackdel(settings.task_stream, "observer", entry, ref_policy="ACKED")
        assert await broker.client.xlen(settings.task_stream) == 0
        await broker.ack("task", entry)
    finally:
        await broker.client.delete(settings.task_stream, settings.event_stream)
        await broker.close()


@pytest.mark.parametrize(
    "envelope", [{"schema_version": 1}, {"schema_version": 1, "execution_id": "invalid"}]
)
async def test_malformed_control_event_does_not_stop_work(
    db: Database, settings: Settings, envelope: dict[str, Any]
) -> None:
    async with running(db, settings) as (context, broker, runtime, _worker):
        entry = await broker.publish("event", envelope)
        async with asyncio.timeout(3):
            while await broker.client.xrange(settings.event_stream, min=entry, max=entry):
                assert runtime.failure is None
                await asyncio.sleep(0.01)
        assert runtime.failure is None
        identifier = await accepted(context, graph(node("input")))
        status = await terminal(context, identifier)
        assert status["state"] == "COMPLETED"
        assert status["nodes"][0]["starts"] == 1


async def events(db: Database, identifier: str, kind: str) -> list[dict[str, Any]]:
    async with db.tx() as connection:
        return await (
            await connection.execute(
                "SELECT * FROM outbox WHERE execution_id=%s AND kind=%s ORDER BY node_id",
                (identifier, kind),
            )
        ).fetchall()


async def test_two_orchestrator_connections_apply_one_start_once(
    db: Database, settings: Settings
) -> None:
    first = Context(db, settings)
    second_db = Database(settings)
    await second_db.open()
    try:
        second = Context(second_db, settings)
        identifier = await accepted(first, graph(node("input")))
        start = event_envelope((await events(db, identifier, "start"))[0])
        await asyncio.gather(*(apply_event(context, start) for context in (first, second) * 4))
        assert len(await events(db, identifier, "task")) == 1
        assert (await events(db, identifier, "start"))[0]["applied_at"] is not None
        status = await storage.status(first, identifier)
        assert status["nodes"][0]["try_number"] == 1
        assert status["nodes"][0]["reason"] == "QUEUED"
        assert status["nodes"][0]["starts"] == 0
    finally:
        await second_db.close()


async def test_distinct_parent_completion_barrier_dispatches_child_once(
    db: Database, settings: Settings
) -> None:
    first = Context(db, settings)
    second_db = Database(settings)
    await second_db.open()
    second = Context(second_db, settings)
    try:
        identifier = await accepted(
            first, graph(node("b"), node("c"), node("d", "output", ["b", "c"]))
        )
        await apply_event(first, event_envelope((await events(db, identifier, "start"))[0]))
        tasks = await events(db, identifier, "task")
        owners = await asyncio.gather(
            *(worker_storage.claim(first, event_envelope(task)) for task in tasks)
        )
        for task, owner in zip(tasks, owners, strict=True):
            assert owner
            assert await worker_storage.store_outcome(
                first, event_envelope(task), owner["token"], True, task["node_id"]
            )
        completions = await events(db, identifier, "completion")
        barrier = asyncio.Event()

        async def consume(context: Context, event: dict[str, Any]) -> None:
            await barrier.wait()
            await apply_event(context, event_envelope(event))

        consumers = [
            asyncio.create_task(consume(context, event))
            for context, event in zip((first, second), completions, strict=True)
        ]
        barrier.set()
        await asyncio.gather(*consumers)
        child_tasks = [
            task for task in await events(db, identifier, "task") if task["node_id"] == "d"
        ]
        assert len(child_tasks) == 1
        owner = await worker_storage.claim(second, event_envelope(child_tasks[0]))
        assert owner and owner["input"]["data"] == {"b": "b", "c": "c"}
        assert await worker_storage.claim(first, event_envelope(child_tasks[0])) is None
    finally:
        await second_db.close()


@pytest.mark.parametrize("seed", [3, 7, 11, 19])
async def test_generated_dags_complete_with_shuffled_handler_delays(
    db: Database, settings: Settings, seed: int
) -> None:
    rng = random.Random(seed)
    definitions = [node("root")]
    fixtures = {}
    for index in range(12):
        previous = rng.randrange(index) if index else None
        dependencies = ["root"] + ([f"n{previous}"] if previous is not None else [])
        definitions.append(
            node(
                f"n{index}",
                "call_external_service",
                dependencies,
                config={"url": "http://mock"},
                data={"seed": "{{ root.seed }}", "index": index},
            )
        )
        fixtures[f"n{index}"] = {"delay": rng.random() / 30}
    definitions.append(node("output", "output", [f"n{i}" for i in range(12)]))
    import json

    settings = settings.model_copy(update={"test_fixtures_json": json.dumps(fixtures)})
    async with running(db, settings) as (context, _, _, _):
        identifier = await accepted(context, graph(*definitions), {"input": {"seed": seed}})
        assert (await terminal(context, identifier))["state"] == "COMPLETED"
        assert (await storage.results(context, identifier))["outputs"] == {
            "output": {
                f"n{i}": {"url": "http://mock", "data": {"seed": seed, "index": i}}
                for i in range(12)
            }
        }
