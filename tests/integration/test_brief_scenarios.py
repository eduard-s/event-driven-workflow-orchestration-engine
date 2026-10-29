"""The brief's fan-in race preserves one dispatch for simultaneous parents."""

import asyncio
from datetime import UTC, datetime

import pytest

from api import storage
from api.context import Context
from orchestrator.messages import event_envelope
from orchestrator.storage import apply_event
from shared.database import Database
from tests.integration import worker_events as worker_storage
from tests.integration.test_engine import accepted, graph, node
from tests.integration.test_protocol import events
from tests.settings import Settings

pytestmark = pytest.mark.integration


async def test_same_millisecond_parent_outcomes_dispatch_and_claim_join_once(
    db: Database, settings: Settings
) -> None:
    """Apply tied parent completions through two replicas to protect one fan-in dispatch.

    Args:
        db: Isolated workflow database for the first replica.
        settings: Test settings with durable execution and lease policies.

    Returns:
        None. Assertions examine one downstream try, claim, and aggregated result.
    """
    first = Context(db, settings)
    other_db = Database(settings)
    await other_db.open()
    second = Context(other_db, settings)
    try:
        identifier = await accepted(
            first,
            graph(
                node("a"),
                *(
                    node(
                        name,
                        "call_external_service",
                        ["a"],
                        config={"url": "http://mock"},
                        data={"received": "{{ a.id }}"},
                    )
                    for name in ("b", "c")
                ),
                node("d", "output", ["b", "c"]),
            ),
            {"input": {"id": 7}},
        )
        await apply_event(first, event_envelope((await events(db, identifier, "start"))[0]))
        root = event_envelope((await events(db, identifier, "task"))[0])
        owner = await worker_storage.claim(first, root)
        assert owner
        assert await worker_storage.store_outcome(first, root, owner["token"], True, {"id": 7})
        await apply_event(first, event_envelope((await events(db, identifier, "completion"))[0]))

        branches = [
            task for task in await events(db, identifier, "task") if task["node_id"] in ("b", "c")
        ]
        assert len(branches) == 2
        for context, task in zip((first, second), branches, strict=True):
            envelope = event_envelope(task)
            claim = await worker_storage.claim(context, envelope)
            assert claim and claim["input"]["data"] == {"received": 7}
            assert await worker_storage.store_outcome(
                context,
                envelope,
                claim["token"],
                True,
                {"branch": task["node_id"], "received": 7},
            )

        # A tied timestamp makes the brief's same-millisecond case explicit.
        finished_at = datetime.now(UTC)
        finished_at = finished_at.replace(microsecond=finished_at.microsecond // 1000 * 1000)
        async with db.tx() as connection:
            await connection.execute(
                "UPDATE tries SET finished_at=%s WHERE execution_id=%s AND node_id IN ('b','c')",
                (finished_at, identifier),
            )
        completions = [
            event
            for event in await events(db, identifier, "completion")
            if event["node_id"] in ("b", "c")
        ]
        assert len(completions) == 2
        barrier = asyncio.Event()

        async def consume(context: Context, envelope: dict) -> None:
            """Release one completion with its duplicates through a common barrier.

            Args:
                context: Independent replica database pool.
                envelope: Parent completion identity from the durable outbox.

            Returns:
                None after the completion transaction ends.
            """
            await barrier.wait()
            await apply_event(context, envelope)

        deliveries = [
            asyncio.create_task(consume(context, event_envelope(event)))
            for event in completions
            for context in (first, second) * 4
        ]
        barrier.set()
        await asyncio.gather(*deliveries)

        child_tasks = [
            task for task in await events(db, identifier, "task") if task["node_id"] == "d"
        ]
        assert len(child_tasks) == 1
        child = event_envelope(child_tasks[0])
        claims = await asyncio.gather(
            *(worker_storage.claim(context, child) for context in (first, second))
        )
        owners = [claim for claim in claims if claim is not None]
        expected = {name: {"branch": name, "received": 7} for name in ("b", "c")}
        assert len(owners) == 1 and owners[0]["input"]["data"] == expected
        assert await worker_storage.store_outcome(first, child, owners[0]["token"], True, expected)
        final = next(
            event for event in await events(db, identifier, "completion") if event["node_id"] == "d"
        )
        await asyncio.gather(
            *(apply_event(context, event_envelope(final)) for context in (first, second))
        )

        status = await storage.status(first, identifier)
        assert status["state"] == "COMPLETED"
        nodes = {item["id"]: item for item in status["nodes"]}
        assert nodes["b"]["tries"][0]["finished_at"] == nodes["c"]["tries"][0]["finished_at"]
        assert all(item["starts"] == 1 and len(item["tries"]) == 1 for item in nodes.values())
        assert (await storage.results(first, identifier))["outputs"] == {"d": expected}
    finally:
        await other_db.close()
