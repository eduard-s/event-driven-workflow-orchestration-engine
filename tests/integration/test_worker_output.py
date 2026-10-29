"""Concurrent prepared outcomes retain database budgets and ownership guards."""

import asyncio
from typing import Any

import pytest

from api.context import Context
from orchestrator.messages import event_envelope
from orchestrator.storage import apply_event
from shared.database import Database
from shared.output import prepare_result
from tests.integration import worker_events as worker_storage
from tests.integration.test_edges import _event, _queued
from tests.integration.test_engine import accepted, graph, node
from tests.settings import Settings

pytestmark = pytest.mark.integration


async def test_concurrent_prepared_outputs_cannot_exceed_saved_aggregate_budget(
    db: Database, settings: Settings
) -> None:
    context = Context(db, settings.model_copy(update={"max_execution_bytes": 200}))
    identifier = await accepted(context, graph(node("a"), node("b")))
    await apply_event(context, event_envelope(await _event(db, identifier, "start")))
    async with db.tx() as connection:
        tasks = await (
            await connection.execute(
                "SELECT * FROM outbox WHERE execution_id=%s AND kind='task' ORDER BY node_id",
                (identifier,),
            )
        ).fetchall()
    envelopes = [event_envelope(task) for task in tasks]
    claims = await asyncio.gather(
        *(worker_storage.claim(context, envelope) for envelope in envelopes)
    )
    value = "x" * 120
    prepared = await prepare_result((True, value, None, None))
    assert all(claims)
    stored = await asyncio.gather(
        *(
            worker_storage.store_outcome(
                context, envelope, claim["token"], True, value, prepared=prepared
            )
            for envelope, claim in zip(envelopes, claims, strict=True)
            if claim is not None
        )
    )
    assert stored == [True, True]
    async with db.tx() as connection:
        outcomes = await (
            await connection.execute(
                "SELECT success,error FROM tries WHERE execution_id=%s ORDER BY success",
                (identifier,),
            )
        ).fetchall()
        execution = await (
            await connection.execute(
                "SELECT output_bytes FROM executions WHERE id=%s", (identifier,)
            )
        ).fetchone()
    assert [outcome["success"] for outcome in outcomes] == [False, True]
    assert outcomes[0]["error"]["code"] == "INVALID_OUTPUT"
    assert execution and execution["output_bytes"] == prepared.output_bytes


async def test_prepared_snapshot_replay_and_expired_lease_keep_guards(
    db: Database, settings: Settings
) -> None:
    context = Context(db, settings)
    _identifier, task = await _queued(context, db)
    claim = await worker_storage.claim(context, task)
    assert claim
    value = {"items": [1]}
    prepared = await prepare_result((True, value, None, None))
    value["items"].append(2)
    assert not await worker_storage.store_outcome(
        context, task, "other-owner", True, value, prepared=prepared
    )
    assert await worker_storage.store_outcome(
        context, task, claim["token"], True, value, prepared=prepared
    )
    assert await worker_storage.store_outcome(
        context, task, claim["token"], True, value, prepared=prepared
    )
    assert not await worker_storage.store_outcome(context, task, claim["token"], True, value)
    async with db.tx() as connection:
        attempt = await (
            await connection.execute(
                "SELECT value FROM tries WHERE event_id=%s", (task["event_id"],)
            )
        ).fetchone()
    assert attempt and attempt["value"] == {"items": [1]}

    _other, task = await _queued(context, db)
    claim = await worker_storage.claim(context, task)
    assert claim
    async with db.tx() as connection:
        await connection.execute(
            "UPDATE tries SET lease_until=clock_timestamp()-interval '1 second' WHERE event_id=%s",
            (task["event_id"],),
        )
    assert not await worker_storage.store_outcome(
        context, task, claim["token"], True, value, prepared=prepared
    )


async def test_raw_output_preparation_does_not_hold_execution_lock(
    db: Database, settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    context = Context(db, settings)
    identifier, task = await _queued(context, db)
    claim = await worker_storage.claim(context, task)
    assert claim
    entered, resume = asyncio.Event(), asyncio.Event()
    original = worker_storage.prepare_result

    async def paused(*args: Any, **kwargs: Any) -> Any:
        entered.set()
        await resume.wait()
        return await original(*args, **kwargs)

    monkeypatch.setattr(worker_storage, "prepare_result", paused)
    writing = asyncio.create_task(
        worker_storage.store_outcome(context, task, claim["token"], True, {"value": 1})
    )
    try:
        await entered.wait()
        async with asyncio.timeout(1), db.tx() as connection:
            await connection.execute(
                "SELECT id FROM executions WHERE id=%s FOR UPDATE", (identifier,)
            )
    finally:
        resume.set()
    assert await writing
