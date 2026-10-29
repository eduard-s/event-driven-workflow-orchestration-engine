"""Durable timer scheduling across orchestrator and worker transactions."""

from datetime import timedelta
from typing import Any
from uuid import uuid4

import pytest

from api import storage, submissions
from api.context import Context
from orchestrator.messages import event_envelope
from orchestrator.storage import apply_event
from shared.database import Database
from tests.integration import worker_events as worker_storage
from tests.integration.test_edges import _event, _queued
from tests.integration.test_engine import accepted, graph, node
from tests.integration.worker_events import maintain_once
from tests.settings import Settings

pytestmark = pytest.mark.integration


async def _execution(db: Database, identifier: str) -> dict[str, Any]:
    async with db.tx() as connection:
        execution = await (
            await connection.execute(
                "SELECT *,xmin::text AS version FROM executions WHERE id=%s", (identifier,)
            )
        ).fetchone()
    assert execution is not None
    return execution


@pytest.mark.parametrize("triggered", [False, True])
async def test_idle_executions_keep_their_actual_deadline_without_polling_writes(
    db: Database, settings: Settings, triggered: bool
) -> None:
    context = Context(db, settings)
    if triggered:
        identifier, _ = await _queued(context, db)
    else:
        identifier = (
            await submissions.submit(context, graph(node("a")), str(uuid4()))
        ).execution_id
    await maintain_once(context)
    before = await _execution(db, identifier)
    expected = (
        before["deadline_at"]
        if triggered
        else before["created_at"] + timedelta(seconds=before["policy"]["pending_seconds"])
    )
    assert before["maintenance_due"] == expected
    for _ in range(3):
        await maintain_once(context)
    after = await _execution(db, identifier)
    assert after["maintenance_due"] == expected
    assert after["version"] == before["version"]


@pytest.mark.parametrize("enable_repair", [False, True])
async def test_worker_claim_advances_deadline_and_expiry_schedules_completion_repair(
    db: Database, settings: Settings, enable_repair: bool
) -> None:
    settings = settings.model_copy(update={"enable_repair": enable_repair})
    context = Context(db, settings)
    identifier, task = await _queued(context, db)
    assert await worker_storage.claim(context, task)
    async with db.tx() as connection:
        attempt = await (
            await connection.execute("SELECT * FROM tries WHERE execution_id=%s", (identifier,))
        ).fetchone()
    assert (await _execution(db, identifier))["maintenance_due"] == attempt["lease_until"]

    # Move both durable timers together to simulate the lease elapsing without a wall-clock sleep.
    async with db.tx() as connection:
        await connection.execute(
            "UPDATE tries SET lease_until=clock_timestamp()-interval '1 second' WHERE execution_id=%s",
            (identifier,),
        )
        await connection.execute(
            "UPDATE executions SET maintenance_due=clock_timestamp()-interval '1 second' WHERE id=%s",
            (identifier,),
        )
    await worker_storage.expire_claim(context, task)
    await maintain_once(context)
    completion = await _event(db, identifier, "completion")
    execution = await _execution(db, identifier)
    expected = (
        completion["created_at"] + timedelta(seconds=settings.repair_interval)
        if enable_repair
        else execution["deadline_at"]
    )
    assert execution["maintenance_due"] == expected
    assert completion["applied_at"] is None


async def test_result_application_schedules_retry_without_a_second_event_pass(
    db: Database, settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = settings.model_copy(update={"enable_repair": True, "retry_base": 5, "retry_max": 5})
    monkeypatch.setattr("orchestrator.storage.random.uniform", lambda low, high: high)
    context = Context(db, settings)
    identifier, task = await _queued(context, db)
    owner = await worker_storage.claim(context, task)
    assert owner
    assert await worker_storage.store_outcome(
        context,
        task,
        owner["token"],
        False,
        error={"code": "TRANSIENT", "message": "Retry this call.", "retryable": True},
    )
    completion = await _event(db, identifier, "completion")
    # The orchestrator commits the result, retry decision, and next timer together.
    assert completion["applied_at"] is not None
    await maintain_once(context)
    await apply_event(context, event_envelope(completion))
    async with db.tx() as connection:
        retry = await (
            await connection.execute("SELECT * FROM nodes WHERE execution_id=%s", (identifier,))
        ).fetchone()
    assert retry["state"] == "RETRY_WAIT"
    execution = await _execution(db, identifier)
    assert execution["maintenance_due"] == retry["next_retry_at"]
    await maintain_once(context)
    assert (await _event(db, identifier, "task"))["try_number"] == 1

    async with db.tx() as connection:
        await connection.execute(
            "UPDATE nodes SET next_retry_at=clock_timestamp()-interval '1 second' WHERE execution_id=%s",
            (identifier,),
        )
        await connection.execute(
            "UPDATE executions SET maintenance_due=clock_timestamp()-interval '1 second' WHERE id=%s",
            (identifier,),
        )
    await maintain_once(context)
    assert (await _event(db, identifier, "task"))["try_number"] == 2
    assert (await _execution(db, identifier))["maintenance_due"] == execution["deadline_at"]


async def test_unconsumed_start_is_scheduled_for_database_repair(
    db: Database, settings: Settings
) -> None:
    settings = settings.model_copy(update={"enable_repair": True})
    context = Context(db, settings)
    identifier = await accepted(context, graph(node("a")))
    start = await _event(db, identifier, "start")
    await maintain_once(context)
    assert (await _execution(db, identifier))["maintenance_due"] == start["created_at"] + timedelta(
        seconds=settings.repair_interval
    )
    async with db.tx() as connection:
        await connection.execute(
            "UPDATE outbox SET created_at=clock_timestamp()-interval '1 second' WHERE execution_id=%s",
            (identifier,),
        )
        await connection.execute(
            "UPDATE executions SET maintenance_due=clock_timestamp()-interval '1 second' WHERE id=%s",
            (identifier,),
        )
    await maintain_once(context)
    assert (await _event(db, identifier, "start"))["applied_at"] is not None
    assert (await _event(db, identifier, "task"))["try_number"] == 1
    execution = await _execution(db, identifier)
    assert execution["maintenance_due"] == execution["deadline_at"]


@pytest.mark.parametrize("delivery", ["event", "repair"])
@pytest.mark.parametrize("outcome", ["success", "retry", "failure"])
async def test_control_delivery_paths_share_retry_failure_and_completion_rules(
    db: Database,
    settings: Settings,
    monkeypatch: pytest.MonkeyPatch,
    delivery: str,
    outcome: str,
) -> None:
    settings = settings.model_copy(update={"enable_repair": True, "retry_base": 5, "retry_max": 5})
    monkeypatch.setattr("orchestrator.storage.random.uniform", lambda low, high: high)
    context = Context(db, settings)
    identifier, task = await _queued(context, db)
    owner = await worker_storage.claim(context, task)
    assert owner
    assert await worker_storage.store_outcome(
        context,
        task,
        owner["token"],
        outcome == "success",
        value={"done": True},
        error=(
            {"code": "MOCK_FAILURE", "message": "Failed.", "retryable": outcome == "retry"}
            if outcome != "success"
            else None
        ),
    )
    completion = await _event(db, identifier, "completion")
    if delivery == "event":
        await apply_event(context, event_envelope(completion))
    else:
        async with db.tx() as connection:
            await connection.execute(
                "UPDATE outbox SET created_at=clock_timestamp()-interval '1 second' WHERE event_id=%s",
                (completion["event_id"],),
            )
            await connection.execute(
                "UPDATE executions SET maintenance_due=clock_timestamp()-interval '1 second' WHERE id=%s",
                (identifier,),
            )
        await maintain_once(context)

    status = await storage.status(context, identifier)
    assert (await _event(db, identifier, "completion"))["applied_at"] is not None
    execution = await _execution(db, identifier)
    if outcome == "retry":
        assert status["state"] == "RUNNING"
        assert status["nodes"][0]["state"] == "RETRY_WAIT"
        assert execution["terminal_at"] is None
        assert execution["maintenance_due"] < execution["deadline_at"]
    else:
        expected = "COMPLETED" if outcome == "success" else "FAILED"
        assert status["state"] == status["nodes"][0]["state"] == expected
        assert execution["has_failure"] is (outcome == "failure")
        assert execution["maintenance_due"] == execution["terminal_at"] + timedelta(
            seconds=settings.retention_seconds
        )
        async with db.tx() as connection:
            unapplied = await (
                await connection.execute(
                    "SELECT count(*) AS n FROM outbox WHERE execution_id=%s AND applied_at IS NULL",
                    (identifier,),
                )
            ).fetchone()
        assert unapplied["n"] == 0

    # Either delivery path must make redelivery harmless, including the retention timer.
    await apply_event(context, event_envelope(completion))
    await maintain_once(context)
    assert await storage.status(context, identifier) == status
    assert (await _execution(db, identifier))["maintenance_due"] == execution["maintenance_due"]
