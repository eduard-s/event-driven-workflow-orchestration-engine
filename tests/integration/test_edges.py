"""Acceptance cases at durable state, transport, and capacity boundaries."""

import asyncio
import json
import os
from typing import Any

import pytest

from api import storage, submissions
from api.context import Context
from api.validation import validate_workflow
from orchestrator.broker import Broker
from orchestrator.messages import event_envelope
from orchestrator.outbox import publish_once, repair_publications
from orchestrator.runtime import Runtime
from orchestrator.storage import apply_event
from shared.database import Database
from shared.errors import DomainError
from tests.integration import worker_events as worker_storage
from tests.integration.test_engine import accepted, graph, node, running, terminal
from tests.integration.worker_events import maintain_once
from tests.settings import Settings

pytestmark = pytest.mark.integration


async def _event(db: Database, identifier: str, kind: str) -> dict[str, Any]:
    async with db.tx() as conn:
        result = await (
            await conn.execute(
                "SELECT * FROM outbox WHERE execution_id=%s AND kind=%s ORDER BY try_number DESC LIMIT 1",
                (identifier, kind),
            )
        ).fetchone()
    assert result is not None
    return result


async def _queued(context: Context, db: Database) -> tuple[str, dict[str, Any]]:
    identifier = await accepted(context, graph(node("a")))
    await apply_event(context, event_envelope(await _event(db, identifier, "start")))
    return identifier, event_envelope(await _event(db, identifier, "task"))


async def _age_terminal(db: Database, identifier: str) -> None:
    async with db.tx() as conn:
        await conn.execute(
            "UPDATE executions SET terminal_at=clock_timestamp()-interval '2 days', maintenance_due=clock_timestamp()-interval '1 second' WHERE id=%s",
            (identifier,),
        )


async def test_queued_redis_entry_is_not_republished_for_backlog(
    db: Database, settings: Settings
) -> None:
    context = Context(db, settings)
    identifier, _ = await _queued(context, db)
    broker = Broker(settings)
    await broker.start()
    try:
        await publish_once(context, broker)
        original = await _event(db, identifier, "task")
        for _ in range(3):
            async with db.tx() as conn:
                await conn.execute(
                    "UPDATE outbox SET checked_at=clock_timestamp()-interval '1 minute' WHERE execution_id=%s",
                    (identifier,),
                )
            await repair_publications(context, broker)
            await publish_once(context, broker)
        final = await _event(db, identifier, "task")
        assert final["publications"] == 1
        assert final["published_id"] == original["published_id"]
        assert await broker.client.xlen(settings.task_stream) == 1
        status = await storage.status(context, identifier)
        assert status["nodes"][0]["starts"] == 0
        assert status["nodes"][0]["reason"] == "QUEUED"
    finally:
        await broker.client.delete(settings.task_stream, settings.event_stream)
        await broker.close()


async def test_expired_claim_result_cannot_replace_new_attempt(
    db: Database, settings: Settings
) -> None:
    context = Context(db, settings)
    identifier, first = await _queued(context, db)
    owner = await worker_storage.claim(context, first)
    assert owner
    async with db.tx() as conn:
        await conn.execute(
            "UPDATE tries SET lease_until=clock_timestamp()-interval '1 second' WHERE execution_id=%s",
            (identifier,),
        )
        await conn.execute(
            "UPDATE executions SET maintenance_due=clock_timestamp()-interval '1 second' WHERE id=%s",
            (identifier,),
        )
    assert not await worker_storage.store_outcome(context, first, owner["token"], True, "stale")
    await worker_storage.expire_claim(context, first)
    await maintain_once(context)
    await apply_event(context, event_envelope(await _event(db, identifier, "completion")))
    async with db.tx() as conn:
        await conn.execute(
            "UPDATE executions SET maintenance_due=clock_timestamp()-interval '1 second' WHERE id=%s",
            (identifier,),
        )
    await maintain_once(context)
    second = event_envelope(await _event(db, identifier, "task"))
    replacement = await worker_storage.claim(context, second)
    assert replacement
    assert second["try_number"] == 2
    assert not await worker_storage.store_outcome(context, first, owner["token"], True, "stale")
    assert await worker_storage.store_outcome(
        context, second, replacement["token"], True, "current"
    )
    await apply_event(context, event_envelope(await _event(db, identifier, "completion")))
    assert (await storage.results(context, identifier))["outputs"] == {"a": "current"}
    status = await storage.status(context, identifier)
    assert status["nodes"][0]["starts"] == 2
    assert status["nodes"][0]["tries"][0]["error"]["code"] == "HANDLER_TIMEOUT"


async def test_retry_delay_past_execution_deadline_fails_without_second_start(
    db: Database, settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = settings.model_copy(
        update={
            "execution_timeout": 5,
            "retry_base": 10,
            "retry_max": 10,
            "test_fixtures_json": json.dumps({"a": {"fail": "transient"}}),
        }
    )
    monkeypatch.setattr("orchestrator.storage.random.uniform", lambda low, high: high)
    async with running(db, settings) as (context, _, _, _):
        identifier = await accepted(context, graph(node("a")))
        state = await terminal(context, identifier)
        assert state["state"] == "FAILED"
        assert state["nodes"][0]["starts"] == 1
        assert state["nodes"][0]["error"]["code"] == "RETRY_DEADLINE_EXCEEDED"
        assert state["nodes"][0]["error"]["retryable"] is False


@pytest.mark.parametrize("mode", ["auto", "process"])
async def test_three_large_parents_aggregate_through_output_handler(
    db: Database, settings: Settings, mode: str
) -> None:
    values = {name: name * (100 * 1024) for name in ("a", "b", "c")}
    settings = settings.model_copy(
        update={
            "handler_mode": mode,
            "test_fixtures_json": json.dumps(
                {key: {"output": value} for key, value in values.items()}
            ),
        }
    )
    payload = graph(
        *(node(name) for name in values),
        node("combined", "output", list(values)),
    )
    async with running(db, settings) as (context, _, _, _):
        identifier = await accepted(context, payload)
        state = await terminal(context, identifier)
        assert state["state"] == "COMPLETED"
        assert (await storage.results(context, identifier))["outputs"] == {"combined": values}
        for current in state["nodes"]:
            isolated = mode == "process" or current["id"] != "combined"
            assert (current["tries"][0]["runtime"]["pid"] != os.getpid()) is isolated
        async with db.tx() as conn:
            row = await (
                await conn.execute(
                    "SELECT input_bytes, output_bytes FROM executions WHERE id=%s", (identifier,)
                )
            ).fetchone()
        assert row is not None
        assert row["input_bytes"] > 300 * 1024
        assert row["output_bytes"] > 600 * 1024


async def test_oversize_worker_output_fails_permanently(db: Database, settings: Settings) -> None:
    settings = settings.model_copy(
        update={
            "max_value_bytes": 1024,
            "test_fixtures_json": json.dumps({"a": {"output": "x" * 2048}}),
        }
    )
    async with running(db, settings) as (context, _, _, _):
        identifier = await accepted(context, graph(node("a"), node("blocked", "output", ["a"])))
        state = await terminal(context, identifier)
        nodes = {n["id"]: n for n in state["nodes"]}
        assert state["state"] == "FAILED"
        assert nodes["a"]["starts"] == 1
        assert nodes["a"]["error"]["code"] == "OUTPUT_TOO_LARGE"
        assert nodes["a"]["error"]["retryable"] is False
        assert nodes["blocked"]["state"] == "BLOCKED"
        assert not (await storage.results(context, identifier))["partial_outputs"]


async def test_execution_input_budget_failure_does_not_start_or_retry_node(
    db: Database, settings: Settings
) -> None:
    settings = settings.model_copy(update={"max_execution_bytes": 140})
    async with running(db, settings) as (context, _, _, _):
        identifier = await accepted(context, graph(node("a"), node("b")), {"input": "x" * 50})
        state = await terminal(context, identifier)
        nodes = {n["id"]: n for n in state["nodes"]}
        assert state["state"] == "FAILED"
        assert nodes["a"]["state"] == "COMPLETED"
        assert nodes["b"]["starts"] == 0
        assert nodes["b"]["tries"] == []
        assert nodes["b"]["error"]["code"] == "INPUT_BUDGET"
        assert nodes["b"]["error"]["retryable"] is False
        assert (await storage.results(context, identifier))["partial_outputs"] == {"a": "x" * 50}


async def test_execution_output_budget_failure_is_permanent(
    db: Database, settings: Settings
) -> None:
    settings = settings.model_copy(
        update={
            "max_execution_bytes": 200,
            "test_fixtures_json": json.dumps({key: {"output": "x" * 120} for key in ("a", "b")}),
        }
    )
    async with running(db, settings) as (context, _, _, _):
        identifier = await accepted(context, graph(node("a"), node("b")))
        state = await terminal(context, identifier)
        assert state["state"] == "FAILED"
        assert sorted(n["state"] for n in state["nodes"]) == ["COMPLETED", "FAILED"]
        failed = next(n for n in state["nodes"] if n["state"] == "FAILED")
        assert failed["starts"] == 1
        assert failed["error"]["code"] == "INVALID_OUTPUT"
        assert failed["error"]["retryable"] is False
        assert len((await storage.results(context, identifier))["partial_outputs"]) == 1


async def test_two_app_runtimes_dispatch_fanin_once_without_repair(
    db: Database, settings: Settings
) -> None:
    assert settings.enable_repair is False
    payload = graph(
        *(node(f"parent_{i}") for i in range(12)),
        node("join", "output", [f"parent_{i}" for i in range(12)]),
    )
    async with running(db, settings) as (context, _, first_runtime, _):
        second_db = Database(settings)
        await second_db.open()
        second_broker = Broker(settings)
        await second_broker.start()
        second_runtime = Runtime(Context(second_db, settings), second_broker)
        await second_runtime.start()
        try:
            identifiers = await asyncio.gather(*(accepted(context, payload) for _ in range(3)))
            for identifier in identifiers:
                state = await terminal(context, identifier)
                assert state["state"] == "COMPLETED"
                assert all(n["starts"] == 1 and len(n["tries"]) == 1 for n in state["nodes"])
                assert len((await storage.results(context, identifier))["outputs"]["join"]) == 12
            assert first_runtime.failure is None
            assert second_runtime.failure is None
        finally:
            await second_runtime.close()
            await second_broker.close()
            await second_db.close()


async def test_expired_execution_remains_until_retention_ends(
    db: Database, settings: Settings
) -> None:
    context = Context(db, settings)
    identifier = await accepted(context, graph(node("a")))
    async with db.tx() as connection:
        await connection.execute(
            "UPDATE executions SET deadline_at=clock_timestamp()-interval '1 second' WHERE id=%s",
            (identifier,),
        )
    await maintain_once(context)
    expired = await storage.status(context, identifier)
    assert expired["state"] == "EXPIRED" and expired["terminal_at"] is not None
    assert expired["nodes"][0]["starts"] == 0
    await _age_terminal(db, identifier)
    await maintain_once(context)
    with pytest.raises(DomainError) as failure:
        await storage.status(context, identifier)
    assert failure.value.status == 404


async def test_late_messages_after_retention_cannot_resurrect_execution(
    db: Database, settings: Settings
) -> None:
    context = Context(db, settings)
    workflow = validate_workflow(graph(node("a")))
    identifier = (await submissions.submit(context, workflow, "retained-key")).execution_id
    await submissions.trigger(context, identifier, {})
    start = event_envelope(await _event(db, identifier, "start"))
    await apply_event(context, start)
    task = event_envelope(await _event(db, identifier, "task"))
    claim = await worker_storage.claim(context, task)
    assert claim
    assert await worker_storage.store_outcome(context, task, claim["token"], True, "old")
    completion = event_envelope(await _event(db, identifier, "completion"))
    await apply_event(context, completion)
    await _age_terminal(db, identifier)
    await maintain_once(context)
    await asyncio.gather(apply_event(context, start), apply_event(context, completion))
    assert await worker_storage.claim(context, task) is None
    assert not await worker_storage.store_outcome(context, task, claim["token"], True, "old")
    with pytest.raises(DomainError) as exc:
        await storage.results(context, identifier)
    assert exc.value.status == 404
    replacement = await submissions.submit(context, workflow, "retained-key")
    assert replacement.execution_id != identifier
    async with db.tx() as conn:
        for table in ("executions", "nodes", "tries", "outbox"):
            column = "id" if table == "executions" else "execution_id"
            result = await (
                await conn.execute(
                    f"SELECT count(*) AS n FROM {table} WHERE {column}=%s", (identifier,)
                )
            ).fetchone()
            assert result is not None
            assert result["n"] == 0


async def test_results_keep_final_snapshot_when_cleanup_commits_mid_read(
    db: Database, settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    context = Context(db, settings)
    identifier, task = await _queued(context, db)
    claim = await worker_storage.claim(context, task)
    assert claim
    assert await worker_storage.store_outcome(
        context, task, claim["token"], True, {"complete": [1, 2, 3]}
    )
    await apply_event(context, event_envelope(await _event(db, identifier, "completion")))
    await _age_terminal(db, identifier)
    entered = asyncio.Event()
    resume = asyncio.Event()
    original_nodes = storage.load_result_nodes

    async def pause_after_execution_read(conn: Any, execution: Any) -> list[dict[str, Any]]:
        entered.set()
        await resume.wait()
        return await original_nodes(conn, execution)

    monkeypatch.setattr(storage, "load_result_nodes", pause_after_execution_read)
    reading = asyncio.create_task(storage.results(context, identifier))
    try:
        await asyncio.wait_for(entered.wait(), 3)
        await maintain_once(Context(db, settings))
        resume.set()
        result = await asyncio.wait_for(reading, 3)
        assert result["state"] == "COMPLETED"
        assert result["outputs"] == {"a": {"complete": [1, 2, 3]}}
        with pytest.raises(DomainError) as exc:
            await storage.results(context, identifier)
        assert exc.value.status == 404
    finally:
        resume.set()
        if not reading.done():
            reading.cancel()
        await asyncio.gather(reading, return_exceptions=True)


@pytest.mark.parametrize(
    ("original", "different"),
    [(True, 1), (False, 0), ({"nested": [True]}, {"nested": [1]}), ([False], [0])],
)
async def test_outcome_replay_compares_json_types(
    db: Database, settings: Settings, original: Any, different: Any
) -> None:
    context = Context(db, settings)
    identifier, task = await _queued(context, db)
    claim = await worker_storage.claim(context, task)
    assert claim
    assert await worker_storage.store_outcome(context, task, claim["token"], True, original)
    assert await worker_storage.store_outcome(context, task, claim["token"], True, original)
    assert not await worker_storage.store_outcome(context, task, claim["token"], True, different)
    await apply_event(context, event_envelope(await _event(db, identifier, "completion")))
    result = (await storage.results(context, identifier))["outputs"]["a"]
    assert json.dumps(result) == json.dumps(original)


@pytest.mark.parametrize("value", [1e20, 1e100, {"nested": [1e20, 1e100]}])
async def test_outcome_replay_accepts_jsonb_numeric_normalization(
    db: Database, settings: Settings, value: Any
) -> None:
    context = Context(db, settings)
    identifier, task = await _queued(context, db)
    claim = await worker_storage.claim(context, task)
    assert claim
    assert await worker_storage.store_outcome(context, task, claim["token"], True, value)
    assert await worker_storage.store_outcome(context, task, claim["token"], True, value)
    assert not await worker_storage.store_outcome(context, task, claim["token"], True, 0)
    await apply_event(context, event_envelope(await _event(db, identifier, "completion")))
    assert (await storage.status(context, identifier))["state"] == "COMPLETED"
