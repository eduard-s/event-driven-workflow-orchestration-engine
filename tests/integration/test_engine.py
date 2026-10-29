import asyncio
import json
from contextlib import asynccontextmanager
from typing import Any
from uuid import uuid4

import pytest

from api import storage, submissions
from api.context import Context
from api.validation import validate_workflow
from orchestrator.broker import Broker
from orchestrator.messages import event_envelope
from orchestrator.runtime import Runtime
from orchestrator.storage import apply_event
from shared.database import Database
from shared.errors import DomainError
from tests.component.test_api import client_for
from tests.integration import worker_events as worker_storage
from tests.integration.worker_events import maintain_once
from tests.settings import Settings
from worker.broker import Broker as WorkerBroker
from worker.service import Worker

pytestmark = pytest.mark.integration

SUPPLIED_WORKFLOW: dict[str, Any] = {
    "name": "Parallel API Fetcher",
    "dag": {
        "nodes": [
            {"id": "input", "handler": "input", "dependencies": []},
            {
                "id": "get_user",
                "handler": "call_external_service",
                "dependencies": ["input"],
                "config": {"url": "http://localhost:8911/document/policy/list"},
            },
            {
                "id": "get_posts",
                "handler": "call_external_service",
                "dependencies": ["input"],
                "config": {"url": "http://localhost:8911/document/policy/list"},
            },
            {
                "id": "get_comments",
                "handler": "call_external_service",
                "dependencies": ["input"],
                "config": {"url": "http://localhost:8911/document/policy/list"},
            },
            {
                "id": "output",
                "handler": "output",
                "dependencies": ["get_user", "get_posts", "get_comments"],
            },
        ]
    },
}


def graph(*nodes: dict[str, Any]) -> dict[str, Any]:
    return {"name": "test", "dag": {"nodes": list(nodes)}}


def node(
    identifier: str, handler: str = "input", dependencies: list[str] | None = None, **kwargs: Any
) -> dict[str, Any]:
    return {"id": identifier, "handler": handler, "dependencies": dependencies or [], **kwargs}


async def accepted(context: Context, payload: dict[str, Any], trigger: Any = None) -> str:
    result = await submissions.submit(context, validate_workflow(payload), str(uuid4()))
    await submissions.trigger(context, result.execution_id, {} if trigger is None else trigger)
    return result.execution_id


async def terminal(context: Context, identifier: str, timeout: float = 20) -> dict[str, Any]:
    async with asyncio.timeout(timeout):
        while True:
            state = await storage.status(context, identifier)
            if state["state"] not in ("RUNNING", "PENDING"):
                return state
            await asyncio.sleep(0.03)


@asynccontextmanager
async def running(db: Database, settings: Settings):
    context = Context(db, settings)
    broker = Broker(settings)
    await broker.start()
    runtime = Runtime(context, broker)
    await runtime.start()
    worker = Worker(settings, WorkerBroker(settings, client=broker.client))
    worker_task = asyncio.create_task(worker.run())
    try:
        yield context, broker, runtime, worker
    finally:
        worker.stop.set()
        await asyncio.wait_for(worker_task, 10)
        await runtime.close()
        await broker.client.delete(settings.task_stream, settings.event_stream)
        await broker.close()


async def test_unchanged_sample_parallel_data_and_results(db: Database, settings: Settings) -> None:
    async with running(db, settings) as (context, _, _, _):
        identifier = await accepted(context, SUPPLIED_WORKFLOW)
        status = await terminal(context, identifier)
        assert status["state"] == "COMPLETED"
        result = await storage.results(context, identifier)
        assert result["outputs"] == {
            "output": {
                name: {"url": "http://localhost:8911/document/policy/list", "data": None}
                for name in ("get_user", "get_posts", "get_comments")
            }
        }
        branches = [n["tries"][0]["runtime"] for n in status["nodes"] if n["id"].startswith("get_")]
        assert len(branches) == 3
        assert max(x["started_at"] for x in branches) < min(x["finished_at"] for x in branches)


async def test_separate_workflows_overlap_handler_waits(db: Database, settings: Settings) -> None:
    payload = graph(node("remote", "call_external_service", config={"url": "http://mock"}))
    async with running(db, settings) as (context, _, _, _):
        identifiers = await asyncio.gather(*(accepted(context, payload) for _ in range(2)))
        statuses = await asyncio.gather(
            *(terminal(context, identifier) for identifier in identifiers)
        )
        assert all(status["state"] == "COMPLETED" for status in statuses)
        runtimes = [status["nodes"][0]["tries"][0]["runtime"] for status in statuses]
        assert max(runtime["started_at"] for runtime in runtimes) < min(
            runtime["finished_at"] for runtime in runtimes
        )
        assert all(status["nodes"][0]["starts"] == 1 for status in statuses)


async def test_linear_transitive_template_and_llm(db: Database, settings: Settings) -> None:
    payload = graph(
        node("a"),
        node(
            "b",
            "call_external_service",
            ["a"],
            config={"url": "http://mock"},
            data={"id": "{{ a.id }}"},
        ),
        node("c", "llm", ["b"], config={"prompt": "ID {{ a.id }} / {{ b.data }}"}),
        node("out", "output", ["c"], data="{{ c }}"),
    )
    async with running(db, settings) as (context, _, _, _):
        identifier = await accepted(context, payload, {"input": {"id": 7}})
        assert (await terminal(context, identifier))["state"] == "COMPLETED"
        result = await storage.results(context, identifier)
        assert result["outputs"] == {"out": 'mock: ID 7 / {"id":7}'}


async def test_wide_fanout_refills_slots_without_repair(db: Database, settings: Settings) -> None:
    payload = graph(
        node("root"),
        *[node(f"n{i}", "llm", ["root"], config={"prompt": str(i)}) for i in range(20)],
        node("sink", "output", [f"n{i}" for i in range(20)]),
    )
    async with running(db, settings) as (context, _, _, _):
        identifier = await accepted(context, payload)
        assert (await terminal(context, identifier))["state"] == "COMPLETED"
        async with db.tx() as conn:
            assert (
                await (
                    await conn.execute(
                        "SELECT count(*) AS n FROM tries WHERE execution_id=%s", (identifier,)
                    )
                ).fetchone()
            )["n"] == 22
        assert len((await storage.results(context, identifier))["outputs"]["sink"]) == 20


async def test_duplicate_tasks_concurrent_claim_and_stale_results(
    db: Database, settings: Settings
) -> None:
    context = Context(db, settings)
    identifier = await accepted(context, graph(node("a")))
    async with db.tx() as conn:
        start = await (
            await conn.execute(
                "SELECT * FROM outbox WHERE execution_id=%s AND kind='start'", (identifier,)
            )
        ).fetchone()
    await apply_event(context, event_envelope(start))
    async with db.tx() as conn:
        task = await (
            await conn.execute(
                "SELECT * FROM outbox WHERE execution_id=%s AND kind='task'", (identifier,)
            )
        ).fetchone()
    envelope = event_envelope(task)
    claims = await asyncio.gather(*(worker_storage.claim(context, envelope) for _ in range(8)))
    owners = [x for x in claims if x]
    assert len(owners) == 1
    owner = owners[0]
    assert await worker_storage.store_outcome(context, envelope, owner["token"], True, {"x": 1})
    assert await worker_storage.store_outcome(context, envelope, owner["token"], True, {"x": 1})
    assert not await worker_storage.store_outcome(context, envelope, str(uuid4()), True, {"x": 2})
    async with db.tx() as conn:
        event = await (
            await conn.execute(
                "SELECT * FROM outbox WHERE execution_id=%s AND kind='completion'", (identifier,)
            )
        ).fetchone()
    await asyncio.gather(*(apply_event(context, event_envelope(event)) for _ in range(8)))
    assert (await storage.status(context, identifier))["nodes"][0]["starts"] == 1
    assert (await storage.results(context, identifier))["outputs"] == {"a": {"x": 1}}


async def test_transient_retry_and_permanent_branch_failure(
    db: Database, settings: Settings
) -> None:
    settings = settings.model_copy(
        update={
            "test_fixtures_json": json.dumps(
                {"retry": {"fail": "transient", "fail_until_try": 1}, "bad": {"fail": "permanent"}}
            )
        }
    )
    payload = graph(
        node("retry", "call_external_service", config={"url": "http://mock"}),
        node("bad", "call_external_service", config={"url": "http://mock"}),
        node("blocked", "output", ["bad"]),
        node("good", "output", ["retry"]),
    )
    async with running(db, settings) as (context, _, _, _):
        identifier = await accepted(context, payload)
        status = await terminal(context, identifier)
        states = {n["id"]: n for n in status["nodes"]}
        assert status["state"] == "FAILED"
        assert states["blocked"]["state"] == "BLOCKED"
        assert states["good"]["state"] == "COMPLETED"
        assert states["retry"]["starts"] == 2
        assert (await storage.results(context, identifier))["partial_outputs"]["good"]


async def test_timeout_retry_budget_and_terminal_fencing(db: Database, settings: Settings) -> None:
    settings = settings.model_copy(
        update={
            "handler_timeout": 0.3,
            "max_starts": 2,
            "test_fixtures_json": json.dumps({"slow": {"delay": 2}}),
        }
    )
    async with running(db, settings) as (context, _, _, _):
        identifier = await accepted(
            context, graph(node("slow", "call_external_service", config={"url": "http://mock"}))
        )
        status = await terminal(context, identifier)
        assert status["state"] == "FAILED"
        assert status["nodes"][0]["starts"] == 2
        assert all(t["error"]["code"] == "HANDLER_TIMEOUT" for t in status["nodes"][0]["tries"])


@pytest.mark.parametrize("fails", [False, True])
async def test_terminal_http_state_keeps_all_tries_and_rejects_another_trigger(
    db: Database, settings: Settings, fails: bool
) -> None:
    settings = settings.model_copy(
        update={
            "max_starts": 3,
            "test_fixtures_json": json.dumps({"a": {"fail": "transient"}} if fails else {}),
        }
    )
    async with running(db, settings) as (context, _, _, _), client_for(settings) as (client, _):
        key = str(uuid4())
        definition = graph(node("a"))
        response = await client.post("/workflow", json=definition, headers={"Idempotency-Key": key})
        identifier = response.json()["execution_id"]
        trigger_path = f"/workflow/trigger/{identifier}"
        assert (await client.post(trigger_path)).status_code == 202
        await terminal(context, identifier)
        before = await client.get(f"/workflows/{identifier}")
        assert before.status_code == 200
        body = before.json()
        assert body["state"] == ("FAILED" if fails else "COMPLETED")
        tries = body["nodes"][0]["tries"]
        assert [t["try_number"] for t in tries] == list(range(1, 4 if fails else 2))
        assert all(t["success"] == (not fails) for t in tries)
        rejected = await client.post(trigger_path)
        assert rejected.status_code == 409
        assert rejected.json()["error"]["code"] == "EXECUTION_ALREADY_TRIGGERED"
        assert (await client.get(f"/workflows/{identifier}")).json() == body
        result = await client.get(f"/workflows/{identifier}/results")
        assert result.status_code == (409 if fails else 200)
        if fails:
            assert result.json()["error"]["code"] == "RESULTS_FAILED"
            assert all(t["error"]["code"] == "MOCK_TRANSIENT_FAILURE" for t in tries)
        repeat = await client.post("/workflow", json=definition, headers={"Idempotency-Key": key})
        assert repeat.status_code == 201 and repeat.json()["execution_id"] == identifier


async def test_deadline_wins_and_queued_work_never_starts(db: Database, settings: Settings) -> None:
    settings = settings.model_copy(update={"execution_timeout": 0.1, "handler_timeout": 0.1})
    context = Context(db, settings)
    identifier = await accepted(context, graph(node("a")))
    await asyncio.sleep(0.12)
    await maintain_once(context)
    status = await storage.status(context, identifier)
    assert status["state"] == "EXPIRED"
    assert status["nodes"][0]["starts"] == 0
    await maintain_once(context)
    assert (await storage.status(context, identifier))["state"] == "EXPIRED"


async def test_submission_keys_and_single_trigger(db: Database, settings: Settings) -> None:
    context = Context(db, settings)
    workflow = validate_workflow(graph(node("a")))
    responses = await asyncio.gather(
        *(submissions.submit(context, workflow, "same") for _ in range(4))
    )
    assert len({r.execution_id for r in responses}) == 1
    repeated = await submissions.submit(context, validate_workflow(graph(node("b"))), "same")
    assert repeated.execution_id == responses[0].execution_id and repeated.repeated
    assert [
        item["id"] for item in (await storage.status(context, repeated.execution_id))["nodes"]
    ] == ["a"]
    separate = await submissions.submit(context, workflow, "new-request")
    assert separate.execution_id != responses[0].execution_id
    identifier = responses[0].execution_id
    await submissions.trigger(context, identifier, {})
    with pytest.raises(DomainError) as exc:
        await submissions.trigger(context, identifier, {})
    assert exc.value.code == "EXECUTION_ALREADY_TRIGGERED"
    with pytest.raises(DomainError) as exc:
        await submissions.trigger(context, identifier, {"input": 1})
    assert exc.value.status == 409


async def test_notification_loss_reconciles_from_postgres(db: Database, settings: Settings) -> None:
    settings = settings.model_copy(update={"enable_repair": True, "repair_interval": 0.1})
    async with running(db, settings) as (context, broker, runtime, worker):
        worker.stop.set()
        identifier = await accepted(context, graph(node("a")))
        await asyncio.sleep(0.2)
        await broker.client.delete(settings.task_stream, settings.event_stream)
        await broker.start()
        replacement = Worker(settings, WorkerBroker(settings, client=broker.client))
        task = asyncio.create_task(replacement.run())
        try:
            assert (await terminal(context, identifier))["state"] == "COMPLETED"
        finally:
            replacement.stop.set()
            await task


async def test_expired_claim_and_durable_outcome_are_distinct(
    db: Database, settings: Settings
) -> None:
    context = Context(db, settings)
    identifier = await accepted(context, graph(node("a")))
    async with db.tx() as conn:
        start = await (
            await conn.execute(
                "SELECT * FROM outbox WHERE execution_id=%s AND kind='start'", (identifier,)
            )
        ).fetchone()
    await apply_event(context, event_envelope(start))
    async with db.tx() as conn:
        task = await (
            await conn.execute(
                "SELECT * FROM outbox WHERE execution_id=%s AND kind='task'", (identifier,)
            )
        ).fetchone()
    claim = await worker_storage.claim(context, event_envelope(task))
    assert claim
    assert await worker_storage.store_outcome(
        context, event_envelope(task), claim["token"], True, None
    )
    async with db.tx() as conn:
        await conn.execute(
            "UPDATE tries SET lease_until=clock_timestamp()-interval '1 second' WHERE execution_id=%s",
            (identifier,),
        )
    await maintain_once(context)
    async with db.tx() as conn:
        count = await (
            await conn.execute(
                "SELECT count(*) AS n FROM tries WHERE execution_id=%s", (identifier,)
            )
        ).fetchone()
        completion = await (
            await conn.execute(
                "SELECT * FROM outbox WHERE execution_id=%s AND kind='completion'", (identifier,)
            )
        ).fetchone()
    assert count["n"] == 1
    await apply_event(context, event_envelope(completion))
    assert (await storage.results(context, identifier))["outputs"] == {"a": None}


async def test_retention_preserves_active_and_removes_terminal(
    db: Database, settings: Settings
) -> None:
    settings = settings.model_copy(update={"retention_seconds": 0.1})
    async with running(db, settings) as (context, _, _, _):
        identifier = await accepted(context, graph(node("a")))
        assert (await terminal(context, identifier))["state"] == "COMPLETED"
        await asyncio.sleep(0.2)
        await maintain_once(context)
        with pytest.raises(DomainError) as exc:
            await storage.results(context, identifier)
        assert exc.value.status == 404
