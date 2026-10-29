import asyncio
from contextlib import asynccontextmanager
from typing import Any
from uuid import uuid4

import httpx
import pytest
from limits import RateLimitItemPerSecond
from psycopg.errors import UniqueViolation
from psycopg.types.json import Jsonb

from api import storage
from api.app import create_app
from api.submissions import prepare_submission
from shared.database import Database
from tests.settings import Settings

pytestmark = pytest.mark.component


@asynccontextmanager
async def client_for(settings: Settings):
    app = create_app(settings)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            yield client, app


def payload() -> dict[str, Any]:
    return {"name": "api", "dag": {"nodes": [{"id": "a", "handler": "input", "dependencies": []}]}}


async def test_submission_accepts_work_within_configured_capacity(
    db: Database, settings: Settings
) -> None:
    settings = settings.model_copy(update={"max_executions": 2000, "max_records": 2000})
    async with db.tx() as connection:
        await connection.execute(
            """INSERT INTO executions(id,definition,policy,state)
            SELECT gen_random_uuid(),%s,%s,'PENDING' FROM generate_series(1,1005)""",
            (Jsonb(payload()), Jsonb(settings.execution_policy())),
        )
    async with client_for(settings) as (client, _):
        response = await client.post(
            "/workflow", json=payload(), headers={"Idempotency-Key": str(uuid4())}
        )
        assert response.status_code == 201, response.text


@pytest.mark.parametrize("state", ["PENDING", "RUNNING", "EXPIRED"])
async def test_unfinished_results_return_state_specific_error(
    db: Database, settings: Settings, state: str
) -> None:
    async with client_for(settings) as (client, _):
        submitted = await client.post(
            "/workflow", json=payload(), headers={"Idempotency-Key": str(uuid4())}
        )
        identifier = submitted.json()["execution_id"]
        async with db.tx() as connection:
            await connection.execute(
                "UPDATE executions SET state=%s WHERE id=%s", (state, identifier)
            )
        response = await client.get(f"/workflows/{identifier}/results")
        assert response.status_code == 409
        assert response.json()["state"] == state
        assert response.json()["error"]["code"] == "RESULTS_" + state
        assert "partial_outputs" in response.json()


async def test_parallel_api_instances_create_and_trigger_one_execution(
    db: Database, settings: Settings
) -> None:
    async with client_for(settings) as (first, _), client_for(settings) as (second, _):
        clients = (first, second) * 4
        responses = await asyncio.gather(
            *(
                client.post(
                    "/workflow",
                    json={**payload(), "name": f"request-{index}"},
                    headers={"Idempotency-Key": "same-operation"},
                )
                for index, client in enumerate(clients)
            )
        )
        assert all(response.status_code == 201 for response in responses)
        identifiers = {response.json()["execution_id"] for response in responses}
        assert len(identifiers) == 1
        identifier = identifiers.pop()
        triggers = await asyncio.gather(
            *(
                client.post(f"/workflow/trigger/{identifier}", json={"input": 7})
                for client in clients
            )
        )
        assert sorted(response.status_code for response in triggers) == [202] + [409] * 7
        async with db.tx() as connection:
            counts = await (
                await connection.execute(
                    """SELECT (SELECT count(*) FROM executions) AS executions,
                (SELECT count(*) FROM submission_keys) AS keys,
                (SELECT count(*) FROM outbox WHERE kind='start') AS starts"""
                )
            ).fetchone()
        assert counts == {"executions": 1, "keys": 1, "starts": 1}
        status = (await first.get(f"/workflows/{identifier}")).json()
        assert status["state"] == "RUNNING" and status["nodes"][0]["starts"] == 0
        conflict = await second.post(f"/workflow/trigger/{identifier}", json={"input": 8})
        assert conflict.status_code == 409
        new_run = await first.post(
            "/workflow", json=payload(), headers={"Idempotency-Key": "another-operation"}
        )
        assert new_run.status_code == 201 and new_run.json()["execution_id"] != identifier


async def test_submissions_without_keys_create_independent_executions(
    db: Database, settings: Settings
) -> None:
    async with client_for(settings) as (first, _), client_for(settings) as (second, _):
        responses = await asyncio.gather(
            *(client.post("/workflow", json=payload()) for client in (first, second) * 4)
        )
    assert all(response.status_code == 201 for response in responses)
    assert len({response.json()["execution_id"] for response in responses}) == 8
    async with db.tx() as connection:
        counts = await (
            await connection.execute(
                """SELECT (SELECT count(*) FROM executions) AS executions,
                (SELECT count(*) FROM submission_keys) AS keys"""
            )
        ).fetchone()
    assert counts == {"executions": 8, "keys": 0}


async def test_failed_graph_pipeline_rolls_back_all_writes(
    db: Database, settings: Settings
) -> None:
    workflow, definition = prepare_submission(payload())
    original, rejected = str(uuid4()), str(uuid4())
    async with db.tx() as connection:
        await storage.insert_execution(
            connection, original, workflow, definition, settings.execution_policy(), "key"
        )
    with pytest.raises(UniqueViolation):
        async with db.tx() as connection:
            await storage.insert_execution(
                connection,
                rejected,
                workflow,
                definition,
                settings.execution_policy(),
                "key",
            )
    async with db.tx() as connection:
        rows = await (await connection.execute("SELECT id FROM executions")).fetchall()
        assert [str(row["id"]) for row in rows] == [original]
        assert await storage.load_nodes(connection, rejected, 0, 100) == []
        saved = await storage.lock_submission(connection, "key")
        assert saved is not None and str(saved["execution_id"]) == original


async def test_rate_limit_prevents_writes_and_recovers_after_expiry(
    db: Database, settings: Settings
) -> None:
    async with client_for(settings) as (client, app):
        app.state.rate_limit = RateLimitItemPerSecond(2)
        responses = await asyncio.gather(
            *(
                client.post("/workflow", json=payload(), headers={"Idempotency-Key": str(uuid4())})
                for _ in range(3)
            )
        )
        assert sorted(response.status_code for response in responses) == [201, 201, 429]
        rejected = next(response for response in responses if response.status_code == 429)
        assert rejected.headers["Retry-After"] == "1"
        async with db.tx() as connection:
            counted = await (
                await connection.execute("SELECT count(*) AS n FROM executions")
            ).fetchone()
        assert counted is not None and counted["n"] == 2
        assert (await client.get("/health/live")).status_code == 200
        await asyncio.sleep(1.05)
        assert (
            await client.post(
                "/workflow", json=payload(), headers={"Idempotency-Key": str(uuid4())}
            )
        ).status_code == 201


async def test_api_submission_trigger_results(db: Database, settings: Settings) -> None:
    async with client_for(settings) as (client, app):
        first = await client.post("/workflow", json=payload(), headers={"Idempotency-Key": "api"})
        assert first.status_code == 201
        second = await client.post("/workflow", json=payload(), headers={"Idempotency-Key": "api"})
        assert first.json() == second.json()
        identifier = first.json()["execution_id"]
        assert (await client.get(f"/workflows/{identifier}/results")).status_code == 409
        triggered = await client.post(f"/workflow/trigger/{identifier}")
        assert triggered.status_code == 202
        assert triggered.headers["Location"] == f"/workflows/{identifier}"
        assert triggered.headers["Retry-After"] == "1"
        assert (await client.post(f"/workflow/trigger/{identifier}", json={})).status_code == 409
        assert (
            await client.post(f"/workflow/trigger/{identifier}", json={"input": 1})
        ).status_code == 409
        assert (await client.get(f"/workflows/{identifier}")).json()["state"] == "RUNNING"
        assert (await client.get("/health/live")).status_code == 200
        assert (await client.get("/metrics")).status_code == 404
        assert (await client.get("/health/ready")).status_code == 200
        assert (await client.post("/workflows", json=payload())).status_code == 404
        for path in (
            "/docs",
            "/redoc",
            "/openapi.json",
            "/static/swagger-ui-bundle.js",
        ):
            assert (await client.get(path)).status_code == 404


async def test_api_accepts_work_before_orchestrator_starts(
    db: Database, settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    from redis.asyncio import Redis

    def unexpected_redis(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("The API must not construct a Redis client.")

    monkeypatch.setattr(Redis, "from_url", unexpected_redis)
    async with client_for(settings) as (client, app):
        assert not hasattr(app.state, "runtime")
        response = await client.post("/workflow", json=payload())
        assert response.status_code == 201
        identifier = response.json()["execution_id"]
        assert (await client.post(f"/workflow/trigger/{identifier}")).status_code == 202
        state = (await client.get(f"/workflows/{identifier}")).json()
        assert state["state"] == "RUNNING"
        assert state["nodes"][0]["state"] == "PENDING"
        assert state["nodes"][0]["starts"] == 0 and state["nodes"][0]["tries"] == []
        assert (await client.get("/health/live")).json() == {"state": "alive"}
    async with db.tx() as conn:
        events = await (
            await conn.execute(
                "SELECT kind,published_id,applied_at FROM outbox WHERE execution_id=%s",
                (identifier,),
            )
        ).fetchall()
    assert events == [{"kind": "start", "published_id": None, "applied_at": None}]


@pytest.mark.parametrize(
    ("raw", "status"),
    [
        (b"{", 422),
        (b'{"name":"a","name":"b","dag":{"nodes":[]}}', 422),
        (b'{"value":NaN}', 422),
        (b'{"value":Infinity}', 422),
        (b'{"value":1e9999}', 422),
        (b'{"value":"\\ud800"}', 422),
        (b"\xff", 400),
        (b"{}", 422),
        (b"null", 422),
        (b"[]", 422),
    ],
)
async def test_api_raw_json_errors(
    db: Database, settings: Settings, raw: bytes, status: int
) -> None:
    async with client_for(settings) as (client, _):
        response = await client.post(
            "/workflow",
            content=raw,
            headers={"Content-Type": "application/json", "Idempotency-Key": str(uuid4())},
        )
        assert response.status_code == status, response.text
        assert "detail" in response.json()


async def test_json_duplicate_fields_use_fastapi_last_value(
    db: Database, settings: Settings
) -> None:
    raw = b'{"name":"first","name":"last","dag":{"nodes":[{"id":"a","handler":"input","dependencies":[]}]}}'
    async with client_for(settings) as (client, _):
        response = await client.post(
            "/workflow",
            content=raw,
            headers={"Content-Type": "application/json", "Idempotency-Key": str(uuid4())},
        )
    assert response.status_code == 201, response.text
    async with db.tx() as connection:
        row = await (
            await connection.execute(
                "SELECT definition FROM executions WHERE id=%s", (response.json()["execution_id"],)
            )
        ).fetchone()
    assert row is not None and row["definition"]["name"] == "last"


async def test_fastapi_body_parsing_and_request_parameters(
    db: Database, settings: Settings
) -> None:
    async with client_for(settings) as (client, _):
        assert (await client.post("/workflow", content=b"{}")).status_code == 422
        assert (
            await client.post(
                "/workflow",
                content=b"{}",
                headers={"Content-Type": "text/plain"},
            )
        ).status_code == 422

        async def chunks():
            for _ in range(4):
                yield b"x" * 100

        assert (
            await client.post(
                "/workflow", content=chunks(), headers={"Content-Type": "application/json"}
            )
        ).status_code == 422
        assert (await client.get("/workflows/not-an-id")).status_code == 422
        assert (await client.get("/workflows/" + str(uuid4()))).status_code == 404
        assert (await client.get("/workflows/" + str(uuid4()) + "?limit=1000")).status_code == 422
        response = await client.post(
            "/workflow",
            content=b"[" * 40 + b"0" + b"]" * 40,
            headers={"Content-Type": "application/json"},
        )
        assert response.status_code == 422
        submitted = await client.post(
            "/workflow", json=payload(), headers={"Idempotency-Key": str(uuid4())}
        )
        identifier = submitted.json()["execution_id"]
        assert (
            await client.post(
                f"/workflow/trigger/{identifier}",
                content=b"null",
                headers={"Content-Type": "application/json"},
            )
        ).status_code == 422


async def test_consistent_status_snapshot_during_transitions(
    db: Database, settings: Settings
) -> None:
    async with client_for(settings) as (client, app):
        identifiers = []
        for _ in range(4):
            response = await client.post(
                "/workflow", json=payload(), headers={"Idempotency-Key": str(uuid4())}
            )
            identifiers.append(response.json()["execution_id"])
        await asyncio.gather(*(client.post(f"/workflow/trigger/{i}") for i in identifiers))
        replies = await asyncio.gather(
            *(client.get(f"/workflows/{i}") for i in identifiers for _ in range(4))
        )
        assert all(r.status_code == 200 for r in replies)
        assert all(r.json()["total_nodes"] == len(r.json()["nodes"]) == 1 for r in replies)
