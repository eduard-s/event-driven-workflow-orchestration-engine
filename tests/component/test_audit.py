"""Audit actual HTTP outcomes in a PostgreSQL database separate from workflow state."""

import asyncio
import logging
from collections.abc import AsyncGenerator
from typing import Any
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from limits import RateLimitItemPerSecond

from api import storage
from api.app import create_app
from shared.database import Database
from tests.component.test_api import client_for, payload
from tests.settings import Settings

pytestmark = pytest.mark.component


@pytest.fixture
async def audit_db(settings: Settings) -> AsyncGenerator[Database]:
    database = Database(settings.model_copy(update={"postgres_dsn": settings.audit_postgres_dsn}))
    await database.open()
    try:
        async with database.tx() as connection:
            await connection.execute("TRUNCATE request_audit")
        yield database
    finally:
        await database.close()


async def records(database: Database) -> list[dict[str, Any]]:
    async with database.tx() as connection:
        return await (
            await connection.execute("SELECT * FROM request_audit ORDER BY created_at,request_id")
        ).fetchall()


@pytest.mark.parametrize(
    ("body", "status", "reason"),
    [
        (b"{", 422, "INVALID_REQUEST"),
        (b"{}", 422, "INVALID_REQUEST"),
        (
            b'{"name":"secret","dag":{"nodes":[{"id":"a","handler":"input","dependencies":["missing"]}]}}',
            422,
            "MISSING_DEPENDENCY",
        ),
    ],
)
async def test_bad_requests_leave_audit_only(
    db: Database, audit_db: Database, settings: Settings, body: bytes, status: int, reason: str
) -> None:
    key = str(uuid4())
    async with client_for(settings) as (client, _):
        response = await client.post(
            "/workflow",
            content=body,
            headers={"Content-Type": "application/json", "Idempotency-Key": key},
        )
    assert response.status_code == status
    rows = await records(audit_db)
    assert len(rows) == 1
    assert rows[0]["reason"] == reason
    assert rows[0]["status"] == status and rows[0]["outcome"] == "rejected"
    assert rows[0]["route"] == "/workflow" and rows[0]["execution_id"] is None
    assert "key_hash" not in rows[0]
    assert "secret" not in str(rows) and key not in str(rows)
    async with db.tx() as connection:
        row = await (await connection.execute("SELECT count(*) AS n FROM executions")).fetchone()
        assert row == {"n": 0}
        row = await (
            await connection.execute("SELECT to_regclass('request_audit') AS table")
        ).fetchone()
        assert row == {"table": None}


async def test_repeated_keys_and_trigger_conflicts_across_api_instances(
    db: Database, audit_db: Database, settings: Settings
) -> None:
    key = str(uuid4())
    async with client_for(settings) as (first, _), client_for(settings) as (second, _):
        clients = (first, second) * 4
        responses = await asyncio.gather(
            *(
                client.post("/workflow", json=payload(), headers={"Idempotency-Key": key})
                for client in clients
            )
        )
        assert all(response.status_code == 201 for response in responses)
        identifier = responses[0].json()["execution_id"]
        assert all(response.json() == {"execution_id": identifier} for response in responses)
        path = f"/workflow/trigger/{identifier}"
        responses = await asyncio.gather(*(client.post(path) for client in clients))
        assert sorted(response.status_code for response in responses) == [202] + [409] * 7
        changed = {**payload(), "name": "different"}
        repeated = await first.post("/workflow", json=changed, headers={"Idempotency-Key": key})
        assert repeated.status_code == 201
        assert repeated.json() == {"execution_id": identifier}
        assert (await second.post(path, json={"input": 1})).status_code == 409
    rows = await records(audit_db)
    assert len(rows) == 16
    assert sum(row["reason"] == "DUPLICATE_SUBMISSION" for row in rows) == 8
    assert sum(row["reason"] == "EXECUTION_ALREADY_TRIGGERED" for row in rows) == 8
    assert sum(row["outcome"] == "rejected" for row in rows) == 8
    assert {row["reason"] for row in rows if row["outcome"] == "rejected"} == {
        "EXECUTION_ALREADY_TRIGGERED",
    }
    async with db.tx() as connection:
        row = await (
            await connection.execute("SELECT count(*) AS n FROM outbox WHERE kind='start'")
        ).fetchone()
        assert row == {"n": 1}


async def test_parameter_rate_and_result_rejections_are_audited(
    db: Database, audit_db: Database, settings: Settings
) -> None:
    async with client_for(settings) as (client, app):
        submitted = await client.post(
            "/workflow", json=payload(), headers={"Idempotency-Key": str(uuid4())}
        )
        identifier = submitted.json()["execution_id"]
        assert (await client.get(f"/workflows/{identifier}?limit=0")).status_code == 422
        assert (await client.get(f"/workflows/{identifier}/results")).status_code == 409
        app.state.rate_limit = RateLimitItemPerSecond(1)
        assert (await client.get(f"/workflows/{identifier}")).status_code == 200
        assert (await client.get(f"/workflows/{identifier}")).status_code == 429
    rows = await records(audit_db)
    assert {row["reason"] for row in rows} == {
        "INVALID_REQUEST",
        "RESULTS_UNAVAILABLE",
        "RATE_LIMITED",
    }
    assert all(
        str(row["execution_id"]) == identifier for row in rows if row["reason"] != "RATE_LIMITED"
    )
    rate_record = next(row for row in rows if row["reason"] == "RATE_LIMITED")
    assert rate_record["execution_id"] is None and rate_record["route"] == "<unmatched>"


async def test_audit_outage_preserves_api_outcomes(
    db: Database, audit_db: Database, settings: Settings, caplog: pytest.LogCaptureFixture
) -> None:
    async with client_for(settings) as (client, app):
        await app.state.audit.close()
        with caplog.at_level(logging.INFO, logger="api"):
            rejected = await client.post(
                "/workflow", json={}, headers={"Idempotency-Key": str(uuid4())}
            )
            key = str(uuid4())
            accepted = await client.post(
                "/workflow", json=payload(), headers={"Idempotency-Key": key}
            )
            repeated = await client.post(
                "/workflow", json=payload(), headers={"Idempotency-Key": key}
            )
        assert rejected.status_code == 422
        assert accepted.status_code == repeated.status_code == 201
        assert accepted.json() == repeated.json()
    assert await records(audit_db) == []
    assert "request_audit" in caplog.text and "audit_storage_unavailable" in caplog.text


async def test_blocked_audit_database_does_not_block_workflow_writes(
    db: Database, audit_db: Database, settings: Settings
) -> None:
    async with client_for(settings) as (client, _):
        async with audit_db.tx() as connection:
            await connection.execute("LOCK TABLE request_audit IN ACCESS EXCLUSIVE MODE")
            async with asyncio.timeout(3):
                responses = await asyncio.gather(
                    *(
                        client.post("/workflow", json={}, headers={"Idempotency-Key": str(uuid4())})
                        for _ in range(8)
                    )
                )
                assert all(response.status_code == 422 for response in responses)
                accepted = await client.post(
                    "/workflow", json=payload(), headers={"Idempotency-Key": str(uuid4())}
                )
                assert accepted.status_code == 201
                identifier = accepted.json()["execution_id"]
                assert (await client.post(f"/workflow/trigger/{identifier}")).status_code == 202
    # The writer can finish queued records after the lock releases.
    assert all(row["reason"] == "INVALID_REQUEST" for row in await records(audit_db))


async def test_requests_do_not_wait_for_audit_storage(
    db: Database, audit_db: Database, settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    entered, release = asyncio.Event(), asyncio.Event()
    original = storage.write_audit

    async def blocked(pool, record):
        entered.set()
        await release.wait()
        await original(pool, record)

    monkeypatch.setattr(storage, "write_audit", blocked)
    async with client_for(settings) as (client, app):
        try:
            async with asyncio.timeout(2):
                rejected = await client.post("/workflow", json={})
                assert rejected.status_code == 422
                await entered.wait()
                accepted = await client.post(
                    "/workflow", json=payload(), headers={"Idempotency-Key": str(uuid4())}
                )
                assert accepted.status_code == 201
                identifier = accepted.json()["execution_id"]
                assert (await client.post(f"/workflow/trigger/{identifier}")).status_code == 202
                assert await records(audit_db) == []
        finally:
            release.set()
        await asyncio.wait_for(app.state.audit_queue.join(), 2)
    assert len(await records(audit_db)) == 1


async def test_unexpected_audit_errors_do_not_stop_the_writer(
    db: Database, audit_db: Database, settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    insert = AsyncMock(side_effect=RuntimeError("unexpected audit failure"))
    monkeypatch.setattr(storage, "insert_audit", insert)
    async with client_for(settings) as (client, app):
        for _ in range(3):
            assert (await client.post("/workflow", json={})).status_code == 422
            await asyncio.wait_for(app.state.audit_queue.join(), 2)
    assert insert.await_count == 3
    assert await records(audit_db) == []


async def test_both_databases_unavailable_preserve_503(db: Database, settings: Settings) -> None:
    async with client_for(settings) as (client, app):
        await app.state.db.close()
        await app.state.audit.close()
        response = await client.get(f"/workflows/{uuid4()}")
        assert response.status_code == 503
        assert response.json()["error"]["code"] == "STORAGE_UNAVAILABLE"
        await asyncio.wait_for(app.state.audit_queue.join(), 2)


async def test_audit_open_failure_closes_workflow_database(
    db: Database, settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = create_app(settings)
    monkeypatch.setattr(app.state.audit, "open", AsyncMock(side_effect=RuntimeError("open failed")))
    close_audit = AsyncMock(wraps=app.state.audit.close)
    monkeypatch.setattr(app.state.audit, "close", close_audit)
    with pytest.raises(RuntimeError, match="open failed"):
        async with app.router.lifespan_context(app):
            pytest.fail("Startup must fail before serving requests")
    assert app.state.db.pool.closed
    close_audit.assert_not_awaited()
