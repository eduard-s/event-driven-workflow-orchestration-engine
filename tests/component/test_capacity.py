"""Admission quotas are shared across API replicas and include retained records."""

import asyncio
from uuid import uuid4

import pytest

from shared.database import Database
from tests.component.test_api import client_for, payload
from tests.settings import Settings

pytestmark = pytest.mark.component


async def test_capacity_is_atomic_across_api_replicas(db: Database, settings: Settings) -> None:
    settings = settings.model_copy(update={"max_executions": 2, "max_records": 10})
    async with client_for(settings) as (first, _), client_for(settings) as (second, _):
        responses = await asyncio.gather(
            *(
                client.post("/workflow", json=payload(), headers={"Idempotency-Key": str(uuid4())})
                for client in (first, second) * 6
            )
        )
        assert sorted(response.status_code for response in responses) == [201] * 2 + [503] * 10
        accepted = next(response for response in responses if response.status_code == 201)
        repeated = await second.post("/workflow", json=payload(), headers=accepted.request.headers)
        assert repeated.status_code == 201 and repeated.json() == accepted.json()
        limited = next(response for response in responses if response.status_code == 503)
        assert limited.json()["error"]["code"] == "EXECUTION_CAPACITY"
        assert limited.headers["Retry-After"] == "1"
    async with db.tx() as connection:
        assert await (
            await connection.execute("SELECT count(*) AS n FROM executions")
        ).fetchone() == {"n": 2}


@pytest.mark.parametrize("terminal", ["COMPLETED", "FAILED", "EXPIRED"])
async def test_terminal_work_releases_unfinished_capacity(
    db: Database, settings: Settings, terminal: str
) -> None:
    settings = settings.model_copy(update={"max_executions": 1, "max_records": 10})
    async with client_for(settings) as (client, _):
        first = await client.post(
            "/workflow", json=payload(), headers={"Idempotency-Key": str(uuid4())}
        )
        identifier = first.json()["execution_id"]
        async with db.tx() as connection:
            await connection.execute(
                "UPDATE executions SET state=%s WHERE id=%s", (terminal, identifier)
            )
        response = await client.post(
            "/workflow", json=payload(), headers={"Idempotency-Key": str(uuid4())}
        )
        assert response.status_code == 201, response.text


async def test_retained_records_use_total_capacity_until_deletion(
    db: Database, settings: Settings
) -> None:
    settings = settings.model_copy(update={"max_executions": 10, "max_records": 1})
    async with client_for(settings) as (client, _):
        first = await client.post(
            "/workflow", json=payload(), headers={"Idempotency-Key": str(uuid4())}
        )
        identifier = first.json()["execution_id"]
        async with db.tx() as connection:
            await connection.execute(
                "UPDATE executions SET state='COMPLETED' WHERE id=%s", (identifier,)
            )
        key = str(uuid4())
        limited = await client.post("/workflow", json=payload(), headers={"Idempotency-Key": key})
        assert limited.status_code == 503
        async with db.tx() as connection:
            await connection.execute("DELETE FROM executions WHERE id=%s", (identifier,))
        accepted = await client.post("/workflow", json=payload(), headers={"Idempotency-Key": key})
        assert accepted.status_code == 201
