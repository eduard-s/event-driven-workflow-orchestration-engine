"""Database constraints and API write guards use real PostgreSQL transactions."""

import asyncio
from datetime import timedelta
from uuid import uuid4

import pytest
from psycopg.errors import CheckViolation, ForeignKeyViolation

from api import storage, submissions
from api.context import Context
from shared.database import Database
from shared.errors import DomainError
from shared.state import database_time
from tests.component.test_api import client_for, payload
from tests.settings import Settings

pytestmark = pytest.mark.component


async def test_trigger_update_has_its_own_atomic_guard(db: Database, settings: Settings) -> None:
    identifier = (
        await submissions.submit(Context(db, settings), payload(), str(uuid4()))
    ).execution_id

    async def accept(value: int) -> int:
        try:
            async with db.tx() as connection:
                now = await database_time(connection)
                await storage.accept_trigger(
                    connection, identifier, {"input": value}, now, now + timedelta(seconds=30)
                )
            return 202
        except DomainError as error:
            return error.status

    assert sorted(await asyncio.gather(accept(1), accept(2))) == [202, 409]
    async with db.tx() as connection:
        stored = await (
            await connection.execute("SELECT trigger FROM executions WHERE id=%s", (identifier,))
        ).fetchone()
    assert stored and stored["trigger"]["input"] in (1, 2)


@pytest.mark.parametrize("parent,child", [("missing", "a"), ("a", "missing")])
async def test_edges_cannot_reference_missing_nodes(
    db: Database, settings: Settings, parent: str, child: str
) -> None:
    identifier = (
        await submissions.submit(Context(db, settings), payload(), str(uuid4()))
    ).execution_id
    other = {
        "name": "other execution",
        "dag": {"nodes": [{"id": "missing", "handler": "input", "dependencies": []}]},
    }
    await submissions.submit(Context(db, settings), other, str(uuid4()))
    with pytest.raises(ForeignKeyViolation):
        async with db.tx() as connection:
            await connection.execute(
                "INSERT INTO edges(execution_id,parent_id,child_id) VALUES (%s,%s,%s)",
                (identifier, parent, child),
            )


async def test_unknown_execution_state_is_rejected_by_database(
    db: Database, settings: Settings
) -> None:
    identifier = (
        await submissions.submit(Context(db, settings), payload(), str(uuid4()))
    ).execution_id
    with pytest.raises(CheckViolation):
        async with db.tx() as connection:
            await connection.execute(
                "UPDATE executions SET state='UNKNOWN' WHERE id=%s", (identifier,)
            )


async def test_missing_completed_output_returns_explicit_error(
    db: Database, settings: Settings
) -> None:
    async with client_for(settings) as (client, _):
        identifier = (
            await client.post(
                "/workflow", json=payload(), headers={"Idempotency-Key": str(uuid4())}
            )
        ).json()["execution_id"]
        async with db.tx() as connection:
            await connection.execute(
                "UPDATE executions SET state='COMPLETED' WHERE id=%s", (identifier,)
            )
        response = await client.get(f"/workflows/{identifier}/results")
        assert response.status_code == 500
        assert response.json()["error"]["code"] == "MISSING_RESULT"


async def test_submission_key_foreign_key_has_index(db: Database) -> None:
    async with db.tx() as connection:
        indexes = await (
            await connection.execute(
                "SELECT indexdef FROM pg_indexes WHERE tablename='submission_keys'"
            )
        ).fetchall()
    assert any("(execution_id)" in row["indexdef"] for row in indexes)
