"""Audit retention deletes bounded batches without HTTP or database-lock waits."""

import asyncio

import pytest

from api import config, storage
from shared.database import Database
from tests.component.test_api import client_for
from tests.component.test_audit import audit_db as audit_db
from tests.component.test_audit import records
from tests.settings import Settings

pytestmark = pytest.mark.component


async def seed_audit(database: Database, count: int, age: int) -> None:
    async with database.tx() as connection:
        await connection.execute(
            """INSERT INTO request_audit(request_id,created_at,method,route,status,outcome,reason)
            SELECT gen_random_uuid(),clock_timestamp()-(%s*interval '1 day'),
            'POST','/workflow',422,'rejected','INVALID_REQUEST' FROM generate_series(1,%s)""",
            (age, count),
        )


async def test_retention_deletes_only_one_bounded_expired_batch(
    audit_db: Database, settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(config, "AUDIT_PRUNE_BATCH", 2)
    await seed_audit(audit_db, 3, 8)
    await seed_audit(audit_db, 1, 0)
    async with storage.audit_pool(settings.audit_postgres_dsn) as pool:
        assert await storage.prune_audit(pool) == 2
        assert len(await records(audit_db)) == 2
        assert await storage.prune_audit(pool) == 1
    remaining = await records(audit_db)
    assert len(remaining) == 1


async def test_retention_skips_rows_locked_by_another_api(
    audit_db: Database, settings: Settings
) -> None:
    await seed_audit(audit_db, 2, 8)
    async with storage.audit_pool(settings.audit_postgres_dsn) as pool:
        async with audit_db.tx() as connection:
            await connection.execute("SELECT request_id FROM request_audit FOR UPDATE")
            async with asyncio.timeout(1):
                assert await storage.prune_audit(pool) == 0
        assert await storage.prune_audit(pool) == 2


async def test_retention_runs_without_an_http_request(
    audit_db: Database, settings: Settings
) -> None:
    await seed_audit(audit_db, 1, 8)
    async with client_for(settings):
        async with asyncio.timeout(2):
            while await records(audit_db):
                await asyncio.sleep(0.01)


async def test_retention_outage_is_deferred(settings: Settings) -> None:
    pool = storage.audit_pool(settings.audit_postgres_dsn)
    assert await storage.prune_audit(pool) == 0
