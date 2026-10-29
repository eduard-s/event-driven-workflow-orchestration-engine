import os
import shutil
import subprocess
from collections.abc import AsyncIterator, Iterator
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo

from shared.database import Database
from tests.settings import Settings


def initialize_databases(base: str, workflow: str, audit: str) -> tuple[str, str]:
    info = conninfo_to_dict(base)
    environment = os.environ.copy()
    for name in ("WF_POSTGRES_USER", "WF_POSTGRES_PASSWORD"):
        environment.pop(name, None)
    password = info.pop("password", None)
    if password is not None:
        environment["PGPASSWORD"] = password
    info["dbname"] = workflow
    result = subprocess.run(
        [
            "psql",
            "--no-psqlrc",
            "--dbname",
            make_conninfo(**info),
            "--set",
            "ON_ERROR_STOP=1",
            "--set",
            f"audit_db={audit}",
            "--file",
            str(Path(__file__).resolve().parents[1] / "db/init.sql"),
        ],
        env=environment,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr
    return (
        make_conninfo(base, dbname=workflow),
        make_conninfo(base, dbname=audit),
    )


def isolated_databases() -> Iterator[tuple[str, str]]:
    if shutil.which("psql") is None:
        pytest.fail("PostgreSQL client required. Use the Docker test command from README.")
    base = Settings().postgres_dsn
    workflow = "workflow_test_" + uuid4().hex[:12]
    audit = "workflow_audit_test_" + uuid4().hex[:12]
    try:
        with psycopg.connect(base, autocommit=True, connect_timeout=3) as conn:
            conn.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(workflow)))
    except psycopg.Error:
        if os.getenv("WF_REQUIRE_INFRA") == "1":
            raise
        pytest.skip("PostgreSQL test infrastructure is unavailable")
    try:
        yield initialize_databases(base, workflow, audit)
    finally:
        with psycopg.connect(base, autocommit=True, connect_timeout=3) as conn:
            for name in (workflow, audit):
                conn.execute(
                    sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(name))
                )


@pytest.fixture(scope="session")
def database_dsns() -> Iterator[tuple[str, str]]:
    yield from isolated_databases()


@pytest.fixture(scope="session")
def test_dsn(database_dsns: tuple[str, str]) -> str:
    return database_dsns[0]


@pytest.fixture(scope="session")
def audit_dsn(database_dsns: tuple[str, str]) -> str:
    return database_dsns[1]


@pytest.fixture
async def settings(test_dsn: str, audit_dsn: str) -> AsyncIterator[Settings]:
    unique = uuid4().hex
    configured = Settings(
        postgres_dsn=test_dsn,
        audit_postgres_dsn=audit_dsn,
        task_stream="test.tasks." + unique,
        event_stream="test.events." + unique,
        maintenance_interval=0.05,
        repair_interval=0.3,
        retry_base=0,
        retry_max=0,
        handler_timeout=3,
        execution_timeout=30,
        enable_test_fixtures=True,
        enable_repair=False,
        requests_per_second=1000,
    )

    yield configured
    from redis.asyncio import Redis

    client = Redis.from_url(configured.redis_url)
    try:
        keys = [key async for key in client.scan_iter(match=configured.task_stream + "*")]
        await client.delete(configured.event_stream, *keys)
    finally:
        await client.aclose()


@pytest.fixture
async def db(settings: Settings) -> AsyncIterator[Database]:
    db = Database(settings)
    await db.open()
    async with db.tx() as conn:
        await conn.execute("TRUNCATE executions CASCADE")
    yield db
    await db.close()
