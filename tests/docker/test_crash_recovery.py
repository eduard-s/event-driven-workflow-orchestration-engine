"""Opt-in destructive tests for the isolated local Compose deployment.

Run serially against an explicit isolated Compose project.
Tests stop services, remove broker data, and restore the test database from backup.
"""

from __future__ import annotations

import json
import os
import subprocess
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import httpx
import psycopg
import pytest
import redis
from psycopg.rows import dict_row

pytestmark = [
    pytest.mark.docker,
    pytest.mark.skipif(
        os.environ.get("WF_RUN_DOCKER_TESTS") != "1",
        reason="Set WF_RUN_DOCKER_TESTS=1 for the isolated Compose crash suite",
    ),
]

ROOT = Path(__file__).resolve().parents[2]
BASE_URL = os.environ.get("WF_TEST_BASE_URL", "http://localhost:18000")
POSTGRES_DSN = os.environ.get(
    "WF_TEST_POSTGRES_DSN",
    "postgresql://workflow:workflow@localhost:15432/workflow",
)
REDIS_URL = os.environ.get("WF_TEST_REDIS_URL", "redis://localhost:16379/0")
TASK_STREAM = "workflow.tasks"


def compose_command(*arguments: str) -> list[str]:
    return ["docker", "compose", "-f", str(ROOT / "infra/docker-compose.yml"), *arguments]


def compose(*arguments: str) -> str:
    result = subprocess.run(
        compose_command(*arguments),
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=90,
        check=True,
    )
    return result.stdout.strip()


def docker(*arguments: str) -> str:
    return subprocess.run(
        ["docker", *arguments],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=30,
        check=True,
    ).stdout.strip()


def container_logs(name: str) -> str:
    result = subprocess.run(
        ["docker", "logs", name],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=30,
        check=True,
    )
    return result.stdout + result.stderr


def one_off(service: str, name: str, environment: dict[str, str]) -> None:
    # Keep failpoint containers inspectable even when the normal service has a
    # restart policy. The override applies only to this Compose command.
    with tempfile.TemporaryDirectory(prefix="workflow-crash-") as directory:
        override = Path(directory) / "compose.json"
        override.write_text(json.dumps({"services": {service: {"restart": "no"}}}))
        command = [
            "docker",
            "compose",
            "-f",
            str(ROOT / "infra/docker-compose.yml"),
            "-f",
            str(override),
            "run",
            "--detach",
            "--no-deps",
            "--name",
            name,
        ]
        for key, value in environment.items():
            command.extend(["-e", f"{key}={value}"])
        subprocess.run(
            [*command, service],
            cwd=ROOT,
            capture_output=True,
            text=True,
            timeout=90,
            check=True,
        )


def eventually(predicate: Any, timeout: float = 45) -> Any:
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            result = predicate()
            if result:
                return result
        except (httpx.HTTPError, psycopg.Error, redis.RedisError) as exc:
            last_error = exc
        time.sleep(0.1)
    raise AssertionError(f"Condition did not complete within {timeout}s. Last error: {last_error}")


def backup_database(path: Path) -> None:
    with path.open("wb") as archive:
        subprocess.run(
            compose_command(
                "exec", "-T", "postgres", "pg_dump", "-U", "workflow", "-d", "workflow", "-Fc"
            ),
            stdout=archive,
            stderr=subprocess.PIPE,
            timeout=60,
            check=True,
        )
    assert path.stat().st_size > 0


def restore_database(path: Path) -> None:
    compose("exec", "-T", "postgres", "dropdb", "-U", "workflow", "--force", "workflow")
    compose("exec", "-T", "postgres", "createdb", "-U", "workflow", "workflow")
    with path.open("rb") as archive:
        subprocess.run(
            compose_command(
                "exec",
                "-T",
                "postgres",
                "pg_restore",
                "-U",
                "workflow",
                "-d",
                "workflow",
                "--exit-on-error",
            ),
            stdin=archive,
            capture_output=True,
            timeout=60,
            check=True,
        )


def ready() -> None:
    def available() -> bool:
        if httpx.get(f"{BASE_URL}/health/live", timeout=3).status_code != 200:
            return False
        return query("SELECT 1 AS ready") == [{"ready": 1}]

    eventually(available)


def query(sql: str, parameters: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    with psycopg.connect(POSTGRES_DSN, row_factory=dict_row, connect_timeout=3) as connection:
        return connection.execute(sql, parameters).fetchall()


def start_execution() -> str:
    response = httpx.post(
        f"{BASE_URL}/workflow",
        json={
            "name": "Docker crash test",
            "dag": {
                "nodes": [
                    {"id": "input", "handler": "input", "dependencies": []},
                ]
            },
        },
        headers={"Idempotency-Key": f"docker-{uuid4()}"},
        timeout=10,
    )
    response.raise_for_status()
    identifier = response.json()["execution_id"]
    response = httpx.post(
        f"{BASE_URL}/workflow/trigger/{identifier}", json={"input": 7}, timeout=10
    )
    assert response.status_code == 202, response.text
    return identifier


def completed(identifier: str) -> None:
    def outcome() -> bool:
        response = httpx.get(f"{BASE_URL}/workflows/{identifier}/results", timeout=3)
        if response.status_code == 200:
            assert response.json()["outputs"] == {"input": 7}
            return True
        return False

    eventually(outcome, timeout=60)


def published_task(identifier: str) -> dict[str, Any]:
    return eventually(
        lambda: next(
            iter(
                query(
                    "SELECT * FROM outbox WHERE execution_id=%s AND kind='task' AND published_id IS NOT NULL",
                    (identifier,),
                )
            ),
            None,
        )
    )


@pytest.fixture(autouse=True)
def running_stack() -> Any:
    project = os.environ.get("COMPOSE_PROJECT_NAME")
    if not project or project == "appliedai-workflow":
        pytest.fail("Recovery tests need an explicit isolated Compose project.")
    ready()
    yield
    # A failed assertion must not leave the local stack stopped.
    compose("start", "postgres", "redis")
    compose("start", "api")
    ready()
    compose("start", "orchestrator", "worker")


def test_worker_crash_after_claim_recovers_through_new_try() -> None:
    compose("stop", "worker")
    identifier = start_execution()
    published_task(identifier)
    name = f"wf-crash-{uuid4().hex[:10]}"
    one_off(
        "worker",
        name,
        {
            "WF_ENABLE_TEST_FIXTURES": "1",
            "WF_WORKER_FAILPOINT": "after_claim_before_call",
        },
    )
    try:
        eventually(lambda: docker("inspect", "--format", "{{.State.Status}}", name) == "exited")
        assert docker("inspect", "--format", "{{.State.ExitCode}}", name) == "86"
        first = eventually(
            lambda: (
                (rows if rows and rows[0]["token"] is not None else None)
                if (
                    rows := query(
                        "SELECT * FROM tries WHERE execution_id=%s ORDER BY try_number",
                        (identifier,),
                    )
                )
                else None
            )
        )
        assert first[0]["token"] is not None
        assert first[0]["runtime"] is None
    finally:
        docker("rm", "-f", name)
        compose("start", "worker")
    completed(identifier)
    tries = query("SELECT * FROM tries WHERE execution_id=%s ORDER BY try_number", (identifier,))
    assert len(tries) == 2
    assert tries[0]["error"]["code"] == "HANDLER_TIMEOUT"
    assert tries[1]["success"] is True and tries[1]["runtime"] is not None
    assert query("SELECT starts FROM nodes WHERE execution_id=%s", (identifier,))[0]["starts"] == 2


def test_worker_crash_after_result_publication_keeps_completed_work() -> None:
    compose("stop", "worker")
    identifier = start_execution()
    published_task(identifier)
    compose("stop", "orchestrator")
    name = f"wf-result-{uuid4().hex[:10]}"
    one_off(
        "worker",
        name,
        {"WF_ENABLE_TEST_FIXTURES": "1", "WF_WORKER_FAILPOINT": "after_result_publish"},
    )
    try:
        eventually(lambda: docker("inspect", "--format", "{{.State.Status}}", name) == "exited")
        assert docker("inspect", "--format", "{{.State.ExitCode}}", name) == "86"
        # The worker cannot change PostgreSQL while the orchestrator is offline.
        attempt = query("SELECT * FROM tries WHERE execution_id=%s", (identifier,))[0]
        assert attempt["token"] is None and not attempt["has_outcome"]
    finally:
        docker("rm", "-f", name)
        compose("start", "orchestrator", "worker")
    completed(identifier)
    assert query("SELECT starts FROM nodes WHERE execution_id=%s", (identifier,))[0]["starts"] == 1


def test_api_sigkill_preserves_acknowledged_execution() -> None:
    compose("stop", "worker")
    identifier = start_execution()
    compose("kill", "-s", "SIGKILL", "api")
    compose("start", "api")
    ready()
    assert httpx.get(f"{BASE_URL}/workflows/{identifier}", timeout=5).status_code == 200
    compose("start", "worker")
    completed(identifier)


def test_orchestrator_sigkill_keeps_api_available_and_recovers_work() -> None:
    compose("stop", "worker")
    identifier = start_execution()
    compose("kill", "-s", "SIGKILL", "orchestrator")
    assert httpx.get(f"{BASE_URL}/health/live", timeout=5).status_code == 200
    assert httpx.get(f"{BASE_URL}/workflows/{identifier}", timeout=5).status_code == 200
    compose("start", "orchestrator", "worker")
    completed(identifier)
    assert query("SELECT starts FROM nodes WHERE execution_id=%s", (identifier,))[0]["starts"] == 1


def test_redis_data_loss_reconstructs_notifications_from_postgres() -> None:
    compose("stop", "worker")
    identifier = start_execution()
    published_task(identifier)
    # The isolated Compose Redis owns broker data only. PostgreSQL is untouched.
    assert compose("exec", "-T", "redis", "redis-cli", "FLUSHDB") == "OK"
    assert query("SELECT state FROM executions WHERE id=%s", (identifier,))
    compose("start", "worker")
    completed(identifier)
    assert query("SELECT starts FROM nodes WHERE execution_id=%s", (identifier,))[0]["starts"] == 1


def test_postgres_restart_preserves_acknowledged_execution() -> None:
    compose("stop", "worker")
    identifier = start_execution()
    compose("restart", "postgres")
    ready()
    compose("start", "worker")
    completed(identifier)


def test_broker_outage_preserves_new_submissions_and_triggers() -> None:
    compose("stop", "redis")
    try:
        identifier = start_execution()
        execution = query(
            "SELECT state,trigger,accepted_at FROM executions WHERE id=%s", (identifier,)
        )[0]
        assert execution["state"] == "RUNNING"
        assert execution["trigger"] == {"input": 7, "node_params": {}} and execution["accepted_at"]
        events = query(
            "SELECT event_id FROM outbox WHERE execution_id=%s AND kind='start'", (identifier,)
        )
        assert len(events) == 1
        assert httpx.get(f"{BASE_URL}/workflows/{identifier}", timeout=5).status_code == 200
        assert (
            query("SELECT starts FROM nodes WHERE execution_id=%s", (identifier,))[0]["starts"] == 0
        )
    finally:
        compose("start", "redis")
    completed(identifier)
    assert query("SELECT starts FROM nodes WHERE execution_id=%s", (identifier,))[0]["starts"] == 1


def test_workers_run_and_report_while_postgres_and_orchestrator_are_offline() -> None:
    compose("stop", "worker")
    identifier = start_execution()
    event = published_task(identifier)
    client = redis.Redis.from_url(REDIS_URL)
    entries = client.xrange(TASK_STREAM, event["published_id"], event["published_id"])
    assert entries
    envelope = json.loads(entries[0][1][b"envelope"])
    from shared.broker import task_claim_key
    from worker.config import Settings

    key = task_claim_key(Settings(), envelope)
    compose("stop", "api", "orchestrator", "postgres")
    since = datetime.now(UTC).isoformat()
    try:
        compose("start", "worker")
        eventually(lambda: client.hget(key, "outcome"))
        # Duplicate deliveries during the database outage still cannot repeat the handler.
        for _ in range(8):
            client.xadd(TASK_STREAM, {"envelope": json.dumps(envelope)})
        eventually(lambda: client.xlen(TASK_STREAM) == 0)
        logs = compose("logs", "--since", since, "worker")
        assert logs.count(f"worker_task_started execution_id={identifier}") == 1
        # Redis persistence also protects the pending report across a broker restart.
        compose("restart", "redis")
        eventually(lambda: client.hget(key, "outcome"))
    finally:
        compose("start", "postgres", "api")
        ready()
        compose("start", "orchestrator")
        client.close()
    completed(identifier)
    assert query("SELECT starts FROM nodes WHERE execution_id=%s", (identifier,))[0]["starts"] == 1


def test_sigkill_with_live_handler_child_uses_new_budgeted_try() -> None:
    compose("stop", "worker")
    identifier = start_execution()
    published_task(identifier)
    name = f"wf-live-child-{uuid4().hex[:10]}"
    one_off(
        "worker",
        name,
        {
            "WF_ENABLE_TEST_FIXTURES": "1",
            "WF_TEST_FIXTURES_JSON": json.dumps({"input": {"delay": 5}}),
        },
    )
    try:
        eventually(lambda: "handler_process_spawned" in container_logs(name))
        processes = docker("top", name, "-eo", "pid,args")
        assert "worker.handlers" in processes
        docker("kill", "-s", "SIGKILL", name)
        eventually(lambda: docker("inspect", "--format", "{{.State.Status}}", name) == "exited")
        first = query(
            "SELECT * FROM tries WHERE execution_id=%s ORDER BY try_number", (identifier,)
        )
        assert first[0]["token"] is not None and first[0]["runtime"] is None
    finally:
        docker("rm", "-f", name)
        compose("start", "worker")
    completed(identifier)
    tries = query("SELECT * FROM tries WHERE execution_id=%s ORDER BY try_number", (identifier,))
    assert len(tries) == 2
    assert tries[0]["error"]["code"] == "HANDLER_TIMEOUT"
    assert tries[1]["success"] is True and tries[1]["runtime"] is not None


def test_publication_crash_before_record_preserves_one_logical_dispatch() -> None:
    compose("stop", "worker", "orchestrator")
    name = f"wf-publisher-{uuid4().hex[:10]}"
    one_off(
        "orchestrator",
        name,
        {
            "WF_ENABLE_TEST_FIXTURES": "1",
            "WF_ORCHESTRATOR_FAILPOINT": "after_publish_before_record",
        },
    )
    try:
        eventually(lambda: "orchestrator_ready" in container_logs(name))
        identifier = start_execution()
        eventually(lambda: docker("inspect", "--format", "{{.State.Status}}", name) == "exited")
        assert docker("inspect", "--format", "{{.State.ExitCode}}", name) == "81", container_logs(
            name
        )
        start_event = query(
            "SELECT * FROM outbox WHERE execution_id=%s AND kind='start'",
            (identifier,),
        )[0]
        assert start_event["published_id"] is None
    finally:
        docker("rm", "-f", name)
        compose("start", "orchestrator")
        ready()
        compose("start", "worker")
    completed(identifier)
    assert query("SELECT starts FROM nodes WHERE execution_id=%s", (identifier,))[0]["starts"] == 1
    assert (
        len(
            query(
                "SELECT event_id FROM outbox WHERE execution_id=%s AND kind='task'",
                (identifier,),
            )
        )
        == 1
    )


def test_database_backup_restore_recovers_accepted_work(tmp_path: Path) -> None:
    previous = start_execution()
    completed(previous)
    compose("stop", "orchestrator", "worker")
    pending = start_execution()
    compose("stop", "api")
    try:
        archive = tmp_path / "workflow.dump"
        backup_database(archive)
        restore_database(archive)
        assert compose("exec", "-T", "redis", "redis-cli", "FLUSHDB") == "OK"
    finally:
        compose("start", "postgres", "redis", "api")
        ready()
        compose("start", "orchestrator", "worker")
    completed(previous)
    completed(pending)
    for identifier in (previous, pending):
        assert (
            query("SELECT starts FROM nodes WHERE execution_id=%s", (identifier,))[0]["starts"] == 1
        )
