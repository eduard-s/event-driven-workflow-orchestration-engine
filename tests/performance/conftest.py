"""Host tests use an explicit isolated stack and a separate API without rate throttling."""

import asyncio
import json
import os
import subprocess
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any
from uuid import uuid4

import httpx
import psycopg
import pytest
from psycopg.rows import dict_row
from tenacity import (
    RetryError,
    Retrying,
    retry_if_exception_type,
    retry_if_result,
    stop_after_delay,
    wait_fixed,
)

ROOT = Path(__file__).resolve().parents[2]
GRAPH = {
    "name": "capacity",
    "dag": {
        "nodes": [
            {"id": "input", "handler": "input", "dependencies": []},
            {
                "id": "llm",
                "handler": "llm",
                "dependencies": ["input"],
                "config": {"prompt": "{{ input.blob }}"},
            },
            {"id": "output", "handler": "output", "dependencies": ["llm"], "data": "{{ llm }}"},
        ]
    },
}


def command(*arguments: str, include_stderr: bool = False) -> str:
    result = subprocess.run(arguments, cwd=ROOT, capture_output=True, text=True, timeout=90)
    assert result.returncode == 0, result.stderr
    return (result.stdout + (result.stderr if include_stderr else "")).strip()


def compose(*arguments: str) -> str:
    return command("docker", "compose", "-f", str(ROOT / "infra/docker-compose.yml"), *arguments)


def record(label: str, **values: Any) -> None:
    line = json.dumps({"test": label, **values}, sort_keys=True)
    print(line, flush=True)
    if path := os.environ.get("WF_PERFORMANCE_REPORT"):
        with Path(path).open("a") as report:
            report.write(line + "\n")


class Stack:
    def __init__(self, url: str, container: str) -> None:
        self.url = url
        self.container = container
        self.ids: list[str] = []
        self.seed_ids: list[str] = []
        self.dsn = os.environ["WF_TEST_POSTGRES_DSN"]

    def query(self, sql: str, parameters: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        with psycopg.connect(self.dsn, row_factory=dict_row) as connection:
            return connection.execute(sql, parameters).fetchall()

    def delete(self, identifiers: list[str]) -> None:
        # Remove only records that this test created. Never truncate a shared database.
        if identifiers:
            self.query(
                "DELETE FROM executions WHERE id=ANY(%s::uuid[]) RETURNING id", (identifiers,)
            )

    async def batch(self, count: int, blob: str = "hello") -> list[str]:
        slots = asyncio.Semaphore(8)
        async with httpx.AsyncClient(base_url=self.url, timeout=30) as client:

            async def start() -> str:
                async with slots:
                    response = await client.post(
                        "/workflow", json=GRAPH, headers={"Idempotency-Key": str(uuid4())}
                    )
                    assert response.status_code == 201, response.text
                    identifier = response.json()["execution_id"]
                    self.ids.append(identifier)
                    response = await client.post(
                        f"/workflow/trigger/{identifier}", json={"input": {"blob": blob}}
                    )
                    assert response.status_code == 202, response.text
                    return identifier

            identifiers = await asyncio.gather(*(start() for _ in range(count)))
            await asyncio.to_thread(self.wait, identifiers)
            for identifier in identifiers:
                response = await client.get(f"/workflows/{identifier}/results")
                assert response.status_code == 200, response.text
                assert response.json()["outputs"] == {"output": "mock: " + blob}
        return identifiers

    def wait(self, identifiers: list[str]) -> None:
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            rows = self.query(
                "SELECT state,count(*) AS n FROM executions WHERE id=ANY(%s::uuid[]) GROUP BY state",
                (identifiers,),
            )
            if rows == [{"state": "COMPLETED", "n": len(identifiers)}]:
                starts = self.query(
                    "SELECT count(*) AS n FROM nodes WHERE execution_id=ANY(%s::uuid[]) AND starts<>1",
                    (identifiers,),
                )
                assert starts[0]["n"] == 0
                return
            assert not any(row["state"] == "FAILED" for row in rows), rows
            time.sleep(0.1)
        pytest.fail(f"Work did not drain: {rows}")


def wait_for_capacity_api(url: str) -> None:
    try:
        Retrying(
            retry=retry_if_exception_type(httpx.HTTPError)
            | retry_if_result(lambda response: response.status_code != 200),
            stop=stop_after_delay(30),
            wait=wait_fixed(0.2),
        )(httpx.get, f"{url}/health/live", timeout=2)
    except RetryError as exc:
        raise AssertionError("Capacity API did not start") from exc


@pytest.fixture(scope="session")
def stack() -> Iterator[Stack]:
    if os.environ.get("WF_RUN_PERFORMANCE") != "1":
        pytest.skip("Set WF_RUN_PERFORMANCE=1 and use an isolated Compose project")
    project = os.environ.get("COMPOSE_PROJECT_NAME", "")
    assert project and project != "appliedai-workflow", "Use an explicit isolated test project"
    assert os.environ.get("WF_TEST_POSTGRES_DSN"), "Supply the isolated database DSN"
    port = int(os.environ.get("WF_CAPACITY_PORT", "28001"))
    name = f"wf-capacity-{uuid4().hex[:10]}"
    compose(
        "run",
        "--detach",
        "--no-deps",
        "--name",
        name,
        "--publish",
        f"127.0.0.1:{port}:8000",
        "-e",
        "WF_REQUESTS_PER_SECOND=10000",
        "-e",
        "WF_MAX_EXECUTIONS=20000",
        "-e",
        "WF_MAX_RECORDS=30000",
        "api",
    )
    state = Stack(f"http://localhost:{port}", name)
    try:
        wait_for_capacity_api(state.url)
        # Warm lazy validation processes before the concurrent workload measurement.
        for count in (1, 31, 32, 32, 32):
            state.seed_ids.extend(asyncio.run(state.batch(count)))
        info = json.loads(command("docker", "info", "--format", "{{json .}}"))
        record(
            "environment",
            cpus=info["NCPU"],
            memory_bytes=info["MemTotal"],
            docker_version=info["ServerVersion"],
            seeds=len(state.seed_ids),
        )
        yield state
    finally:
        command("docker", "rm", "--force", name)
        state.delete(state.ids)
