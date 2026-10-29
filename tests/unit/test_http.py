import asyncio
import os
import subprocess
import sys
from collections.abc import AsyncGenerator
from typing import Any
from unittest.mock import Mock

import httpx
import pytest
from fastapi import FastAPI
from limits import RateLimitItemPerSecond
from limits.aio.storage import MemoryStorage
from limits.aio.strategies import MovingWindowRateLimiter

import api.http as http
from api.app import create_app
from tests.settings import Settings


@pytest.fixture(autouse=True)
async def close_application_resources(monkeypatch: pytest.MonkeyPatch) -> AsyncGenerator[None]:
    applications: list[FastAPI] = []
    original = create_app

    def create(settings=None):
        app = original(settings)
        applications.append(app)
        return app

    monkeypatch.setattr(sys.modules[__name__], "create_app", create)
    yield
    for app in applications:
        await app.state.db.close()


def test_import_does_not_read_settings() -> None:
    result = subprocess.run(
        [sys.executable, "-c", "from api.app import create_app"],
        env={**os.environ, "WF_REQUESTS_PER_SECOND": "invalid"},
        capture_output=True,
        text=True,
        timeout=5,
    )
    assert result.returncode == 0, result.stderr


async def test_full_audit_queue_preserves_rejection(caplog: pytest.LogCaptureFixture) -> None:
    app = create_app()
    app.state.audit_queue = asyncio.Queue(maxsize=1)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app), base_url="http://test"
    ) as client:
        for _ in range(3):
            response = await client.post("/workflow", json={})
            assert response.status_code == 422
    assert app.state.audit_queue.qsize() == 1
    assert "audit_queue_full" in caplog.text


@pytest.mark.parametrize("route", ["/workflow"])
@pytest.mark.parametrize(
    ("key", "code"),
    [
        ("", "INVALID_IDEMPOTENCY_KEY"),
        ("x" * 129, "INVALID_IDEMPOTENCY_KEY"),
    ],
)
async def test_provided_submission_key_is_valid_before_database_access(
    route: str, key: str | None, code: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = create_app()
    transaction = Mock(side_effect=AssertionError("Invalid keys must not reach the database"))
    monkeypatch.setattr(app.state.db, "tx", transaction)
    headers = {} if key is None else {"Idempotency-Key": key}
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app), base_url="http://test"
    ) as client:
        response = await client.post(
            route,
            headers=headers,
            json={
                "name": "key test",
                "dag": {"nodes": [{"id": "input", "handler": "input", "dependencies": []}]},
            },
        )
    assert response.status_code == 422
    assert response.json()["error"]["code"] == code
    transaction.assert_not_called()


@pytest.mark.parametrize("route", ["/workflow"])
@pytest.mark.parametrize(
    ("nodes", "code", "path"),
    [
        (
            [{"id": "a", "handler": "input", "dependencies": ["missing"]}],
            "MISSING_DEPENDENCY",
            "dag.nodes[0].dependencies",
        ),
        (
            [
                {"id": "a", "handler": "input", "dependencies": ["b"]},
                {"id": "b", "handler": "input", "dependencies": ["a"]},
            ],
            "GRAPH_CYCLE",
            "dag.nodes",
        ),
    ],
)
async def test_invalid_workflow_is_rejected_before_database_access(
    route: str, nodes: list[dict[str, Any]], code: str, path: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = create_app()
    transaction = Mock(side_effect=AssertionError("Invalid workflows must not reach the database"))
    monkeypatch.setattr(app.state.db, "tx", transaction)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app), base_url="http://test"
    ) as client:
        response = await client.post(
            route,
            json={"name": "invalid request", "dag": {"nodes": nodes}},
            headers={"Idempotency-Key": "unit-invalid-definition"},
        )
    assert response.status_code == 422
    assert response.json()["error"]["code"] == code
    assert response.json()["error"]["path"] == path
    transaction.assert_not_called()


async def test_request_limit_returns_overload_and_releases_capacity() -> None:
    app = FastAPI()
    app.state.request_slots = asyncio.Semaphore(1)
    app.state.rate_limiter = MovingWindowRateLimiter(MemoryStorage())
    app.state.rate_limit = RateLimitItemPerSecond(100)
    http.register_error_handlers(app)
    app.middleware("http")(http.limit_requests)
    entered, release = asyncio.Event(), asyncio.Event()

    @app.get("/")
    async def route() -> dict[str, bool]:
        entered.set()
        await release.wait()
        return {"ok": True}

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app), base_url="http://test"
    ) as client:
        first = asyncio.create_task(client.get("/"))
        await asyncio.wait_for(entered.wait(), 1)
        try:
            overloaded = await client.get("/")
            assert overloaded.status_code == 503
            assert overloaded.json()["error"]["code"] == "OVERLOADED"
            assert overloaded.headers["Retry-After"] == "1"
        finally:
            release.set()
            assert (await first).status_code == 200
        assert (await client.get("/")).status_code == 200


@pytest.mark.parametrize(
    ("raw", "location", "error_type"),
    [
        (b"{", ["body", 1], "json_invalid"),
        (b"{}", ["body", "name"], "missing"),
        (b'{"name":42,"dag":{"nodes":[]}}', ["body", "name"], "string_type"),
        (
            b'{"name":"x","dag":{"nodes":[{"id":"a","handler":"unknown","dependencies":[]}]}}',
            ["body", "dag", "nodes", 0],
            "union_tag_invalid",
        ),
        (
            b'{"name":"x","dag":{"nodes":[{"id":"a","handler":"llm","dependencies":[],"config":{"prompt":42}}]}}',
            ["body", "dag", "nodes", 0, "llm", "config", "prompt"],
            "string_type",
        ),
        (b'{"secret":NaN}', ["body", "secret"], "extra_forbidden"),
    ],
)
async def test_fastapi_rejects_invalid_bodies_before_storage(
    raw: bytes, location: list[str | int], error_type: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = create_app()
    transaction = Mock(side_effect=AssertionError("Invalid bodies must not reach the database"))
    monkeypatch.setattr(app.state.db, "tx", transaction)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app), base_url="http://test"
    ) as client:
        response = await client.post(
            "/workflow", content=raw, headers={"Content-Type": "application/json"}
        )
    assert response.status_code == 422
    errors = response.json()["detail"]
    assert any(error["loc"] == location and error["type"] == error_type for error in errors)
    assert all("input" not in error and "ctx" not in error for error in errors)
    transaction.assert_not_called()


@pytest.mark.parametrize(
    "value", [b"NaN", b"Infinity", b"-Infinity", b"1e9999", b'"\\ud800"', b'"\\u0000"']
)
async def test_invalid_json_values_never_reach_workflow_storage(
    value: bytes, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = create_app()
    transaction = Mock(side_effect=AssertionError("Invalid values must not reach the database"))
    monkeypatch.setattr(app.state.db, "tx", transaction)
    raw = (
        b'{"name":"x","dag":{"nodes":[{"id":"a","handler":"output","dependencies":[],"data":'
        + value
        + b"}]}}"
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app), base_url="http://test"
    ) as client:
        response = await client.post(
            "/workflow",
            content=raw,
            headers={"Content-Type": "application/json", "Idempotency-Key": "invalid-value"},
        )
    assert response.status_code == 422, response.text
    transaction.assert_not_called()


async def test_rate_limit_bounds_parallel_requests_and_recovers_after_expiry() -> None:
    app = create_app(Settings(requests_per_second=2))
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app), base_url="http://test"
    ) as client:
        responses = await asyncio.gather(*(client.get("/workflows/invalid") for _ in range(20)))
        assert sum(response.status_code == 422 for response in responses) == 2
        rejected = [response for response in responses if response.status_code == 429]
        assert len(rejected) == 18
        assert all(response.json()["error"]["code"] == "RATE_LIMITED" for response in rejected)
        assert all(response.headers["Retry-After"] == "1" for response in rejected)

        await asyncio.sleep(1.05)
        responses = await asyncio.gather(*(client.get("/workflows/invalid") for _ in range(3)))
        assert sorted(response.status_code for response in responses) == [422, 422, 429]


async def test_liveness_needs_no_database(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = create_app()
    transaction = Mock(side_effect=AssertionError("Liveness must not read the database"))
    monkeypatch.setattr(app.state.db, "tx", transaction)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app), base_url="http://test"
    ) as client:
        response = await client.get("/health/live")
        assert response.status_code == 200 and response.json() == {"state": "alive"}
    transaction.assert_not_called()


async def test_rate_limit_precedes_body_read_and_excludes_health() -> None:
    app = create_app(Settings(requests_per_second=1))
    body_read = False

    async def body():
        nonlocal body_read
        body_read = True
        yield b'{"invalid": true}'

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app), base_url="http://test"
    ) as client:
        assert (await client.get("/workflows/invalid")).status_code == 422
        for path in ("/workflow", "/workflow/trigger/invalid"):
            response = await client.post(
                path, content=body(), headers={"Content-Type": "application/json"}
            )
            assert response.status_code == 429
        assert not body_read
        assert (await client.get("/health/live")).status_code == 200
