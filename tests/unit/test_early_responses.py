"""Early HTTP/1 upload rejections close connections without upload reads."""

import asyncio
from unittest.mock import AsyncMock

import httpx
import pytest
from fastapi import FastAPI, Request

from api.app import create_app
from api.http import domain_error
from shared.errors import DomainError
from tests.settings import Settings


@pytest.mark.parametrize(
    ("version", "method", "complete", "connection"),
    [
        ("1.0", "POST", False, "close"),
        ("1.1", "PUT", False, "close"),
        ("1.1", "PATCH", False, "close"),
        ("1.1", "GET", False, None),
        ("1.1", "POST", True, None),
        ("2", "POST", False, None),
        ("3", "POST", False, None),
    ],
)
async def test_connection_close_is_limited_to_unread_http1_uploads(
    version: str, method: str, complete: bool, connection: str | None
) -> None:
    request = Request(
        {
            "type": "http",
            "method": method,
            "path": "/workflow",
            "headers": [],
            "http_version": version,
            "app": FastAPI(),
            "state": {"body_read_complete": complete},
        }
    )
    response = await domain_error(request, DomainError(503, "OVERLOADED", "Capacity is full."))
    assert response.headers.get("Connection") == connection
    assert response.headers["Retry-After"] == "1"


@pytest.mark.parametrize("full", [False, True])
async def test_early_rejections_close_without_upload_reads(full: bool) -> None:
    app = create_app(Settings(requests_per_second=1))

    async def forbidden():
        raise AssertionError("Rejected uploads must not reach the body reader")
        yield b""

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app), base_url="http://test"
    ) as client:
        if full:
            app.state.request_slots = asyncio.Semaphore(0)
        else:
            assert (await client.get("/workflows/invalid")).status_code == 422
        response = await client.post("/workflow", content=forbidden())
    assert response.status_code == (503 if full else 429)
    assert response.headers["Connection"] == "close"


async def test_completed_upload_rejection_keeps_keepalive(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = create_app()
    submit = AsyncMock(side_effect=DomainError(503, "EXECUTION_CAPACITY", "Storage is full."))
    monkeypatch.setattr("api.submissions.submit", submit)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app), base_url="http://test"
    ) as client:
        response = await client.post(
            "/workflow",
            json={
                "name": "valid",
                "dag": {"nodes": [{"id": "a", "handler": "input", "dependencies": []}]},
            },
            headers={"Idempotency-Key": "capacity"},
        )
    assert response.status_code == 503
    assert "Connection" not in response.headers
    submit.assert_awaited_once()
