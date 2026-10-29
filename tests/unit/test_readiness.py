"""Readiness, request IDs, and pagination protect workflow requests."""

import asyncio
from unittest.mock import AsyncMock
from uuid import UUID

import httpx
import psycopg
import pytest
from pydantic import ValidationError

from api.app import create_app
from tests.settings import Settings


@pytest.mark.parametrize("offset", ["-1", "501", "9" * 100])
async def test_status_offset_bounds_precede_database_access(offset: str) -> None:
    app = create_app()
    transaction = AsyncMock(side_effect=AssertionError("Invalid offsets must not read storage"))
    app.state.db.tx = transaction
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app), base_url="http://test"
    ) as client:
        response = await client.get(f"/workflows/{'0' * 32}?offset={offset}")
    assert response.status_code == 422
    transaction.assert_not_called()


async def test_request_ids_are_generated_and_match_rejection_audit() -> None:
    app = create_app()
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app), base_url="http://test"
    ) as client:
        first = await client.get(
            "/workflows/invalid", headers={"X-Request-ID": "untrusted-client-id"}
        )
        second = await client.get("/health/live")
    identifier = UUID(first.headers["X-Request-ID"])
    assert identifier != UUID(second.headers["X-Request-ID"])
    assert app.state.audit_queue.get_nowait()["request_id"] == identifier


def test_configuration_errors_hide_raw_values() -> None:
    with pytest.raises(ValidationError) as failure:
        Settings(body_timeout="invalid-private-config-value")
    assert "invalid-private-config-value" not in str(failure.value)


@pytest.mark.parametrize("healthy", [True, False])
async def test_readiness_examines_postgres_without_broker_access(healthy: bool) -> None:
    app = create_app(Settings(redis_url="redis://unavailable:1"))
    app.state.db.healthy = AsyncMock(return_value=healthy)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app), base_url="http://test"
    ) as client:
        response = await client.get("/health/ready")
    assert response.status_code == (200 if healthy else 503)
    app.state.db.healthy.assert_awaited_once()


@pytest.mark.parametrize("error", [TimeoutError(), psycopg.OperationalError("database-password")])
async def test_readiness_dependency_errors_return_safe_503(error: Exception) -> None:
    app = create_app()
    app.state.db.healthy = AsyncMock(side_effect=error)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app), base_url="http://test"
    ) as client:
        response = await client.get("/health/ready")
        assert (await client.get("/health/live")).status_code == 200
    assert response.status_code == 503
    assert "database-password" not in response.text


async def test_readiness_deadline_cancels_a_stalled_dependency(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = create_app()
    cancelled = asyncio.Event()

    async def blocked():
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    app.state.db.healthy = AsyncMock(side_effect=blocked)
    monkeypatch.setattr("api.routes.READINESS_TIMEOUT_SECONDS", 0.02)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app), base_url="http://test"
    ) as client:
        async with asyncio.timeout(1):
            response = await client.get("/health/ready")
    assert response.status_code == 503
    assert cancelled.is_set()
