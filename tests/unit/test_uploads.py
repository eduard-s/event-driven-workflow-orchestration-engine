"""Uploads stay within byte and time limits before JSON parsing."""

import asyncio
from unittest.mock import AsyncMock, Mock

import httpx
import pytest

from api.app import create_app
from api.submissions import Acceptance
from tests.settings import Settings


async def test_large_content_length_rejects_before_upload_read() -> None:
    app = create_app(Settings(max_body_bytes=1024))

    async def forbidden():
        raise AssertionError("The API must reject the length before body reads")
        yield b""

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app), base_url="http://test"
    ) as client:
        response = await client.post(
            "/workflow", content=forbidden(), headers={"Content-Length": "1025"}
        )
    assert response.status_code == 413
    assert response.json()["error"]["code"] == "BODY_TOO_LARGE"


async def test_full_request_capacity_rejects_before_upload_read() -> None:
    app = create_app()
    app.state.request_slots = asyncio.Semaphore(0)

    async def forbidden():
        raise AssertionError("Overloaded requests must not consume a body")
        yield b""

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app), base_url="http://test"
    ) as client:
        response = await client.post("/workflow", content=forbidden())
    assert response.status_code == 503
    assert response.json()["error"]["code"] == "OVERLOADED"


@pytest.mark.parametrize("length", [None, "1"])
async def test_stream_limit_covers_missing_and_false_content_lengths(
    length: str | None, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = create_app(Settings(max_body_bytes=1024))
    transaction = Mock(side_effect=AssertionError("Oversized bodies must not reach storage"))
    monkeypatch.setattr(app.state.db, "tx", transaction)
    produced = []

    async def chunks():
        for index in range(4):
            produced.append(index)
            yield b"secret-request-data" * 25

    headers = {"Content-Type": "application/json"}
    if length is not None:
        headers["Content-Length"] = length
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app), base_url="http://test"
    ) as client:
        response = await client.post("/workflow", content=chunks(), headers=headers)
    assert response.status_code == 413
    assert produced == [0, 1, 2]
    assert "secret-request-data" not in response.text
    transaction.assert_not_called()


async def test_body_at_exact_limit_keeps_typed_fastapi_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = create_app(Settings(max_body_bytes=1024))
    submit = AsyncMock(return_value=Acceptance("execution"))
    monkeypatch.setattr("api.submissions.submit", submit)
    raw = b'{"name":"x","dag":{"nodes":[{"id":"a","handler":"input","dependencies":[]}]}}'
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app), base_url="http://test"
    ) as client:
        response = await client.post(
            "/workflow",
            content=raw + b" " * (1024 - len(raw)),
            headers={"Content-Type": "application/json", "Idempotency-Key": "exact-limit"},
        )
    assert response.status_code == 201
    assert submit.await_args.args[1].dag.nodes[0].id == "a"


async def test_stalled_upload_times_out_and_releases_request_slot() -> None:
    app = create_app(Settings(body_timeout=0.02))
    app.state.request_slots = asyncio.Semaphore(1)

    async def stalled():
        yield b"{"
        await asyncio.Event().wait()

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app), base_url="http://test"
    ) as client:
        async with asyncio.timeout(1):
            response = await client.post("/workflow", content=stalled())
        assert response.status_code == 408
        assert response.json()["error"]["code"] == "UPLOAD_TIMEOUT"
        assert (await client.get("/workflows/invalid")).status_code == 422


async def test_cancelled_upload_releases_request_slot() -> None:
    app = create_app()
    app.state.request_slots = asyncio.Semaphore(1)
    entered = asyncio.Event()

    async def stalled():
        entered.set()
        yield b"{"
        await asyncio.Event().wait()

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app), base_url="http://test"
    ) as client:
        request = asyncio.create_task(client.post("/workflow", content=stalled()))
        await asyncio.wait_for(entered.wait(), timeout=1)
        request.cancel()
        with pytest.raises(asyncio.CancelledError):
            await request
        assert (await client.get("/workflows/invalid")).status_code == 422


async def test_slow_uploads_leave_read_capacity_and_release_body_slots() -> None:
    app = create_app(Settings(body_concurrency=1))
    app.state.request_slots = asyncio.Semaphore(2)
    entered = asyncio.Event()

    async def blocked():
        entered.set()
        yield b"{"
        await asyncio.Event().wait()

    async def forbidden():
        raise AssertionError("A full body gate must reject before upload reads")
        yield b""

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app), base_url="http://test"
    ) as client:
        first = asyncio.create_task(client.post("/workflow", content=blocked()))
        await asyncio.wait_for(entered.wait(), timeout=1)
        try:
            response = await client.post("/workflow", content=forbidden())
            assert response.status_code == 503
            assert response.json()["error"]["code"] == "OVERLOADED"
            assert (await client.get("/workflows/invalid")).status_code == 422
        finally:
            first.cancel()
            with pytest.raises(asyncio.CancelledError):
                await first
        assert (await client.post("/workflow", json={})).status_code == 422


@pytest.mark.parametrize("length", ["invalid", "-1"])
async def test_invalid_content_length_has_a_generic_error(length: str) -> None:
    app = create_app()
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app), base_url="http://test"
    ) as client:
        response = await client.post("/workflow", headers={"Content-Length": length})
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "INVALID_LENGTH"
