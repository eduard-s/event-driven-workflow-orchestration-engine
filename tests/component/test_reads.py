"""Direct status and result reads with real PostgreSQL and bounded HTTP work."""

import asyncio
from collections.abc import AsyncGenerator
from typing import Any
from uuid import uuid4

import httpx
import pytest

from api import storage
from shared.database import Database
from tests.component.test_api import client_for, payload
from tests.settings import Settings

pytestmark = pytest.mark.component


@pytest.fixture
async def read_client(db: Database, settings: Settings) -> AsyncGenerator[Any]:
    async with client_for(
        settings.model_copy(update={"redis_url": "redis://127.0.0.1:1/0"})
    ) as pair:
        yield pair


async def submit(client: httpx.AsyncClient, *, trigger: bool = True) -> str:
    response = await client.post(
        "/workflow", json=payload(), headers={"Idempotency-Key": str(uuid4())}
    )
    assert response.status_code == 201, response.text
    identifier = response.json()["execution_id"]
    if trigger:
        response = await client.post(f"/workflow/trigger/{identifier}")
        assert response.status_code == 202, response.text
    return identifier


async def test_status_reads_latest_committed_state(read_client: Any, db: Database) -> None:
    client, app = read_client
    identifier = await submit(client)
    path = f"/workflows/{identifier}"
    assert (await client.get(path)).json()["has_failure"] is False
    async with db.tx() as connection:
        await connection.execute(
            "UPDATE executions SET has_failure=true WHERE id=%s", (identifier,)
        )
    assert (await client.get(path)).json()["has_failure"] is True
    assert not hasattr(app.state, "broker")


async def test_status_sees_trigger_from_another_api(read_client: Any, settings: Settings) -> None:
    client, _app = read_client
    identifier = await submit(client, trigger=False)
    path = f"/workflows/{identifier}"
    assert (await client.get(path)).json()["state"] == "PENDING"
    async with client_for(settings) as (other, _other_app):
        assert (await other.post(f"/workflow/trigger/{identifier}")).status_code == 202
    assert (await client.get(path)).json()["state"] == "RUNNING"


async def test_reads_return_full_json_without_etags(read_client: Any) -> None:
    client, _app = read_client
    identifier = await submit(client)
    for suffix, code in (("", 200), ("/results", 409)):
        response = await client.get(
            f"/workflows/{identifier}{suffix}", headers={"If-None-Match": "*"}
        )
        assert response.status_code == code
        assert response.json()["execution_id"] == identifier
        assert response.headers["cache-control"] == "no-store"
        assert "etag" not in response.headers


async def test_deleted_execution_disappears_immediately(read_client: Any, db: Database) -> None:
    client, _app = read_client
    identifier = await submit(client)
    assert (await client.get(f"/workflows/{identifier}")).status_code == 200
    async with db.tx() as connection:
        await connection.execute("DELETE FROM executions WHERE id=%s", (identifier,))
    for suffix in ("", "/results"):
        response = await client.get(f"/workflows/{identifier}{suffix}")
        assert response.status_code == 404
        assert response.json()["error"]["code"] == "NOT_FOUND"


async def test_status_pagination(read_client: Any) -> None:
    client, _app = read_client
    definition = {
        "name": "pages",
        "dag": {
            "nodes": [
                {"id": name, "handler": "input", "dependencies": []} for name in ("c", "a", "b")
            ]
        },
    }
    submitted = await client.post(
        "/workflow", json=definition, headers={"Idempotency-Key": str(uuid4())}
    )
    assert submitted.status_code == 201
    path = f"/workflows/{submitted.json()['execution_id']}"
    page = await client.get(path + "?offset=1&limit=1")
    assert page.status_code == 200
    assert [node["id"] for node in page.json()["nodes"]] == ["b"]
    assert page.json()["total_nodes"] == 3
    assert page.json()["offset"] == page.json()["limit"] == 1
    assert (await client.get(path + "?offset=3")).json()["nodes"] == []


@pytest.mark.parametrize("occupied_route", ["status", "results"])
async def test_status_and_results_share_two_slots_without_blocking_submission(
    read_client: Any, monkeypatch: pytest.MonkeyPatch, occupied_route: str
) -> None:
    client, _app = read_client
    identifiers = [await submit(client) for _ in range(3)]
    original = getattr(storage, occupied_route)
    both_entered, release = asyncio.Event(), asyncio.Event()
    active = 0

    async def hold(*args: Any, **kwargs: Any):
        nonlocal active
        active += 1
        if active == 2:
            both_entered.set()
        await release.wait()
        return await original(*args, **kwargs)

    monkeypatch.setattr(storage, occupied_route, hold)
    suffix = "" if occupied_route == "status" else "/results"
    requests = [
        asyncio.create_task(client.get(f"/workflows/{identifier}{suffix}"))
        for identifier in identifiers[:2]
    ]
    try:
        await asyncio.wait_for(both_entered.wait(), 2)
        for other_suffix in ("", "/results"):
            limited = await client.get(f"/workflows/{identifiers[2]}{other_suffix}")
            assert limited.status_code == 503, limited.text
            assert limited.json()["error"]["code"] == "OVERLOADED"
            assert limited.headers["retry-after"] == "1"
        assert active == 2
        assert await submit(client, trigger=False)
    finally:
        release.set()
    responses = await asyncio.gather(*requests)
    assert all(response.status_code == (200 if suffix == "" else 409) for response in responses)
    assert (await client.get(f"/workflows/{identifiers[2]}")).status_code == 200


async def test_cancelled_reads_release_slots(
    read_client: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    client, _app = read_client
    identifier = await submit(client)
    original = storage.status
    entered = asyncio.Event()
    active = 0

    async def hold(*args: Any, **kwargs: Any):
        nonlocal active
        active += 1
        if active == 2:
            entered.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(storage, "status", hold)
    requests = [asyncio.create_task(client.get(f"/workflows/{identifier}")) for _ in range(2)]
    try:
        await asyncio.wait_for(entered.wait(), 2)
    finally:
        for request in requests:
            request.cancel()
        outcomes = await asyncio.gather(*requests, return_exceptions=True)
    assert all(isinstance(outcome, asyncio.CancelledError) for outcome in outcomes)
    monkeypatch.setattr(storage, "status", original)
    assert (await client.get(f"/workflows/{identifier}")).status_code == 200
