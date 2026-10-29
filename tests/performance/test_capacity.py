"""Measure HTTP capacity separately from handler throughput."""

import asyncio
import json
import math
import os
import statistics
import time
from collections import Counter
from contextlib import AsyncExitStack
from typing import Any
from uuid import uuid4

import httpx
import pytest

from api.submissions import prepare_submission
from api.validation import Workflow
from shared.cpu import BoundedProcessPool

from .conftest import GRAPH, Stack, record

pytestmark = pytest.mark.performance
LARGE_GRAPH = {
    "name": "large capacity",
    "dag": {
        "nodes": [
            {
                "id": "output",
                "handler": "output",
                "dependencies": [],
                "data": "x" * (192 * 1024),
            }
        ]
    },
}


def percentile(values: list[float], fraction: float) -> float:
    return round(sorted(values)[math.ceil(len(values) * fraction) - 1] * 1000, 2)


def validation_body(case: str) -> bytes:
    if case in ("small", "chain500"):
        nodes = [
            {
                "id": f"n{i}",
                "handler": "llm",
                "dependencies": [f"n{i - 1}"] if i else [],
                "config": {"prompt": "{{ n0 }}" if i else "hello"},
            }
            for i in range(500 if case == "chain500" else 5)
        ]
    else:
        size = 192 * 1024 if case == "192KiB" else 1024 * 1024 - 200
        nodes = [{"id": "output", "handler": "output", "dependencies": [], "data": "x" * size}]
    return json.dumps({"name": "benchmark", "dag": {"nodes": nodes}}).encode()


async def measure_loop_gaps(gaps: list[float]) -> None:
    previous = time.perf_counter()
    while True:
        await asyncio.sleep(0.001)
        now = time.perf_counter()
        gaps.append(now - previous)
        previous = now


@pytest.mark.parametrize("case", ["small", "chain500", "192KiB", "near1MiB"])
@pytest.mark.parametrize("mode", ["inline", "thread", "process"])
async def test_validation_cost_and_event_loop_delay(case: str, mode: str) -> None:
    if os.environ.get("WF_RUN_PERFORMANCE") != "1":
        pytest.skip("Set WF_RUN_PERFORMANCE=1 for validation measurements")
    raw = validation_body(case)

    samples = []
    for _ in range(8):
        started = time.perf_counter()
        expected = prepared_definition(Workflow.model_validate(json.loads(raw)))
        samples.append(time.perf_counter() - started)
    gaps: list[float] = []

    pool = BoundedProcessPool()
    if mode == "process":
        await pool.run(prepared_definition, Workflow.model_validate(json.loads(raw)))

    async def request() -> dict[str, Any]:
        payload = Workflow.model_validate(json.loads(raw))
        if mode == "process":
            return await pool.run(prepared_definition, payload)
        if mode == "thread":
            return await asyncio.to_thread(prepared_definition, payload)
        return prepared_definition(payload)

    tick = asyncio.create_task(measure_loop_gaps(gaps))
    try:
        await asyncio.sleep(0.01)
        started = time.perf_counter()
        assert await asyncio.gather(*(request() for _ in range(4))) == [expected] * 4
        elapsed = time.perf_counter() - started
        await asyncio.sleep(0.01)
    finally:
        tick.cancel()
        await asyncio.gather(tick, return_exceptions=True)
        await pool.close()
    record(
        "validation",
        case=case,
        mode=mode,
        body_bytes=len(raw),
        median_ms=round(statistics.median(samples) * 1000, 2),
        batch_ms=round(elapsed * 1000, 2),
        max_loop_gap_ms=round(max(gaps) * 1000, 2),
    )


def prepared_definition(payload: Workflow) -> dict[str, Any]:
    return prepare_submission(payload)[1]


async def send(
    client: httpx.AsyncClient, stack: Stack, route: str, index: int
) -> tuple[httpx.Response, float]:
    started = time.perf_counter()
    if route.endswith("submit"):
        response = await client.post(
            "/workflow",
            json=GRAPH if route == "submit" else LARGE_GRAPH,
            headers={"Idempotency-Key": str(uuid4())},
        )
    else:
        identifier = stack.seed_ids[index % 128]
        suffix = "/results" if route == "results" else ""
        response = await client.get(f"/workflows/{identifier}{suffix}")
    return response, time.perf_counter() - started


async def burst(
    stack: Stack, route: str, concurrency: int
) -> tuple[list[tuple[httpx.Response, float]], float]:
    async with AsyncExitStack() as contexts:
        # One connection per lane avoids load-generator pool contention at high concurrency.
        clients = [
            await contexts.enter_async_context(
                httpx.AsyncClient(
                    base_url=stack.url,
                    timeout=30,
                    limits=httpx.Limits(max_connections=1, max_keepalive_connections=1),
                )
            )
            for _ in range(concurrency)
        ]
        await asyncio.gather(*(client.get("/health/live") for client in clients))

        async def lane(index: int, client: httpx.AsyncClient) -> list[tuple[httpx.Response, float]]:
            return [
                await send(client, stack, route, offset)
                for offset in range(index, 128, concurrency)
            ]

        started = time.perf_counter()
        groups = await asyncio.gather(
            *(lane(index, client) for index, client in enumerate(clients))
        )
        return [sample for group in groups for sample in group], time.perf_counter() - started


@pytest.mark.parametrize("concurrency", [1, 4, 8, 16, 32, 64, 128])
@pytest.mark.parametrize("route", ["status", "results", "submit", "large_submit"])
async def test_api_concurrency(stack: Stack, route: str, concurrency: int) -> None:
    statuses: Counter[int] = Counter()
    latency: list[float] = []
    created: list[str] = []
    elapsed = 0.0
    try:
        for _ in range(2):
            readings, duration = await burst(stack, route, concurrency)
            elapsed += duration
            for response, seconds in readings:
                latency.append(seconds)
                statuses[response.status_code] += 1
                assert response.status_code in (200, 201, 503), response.text
                if response.status_code == 201:
                    created.append(response.json()["execution_id"])
                elif response.status_code == 200:
                    assert response.json()["state"] == "COMPLETED"
                else:
                    assert response.headers["Retry-After"] == "1"
                    assert response.json()["error"]["code"] == "OVERLOADED", response.text
    finally:
        stack.delete(created)
    record(
        "api_concurrency",
        route=route,
        concurrency=concurrency,
        requests=len(latency),
        statuses=dict(statuses),
        rps=round(len(latency) / elapsed, 2),
        successful_rps=round((statuses[200] + statuses[201]) / elapsed, 2),
        p50_ms=percentile(latency, 0.5),
        p95_ms=percentile(latency, 0.95),
    )
    assert statuses[200] + statuses[201] > 0
    if concurrency <= 4:
        assert statuses[503] == 0, "The conservative baseline must complete without overload"


@pytest.mark.parametrize("route", ["status", "large_submit"])
@pytest.mark.parametrize("rate", [5, 10, 20])
async def test_default_rate_policy(stack: Stack, route: str, rate: int) -> None:
    url = os.environ["WF_TEST_BASE_URL"]
    duration = 10
    statuses: Counter[int] = Counter()
    latency: list[float] = []
    key = f"capacity-rate-{route}-{rate}-{time.time_ns()}"
    async with httpx.AsyncClient(
        base_url=url,
        timeout=10,
        limits=httpx.Limits(max_connections=128, max_keepalive_connections=128),
    ) as client:
        await asyncio.sleep(1.1)  # Let all timestamps from the previous rate window expire.
        started = time.perf_counter()

        async def request(index: int) -> None:
            await asyncio.sleep(max(0, started + index / rate - time.perf_counter()))
            before = time.perf_counter()
            if route == "large_submit":
                response = await client.post(
                    "/workflow", json=LARGE_GRAPH, headers={"Idempotency-Key": key}
                )
            else:
                response = await client.get(f"/workflows/{stack.seed_ids[index % 128]}")
            latency.append(time.perf_counter() - before)
            statuses[response.status_code] += 1
            assert response.status_code in (200, 201, 429), response.text
            if response.status_code == 201:
                stack.ids.append(response.json()["execution_id"])
            if response.status_code == 429:
                assert response.headers["Retry-After"] == "1"

        await asyncio.gather(*(request(index) for index in range(rate * duration)))
    record(
        "rate_policy",
        offered_rps=rate,
        route=route,
        duration_seconds=duration,
        statuses=dict(statuses),
        p95_ms=percentile(latency, 0.95),
    )
    if rate < 10:
        assert statuses[429] == 0
    elif rate == 10:
        # Arrival jitter can put more than ten requests in a rolling second.
        assert statuses[200] + statuses[201] >= rate * duration * 0.9
    else:
        assert statuses[429] > 0
        assert statuses[200] + statuses[201] <= 10 * (duration + 1)
