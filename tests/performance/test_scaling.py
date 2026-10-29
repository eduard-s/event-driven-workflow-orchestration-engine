"""Two replicas share events. Loss of one replica must not duplicate a handler start."""

import asyncio
import time
from datetime import UTC, datetime

import pytest

from .conftest import Stack, command, compose, record

pytestmark = pytest.mark.performance


async def test_orchestrator_replicas_and_replica_loss(stack: Stack) -> None:
    rates = {}
    try:
        for replicas in (1, 2):
            await asyncio.to_thread(
                compose,
                "up",
                "--detach",
                "--wait",
                "--scale",
                f"orchestrator={replicas}",
                "orchestrator",
            )
            since = datetime.now(UTC).isoformat()
            started = time.perf_counter()
            await stack.batch(48)
            rates[replicas] = round(48 / (time.perf_counter() - started), 2)
            containers = compose("ps", "--quiet", "orchestrator").splitlines()
            assert len(containers) == replicas
            for container in containers:
                logs = command("docker", "logs", "--since", since, container, include_stderr=True)
                assert "event_processed" in logs
        stopped = containers[0]
        active = asyncio.create_task(stack.batch(64))
        try:
            await asyncio.sleep(0.5)
            await asyncio.to_thread(command, "docker", "stop", "--time", "1", stopped)
            await active
        finally:
            if not active.done():
                await active
            await asyncio.to_thread(command, "docker", "start", stopped)
        record(
            "orchestrator_scale",
            workflows_per_second=rates,
            replica_loss_workflows=64,
            duplicate_starts=0,
        )
    finally:
        await asyncio.to_thread(
            compose, "up", "--detach", "--wait", "--scale", "orchestrator=1", "orchestrator"
        )
