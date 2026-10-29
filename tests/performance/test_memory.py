"""Bounded memory regression test. A finite run cannot prove the absence of leaks."""

import asyncio
import inspect
import json
import time

import httpx
import pytest

from api.config import READ_CONCURRENCY

from .conftest import Stack, command, compose, record

pytestmark = pytest.mark.performance


def _python_command(args: list[bytes]) -> tuple[bytes, bytes] | None:
    if not args or not args[0].rsplit(b"/", 1)[-1].startswith(b"python"):
        return None
    index = 1
    while index < len(args):
        argument = args[index]
        if argument in (b"-m", b"-c"):
            if index + 1 >= len(args):
                return None
            return argument, args[index + 1]
        if argument in (b"-W", b"-X"):
            index += 2
        elif argument.startswith(b"-") and argument != b"--":
            index += 1
        else:
            return None
    return None


def process_kind(args: list[bytes]) -> str | None:
    command = _python_command(args)
    if command is None:
        return None
    mode, program = command
    if mode == b"-m":
        if program == b"worker.handlers":
            return "handler"
        return "service" if program in (b"api", b"orchestrator", b"worker") else None
    if program.startswith(b"from multiprocessing.spawn import "):
        return "cpu_pool"
    if program.startswith(b"from multiprocessing.resource_tracker import "):
        return "resource_tracker"
    return None


# Include persistent services, CPU pools, and resource trackers. Exclude temporary health probes.
PROCESS_MEMORY = (
    inspect.getsource(_python_command)
    + inspect.getsource(process_kind)
    + """
import json
from pathlib import Path
processes = []
children = 0
for path in Path('/proc').glob('[0-9]*'):
    try:
        args = (path / 'cmdline').read_bytes().split(b'\\0')
        kind = process_kind(args)
        if kind is None:
            continue
        if kind == 'handler':
            children += 1
            continue
        rss = next(line.split()[1] for line in (path / 'status').read_text().splitlines()
                   if line.startswith('VmRSS:'))
        process = {'pid': int(path.name), 'kind': kind, 'rss_mib': int(rss) / 1024}
        if kind == 'service':
            process['module'] = _python_command(args)[1].decode()
        processes.append(process)
    except (FileNotFoundError, ProcessLookupError):
        continue
print(json.dumps({'processes': processes, 'handler_children': children}))
"""
)


def memory(containers: list[str]) -> dict[str, dict]:
    values = {}
    for container in containers:
        state = json.loads(command("docker", "exec", container, "python", "-c", PROCESS_MEMORY))
        processes = state["processes"]
        parents = [process for process in processes if process["kind"] == "service"]
        assert len(parents) == 1, state
        assert state["handler_children"] == 0, state
        if parents[0]["module"] in ("api", "worker"):
            kinds = {process["kind"] for process in processes}
            assert {"cpu_pool", "resource_tracker"} <= kinds, state
        values[container] = {
            "module": parents[0]["module"],
            "pid": parents[0]["pid"],
            "pids": sorted(process["pid"] for process in processes),
            "rss_mib": sum(process["rss_mib"] for process in processes),
        }
    return values


async def test_memory_plateau_after_repeated_work(stack: Stack) -> None:
    containers = [stack.container, *compose("ps", "--quiet", "orchestrator", "worker").splitlines()]
    blob = "x" * (64 * 1024)
    await stack.batch(64, blob)  # Warm pools, child supervision, JSON buffers, and result paths.
    samples = [await asyncio.to_thread(memory, containers)]
    started = time.perf_counter()
    async with httpx.AsyncClient(base_url=stack.url, timeout=10) as client:
        for epoch in range(10):
            identifiers = await stack.batch(32, blob)
            for offset in range(4):
                # Keep memory measurements within the API read admission limit.
                for index in range(0, 128, READ_CONCURRENCY):
                    responses = await asyncio.gather(
                        *(
                            client.get(f"/workflows/{identifier}?offset={offset}")
                            for identifier in stack.seed_ids[index : index + READ_CONCURRENCY]
                        )
                    )
                    failures = [
                        (response.status_code, response.text)
                        for response in responses
                        if response.status_code != 200
                    ]
                    assert not failures, failures
            stack.delete(identifiers)
            await asyncio.sleep(max(0, started + (epoch + 1) * 30 - time.perf_counter()))
            samples.append(await asyncio.to_thread(memory, containers))
    growth = {}
    for container in containers:
        series = [sample[container] for sample in samples]
        assert len({sample["pid"] for sample in series}) == 1, "A restart can hide memory growth"
        assert len({tuple(sample["pids"]) for sample in series}) == 1, (
            "A CPU pool restart can hide memory growth"
        )
        rss = [round(sample["rss_mib"], 2) for sample in series]
        allowance = 16 if container == stack.container else 12
        growth[container] = {
            "module": series[0]["module"],
            "pids": series[0]["pids"],
            "rss_mib": rss,
            "allowance_mib": allowance,
        }
        assert max(rss[1:]) - rss[0] <= allowance, growth[container]
        assert max(rss[-3:]) - min(rss[1:4]) <= allowance, growth[container]
    states = json.loads(command("docker", "inspect", *containers))
    assert all(state["RestartCount"] == 0 and not state["State"]["OOMKilled"] for state in states)
    record(
        "memory",
        duration_seconds=round(time.perf_counter() - started, 1),
        workflows=320,
        trigger_bytes=len(blob),
        status_requests=5120,
        processes=growth,
    )
