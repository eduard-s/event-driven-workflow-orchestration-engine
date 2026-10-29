import asyncio
import os
import time
from pathlib import Path

import pytest

from api.context import Context
from shared.cpu import BoundedProcessPool
from shared.database import Database
from shared.errors import DomainError
from tests.cpu_helpers import crash, invalid, prepare_paused, rejected, wait_for_release
from tests.settings import Settings


async def wait_for_marker(path: Path) -> None:
    async with asyncio.timeout(5):
        while not path.exists():
            await asyncio.sleep(0.005)


async def test_canceled_request_keeps_capacity_until_process_finishes(tmp_path: Path) -> None:
    pool = BoundedProcessPool(workers=1, capacity=1)
    entered, release = tmp_path / "entered", tmp_path / "release"
    first = asyncio.create_task(pool.run(wait_for_release, str(entered), str(release)))
    try:
        await wait_for_marker(entered)
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first
        with pytest.raises(DomainError) as failure:
            await pool.run(abs, -9)
        assert failure.value.status == 503 and failure.value.code == "OVERLOADED"
        release.touch()
        async with asyncio.timeout(3):
            await pool.slots.acquire()
        pool.slots.release()
        assert await pool.run(abs, -9) == 9
    finally:
        release.touch()
        await pool.close()


async def test_failure_releases_capacity_and_domain_error_crosses_process() -> None:
    pool = BoundedProcessPool(workers=1, capacity=1)
    try:
        with pytest.raises(ValueError, match="Invalid fixture"):
            await pool.run(invalid)
        with pytest.raises(DomainError) as failure:
            await pool.run(rejected)
        assert vars(failure.value) == {
            "status": 422,
            "code": "INVALID_FIXTURE",
            "message": "Invalid fixture",
            "path": "dag.nodes",
        }
        assert await pool.run(abs, -3) == 3
    finally:
        await pool.close()


async def test_broken_pool_recovers_without_lost_capacity() -> None:
    pool = BoundedProcessPool(workers=1, capacity=1)
    try:
        with pytest.raises(DomainError) as failure:
            await pool.run(crash)
        assert failure.value.code == "CPU_UNAVAILABLE"
        assert await pool.run(os.getpid) != os.getpid()
        assert await pool.run(abs, -3) == 3
    finally:
        await pool.close()


async def test_broken_pool_releases_all_submitted_jobs(tmp_path: Path) -> None:
    pool = BoundedProcessPool(workers=1, capacity=4)
    entered, release = tmp_path / "entered", tmp_path / "release"
    first = asyncio.create_task(pool.run(prepare_paused, str(entered), str(release), crash))
    try:
        await wait_for_marker(entered)
        remaining = [asyncio.create_task(pool.run(abs, -value)) for value in (1, 2, 3)]
        async with asyncio.timeout(2):
            while len(pool._pending) < 4:
                await asyncio.sleep(0.005)
        release.touch()
        results = await asyncio.gather(first, *remaining, return_exceptions=True)
        assert all(isinstance(result, DomainError) for result in results)
        assert await pool.run(abs, -3) == 3
    finally:
        release.touch()
        await pool.close()


async def test_pool_start_failure_releases_capacity(monkeypatch: pytest.MonkeyPatch) -> None:
    pool = BoundedProcessPool(workers=1, capacity=1)

    def unavailable():
        raise OSError("Process resources are unavailable")

    try:
        with monkeypatch.context() as patch:
            patch.setattr(pool, "_pool", unavailable)
            with pytest.raises(DomainError) as failure:
                await pool.run(abs, -3)
            assert failure.value.code == "CPU_UNAVAILABLE"
        assert await pool.run(abs, -3) == 3
    finally:
        await pool.close()


async def test_shutdown_terminates_unfinished_work_and_rejects_new_work(tmp_path: Path) -> None:
    pool = BoundedProcessPool(workers=1, capacity=1, shutdown_seconds=0.05)
    child_pid = await pool.run(os.getpid)
    entered, release = tmp_path / "entered", tmp_path / "release"
    pending = asyncio.create_task(pool.run(wait_for_release, str(entered), str(release)))
    await wait_for_marker(entered)
    started = time.monotonic()
    await pool.close()
    assert time.monotonic() - started < 2
    with pytest.raises(DomainError) as failure:
        await pending
    assert failure.value.code == "CPU_UNAVAILABLE"
    async with asyncio.timeout(2):
        while True:
            try:
                os.kill(child_pid, 0)
            except ProcessLookupError:
                break
            await asyncio.sleep(0.005)
    with pytest.raises(DomainError):
        await pool.run(abs, -3)
    await pool.close()


async def test_contexts_share_one_lazy_pool_and_database_closes_it() -> None:
    settings = Settings()
    db = Database(settings)
    first, second = Context(db, settings), Context(db, settings)
    assert first.validation is second.validation
    assert first.validation._executor is None
    assert await first.validation.run(abs, -3) == 3
    await db.close()
    with pytest.raises(DomainError):
        await second.validation.run(abs, -3)


async def test_canceled_close_continues_and_a_second_close_waits(tmp_path: Path) -> None:
    pool = BoundedProcessPool(workers=1, capacity=1)
    entered, release = tmp_path / "entered", tmp_path / "release"
    pending = asyncio.create_task(pool.run(wait_for_release, str(entered), str(release)))
    try:
        await wait_for_marker(entered)
        closing = asyncio.create_task(pool.close())
        await asyncio.sleep(0)
        closing.cancel()
        with pytest.raises(asyncio.CancelledError):
            await closing
        release.touch()
        await pool.close()
        assert await pending == 7
    finally:
        release.touch()
        await pool.close()


async def test_database_closes_every_resource_after_one_cleanup_fails() -> None:
    db = Database(Settings())
    closed = []

    async def close_first() -> None:
        closed.append("first")

    async def close_failing() -> None:
        closed.append("failing")
        raise RuntimeError("cleanup failed")

    db.add_cleanup(close_first)
    db.add_cleanup(close_failing)
    with pytest.raises(RuntimeError, match="cleanup failed"):
        await db.close()
    assert closed == ["failing", "first"]
    await db.close()
