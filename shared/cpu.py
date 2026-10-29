# Run bounded CPU work without the service event loop or its GIL.

import asyncio
import multiprocessing
from collections.abc import Callable
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from dataclasses import dataclass
from typing import Any

from shared.errors import DomainError


@dataclass(frozen=True)
class _Rejected:
    # Keep a domain failure as plain data so the process boundary preserves its fields.
    #
    # Args:
    #     status: HTTP status carried by the original domain failure.
    #     code: Stable failure code.
    #     message: Safe explanation of the failure.
    #     path: Invalid input path, or None when no path applies.
    #
    # Returns:
    #     An immutable failure envelope.
    status: int
    code: str
    message: str
    path: str | None


def _execute[T](operation: Callable[..., T], args: tuple[Any, ...]) -> T | _Rejected:
    # Run one CPU operation and preserve domain failures across process serialization.
    #
    # Args:
    #     operation: Picklable callable for the child process.
    #     args: Positional values for the callable.
    #
    # Returns:
    #     The callable result, or a plain domain-failure envelope.
    #
    # Raises:
    #     Exception: The callable fails with a non-domain error.
    try:
        return operation(*args)
    except DomainError as error:
        # DomainError's constructor needs more than Exception.args can preserve.
        return _Rejected(error.status, error.code, error.message, error.path)


class BoundedProcessPool:
    # Limit CPU processes and admitted jobs so expensive work cannot create an unbounded queue.
    #
    # Attributes:
    #     slots: Semaphore for active and queued jobs.
    #
    # Returns:
    #     A pool manager. Its child processes start only when work arrives.
    def __init__(
        self,
        *,
        workers: int = 2,
        capacity: int = 4,
        wait_seconds: float = 0.1,
        shutdown_seconds: float = 5,
    ) -> None:
        # Create admission limits without starting child processes so idle services use fewer resources.
        #
        # Args:
        #     workers: Maximum child processes.
        #     capacity: Maximum admitted jobs, including active and queued work.
        #     wait_seconds: Maximum wait for an admission slot.
        #     shutdown_seconds: Grace period before unfinished workers are killed.
        #
        # Returns:
        #     None.
        #
        # Raises:
        #     ValueError: Process counts or time limits are invalid.
        if not 1 <= workers <= capacity or wait_seconds <= 0 or shutdown_seconds <= 0:
            raise ValueError("Invalid process pool limits.")
        self.slots = asyncio.Semaphore(capacity)
        self._workers = workers
        self._wait_seconds = wait_seconds
        self._shutdown_seconds = shutdown_seconds
        self._executor: ProcessPoolExecutor | None = None
        self._pending: set[asyncio.Future[Any]] = set()
        self._retired: set[asyncio.Task[None]] = set()
        self._closed = False
        self._closing: asyncio.Task[None] | None = None

    async def _acquire(self) -> None:
        # Reserve one admission slot so pending CPU work stays bounded.
        #
        # No arguments.
        #
        # Returns:
        #     None after one slot is reserved.
        #
        # Raises:
        #     DomainError: The pool is closed or admission does not become available in time.
        if self._closed:
            raise DomainError(503, "CPU_UNAVAILABLE", "CPU capacity is unavailable.")
        try:
            async with asyncio.timeout(self._wait_seconds):
                await self.slots.acquire()
        except TimeoutError as error:
            raise DomainError(503, "OVERLOADED", "Request capacity is full.") from error
        if self._closed:
            self.slots.release()
            raise DomainError(503, "CPU_UNAVAILABLE", "CPU capacity is unavailable.")

    def _pool(self) -> ProcessPoolExecutor:
        # Create the executor on first use so startup does not allocate idle CPU workers.
        #
        # No arguments.
        #
        # Returns:
        #     The current spawned-process executor.
        #
        # Raises:
        #     OSError: Executor resources cannot be allocated.
        if self._executor is None:
            self._executor = ProcessPoolExecutor(
                max_workers=self._workers, mp_context=multiprocessing.get_context("spawn")
            )
        return self._executor

    def _retire(self, executor: ProcessPoolExecutor) -> None:
        # Detach a failed current executor and track its cleanup so a later call can create another pool.
        #
        # Args:
        #     executor: Failed executor to retire. An older detached executor is ignored.
        #
        # Returns:
        #     None. Current executors receive one tracked cleanup task.
        if self._executor is not executor:
            return
        self._executor = None
        task = asyncio.create_task(self._shutdown(executor, tuple(self._pending)))
        self._retired.add(task)
        task.add_done_callback(self._retirement_finished)

    def _retirement_finished(self, task: asyncio.Task[None]) -> None:
        # Release a completed cleanup task and read its error so no unhandled task warning remains.
        #
        # Args:
        #     task: Completed executor cleanup task.
        #
        # Returns:
        #     None.
        self._retired.discard(task)
        if not task.cancelled():
            task.exception()

    def _finished(self, executor: ProcessPoolExecutor, future: asyncio.Future[Any]) -> None:
        # Return admission after the real CPU future ends so caller cancellation cannot release capacity early.
        #
        # Args:
        #     executor: Executor that owns the finished job.
        #     future: Completed or canceled executor future.
        #
        # Returns:
        #     None. A broken current executor also starts retirement.
        self._pending.discard(future)
        self.slots.release()
        if not future.cancelled() and isinstance(future.exception(), BrokenProcessPool):
            self._retire(executor)

    async def run[T](self, operation: Callable[..., T], *args: Any) -> T:
        # Submit bounded CPU work without the service event loop and restore domain failures in the caller.
        #
        # Caller cancellation leaves submitted work active until its future ends. That work keeps its admission slot.
        #
        # Args:
        #     operation: Picklable callable for a child process.
        #     args: Picklable positional arguments for the callable.
        #
        # Returns:
        #     The callable result after the process response arrives.
        #
        # Raises:
        #     DomainError: Admission, pool availability, or the callable reports a domain failure.
        #     CancelledError: The caller cancels its await.
        #     Exception: The callable or its serialization fails.
        await self._acquire()
        executor = None
        try:
            executor = self._pool()
            future = asyncio.get_running_loop().run_in_executor(executor, _execute, operation, args)
        except BaseException as error:
            self.slots.release()
            if executor is not None:
                self._retire(executor)
            if isinstance(error, (BrokenProcessPool, OSError)):
                raise DomainError(503, "CPU_UNAVAILABLE", "CPU capacity is unavailable.") from error
            raise
        self._pending.add(future)
        future.add_done_callback(lambda task: self._finished(executor, task))
        try:
            result = await asyncio.shield(future)
        except BrokenProcessPool as error:
            raise DomainError(503, "CPU_UNAVAILABLE", "CPU capacity is unavailable.") from error
        if isinstance(result, _Rejected):
            raise DomainError(result.status, result.code, result.message, result.path)
        return result

    async def _shutdown(
        self, executor: ProcessPoolExecutor, futures: tuple[asyncio.Future[Any], ...]
    ) -> None:
        # Drain one executor within its grace period so unfinished CPU work cannot block normal shutdown indefinitely.
        #
        # Args:
        #     executor: Current or retired executor to stop.
        #     futures: Submitted futures associated with the shutdown pass.
        #
        # Returns:
        #     None after graceful shutdown or worker termination.
        #
        # Raises:
        #     Exception: Executor shutdown or worker termination fails.
        if futures:
            _, unfinished = await asyncio.wait(futures, timeout=self._shutdown_seconds)
            if unfinished:
                await asyncio.to_thread(executor.kill_workers)
                await asyncio.gather(*futures, return_exceptions=True)
                return
        await asyncio.to_thread(executor.shutdown, wait=True, cancel_futures=True)

    async def _close(self) -> None:
        # Close the current executor and await retired executors so no tracked pool cleanup is skipped.
        #
        # No arguments.
        #
        # Returns:
        #     None after tracked executor cleanup finishes.
        #
        # Raises:
        #     Exception: An executor cleanup operation fails.
        executor, self._executor = self._executor, None
        if executor is not None:
            await self._shutdown(executor, tuple(self._pending))
        if self._retired:
            await asyncio.gather(*tuple(self._retired))

    async def close(self) -> None:
        # Reject new work and share one shielded cleanup task so repeated close calls do not duplicate shutdown.
        #
        # No arguments.
        #
        # Returns:
        #     None after cleanup finishes.
        #
        # Raises:
        #     CancelledError: The caller cancels its await. The shared cleanup task continues.
        #     Exception: Executor cleanup fails.
        self._closed = True
        if self._closing is None:
            self._closing = asyncio.create_task(self._close())
        await asyncio.shield(self._closing)
