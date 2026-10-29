# Select handler execution while claim and lease ownership stay fixed.

import asyncio
import os
import time
from dataclasses import dataclass
from typing import Literal

from shared.cpu import BoundedProcessPool
from shared.output import PreparedOutput, prepare_result, prepare_value
from worker.handlers import MockFailure, handler_value
from worker.messages import HandlerRequest
from worker.runner import HandlerResult, run_prepared_handler_process


@dataclass(frozen=True)
class HandlerExecution:
    # Keep the native handler result and its immutable JSON payload together.
    #
    # Args:
    #     result: Success, native value or error, and runtime metadata.
    #     prepared: Encoded output reused by outcome write retries.
    #
    # Returns:
    #     An immutable result bundle. Its native result can still contain mutable JSON values.

    result: HandlerResult
    prepared: PreparedOutput


def _failure(code: str, retryable: bool, runtime: dict | None = None) -> HandlerExecution:
    # Create matching failure objects for handler dispatch and result publication.
    #
    # Args:
    #     code: Stable failure code.
    #     retryable: Whether recovery can authorize another try.
    #     runtime: Optional process ID and start and finish times.
    #
    # Returns:
    #     A failed handler result with its prepared error JSON.
    result = HandlerResult(
        False,
        error={"code": code, "message": "The handler failed.", "retryable": retryable},
        runtime=runtime,
    )
    return HandlerExecution(result, prepare_value(False, None, result.error))


async def _trusted_handler(request: HandlerRequest, pool: BoundedProcessPool) -> HandlerExecution:
    # Run trusted mocks without child startup and prepare their outputs before storage.
    #
    # Args:
    #     request: Frozen handler input with its configuration.
    #     pool: Bounded process pool for large output preparation.
    #
    # Returns:
    #     A native result and prepared output. Mock and handler errors become failure results.
    #
    # Raises:
    #     DomainError: Large output preparation cannot get CPU capacity.
    #     CancelledError: The caller cancels the handler or its wait.
    runtime = {"pid": os.getpid(), "started_at": time.time()}
    try:
        if request["handler"] == "call_external_service":
            # The mock represents asynchronous remote I/O and makes no network call.
            await asyncio.sleep(1)
        value = handler_value(request)
    except MockFailure as error:
        return _failure(error.code, error.retryable, runtime)
    except Exception:
        return _failure("INTERNAL_HANDLER_ERROR", True, runtime)
    finally:
        runtime["finished_at"] = time.time()
    result = HandlerResult(True, value, runtime=runtime)
    prepared = await prepare_result(result, pool=pool)
    return HandlerExecution(result, prepared)


async def run_handler(
    request: HandlerRequest,
    *,
    pool: BoundedProcessPool,
    remaining_seconds: float,
    max_output_bytes: int,
    max_depth: int,
    mode: Literal["auto", "process"] = "auto",
) -> HandlerExecution:
    # Select direct, coroutine, or isolated execution within the remaining claim budget.
    #
    # Args:
    #     request: Frozen input and optional test fixture.
    #     pool: Bounded process pool for large output preparation.
    #     remaining_seconds: Remaining time for handler work and output preparation.
    #     max_output_bytes: Maximum encoded bytes for the node value.
    #     max_depth: Maximum output container depth.
    #     mode: Automatic dispatch or forced process isolation.
    #
    # Returns:
    #     A result bundle. Expired time or invalid output produces a failure bundle.
    #
    # Raises:
    #     DomainError: CPU capacity is unavailable.
    #     CancelledError: The caller cancels the operation. Isolated children are reaped.
    if remaining_seconds <= 0:
        return _failure("HANDLER_TIMEOUT", True)
    if (
        mode == "process"
        or request.get("fixture")
        or request["handler"] not in ("input", "output", "call_external_service", "llm")
    ):
        result, prepared = await run_prepared_handler_process(
            request,
            remaining_seconds=remaining_seconds,
            max_output_bytes=max_output_bytes,
            max_depth=max_depth,
            pool=pool,
        )
        return HandlerExecution(result, prepared)
    try:
        async with asyncio.timeout(max(0, remaining_seconds)):
            execution = await _trusted_handler(request, pool)
            if execution.prepared.payload is None:
                return _failure("INVALID_HANDLER_OUTPUT", False, execution.result.runtime)
            if execution.prepared.output_bytes > max_output_bytes:
                return _failure("OUTPUT_TOO_LARGE", False, execution.result.runtime)
            if execution.prepared.depth > max_depth:
                return _failure("OUTPUT_DEPTH_EXCEEDED", False, execution.result.runtime)
            return execution
    except TimeoutError:
        return _failure("HANDLER_TIMEOUT", True)
