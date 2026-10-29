# A child process runs one mock handler within time and output limits.

from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import sys
from typing import Any, NamedTuple

from shared.cpu import BoundedProcessPool
from shared.output import INLINE_BYTES, PreparedOutput, prepare_value
from worker.messages import HandlerRequest

LOG = logging.getLogger(__name__)


class OutputLimitExceeded(Exception):
    # Stop a pipe read before a child can consume unbounded worker memory.
    #
    # Args:
    #     *args: Optional exception details inherited from Exception.
    #
    # Returns:
    #     An exception that the process runner converts to an output-limit failure.

    pass


class HandlerResult(NamedTuple):
    # Describe handler success independently of the prepared JSON payload.
    #
    # Args:
    #     success: Whether the handler succeeds.
    #     value: Native JSON output, which can be a valid null result.
    #     error: Failure details, or None for success.
    #     runtime: Optional process ID and start and finish times.
    #
    # Returns:
    #     A four-field named tuple used by handlers and outcome storage.

    success: bool
    value: Any = None
    error: dict[str, Any] | None = None
    runtime: dict[str, Any] | None = None


def _failed_result(
    code: str,
    retryable: bool,
    runtime: dict[str, Any] | None = None,
) -> HandlerResult:
    # Create one failure shape so protocol errors use consistent retry decisions.
    #
    # Args:
    #     code: Stable failure code.
    #     retryable: Whether another try can recover.
    #     runtime: Optional child process and timing metadata.
    #
    # Returns:
    #     A failed HandlerResult with no successful value.
    return HandlerResult(
        False,
        error={"code": code, "message": "The handler failed.", "retryable": retryable},
        runtime=runtime,
    )


async def _read_limited(reader: asyncio.StreamReader, limit: int) -> bytes:
    # Read one child pipe within its byte limit to bound worker memory.
    #
    # Args:
    #     reader: Child output or error stream.
    #     limit: Maximum total bytes to accept.
    #
    # Returns:
    #     All accepted bytes after the stream ends.
    #
    # Raises:
    #     OutputLimitExceeded: The pipe produces more than the allowed bytes.
    chunks: list[bytes] = []
    size = 0
    while True:
        chunk = await reader.read(min(65536, limit + 1 - size))
        if not chunk:
            return b"".join(chunks)
        size += len(chunk)
        if size > limit:
            raise OutputLimitExceeded
        chunks.append(chunk)


async def _stop_process(process: asyncio.subprocess.Process) -> None:
    # Kill an unfinished child and reap it before its worker slot returns.
    #
    # Args:
    #     process: Child process whose pipes must also drain.
    #
    # Returns:
    #     None after pipe drains and process exit complete.
    if process.returncode is None:
        try:
            if os.name == "posix":
                os.killpg(process.pid, signal.SIGKILL)
            else:
                process.kill()
        except ProcessLookupError:
            pass

    async def discard(reader: asyncio.StreamReader | None) -> None:
        # Drain a remaining pipe so process exit cannot wait on a full buffer.
        #
        # Args:
        #     reader: Child output or error stream, or None if absent.
        #
        # Returns:
        #     None after the stream ends.
        if reader is not None:
            while await reader.read(65536):
                pass

    # Drain the pipes after the readers stop. A full pipe can block
    # Process.wait(), even after the child exits.
    await asyncio.gather(discard(process.stdout), discard(process.stderr), process.wait())


def _reject_number(value: str) -> None:
    # Reject non-finite constants during child JSON decoding.
    #
    # Args:
    #     value: JSON constant text supplied by the decoder.
    #
    # Returns:
    #     No value. The function always raises.
    #
    # Raises:
    #     ValueError: The decoder encounters a non-finite JSON constant.
    raise ValueError("Non-finite JSON number")


async def _start_handler() -> asyncio.subprocess.Process:
    # Start one isolated handler with separate pipes and a process group.
    #
    # No arguments.
    #
    # Returns:
    #     The child process with standard input, output, and error pipes.
    #
    # Raises:
    #     OSError: The operating system cannot start the child.
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "worker.handlers",
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        start_new_session=os.name == "posix",
    )
    LOG.info("handler_process_spawned pid=%s", process.pid)
    return process


async def _send_request(writer: asyncio.StreamWriter, request: HandlerRequest) -> None:
    # Send one request and close input so the child can decode the complete body.
    #
    # Args:
    #     writer: Child standard-input stream.
    #     request: Frozen handler input and optional fixture.
    #
    # Returns:
    #     None. A child that closes its input early does not prevent pipe cleanup.
    #
    # Raises:
    #     ValueError: The request contains a non-finite JSON number.
    #     TypeError: The request contains a value that JSON cannot encode.
    encoded = json.dumps(
        request,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
    ).encode("utf-8")
    try:
        writer.write(encoded)
        await writer.drain()
    except BrokenPipeError, ConnectionResetError:
        pass
    finally:
        writer.close()


async def _exchange(
    process: asyncio.subprocess.Process,
    request: HandlerRequest,
    output_limit: int,
) -> tuple[bytes, bytes]:
    # Send input while bounded output readers prevent full-pipe deadlocks.
    #
    # Args:
    #     process: Child with all three standard pipes.
    #     request: Frozen handler input.
    #     output_limit: Node value limit in bytes. Protocol metadata has a separate allowance.
    #
    # Returns:
    #     Standard-output bytes and standard-error bytes.
    #
    # Raises:
    #     OutputLimitExceeded: A pipe exceeds its bounded allowance.
    #     CancelledError: The caller cancels the exchange. Reader and writer tasks are canceled.
    assert process.stdin and process.stdout and process.stderr
    stdout = asyncio.create_task(_read_limited(process.stdout, output_limit + 4096))
    stderr = asyncio.create_task(_read_limited(process.stderr, 4096))
    tasks = [asyncio.create_task(_send_request(process.stdin, request)), stdout, stderr]
    try:
        await asyncio.gather(*tasks)
        return stdout.result(), stderr.result()
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


def _decode_failure(stderr: bytes) -> HandlerResult:
    # Translate child error JSON into a stable failure result.
    #
    # Args:
    #     stderr: Bounded child error bytes.
    #
    # Returns:
    #     The reported failure, or a retryable process-exit failure for an invalid error shape.
    try:
        failure = json.loads(stderr)
        code, retryable = failure["code"], failure["retryable"]
        if not isinstance(code, str) or len(code) > 100 or type(retryable) is not bool:
            raise ValueError("Invalid child error")
        return _failed_result(code, retryable, failure.get("runtime"))
    except ValueError, TypeError, KeyError:
        return _failed_result("HANDLER_PROCESS_EXIT", True)


def _prepared_failure(result: HandlerResult) -> tuple[HandlerResult, PreparedOutput]:
    # Pair a failure with its encoded payload for consistent outcome writes.
    #
    # Args:
    #     result: Failed handler result and optional runtime metadata.
    #
    # Returns:
    #     The original result and its prepared error snapshot.
    return result, prepare_value(result.success, result.value, result.error)


def _decode_prepared_output(
    stdout: bytes, max_output_bytes: int, max_depth: int
) -> tuple[HandlerResult, PreparedOutput]:
    # Decode child success once and enforce value limits before result publication.
    #
    # Args:
    #     stdout: Child success JSON with value and runtime fields.
    #     max_output_bytes: Maximum encoded bytes for the value alone.
    #     max_depth: Maximum container depth of the value alone.
    #
    # Returns:
    #     Native result and immutable output. Invalid, excessive, or deep output produces a permanent failure.
    try:
        response = json.loads(stdout, parse_constant=_reject_number)
        value, runtime = response["value"], response["runtime"]
    except ValueError, UnicodeError, RecursionError, TypeError, KeyError:
        return _prepared_failure(_failed_result("INVALID_HANDLER_OUTPUT", False))
    prepared = prepare_value(True, value, None)
    if prepared.payload is None:
        return _prepared_failure(_failed_result("INVALID_HANDLER_OUTPUT", False))
    if prepared.output_bytes > max_output_bytes:
        return _prepared_failure(_failed_result("OUTPUT_TOO_LARGE", False, runtime))
    if prepared.depth > max_depth:
        return _prepared_failure(_failed_result("OUTPUT_DEPTH_EXCEEDED", False, runtime))
    return HandlerResult(True, value, runtime=runtime), prepared


def _decode_output(stdout: bytes, max_output_bytes: int, max_depth: int) -> HandlerResult:
    # Expose a native result when a caller does not need its prepared snapshot.
    #
    # Args:
    #     stdout: Child success protocol bytes.
    #     max_output_bytes: Maximum encoded bytes for the value.
    #     max_depth: Maximum value container depth.
    #
    # Returns:
    #     The decoded result or a permanent output-contract failure.
    return _decode_prepared_output(stdout, max_output_bytes, max_depth)[0]


async def _prepare_protocol(
    stdout: bytes, max_output_bytes: int, max_depth: int, pool: BoundedProcessPool | None
) -> tuple[HandlerResult, PreparedOutput]:
    # Move large child decoding and output encoding off the worker event loop.
    #
    # Args:
    #     stdout: Immutable child success bytes.
    #     max_output_bytes: Maximum encoded bytes for the value.
    #     max_depth: Maximum value container depth.
    #     pool: Shared CPU pool, or None for a temporary one-worker pool.
    #
    # Returns:
    #     Native result and immutable output.
    #
    # Raises:
    #     DomainError: CPU capacity or a CPU process is unavailable.
    if len(stdout) <= INLINE_BYTES:
        return _decode_prepared_output(stdout, max_output_bytes, max_depth)
    if pool is not None:
        return await pool.run(_decode_prepared_output, stdout, max_output_bytes, max_depth)
    temporary = BoundedProcessPool(workers=1, capacity=1)
    try:
        return await temporary.run(_decode_prepared_output, stdout, max_output_bytes, max_depth)
    finally:
        await temporary.close()


async def run_prepared_handler_process(
    request: HandlerRequest,
    *,
    remaining_seconds: float,
    max_output_bytes: int,
    max_depth: int,
    pool: BoundedProcessPool | None = None,
) -> tuple[HandlerResult, PreparedOutput]:
    # Run one isolated handler and reap it before the worker releases its slot.
    #
    # Args:
    #     request: Frozen handler input and optional fixture.
    #     remaining_seconds: Remaining claim budget for the child and output preparation.
    #     max_output_bytes: Maximum encoded bytes for the value alone.
    #     max_depth: Maximum value container depth.
    #     pool: Shared output pool, or None for a temporary CPU pool.
    #
    # Returns:
    #     Native result and prepared output. Timeout, exit, or output-limit errors become failure results.
    #
    # Raises:
    #     OSError: The child cannot start.
    #     DomainError: CPU capacity is unavailable.
    #     CancelledError: The caller cancels execution. The child still stops and is reaped.
    if remaining_seconds <= 0:
        return _prepared_failure(_failed_result("HANDLER_TIMEOUT", True))
    process: asyncio.subprocess.Process | None = None
    try:
        async with asyncio.timeout(remaining_seconds):
            process = await _start_handler()
            stdout, stderr = await _exchange(process, request, max_output_bytes)
            if await process.wait():
                return _prepared_failure(_decode_failure(stderr))
            return await _prepare_protocol(stdout, max_output_bytes, max_depth, pool)
    except TimeoutError:
        return _prepared_failure(_failed_result("HANDLER_TIMEOUT", True))
    except OutputLimitExceeded:
        return _prepared_failure(_failed_result("OUTPUT_TOO_LARGE", False))
    finally:
        if process is not None:
            await _stop_process(process)


async def run_handler_process(
    request: HandlerRequest,
    *,
    remaining_seconds: float,
    max_output_bytes: int,
    max_depth: int,
) -> HandlerResult:
    # Return the native result of one isolated handler for callers without prepared storage.
    #
    # Args:
    #     request: Frozen handler input and optional fixture.
    #     remaining_seconds: Remaining claim budget in seconds.
    #     max_output_bytes: Maximum encoded bytes for the value alone.
    #     max_depth: Maximum value container depth.
    #
    # Returns:
    #     HandlerResult with a successful value or classified failure.
    #
    # Raises:
    #     OSError: The child cannot start.
    #     DomainError: CPU preparation is unavailable.
    #     CancelledError: The caller cancels its wait. The child is reaped.
    result, _prepared = await run_prepared_handler_process(
        request,
        remaining_seconds=remaining_seconds,
        max_output_bytes=max_output_bytes,
        max_depth=max_depth,
    )
    return result
