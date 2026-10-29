# Prepare immutable JSON snapshots before result publication or database writes.

from __future__ import annotations

import json
import math
import pickle
from dataclasses import dataclass
from typing import Any

from shared.contracts import examine_json
from shared.cpu import BoundedProcessPool
from shared.errors import DomainError

INLINE_BYTES = 16_384
INVALID_OUTPUT = {
    "code": "INVALID_OUTPUT",
    "message": "Handler output exceeds its contract.",
    "retryable": False,
}


@dataclass(frozen=True)
class PreparedOutput:
    # Keep immutable JSON and measured limits outside database transactions.
    #
    # Args:
    #     success: Original handler success flag.
    #     payload: Encoded value or error, or None for invalid output.
    #     output_bytes: Successful UTF-8 value bytes. Failures use zero.
    #     depth: Maximum container depth of the encoded value or error.
    #
    # Returns:
    #     A frozen snapshot reused across outcome write retries.

    success: bool
    payload: str | None
    output_bytes: int = 0
    depth: int = 0


def normalize_error(error: dict[str, Any] | None) -> dict[str, Any]:
    # Limit failure fields and remove text that PostgreSQL cannot store.
    #
    # Args:
    #     error: Handler failure details, or None for a default internal failure.
    #
    # Returns:
    #     A dictionary with bounded code, message, and retryable fields.
    failure = error or {
        "code": "INTERNAL_HANDLER_ERROR",
        "message": "The handler failed.",
        "retryable": True,
    }

    def text(key: str, default: str, limit: int) -> str:
        # Prepare one bounded error field without invalid Unicode or NUL text.
        #
        # Args:
        #     key: Field name in the enclosing failure dictionary.
        #     default: Text used when the field is absent.
        #     limit: Maximum number of characters before Unicode replacement.
        #
        # Returns:
        #     Storage-safe text for the requested field.
        value = str(failure.get(key, default))[:limit]
        return value.encode("utf-8", errors="replace").decode().replace("\x00", "\ufffd")

    return {
        "code": text("code", "INTERNAL_HANDLER_ERROR", 80),
        "message": text("message", "The handler failed.", 256),
        "retryable": bool(failure.get("retryable", False)),
    }


def prepare_value(success: bool, value: Any, error: dict[str, Any] | None) -> PreparedOutput:
    # Examine and encode one output before locked policy decisions.
    #
    # Args:
    #     success: Whether to prepare the value or normalize an error.
    #     value: Native JSON output for success.
    #     error: Handler failure details for failure.
    #
    # Returns:
    #     Encoded output and measurements. Invalid JSON produces a snapshot whose payload is None.
    try:
        proposed = value if success else normalize_error(error)
        depth = examine_json(proposed, max_depth=1_000, storage_safe=True)
        payload = json.dumps(
            proposed, ensure_ascii=False, allow_nan=False, sort_keys=True, separators=(",", ":")
        )
        return PreparedOutput(
            success, payload, len(payload.encode("utf-8")) if success else 0, depth
        )
    except DomainError, ValueError, TypeError, RecursionError, UnicodeError:
        return PreparedOutput(success, None)


def _prepare_snapshot(snapshot: bytes) -> PreparedOutput:
    # The snapshot comes only from this process, never from a broker or API request.
    # Decode a process-owned snapshot so CPU work cannot see later caller mutations.
    #
    # Args:
    #     snapshot: Pickle bytes created only by this process, never by external requests.
    #
    # Returns:
    #     The prepared value or normalized error.
    #
    # Raises:
    #     pickle.UnpicklingError: The internal snapshot is invalid.
    return prepare_value(*pickle.loads(snapshot))


def _small_value(value: Any) -> bool:
    # Bound the amount of native traversal allowed on the service event loop.
    #
    # Args:
    #     value: Native value or error considered for inline preparation.
    #
    # Returns:
    #     True if its estimated traversal fits the inline budget. Otherwise False.
    pending = [value]
    budget = INLINE_BYTES
    while pending and budget > 0:
        current = pending.pop()
        budget -= 32
        if type(current) is str:
            budget -= len(current) * 4
        elif type(current) is dict:
            if len(current) > budget // 32:
                return False
            pending.extend(current)
            pending.extend(current.values())
        elif type(current) is list:
            if len(current) > budget // 32:
                return False
            pending.extend(current)
    return not pending and budget >= 0


async def prepare_result(
    result: tuple[bool, Any, dict[str, Any] | None, dict[str, Any] | None],
    *,
    pool: BoundedProcessPool | None = None,
) -> PreparedOutput:
    # Capture a stable native result before an await and prepare large snapshots in a process.
    #
    # Args:
    #     result: Success flag, native value, optional error, and optional runtime metadata.
    #     pool: Shared output pool, or None to use a temporary one-worker pool.
    #
    # Returns:
    #     Immutable output. An invalid native snapshot produces a payload of None.
    #
    # Raises:
    #     DomainError: CPU admission or process execution is unavailable.
    #     CancelledError: The caller cancels its wait. Running CPU work keeps its admission slot.
    success, value, error, _runtime = result
    try:
        # Native snapshot copying holds the GIL. Dense collections can briefly delay the loop.
        snapshot = pickle.dumps((success, value, error), protocol=pickle.HIGHEST_PROTOCOL)
    except pickle.PickleError, ValueError, TypeError, RecursionError, AttributeError:
        return PreparedOutput(success, None)
    if len(snapshot) <= INLINE_BYTES and _small_value(value if success else error):
        return _prepare_snapshot(snapshot)
    if pool is not None:
        return await pool.run(_prepare_snapshot, snapshot)
    temporary = BoundedProcessPool(workers=1, capacity=1)
    try:
        return await temporary.run(_prepare_snapshot, snapshot)
    finally:
        await temporary.close()


def safe_runtime(runtime: dict[str, Any] | None) -> dict[str, Any] | None:
    # Keep finite runtime fields so optional metadata cannot break result transport or storage.
    # Args: runtime contains a process ID and start and finish times, or None.
    # Returns: Allowed numeric fields, an empty dictionary, or None for an invalid input shape.
    if not isinstance(runtime, dict):
        return None
    return {
        key: runtime[key]
        for key in ("pid", "started_at", "finished_at")
        if key in runtime
        and (
            type(runtime[key]) is int
            or (type(runtime[key]) is float and math.isfinite(runtime[key]))
        )
    }
