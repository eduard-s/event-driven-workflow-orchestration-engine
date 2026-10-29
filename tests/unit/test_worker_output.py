"""Output preparation, immutable snapshots, and saved policy checks."""

import asyncio
import json
from typing import Any

import pytest

from orchestrator import storage
from shared.contracts import examine_json
from shared.errors import DomainError
from shared.output import INLINE_BYTES, PreparedOutput, prepare_result, prepare_value, safe_runtime
from worker.runner import _decode_prepared_output, _prepare_protocol


def test_output_metadata_uses_exact_utf8_bytes_and_container_depth() -> None:
    prepared = prepare_value(True, {"one": [{"two": "é"}]}, None)
    assert prepared.payload == '{"one":[{"two":"é"}]}'
    assert prepared.output_bytes == len(prepared.payload.encode("utf-8"))
    assert prepared.depth == 3


@pytest.mark.parametrize(
    "value", [float("nan"), float("inf"), {1: "value"}, (1,), {1}, "\x00", "\ud800"]
)
def test_invalid_native_output_never_becomes_database_json(value: Any) -> None:
    assert prepare_value(True, value, None).payload is None


def test_shared_containers_are_valid_but_cycles_are_rejected() -> None:
    shared: list[Any] = [1]
    assert examine_json([shared, shared]) == 2
    shared.append(shared)
    with pytest.raises(DomainError, match="cycles"):
        examine_json(shared)
    assert prepare_value(True, shared, None).payload is None


async def test_native_snapshot_does_not_change_while_cpu_capacity_waits() -> None:
    entered, resume = asyncio.Event(), asyncio.Event()

    class WaitingPool:
        async def run(self, operation: Any, snapshot: bytes) -> PreparedOutput:
            entered.set()
            await resume.wait()
            return operation(snapshot)

    value = {"text": "x" * (INLINE_BYTES + 1), "items": [1]}
    preparing = asyncio.create_task(prepare_result((True, value, None, None), pool=WaitingPool()))  # type: ignore[arg-type]
    await entered.wait()
    value["items"].append(2)  # type: ignore[union-attr]
    value["text"] = "changed"
    resume.set()
    prepared = await preparing
    assert prepared.payload is not None
    assert json.loads(prepared.payload) == {"text": "x" * (INLINE_BYTES + 1), "items": [1]}


async def test_repeated_references_cannot_expand_into_expensive_inline_work() -> None:
    called = False

    class RecordingPool:
        async def run(self, operation: Any, snapshot: bytes) -> PreparedOutput:
            nonlocal called
            called = True
            return operation(snapshot)

    value: Any = "x"
    for _ in range(10):
        value = [value, value]
    prepared = await prepare_result((True, value, None, None), pool=RecordingPool())  # type: ignore[arg-type]
    assert called
    assert prepared.depth == 10


async def test_large_process_protocol_has_one_decode_and_preparation_job() -> None:
    operations: list[Any] = []

    class RecordingPool:
        async def run(self, operation: Any, *args: Any) -> Any:
            operations.append(operation)
            return operation(*args)

    value = {"text": "x" * INLINE_BYTES}
    protocol = json.dumps({"value": value, "runtime": {"pid": 7}}).encode()
    result, prepared = await _prepare_protocol(protocol, INLINE_BYTES * 2, 2, RecordingPool())  # type: ignore[arg-type]
    assert operations == [_decode_prepared_output]
    assert result.success and result.value == value
    assert result.runtime == {"pid": 7}
    assert prepared.payload is not None and json.loads(prepared.payload) == value
    result.value["text"] = "changed"
    assert json.loads(prepared.payload) == {"text": "x" * INLINE_BYTES}


def test_policy_checks_use_saved_value_depth_and_aggregate_limits() -> None:
    execution = {
        "policy": {"max_value_bytes": 10, "max_execution_bytes": 100, "max_depth": 2},
        "output_bytes": 0,
    }
    value = prepare_value(True, "x" * 20, None)
    assert not storage._prepare_outcome(execution, {"handler": "input"}, value).success
    assert storage._prepare_outcome(execution, {"handler": "output"}, value).success
    deep = prepare_value(True, [[[1]]], None)
    assert not storage._prepare_outcome(execution, {"handler": "output"}, deep).success
    execution["output_bytes"] = 90
    assert not storage._prepare_outcome(execution, {"handler": "output"}, value).success


def test_failure_text_is_bounded_and_safe_for_jsonb() -> None:
    prepared = prepare_value(False, None, {"code": "\x00\ud800", "message": "x" * 300})
    assert prepared.payload is not None
    failure = json.loads(prepared.payload)
    assert "\x00" not in failure["code"]
    assert len(failure["message"]) == 256
    assert prepared.output_bytes == 0


def test_nonfinite_runtime_cannot_prevent_outcome_storage() -> None:
    assert safe_runtime({"pid": 7, "started_at": float("nan"), "finished_at": 1.5}) == {
        "pid": 7,
        "finished_at": 1.5,
    }
    assert safe_runtime(["pid"]) is None  # type: ignore[arg-type]
