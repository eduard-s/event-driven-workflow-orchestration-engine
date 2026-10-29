# Mock handler values shared by direct, coroutine, and isolated execution.

from __future__ import annotations

import json
import os
import sys
import time
from collections.abc import Mapping
from typing import Any

MAX_REQUEST_BYTES = 16 * 1024 * 1024


class MockFailure(Exception):
    # Carry a stable mock failure code and its retry decision to the child protocol.
    #
    # Attributes:
    #     code: Failure code to report.
    #     retryable: Whether another try can recover.
    #
    # Constructor arguments are documented by __init__.
    #
    # Returns:
    #     A MockFailure instance with its failure code and retry decision.

    def __init__(self, code: str, retryable: bool) -> None:
        # Set the failure details used by direct and isolated mock execution.
        #
        # Args:
        #     code: Stable mock failure code.
        #     retryable: Whether another try can recover.
        #
        # Returns:
        #     None.
        super().__init__(code)
        self.code = code
        self.retryable = retryable


def run_handler(request: dict[str, Any]) -> Any:
    # Apply test fixtures and synchronous mock delays inside an isolated child.
    #
    # Args:
    #     request: Handler name, input, configuration, and optional fixture.
    #
    # Returns:
    #     The fixture output or handler's JSON value.
    #
    # Raises:
    #     MockFailure: A configured failure or invalid handler input occurs.
    #     KeyError: Required handler configuration is absent.
    #     ValueError: A fixture delay is invalid.
    #
    # A crash fixture exits the child immediately and returns no result.
    fixture = request.get("fixture") or {}
    if fixture.get("crash"):
        os._exit(71)
    if "delay" in fixture:
        time.sleep(max(0.0, float(fixture["delay"])))
    failure = fixture.get("fail")
    if failure in ("transient", "permanent"):
        raise MockFailure(
            "MOCK_TRANSIENT_FAILURE" if failure == "transient" else "MOCK_PERMANENT_FAILURE",
            failure == "transient",
        )
    if "output" in fixture:
        return fixture["output"]

    if request["handler"] == "call_external_service" and "delay" not in fixture:
        time.sleep(1)
    return handler_value(request)


def handler_value(request: Mapping[str, Any]) -> Any:
    # Compute the mock value shared by direct and isolated execution.
    #
    # Args:
    #     request: Handler name, optional data, and handler configuration.
    #
    # Returns:
    #     Input data, output data, dummy HTTP JSON, or a mock LLM string.
    #
    # Raises:
    #     MockFailure: The handler is unknown or its prompt is not text.
    #     KeyError: A required handler name, URL, or prompt is absent.
    handler = request["handler"]
    data = request.get("data")
    config = request.get("config") or {}
    if handler in ("input", "output"):
        return data
    if handler == "call_external_service":
        return {"url": config["url"], "data": data}
    if handler == "llm":
        prompt = config["prompt"]
        if not isinstance(prompt, str):
            raise MockFailure("INVALID_HANDLER_INPUT", False)
        return "mock: " + prompt
    raise MockFailure("UNKNOWN_HANDLER", False)


def main() -> int:
    # Serve one bounded JSON request over the child process pipes.
    #
    # No arguments. The request comes from standard input.
    #
    # Returns:
    #     Zero after a JSON result on standard output, or two after a JSON failure on standard error.
    #
    # A crash fixture exits immediately. Other handler errors become protocol failures.
    runtime: dict[str, Any] | None = None
    try:
        encoded = sys.stdin.buffer.read(MAX_REQUEST_BYTES + 1)
        if len(encoded) > MAX_REQUEST_BYTES:
            raise MockFailure("HANDLER_INPUT_TOO_LARGE", False)
        request = json.loads(encoded)
        if not isinstance(request, dict):
            raise MockFailure("INVALID_HANDLER_INPUT", False)
        runtime = {"pid": os.getpid(), "started_at": time.time()}
        try:
            value = run_handler(request)
        finally:
            runtime["finished_at"] = time.time()
        output = json.dumps(
            {"value": value, "runtime": runtime},
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
        sys.stdout.buffer.write(output)
        sys.stdout.buffer.flush()
        return 0
    except MockFailure as exc:
        error = {"code": exc.code, "retryable": exc.retryable}
    except Exception:
        error = {"code": "INTERNAL_HANDLER_ERROR", "retryable": True}
    if runtime is not None:
        error["runtime"] = runtime
    sys.stderr.write(json.dumps(error, separators=(",", ":")))
    sys.stderr.flush()
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
