"""Spawn-safe operations for process pool tests."""

import os
import time
from pathlib import Path
from typing import Any

from shared.errors import DomainError


def wait_for_release(entered: str, release: str, value: Any = 7) -> Any:
    Path(entered).touch()
    deadline = time.monotonic() + 5
    while not Path(release).exists():
        if time.monotonic() >= deadline:
            raise RuntimeError("The test did not release the process.")
        time.sleep(0.005)
    return value


def invalid() -> None:
    raise ValueError("Invalid fixture")


def rejected() -> None:
    raise DomainError(422, "INVALID_FIXTURE", "Invalid fixture", "dag.nodes")


def crash() -> None:
    os._exit(71)


def prepare_paused(entered: str, release: str, operation: Any, *args: Any) -> Any:
    wait_for_release(entered, release)
    return operation(*args)
