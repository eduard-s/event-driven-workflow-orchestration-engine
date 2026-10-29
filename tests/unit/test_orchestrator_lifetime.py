import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from orchestrator import __main__ as entrypoint


@pytest.mark.parametrize(
    "failure",
    [None, "runtime", "broker", "database", "logging", "startup", "all"],
)
async def test_orchestrator_attempts_all_cleanup_and_closes_logging_last(
    monkeypatch: pytest.MonkeyPatch, failure: str | None
) -> None:
    closed: list[str] = []

    def finish(name: str) -> None:
        closed.append(name)
        if failure in (name, "all"):
            raise RuntimeError(f"{name} close failed")

    def closer(name: str) -> AsyncMock:
        return AsyncMock(side_effect=lambda: finish(name))

    database = SimpleNamespace(open=AsyncMock(), close=closer("database"))
    broker = SimpleNamespace(start=AsyncMock(), close=closer("broker"))
    stop = asyncio.Event()
    stop.set()
    runtime = SimpleNamespace(start=AsyncMock(), close=closer("runtime"), stop=stop, failure=None)
    output = SimpleNamespace(close=lambda: finish("logging"))
    monkeypatch.setattr(entrypoint, "Database", Mock(return_value=database))
    monkeypatch.setattr(entrypoint, "Broker", Mock(return_value=broker))
    monkeypatch.setattr(entrypoint, "Runtime", Mock(return_value=runtime))
    monkeypatch.setattr(entrypoint, "configure_logging", Mock(return_value=output))
    monkeypatch.setattr(asyncio.get_running_loop(), "add_signal_handler", Mock())
    if failure == "startup":
        database.open.side_effect = RuntimeError("startup failed")
    if failure is None:
        await entrypoint.main()
    else:
        with pytest.raises(RuntimeError):
            await entrypoint.main()
    assert closed == ["runtime", "broker", "database", "logging"]
