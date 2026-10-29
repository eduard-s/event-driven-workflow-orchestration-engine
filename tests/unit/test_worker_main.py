import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

import shared.database as database_module
from worker import service


@pytest.mark.parametrize("failure", [None, "broker", "pool", "logging", "startup"])
async def test_worker_closes_all_resources_even_after_failure(monkeypatch, failure):
    closed = []

    def finish(name):
        closed.append(name)
        if failure == name:
            raise RuntimeError(f"{name} close failed")

    def closer(name):
        return AsyncMock(side_effect=lambda: finish(name))

    database = Mock(side_effect=AssertionError("Workers must not open PostgreSQL"))
    broker = SimpleNamespace(close=closer("broker"))
    output = SimpleNamespace(close=lambda: finish("logging"))
    worker = SimpleNamespace(
        run=AsyncMock(), stop=asyncio.Event(), output_pool=SimpleNamespace(close=closer("pool"))
    )
    monkeypatch.setattr(database_module, "Database", database)
    monkeypatch.setattr(service, "Broker", Mock(return_value=broker))
    monkeypatch.setattr(service, "Worker", Mock(return_value=worker))
    monkeypatch.setattr(service, "configure_logging", Mock(return_value=output))
    monkeypatch.setattr(asyncio.get_running_loop(), "add_signal_handler", Mock())
    if failure == "startup":
        worker.run.side_effect = RuntimeError("startup failed")
    if failure is None:
        await service.main()
    else:
        with pytest.raises(RuntimeError):
            await service.main()
    assert closed == ["broker", "pool", "logging"]
    database.assert_not_called()
