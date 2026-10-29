"""Capacity startup preserves retry budgets, HTTP timeouts, and error behavior."""

from types import SimpleNamespace
from unittest.mock import Mock, call

import httpx
import pytest
import tenacity

from tests.performance import conftest as startup


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch):
    state = SimpleNamespace(now=0.0, waits=[])

    def sleep(seconds):
        state.waits.append(float(seconds))
        state.now += float(seconds)

    retrying = startup.Retrying
    monkeypatch.setattr(tenacity, "time", SimpleNamespace(monotonic=lambda: state.now))
    monkeypatch.setattr(startup, "Retrying", lambda **kwargs: retrying(sleep=sleep, **kwargs))
    return state


def test_startup_retries_transient_http_errors_and_nonready_responses(clock, monkeypatch) -> None:
    get = Mock(
        side_effect=[httpx.ConnectError("starting"), httpx.Response(503), httpx.Response(200)]
    )
    monkeypatch.setattr(startup.httpx, "get", get)
    startup.wait_for_capacity_api("http://capacity")
    assert get.call_args_list == [call("http://capacity/health/live", timeout=2)] * 3
    assert clock.waits == [0.2, 0.2]


@pytest.mark.parametrize("http_error", [False, True])
def test_startup_exhaustion_keeps_thirty_second_budget_and_message(
    clock, monkeypatch, http_error: bool
) -> None:
    def request(*args, **kwargs):
        clock.now += 2
        if http_error:
            raise httpx.ReadTimeout("starting")
        return httpx.Response(503)

    get = Mock(side_effect=request)
    monkeypatch.setattr(startup.httpx, "get", get)
    with pytest.raises(AssertionError, match="^Capacity API did not start$"):
        startup.wait_for_capacity_api("http://capacity")
    assert get.call_count == 14
    assert clock.now == pytest.approx(30.6)
    assert clock.waits == [0.2] * 13


def test_startup_accepts_success_after_the_last_wait_crosses_its_budget(clock, monkeypatch) -> None:
    def request(*args, **kwargs):
        if get.call_count <= 14:
            clock.now += 1.95
            return httpx.Response(503)
        return httpx.Response(200)

    get = Mock(side_effect=request)
    monkeypatch.setattr(startup.httpx, "get", get)
    startup.wait_for_capacity_api("http://capacity")
    assert get.call_count == 15
    assert clock.now == pytest.approx(30.1)
    assert clock.waits == [0.2] * 14


def test_unexpected_startup_errors_propagate_without_retry(clock, monkeypatch) -> None:
    get = Mock(side_effect=ValueError("invalid startup"))
    monkeypatch.setattr(startup.httpx, "get", get)
    with pytest.raises(ValueError, match="invalid startup"):
        startup.wait_for_capacity_api("http://capacity")
    get.assert_called_once_with("http://capacity/health/live", timeout=2)
    assert clock.waits == []
