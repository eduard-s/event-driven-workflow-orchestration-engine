import logging
from unittest.mock import Mock

import pytest

from api import __main__ as api_main


@pytest.mark.parametrize("error", [None, RuntimeError("startup failure")])
def test_uvicorn_uses_queued_root_logging_and_closes_it(
    monkeypatch: pytest.MonkeyPatch, error: Exception | None
) -> None:
    output = Mock()
    run = Mock(side_effect=error)
    monkeypatch.setattr(api_main, "configure_logging", Mock(return_value=output))
    monkeypatch.setattr(api_main, "create_app", Mock())
    monkeypatch.setattr(api_main.uvicorn, "run", run)
    loggers = [logging.getLogger(name) for name in ("uvicorn", "uvicorn.error", "uvicorn.access")]
    original = [(logger.handlers[:], logger.propagate) for logger in loggers]
    for logger in loggers:
        logger.handlers = [logging.NullHandler()]
        logger.propagate = False
    try:
        if error is None:
            api_main.main()
        else:
            with pytest.raises(RuntimeError):
                api_main.main()
        assert run.call_args.kwargs["log_config"] is None
        assert all(not logger.handlers and logger.propagate for logger in loggers)
        output.close.assert_called_once()
    finally:
        for logger, (handlers, propagate) in zip(loggers, original):
            logger.handlers, logger.propagate = handlers, propagate
