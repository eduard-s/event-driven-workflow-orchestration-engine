import asyncio
import io
import logging
import threading
import time

import pytest

from shared.logging_support import QueuedLogging


class SlowStream(io.StringIO):
    def __init__(self) -> None:
        super().__init__()
        self.entered = threading.Event()
        self.release = threading.Event()
        self.output_thread: threading.Thread | None = None

    def write(self, value: str) -> int:
        self.output_thread = threading.current_thread()
        self.entered.set()
        self.release.wait(timeout=2)
        return super().write(value)


def logger_for(logging_output: QueuedLogging) -> logging.Logger:
    logger = logging.Logger("test", logging.INFO)
    logger.addHandler(logging_output.handler)
    return logger


async def test_slow_log_output_does_not_block_event_loop() -> None:
    stream = SlowStream()
    output = QueuedLogging(stream, capacity=2)
    logger = logger_for(output)
    try:
        logger.info("first")
        assert await asyncio.to_thread(stream.entered.wait, 1)
        completed = asyncio.Event()
        asyncio.get_running_loop().call_later(0.01, completed.set)
        logger.info("second")
        async with asyncio.timeout(0.5):
            await completed.wait()
        assert stream.output_thread is output.thread
        assert stream.output_thread is not threading.current_thread()
    finally:
        stream.release.set()
        assert await asyncio.to_thread(output.close)
    assert "first" in stream.getvalue()
    assert "second" in stream.getvalue()


async def test_log_overflow_is_bounded_and_shutdown_has_a_deadline() -> None:
    stream = SlowStream()
    output = QueuedLogging(stream, capacity=2)
    logger = logger_for(output)
    try:
        logger.info("active")
        assert await asyncio.to_thread(stream.entered.wait, 1)
        for index in range(100):
            logger.info("queued %s", index)
        assert output.queue.qsize() == 2
        assert output.dropped_records == 98
        started = time.monotonic()
        assert not output.close(timeout=0.01)
        assert time.monotonic() - started < 0.5
        assert output.queue.empty()
        assert output.dropped_records == 100
        logger.info("after shutdown")
        assert output.dropped_records == 101
    finally:
        stream.release.set()
        assert await asyncio.to_thread(output.close)
    assert not output.thread.is_alive()
    assert "log_records_dropped count=101" in stream.getvalue()


async def test_log_formatting_runs_on_the_output_thread() -> None:
    stream = io.StringIO()
    output = QueuedLogging(stream)
    formatted_by: list[threading.Thread] = []

    class Formatter(logging.Formatter):
        def format(self, record: logging.LogRecord) -> str:
            formatted_by.append(threading.current_thread())
            return super().format(record)

    output.formatter = Formatter()
    logger_for(output).info("value=%s", "rendered")
    assert await asyncio.to_thread(output.close)
    assert formatted_by == [output.thread]
    assert "value=rendered" in stream.getvalue()


async def test_failed_log_sink_does_not_crash_the_output_thread() -> None:
    class FailedStream(io.StringIO):
        def write(self, value: str) -> int:
            raise OSError("closed pipe")

    output = QueuedLogging(FailedStream())
    logger_for(output).error("unavailable")
    assert await asyncio.to_thread(output.close)
    assert output.failed_records == 1


async def test_log_mapping_arguments_reach_the_sink() -> None:
    stream = io.StringIO()
    output = QueuedLogging(stream)
    logger_for(output).info("id=%(id)s", {"id": "example"})
    assert await asyncio.to_thread(output.close)
    assert "id=example" in stream.getvalue()


@pytest.mark.parametrize("key", [1, 1.5, None, False, (1, 2)])
async def test_single_mapping_with_nonstring_keys_does_not_break_logging(key: object) -> None:
    stream = io.StringIO()
    output = QueuedLogging(stream)
    try:
        logger_for(output).info("value=%s", {key: "example"})
    finally:
        assert await asyncio.to_thread(output.close)
    assert output.failed_records == 0
    assert "example" in stream.getvalue()
    if isinstance(key, (int, float)) or key is None:
        assert f"value={{{key!r}: 'example'}}" in stream.getvalue()


async def test_queued_records_bound_fields_and_do_not_keep_traceback_frames() -> None:
    stream = SlowStream()
    output = QueuedLogging(stream, capacity=2)
    logger = logger_for(output)
    try:
        logger.info("active")
        assert await asyncio.to_thread(stream.entered.wait, 1)
        try:
            raise ValueError("x" * 100_000)
        except ValueError:
            logger.exception("failed %s", "y" * 100_000)
        record = output.queue.queue[0]
        assert record.exc_info is None
        assert record.exc_text is not None and len(record.exc_text) < 10_000
        assert record.args is not None and len(record.args[0]) == 2048
    finally:
        stream.release.set()
        assert await asyncio.to_thread(output.close)
