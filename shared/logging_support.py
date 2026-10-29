# Send bounded log records to one output thread without blocking callers.

import logging
import queue
import threading
from collections.abc import Mapping
from itertools import islice
from typing import Any, TextIO
from uuid import UUID

MAX_LOG_FIELD_CHARS = 2048
MAX_LOG_ARGUMENTS = 16


def _bounded(value: object) -> object:
    # Keep small primitive log values so a queued record cannot hold an arbitrary workflow object.
    #
    # Args:
    #     value: Proposed message, argument, or exception value.
    #
    # Returns:
    #     A limited primitive value or a short type label.
    if isinstance(value, str):
        return value[:MAX_LOG_FIELD_CHARS]
    if value is None or isinstance(value, (bool, float)):
        return value
    if isinstance(value, int):
        return value if value.bit_length() <= 128 else "<large integer>"
    if isinstance(value, UUID):
        return str(value)
    return f"<{type(value).__name__}>"


def _snapshot(record: logging.LogRecord) -> logging.LogRecord:
    # Copy limited log fields and exception locations so queued records do not keep live traceback frames.
    #
    # Args:
    #     record: Original log record from the caller.
    #
    # Returns:
    #     A detached record with bounded arguments and exception text.
    # Logging also permits a single mapping with non-string keys for a plain %s.
    arguments: tuple[object, ...] | Mapping[Any, object] | None = record.args
    if isinstance(arguments, Mapping):
        arguments = {
            key[:80] if isinstance(key, str) else _bounded(key): _bounded(value)
            for key, value in islice(arguments.items(), MAX_LOG_ARGUMENTS)
        }
    elif isinstance(arguments, tuple):
        arguments = tuple(_bounded(value) for value in arguments[:MAX_LOG_ARGUMENTS])
    result = logging.LogRecord(
        record.name[:128],
        record.levelno,
        record.pathname[-512:],
        record.lineno,
        _bounded(record.msg),
        None,
        None,
        func=record.funcName,
    )
    result.args = arguments
    result.created, result.msecs = record.created, record.msecs
    if record.exc_info:
        kind, error, traceback = record.exc_info
        details = [kind.__name__ if kind is not None else "Exception"]
        if error is not None:
            details.append(repr(tuple(_bounded(value) for value in error.args[:4])))
        # Keep locations, without keeping live traceback frames and their workflow data.
        for _ in range(20):
            if traceback is None:
                break
            code = traceback.tb_frame.f_code
            details.append(
                f"{code.co_filename[-512:]}:{traceback.tb_lineno} in {code.co_name[:128]}"
            )
            traceback = traceback.tb_next
        result.exc_text = "\n".join(details)
    return result


class _QueueHandler(logging.Handler):
    # Route caller records to the bounded output queue so stream writes stay outside the event loop.
    #
    # Args:
    #     owner: Queue controller that owns the output thread.
    #
    # Returns:
    #     A logging handler for that controller.
    def __init__(self, owner: QueuedLogging) -> None:
        # Attach the queue controller so each emitted record reaches its bounded queue.
        #
        # Args:
        #     owner: Queue controller that owns the output thread.
        #
        # Returns:
        #     None.
        super().__init__()
        self.owner = owner

    def emit(self, record: logging.LogRecord) -> None:
        # Capture one limited record without stream I/O so slow output cannot block the caller.
        #
        # Args:
        #     record: Original log record to capture.
        #
        # Returns:
        #     None. The controller counts a dropped record when its queue cannot accept it.
        # Defer formatting and stream writes to the output thread.
        self.owner.enqueue(_snapshot(record))


class QueuedLogging:
    # Bound queued diagnostics and write them on one thread so slow output does not stall service coroutines.
    #
    # Attributes:
    #     queue: Bounded queue of detached log records.
    #     handler: Handler installed on application loggers.
    #     dropped_records: Count of records rejected or discarded by queue bounds.
    #     failed_records: Count of output failures.
    #     thread: Dedicated daemon thread for record output.
    #
    # Returns:
    #     A controller with an active output thread.

    def __init__(self, stream: TextIO, capacity: int = 1024) -> None:
        # Start bounded asynchronous log output so service code does not wait for stream writes.
        #
        # Args:
        #     stream: Text destination for formatted records.
        #     capacity: Maximum records waiting in the queue.
        #
        # Returns:
        #     None.
        #
        # Raises:
        #     ValueError: Queue capacity is less than one.
        if capacity < 1:
            raise ValueError("Log capacity must be positive")
        self.queue: queue.Queue[logging.LogRecord] = queue.Queue(maxsize=capacity)
        self.handler = _QueueHandler(self)
        self.stream = stream
        self.formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")
        self.dropped_records = 0
        self.failed_records = 0
        self._reported_drops = 0
        self._closing = threading.Event()
        self._root: logging.Logger | None = None
        self._old_handlers: list[logging.Handler] = []
        self._old_level = logging.NOTSET
        self.thread = threading.Thread(target=self._drain, name="log-output", daemon=True)
        self.thread.start()

    def install(self, level: str) -> None:
        # Replace root handlers with the queue handler so application logs use the output thread.
        #
        # Args:
        #     level: Root log level name accepted by Python logging.
        #
        # Returns:
        #     None. Previous root handlers and level remain available for restoration.
        #
        # Raises:
        #     ValueError: The level name is invalid.
        root = logging.getLogger()
        self._root = root
        self._old_handlers = root.handlers[:]
        self._old_level = root.level
        root.handlers = [self.handler]
        root.setLevel(level)

    def enqueue(self, record: logging.LogRecord) -> None:
        # Add one record without a wait so queue congestion cannot stall service work.
        #
        # Args:
        #     record: Detached record with bounded fields.
        #
        # Returns:
        #     None. A full or closed queue increments the dropped-record count.
        if self._closing.is_set():
            self.dropped_records += 1
            return
        try:
            self.queue.put_nowait(record)
        except queue.Full:
            self.dropped_records += 1

    def _drain(self) -> None:
        # Drain records on the output thread and report dropped records after the stream resumes.
        #
        # No arguments.
        #
        # Returns:
        #     None after shutdown is requested and the queue becomes empty.
        #
        # Output failures increment failed_records. They do not enter the same queue again.
        while not self._closing.is_set() or not self.queue.empty():
            try:
                record = self.queue.get(timeout=0.05)
            except queue.Empty:
                continue
            try:
                self._emit(record)
                if self.dropped_records > self._reported_drops:
                    self._reported_drops = self.dropped_records
                    self._emit(
                        logging.LogRecord(
                            __name__,
                            logging.WARNING,
                            __file__,
                            0,
                            "log_records_dropped count=%s",
                            (self._reported_drops,),
                            None,
                        )
                    )
            except Exception:
                # Never report a failed sink through the same log queue.
                self.failed_records += 1
            finally:
                self.queue.task_done()

    def _emit(self, record: logging.LogRecord) -> None:
        # Format and write one record on the output thread so callers avoid stream I/O.
        #
        # Args:
        #     record: Detached record to format and write.
        #
        # Returns:
        #     None after the record and stream flush finish.
        #
        # Raises:
        #     Exception: Formatting or the output stream fails.
        # A StreamHandler registers a flush that can block logging.shutdown().
        self.stream.write(self.formatter.format(record) + "\n")
        self.stream.flush()

    def close(self, timeout: float = 1.0) -> bool:
        # Restore root handlers and bound the final drain so a blocked stream cannot delay this call indefinitely.
        #
        # Args:
        #     timeout: Maximum seconds to wait for the output thread. Negative values use zero.
        #
        # Returns:
        #     True when the thread stops, or False when it exceeds the wait.
        #     Records still queued after the wait are discarded.
        self._closing.set()
        if self._root is not None and self.handler in self._root.handlers:
            self._root.handlers = self._old_handlers
            self._root.setLevel(self._old_level)
        self.thread.join(timeout=max(0.0, timeout))
        if self.thread.is_alive():
            # Release queued records even if the sink holds one record indefinitely.
            while True:
                try:
                    self.queue.get_nowait()
                    self.queue.task_done()
                    self.dropped_records += 1
                except queue.Empty:
                    break
        return not self.thread.is_alive()
