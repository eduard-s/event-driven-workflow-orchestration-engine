# Bound uploads before FastAPI parses JSON or creates models.

import asyncio

from fastapi import Request
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from api.context import acquire_slot
from api.http import domain_error
from shared.errors import DomainError


async def read_body(receive: Receive, max_bytes: int, seconds: float) -> bytes:
    # Read a bounded upload before JSON parsing can consume more resources.
    #
    # Args:
    #     receive: The ASGI callback that supplies upload messages.
    #     max_bytes: The largest accepted body size in bytes.
    #     seconds: The total time limit for the upload.
    #
    # Returns:
    #     The complete request body as bytes.
    #
    # Raises:
    #     DomainError: The client disconnects, exceeds the byte limit, or exceeds the time limit.
    buffer = bytearray()
    try:
        async with asyncio.timeout(seconds):
            while True:
                message = await receive()
                if message["type"] == "http.disconnect":
                    raise DomainError(400, "CLIENT_DISCONNECTED", "The upload did not finish.")
                chunk = message.get("body", b"")
                if len(buffer) + len(chunk) > max_bytes:
                    raise DomainError(413, "BODY_TOO_LARGE", "The request body exceeds its limit.")
                buffer.extend(chunk)
                if not message.get("more_body", False):
                    return bytes(buffer)
    except TimeoutError as exc:
        raise DomainError(408, "UPLOAD_TIMEOUT", "The upload exceeded its time limit.") from exc


class BodyLimits:
    # Bound concurrent uploads before FastAPI creates request models.
    #
    # The middleware keeps upload buffers and upload waits within fixed limits.
    # Constructor arguments are documented on __init__.

    def __init__(self, app: ASGIApp, max_bytes: int, seconds: float, concurrency: int) -> None:
        # Store upload limits and create the shared upload semaphore.
        #
        # Args:
        #     app: The next ASGI application.
        #     max_bytes: The byte limit for each upload.
        #     seconds: The time limit for each upload.
        #     concurrency: The maximum number of active upload readers.
        #
        # Returns:
        #     None. The instance owns the limits and semaphore.
        self.app, self.max_bytes, self.seconds = app, max_bytes, seconds
        self.slots = asyncio.Semaphore(concurrency)

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        # Apply upload admission and return rejected uploads as HTTP errors.
        #
        # Args:
        #     scope: ASGI connection metadata, including the method and application.
        #     receive: The callback that supplies request messages.
        #     send: The callback that sends response messages.
        #
        # Returns:
        #     None after the next application or an error response finishes.
        if scope["type"] != "http" or scope["method"] not in {"POST", "PUT", "PATCH"}:
            await self.app(scope, receive, send)
            return
        acquired = False
        try:
            await acquire_slot(self.slots)
            acquired = True
            await self.serve(scope, receive, send)
        except DomainError as exc:
            response = await domain_error(Request(scope), exc)
            await response(scope, receive, send)
        finally:
            if acquired:
                self.slots.release()

    async def serve(self, scope: Scope, receive: Receive, send: Send) -> None:
        # Buffer an accepted upload and replay it once to FastAPI.
        #
        # Args:
        #     scope: The request metadata and state to mark after the upload.
        #     receive: The original callback for upload and disconnect messages.
        #     send: The callback for response messages.
        #
        # Returns:
        #     None after the next application finishes.
        #
        # Raises:
        #     DomainError: The length header, upload size, client connection, or time limit is invalid.
        try:
            length = dict(scope["headers"]).get(b"content-length")
            if length is not None and not length.isdigit():
                raise ValueError("Invalid Content-Length")
            if length is not None and int(length) > self.max_bytes:
                raise DomainError(413, "BODY_TOO_LARGE", "The request body exceeds its limit.")
        except ValueError as exc:
            raise DomainError(400, "INVALID_LENGTH", "Content-Length is invalid.") from exc
        body: bytes | None = await read_body(receive, self.max_bytes, self.seconds)
        scope.setdefault("state", {})["body_read_complete"] = True

        async def replay() -> Message:
            # Supply the buffered body once so downstream JSON parsing sees the upload.
            #
            # No arguments. The callback uses the enclosing body and receive callback.
            #
            # Returns:
            #     One body message first, then messages from the original receive callback.
            nonlocal body
            if body is not None:
                value, body = body, None
                return {"type": "http.request", "body": value, "more_body": False}
            return await receive()

        await self.app(scope, replay, send)
