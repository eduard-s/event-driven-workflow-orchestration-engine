# HTTP admission and audit around FastAPI's request handling.

import asyncio
import logging
from collections.abc import AsyncIterator
from typing import Any, cast
from uuid import UUID, uuid4

import psycopg
from fastapi import FastAPI, Request
from fastapi.exception_handlers import http_exception_handler, request_validation_exception_handler
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, Response
from limits import RateLimitItemPerSecond
from limits.aio.strategies import MovingWindowRateLimiter
from starlette.exceptions import HTTPException
from starlette.middleware.base import RequestResponseEndpoint

from api.context import acquire_slot
from shared.errors import DomainError

LOG = logging.getLogger(__name__)


async def limit_requests(request: Request, call_next: RequestResponseEndpoint) -> Response:
    # Assign a request ID so responses and audit records share one identity.
    #
    # Args:
    #     request: The incoming HTTP request and application state.
    #     call_next: The next HTTP handler.
    #
    # Returns:
    #     The handler or admission-error response with an X-Request-ID header.
    request.state.request_id = uuid4()
    response = await admit_request(request, call_next)
    response.headers["X-Request-ID"] = str(request.state.request_id)
    return response


async def admit_request(request: Request, call_next: RequestResponseEndpoint) -> Response:
    # Bound request rate and active requests before route work starts.
    #
    # Args:
    #     request: The request with rate and capacity resources in application state.
    #     call_next: The handler called after admission.
    #
    # Returns:
    #     The handler response, or a rate-limit or capacity error response.
    #     Liveness requests bypass admission.
    if request.url.path == "/health/live":
        return await call_next(request)
    limiter = cast(MovingWindowRateLimiter, request.app.state.rate_limiter)
    rule = cast(RateLimitItemPerSecond, request.app.state.rate_limit)
    semaphore = cast(asyncio.Semaphore, request.app.state.request_slots)
    try:
        if not await limiter.hit(rule, "workflow-api"):
            raise DomainError(429, "RATE_LIMITED", "The request rate limit is full.")
        await acquire_slot(semaphore)
    except DomainError as exc:
        return await domain_error(request, exc)
    try:
        return await call_next(request)
    finally:
        semaphore.release()


async def limit_reads(request: Request) -> AsyncIterator[None]:
    # Status and results share a small limit so writes can still use the database pool.
    # Reserve read capacity so status requests leave database capacity for writes.
    #
    # Args:
    #     request: The request whose application owns the read semaphore.
    #
    # Yields:
    #     None while the route owns one read slot.
    #
    # Raises:
    #     DomainError: No read slot becomes available within the admission time limit.
    semaphore = cast(asyncio.Semaphore, request.app.state.read_slots)
    await acquire_slot(semaphore)
    try:
        yield
    finally:
        semaphore.release()


async def domain_error(request: Request, exc: Exception) -> JSONResponse:
    # Convert a domain failure into safe JSON and an audit record.
    #
    # Args:
    #     request: The failed request and its upload state.
    #     exc: A DomainError with an HTTP status, code, message, and optional path.
    #
    # Returns:
    #     A JSON error response with retry or connection headers when necessary.
    failure = cast(DomainError, exc)
    record_request(request, failure.status, failure.code)
    headers = {"Retry-After": "1"} if failure.status in (429, 503) else {}
    if (
        request.scope.get("http_version", "1.1") in {"1.0", "1.1"}
        and request.method in {"POST", "PUT", "PATCH"}
        and not getattr(request.state, "body_read_complete", False)
    ):
        headers["Connection"] = "close"
    error = {"code": failure.code, "message": failure.message}
    if failure.path is not None:
        error["path"] = failure.path
    return JSONResponse({"error": error}, status_code=failure.status, headers=headers)


async def schema_error(request: Request, exc: Exception) -> JSONResponse:
    # Remove raw input values from FastAPI errors before the response.
    #
    # Args:
    #     request: The request that failed schema validation.
    #     exc: A RequestValidationError from FastAPI.
    #
    # Returns:
    #     A 422 JSON response with field errors and no raw input values.
    record_request(request, 422, "INVALID_REQUEST")
    # Keep FastAPI's errors without raw values, including non-finite numbers.
    errors = [
        {key: value for key, value in error.items() if key not in {"input", "ctx"}}
        for error in cast(RequestValidationError, exc).errors()
    ]
    return await request_validation_exception_handler(request, RequestValidationError(errors))


async def storage_error(request: Request, exc: Exception) -> JSONResponse:
    # Hide storage details and expose temporary database failure as an HTTP error.
    #
    # Args:
    #     request: The request that encountered the storage failure.
    #     exc: The database exception to identify in the log.
    #
    # Returns:
    #     A safe 503 JSON response with a retry header.
    LOG.warning("storage_unavailable type=%s", type(exc).__name__)
    return await domain_error(
        request, DomainError(503, "STORAGE_UNAVAILABLE", "Persistent storage is unavailable.")
    )


async def http_error(request: Request, exc: Exception) -> Response:
    # Record an HTTP failure while preserving FastAPI response behavior.
    #
    # Args:
    #     request: The failed request.
    #     exc: A Starlette HTTPException with the intended status and details.
    #
    # Returns:
    #     The HTTP exception response from FastAPI.
    failure = cast(HTTPException, exc)
    record_request(request, failure.status_code, f"HTTP_{failure.status_code}")
    return await http_exception_handler(request, failure)


def register_error_handlers(app: FastAPI) -> None:
    # Install consistent error responses for routes and database failures.
    #
    # Args:
    #     app: The FastAPI application that receives the exception handlers.
    #
    # Returns:
    #     None. The function updates the application handler registry.
    app.add_exception_handler(DomainError, domain_error)
    app.add_exception_handler(RequestValidationError, schema_error)
    app.add_exception_handler(HTTPException, http_error)
    app.add_exception_handler(psycopg.Error, storage_error)


def record_request(
    request: Request,
    status: int,
    reason: str,
    *,
    identifier: str | None = None,
    duplicate: bool = False,
) -> None:
    # Log a rejected or repeated request without delaying it for audit storage.
    #
    # Args:
    #     request: The request with route, method, identity, and audit queue.
    #     status: The HTTP response status.
    #     reason: A safe machine-readable reason code.
    #     identifier: An execution ID override, or None to use the route parameter.
    #     duplicate: True when the response repeats an earlier submission receipt.
    #
    # Returns:
    #     None. A full audit queue drops the record after a warning.
    candidate = identifier or request.path_params.get("identifier", "")
    try:
        execution_id = UUID(candidate)
    except ValueError:
        execution_id = None
    route = request.scope.get("route")
    record = {
        "request_id": getattr(request.state, "request_id", None) or uuid4(),
        "method": request.method,
        "route": route.path if route else "<unmatched>",
        "status": status,
        "outcome": "duplicate" if duplicate else "rejected",
        "reason": reason,
        "execution_id": execution_id,
    }
    LOG.info(
        "request_audit request_id=%(request_id)s method=%(method)s route=%(route)s "
        "status=%(status)s outcome=%(outcome)s reason=%(reason)s execution_id=%(execution_id)s",
        record,
    )
    queue = cast(
        asyncio.Queue[dict[str, Any]] | None, getattr(request.app.state, "audit_queue", None)
    )
    if queue is not None:
        try:
            queue.put_nowait(record)
        except asyncio.QueueFull:
            LOG.warning("audit_queue_full request_id=%s", record["request_id"])
