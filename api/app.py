# Application assembly and service lifetime.

import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from limits import RateLimitItemPerSecond
from limits.aio.storage import MemoryStorage
from limits.aio.strategies import MovingWindowRateLimiter

from api.body import BodyLimits
from api.config import AUDIT_QUEUE_SIZE, READ_CONCURRENCY, REQUEST_CONCURRENCY, Settings
from api.context import Context
from api.http import limit_requests, register_error_handlers
from api.routes import system_routes, workflow_routes
from api.storage import audit_pool, audit_writer
from shared.database import Database


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None]:
    # Own API resources so startup and shutdown release their connections.
    #
    # Args:
    #     app: The FastAPI application with initialized state.
    #
    # Yields:
    #     None while the database and audit consumers are active.
    db = app.state.db
    await db.open()
    try:
        async with audit_writer(app.state.audit, app.state.audit_queue):
            yield
    finally:
        await db.close()


def initialize_state(app: FastAPI, settings: Settings) -> None:
    # Create shared request resources so handlers use bounded capacity.
    #
    # Args:
    #     app: The application whose state receives the resources.
    #     settings: Connection settings, request limits, and policy defaults.
    #
    # Returns:
    #     None. The function stores resources on app.state without opening connections.
    app.state.settings = settings
    app.state.db = Database(settings)
    app.state.audit = audit_pool(settings.audit_postgres_dsn)
    app.state.audit_queue = asyncio.Queue(maxsize=AUDIT_QUEUE_SIZE)
    app.state.context = Context(app.state.db, settings)
    app.state.request_slots = asyncio.Semaphore(REQUEST_CONCURRENCY)
    app.state.rate_limiter = MovingWindowRateLimiter(MemoryStorage())
    app.state.rate_limit = RateLimitItemPerSecond(settings.requests_per_second)
    app.state.read_slots = asyncio.Semaphore(READ_CONCURRENCY)


def create_app(settings: Settings | None = None) -> FastAPI:
    # Assemble routes, error handlers, and middleware for one API process.
    #
    # Args:
    #     settings: Explicit settings, or None to read the environment defaults.
    #
    # Returns:
    #     A FastAPI application. Its lifespan opens and closes service resources.
    app = FastAPI(lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)
    initialize_state(app, settings or Settings())
    register_error_handlers(app)
    app.add_middleware(
        BodyLimits,
        max_bytes=app.state.settings.max_body_bytes,
        seconds=app.state.settings.body_timeout,
        concurrency=app.state.settings.body_concurrency,
    )
    app.middleware("http")(limit_requests)
    app.include_router(workflow_routes)
    app.include_router(system_routes)
    return app
