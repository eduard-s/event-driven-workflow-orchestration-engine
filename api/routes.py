# Public workflow routes and operational endpoints.

import asyncio
from typing import Any, cast

from fastapi import APIRouter, Body, Depends, Query, Request, Response

from api import storage, submissions
from api.config import MAX_NODES, MAX_STATUS_PAGE, READINESS_TIMEOUT_SECONDS
from api.context import Context
from api.http import limit_reads, record_request
from api.validation import Trigger, Workflow
from shared.errors import DomainError

workflow_routes = APIRouter()
system_routes = APIRouter()


def context(request: Request) -> Context:
    # Read the initialized API context so routes share bounded resources.
    #
    # Args:
    #     request: The HTTP request whose application owns the context.
    #
    # Returns:
    #     The API context from application state.
    return cast(Context, request.app.state.context)


@workflow_routes.post("/workflow", status_code=201)
async def submission(payload: Workflow, request: Request) -> dict[str, str]:
    # Accept a workflow definition and return its receipt after commit.
    #
    # Args:
    #     payload: The workflow model supplied by FastAPI.
    #     request: The request with application context and optional Idempotency-Key.
    #
    # Returns:
    #     A dictionary with the new or original execution_id.
    #
    # Raises:
    #     DomainError: Graph validation, key validation, or execution admission fails.
    result = await submissions.submit(
        context(request), payload, request.headers.get("Idempotency-Key")
    )
    if result.repeated:
        record_request(
            request, 201, "DUPLICATE_SUBMISSION", identifier=result.execution_id, duplicate=True
        )
    return {"execution_id": result.execution_id}


@workflow_routes.post("/workflow/trigger/{identifier}", status_code=202)
async def trigger(
    identifier: str,
    request: Request,
    response: Response,
    payload: Trigger = Body(default_factory=Trigger),
) -> dict[str, str]:
    # Accept one execution trigger without waiting for workflow completion.
    #
    # Args:
    #     identifier: The execution ID from the URL.
    #     request: The request with the API context.
    #     response: The response whose location and retry headers receive values.
    #     payload: Optional literal input and node overrides.
    #
    # Returns:
    #     A dictionary with the accepted execution_id.
    #
    # Raises:
    #     DomainError: The execution is missing, expired, already triggered, or has invalid overrides.
    identifier = await submissions.trigger(context(request), identifier, payload)
    response.headers.update({"Location": f"/workflows/{identifier}", "Retry-After": "1"})
    return {"execution_id": identifier}


@workflow_routes.get("/workflows/{identifier}", dependencies=[Depends(limit_reads)])
async def status(
    identifier: str,
    request: Request,
    response: Response,
    offset: int = Query(0, ge=0, le=MAX_NODES),
    limit: int = Query(MAX_STATUS_PAGE, ge=1, le=MAX_STATUS_PAGE),
) -> dict[str, Any]:
    # Return a bounded page of node histories for workflow progress.
    #
    # Args:
    #     identifier: The execution ID from the URL.
    #     request: The request with the API context.
    #     response: The response that receives cache and retry headers.
    #     offset: The number of sorted nodes to skip.
    #     limit: The maximum number of node histories to return.
    #
    # Returns:
    #     Execution state, timestamps, pagination fields, and node histories.
    #
    # Raises:
    #     DomainError: The execution ID is invalid or the execution does not exist.
    response.headers.update({"Cache-Control": "no-store", "Retry-After": "1"})
    return await storage.status(context(request), identifier, offset, limit)


@workflow_routes.get("/workflows/{identifier}/results", dependencies=[Depends(limit_reads)])
async def results(identifier: str, request: Request, response: Response) -> dict[str, Any]:
    # Return final or partial outputs so clients can inspect workflow results.
    #
    # Args:
    #     identifier: The execution ID from the URL.
    #     request: The request with the API context and audit resources.
    #     response: The response that receives cache headers and a possible 409 status.
    #
    # Returns:
    #     Final selected outputs, or partial outputs and errors for an unfinished execution.
    #
    # Raises:
    #     DomainError: The execution is missing or a completed execution lacks a required output.
    result = await storage.results(context(request), identifier)
    response.headers["Cache-Control"] = "no-store"
    if result["state"] != "COMPLETED":
        response.status_code = 409
        record_request(request, 409, "RESULTS_UNAVAILABLE", identifier=result["execution_id"])
    return result


@system_routes.get("/health/live")
async def live() -> dict[str, str]:
    # Report that the HTTP loop can answer without dependency probes.
    #
    # No arguments.
    #
    # Returns:
    #     A dictionary with state set to alive.
    return {"state": "alive"}


@system_routes.get("/health/ready")
async def ready(request: Request) -> dict[str, str]:
    # Examine workflow storage so clients can distinguish readiness from liveness.
    #
    # Args:
    #     request: The request with the workflow database context.
    #
    # Returns:
    #     A dictionary with state set to ready when the database schema responds.
    #
    # Raises:
    #     DomainError: The database probe fails its schema result or time limit.
    #     psycopg.Error: The database connection or query fails.
    try:
        async with asyncio.timeout(READINESS_TIMEOUT_SECONDS):
            healthy = await context(request).db.healthy()
    except TimeoutError as exc:
        raise DomainError(503, "STORAGE_UNAVAILABLE", "Persistent storage is unavailable.") from exc
    if not healthy:
        raise DomainError(503, "STORAGE_UNAVAILABLE", "Persistent storage is unavailable.")
    return {"state": "ready"}
