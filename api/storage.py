# API database operations and transaction boundaries.

import asyncio
import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import Any
from uuid import uuid4

from psycopg import AsyncConnection
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool

import shared.state as state
from api import config
from api.config import MAX_STATUS_PAGE
from api.context import Context
from api.validation import Workflow
from shared.database import Connection
from shared.errors import DomainError
from shared.identifiers import execution_id


async def store_submission(
    context: Context,
    workflow: Workflow,
    definition: dict[str, Any],
    key: str | None,
) -> tuple[str, bool]:
    # Commit receipt identity, quota admission, and graph writes together so replicas cannot accept conflicting submissions.
    #
    # Args:
    #     context: API database access, quotas, and defaults for the saved policy.
    #     workflow: Workflow model already examined outside the transaction.
    #     definition: JSON-compatible definition that matches the examined workflow.
    #     key: Optional submission key. None permits a fresh execution on each call.
    #
    # Returns:
    #     The execution ID and duplicate flag after the transaction commits.
    #
    # Raises:
    #     DomainError: The execution storage quota is full.
    #     psycopg.Error: A write fails. The complete transaction rolls back.
    async with context.db.tx() as connection:
        old = await lock_submission(connection, key) if key is not None else None
        if old:
            return str(old["execution_id"]), True
        await admit_execution(
            connection, context.settings.max_executions, context.settings.max_records
        )
        identifier = str(uuid4())
        await insert_execution(
            connection,
            identifier,
            workflow,
            definition,
            context.settings.execution_policy(),
            key,
        )
    return identifier, False


async def store_trigger(context: Context, identifier: str, trigger: dict[str, Any]) -> str:
    # Commit a trigger and its start event under one execution lock so background execution sees a complete accepted trigger.
    #
    # Args:
    #     context: API database access.
    #     identifier: Examined execution UUID as text.
    #     trigger: Normalized literal input and node overrides examined outside the transaction.
    #
    # Returns:
    #     The execution ID after the accepted trigger and start event commit.
    #
    # Raises:
    #     DomainError: The execution is absent, already triggered, or expired before acceptance.
    #     psycopg.Error: A database operation fails. Trigger and event writes roll back together.
    async with context.db.tx() as connection:
        execution = await state.lock_execution(connection, identifier)
        now = await state.database_time(connection)
        if execution["accepted_at"] is not None or execution["state"] in (
            "RUNNING",
            "COMPLETED",
            "FAILED",
        ):
            raise DomainError(
                409,
                "EXECUTION_ALREADY_TRIGGERED",
                f"Execution already started. Current state: {execution['state']}.",
            )
        expired = await state.expire_execution(connection, execution, now)
        if not expired:
            deadline = now + timedelta(seconds=execution["policy"]["execution_timeout"])
            await accept_trigger(connection, identifier, trigger, now, deadline)
            await state.add_event(connection, identifier, "start")
    # Commit expiry before reporting rejection so a later request cannot revive stale work.
    if expired:
        raise DomainError(409, "EXECUTION_EXPIRED", "The execution expired before its trigger.")
    return identifier


async def lock_submission(connection: Connection, key: str) -> dict[str, Any] | None:
    # This transaction lock works across API replicas. Unrelated keys do not share a global lock.
    # Serialize one submission key so API replicas return one receipt.
    #
    # Args:
    #     connection: The caller's active write transaction.
    #     key: The submission key to lock for the transaction lifetime.
    #
    # Returns:
    #     The existing key record, or None when the key has no saved receipt.
    await connection.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (key,))
    return await (
        await connection.execute("SELECT * FROM submission_keys WHERE key=%s", (key,))
    ).fetchone()


async def admit_execution(connection: Connection, max_executions: int, max_records: int) -> None:
    # Two-key advisory locks use a different namespace from submission-key locks.
    # Keep this lock until the caller commits the new execution.
    # Serialize quota admission so concurrent submissions cannot exceed shared quotas.
    #
    # Args:
    #     connection: The transaction that will insert the accepted execution.
    #     max_executions: The maximum number of pending and running executions.
    #     max_records: The maximum number of saved executions in all states.
    #
    # Returns:
    #     None when the caller can insert an execution within both quotas.
    #
    # Raises:
    #     DomainError: Either shared storage quota is full.
    await connection.execute("SELECT pg_advisory_xact_lock(0, 1)")
    counts = await (
        await connection.execute(
            """SELECT count(*) AS records,
            count(*) FILTER (WHERE state IN ('PENDING','RUNNING')) AS unfinished
            FROM executions"""
        )
    ).fetchone()
    assert counts is not None
    if counts["unfinished"] >= max_executions or counts["records"] >= max_records:
        raise DomainError(503, "EXECUTION_CAPACITY", "Execution storage capacity is full.")


async def insert_execution(
    connection: Connection,
    identifier: str,
    workflow: Workflow,
    definition: dict[str, Any],
    policy: dict[str, int | float],
    key: str | None,
) -> None:
    # Send graph writes together. The caller still owns transaction commit and rollback.
    # Insert a definition, graph, policy, and optional receipt key atomically.
    #
    # Args:
    #     connection: The caller's write transaction, which owns commit or rollback.
    #     identifier: The new execution UUID as text.
    #     workflow: The examined workflow model for node and edge rows.
    #     definition: The JSON-compatible workflow definition to save.
    #     policy: The fixed execution limits to save with the execution.
    #     key: The optional submission key to associate with this receipt.
    #
    # Returns:
    #     None. All graph writes remain inside the caller's transaction.
    async with connection.pipeline(), connection.cursor() as cursor:
        await cursor.execute(
            "INSERT INTO executions(id,definition,policy,state) VALUES (%s,%s,%s,'PENDING')",
            (identifier, Jsonb(definition), Jsonb(policy)),
        )
        await cursor.executemany(
            "INSERT INTO nodes(execution_id,node_id,handler,dependencies) VALUES (%s,%s,%s,%s)",
            [(identifier, node.id, node.handler, node.dependencies) for node in workflow.dag.nodes],
        )
        await cursor.executemany(
            "INSERT INTO edges(execution_id,parent_id,child_id) VALUES (%s,%s,%s)",
            [
                (identifier, parent, node.id)
                for node in workflow.dag.nodes
                for parent in node.dependencies
            ],
        )
        if key is not None:
            await cursor.execute(
                "INSERT INTO submission_keys(key,execution_id) VALUES (%s,%s)",
                (key, identifier),
            )


async def load_definition(context: Context, identifier: str) -> dict[str, Any]:
    # Read a saved graph snapshot for trigger examination.
    #
    # Args:
    #     context: The API context that owns the workflow database.
    #     identifier: The execution UUID as text.
    #
    # Returns:
    #     The saved definition dictionary.
    #
    # Raises:
    #     DomainError: The ID is invalid or the execution does not exist.
    identifier = execution_id(identifier)
    async with context.db.tx(snapshot=True) as connection:
        record = await (
            await connection.execute("SELECT definition FROM executions WHERE id=%s", (identifier,))
        ).fetchone()
        if not record:
            raise DomainError(404, "NOT_FOUND", "Execution does not exist.")
    return record["definition"]


async def accept_trigger(
    connection: Connection,
    identifier: str,
    trigger: dict[str, Any],
    now: datetime,
    deadline: datetime,
) -> None:
    # Accept a pending trigger once and schedule immediate orchestration work.
    #
    # Args:
    #     connection: The caller's write transaction with the execution lock.
    #     identifier: The execution UUID as text.
    #     trigger: The normalized input and node override dictionary.
    #     now: The database timestamp for trigger acceptance.
    #     deadline: The execution deadline computed from the saved policy.
    #
    # Returns:
    #     None after the pending execution row receives the trigger and timestamps.
    #
    # Raises:
    #     DomainError: The execution is no longer pending or already accepted a trigger.
    updated = await connection.execute(
        """UPDATE executions SET trigger=%s,state='RUNNING',accepted_at=%s,
        deadline_at=%s,maintenance_due=%s
        WHERE id=%s AND state='PENDING' AND accepted_at IS NULL""",
        (Jsonb(trigger), now, deadline, now, identifier),
    )
    if updated.rowcount != 1:
        raise DomainError(409, "EXECUTION_ALREADY_TRIGGERED", "Execution is no longer pending.")


async def load_execution(connection: Connection, identifier: str) -> dict[str, Any]:
    # Read execution metadata for a consistent status or result response.
    #
    # Args:
    #     connection: The caller's snapshot transaction.
    #     identifier: The execution UUID as text.
    #
    # Returns:
    #     The execution state, failure flag, and lifecycle timestamps.
    #
    # Raises:
    #     DomainError: The execution does not exist.
    execution = await (
        await connection.execute(
            """SELECT id,state,has_failure,created_at,accepted_at,deadline_at,terminal_at
            FROM executions WHERE id=%s""",
            (identifier,),
        )
    ).fetchone()
    if execution is None:
        raise DomainError(404, "NOT_FOUND", "Execution does not exist.")
    return execution


async def load_nodes(
    connection: Connection, identifier: str, offset: int, limit: int
) -> list[dict[str, Any]]:
    # Read one stable page of nodes to bound status response size.
    #
    # Args:
    #     connection: The caller's snapshot transaction.
    #     identifier: The execution UUID as text.
    #     offset: The number of nodes to skip in node-ID order.
    #     limit: The largest number of rows to return.
    #
    # Returns:
    #     Node state and error records, or an empty list for an empty page.
    return await (
        await connection.execute(
            """SELECT node_id,state,reason,try_number,starts,error FROM nodes
            WHERE execution_id=%s ORDER BY node_id OFFSET %s LIMIT %s""",
            (identifier, offset, limit),
        )
    ).fetchall()


async def count_nodes(connection: Connection, identifier: str) -> int:
    # Count nodes so a paginated response reports the full graph size.
    #
    # Args:
    #     connection: The caller's snapshot transaction.
    #     identifier: The execution UUID as text.
    #
    # Returns:
    #     The number of saved nodes for the execution.
    row = await (
        await connection.execute(
            "SELECT count(*) AS n FROM nodes WHERE execution_id=%s", (identifier,)
        )
    ).fetchone()
    assert row is not None
    return int(row["n"])


async def load_tries(
    connection: Connection, identifier: str, nodes: list[str]
) -> list[dict[str, Any]]:
    # Read histories only for the nodes on the requested status page.
    #
    # Args:
    #     connection: The caller's snapshot transaction.
    #     identifier: The execution UUID as text.
    #     nodes: The node IDs whose histories are necessary.
    #
    # Returns:
    #     Try records ordered by node ID and try number, or an empty list.
    return await (
        await connection.execute(
            """SELECT node_id,try_number,started_at,finished_at,lease_until,success,error,runtime
            FROM tries WHERE execution_id=%s AND node_id=ANY(%s) ORDER BY node_id,try_number""",
            (identifier, nodes),
        )
    ).fetchall()


async def load_result_nodes(connection: Connection, identifier: str) -> list[dict[str, Any]]:
    # Read current outcomes and graph links for final output selection.
    #
    # Args:
    #     connection: The caller's snapshot transaction.
    #     identifier: The execution UUID as text.
    #
    # Returns:
    #     Node records with current output values and errors, including nodes without outcomes.
    return await (
        await connection.execute(
            """SELECT n.node_id,n.handler,n.dependencies,n.has_output,n.error,t.value AS output
            FROM nodes n LEFT JOIN tries t ON t.execution_id=n.execution_id
              AND t.node_id=n.node_id AND t.try_number=n.try_number
            WHERE n.execution_id=%s ORDER BY n.ready_at,n.node_id""",
            (identifier,),
        )
    ).fetchall()


async def insert_audit(connection: AsyncConnection[Any], record: dict[str, Any]) -> None:
    # Insert one diagnostic request record into the separate audit database.
    #
    # Args:
    #     connection: An audit database connection.
    #     record: Request identity, method, route, status, outcome, reason, and execution identity.
    #
    # Returns:
    #     None after the insert. Database errors propagate to the audit writer.
    await connection.execute(
        """INSERT INTO request_audit
        (request_id,method,route,status,outcome,reason,execution_id)
        VALUES (%(request_id)s,%(method)s,%(route)s,%(status)s,%(outcome)s,
                %(reason)s,%(execution_id)s)""",
        record,
    )


async def status(
    context: Context, identifier: str, offset: int = 0, limit: int = MAX_STATUS_PAGE
) -> dict[str, Any]:
    # Read metadata and node histories from one consistent database snapshot.
    #
    # Args:
    #     context: API settings and workflow database access.
    #     identifier: The execution UUID as text.
    #     offset: The number of sorted nodes to skip.
    #     limit: The largest number of node histories to return.
    #
    # Returns:
    #     Execution metadata, a page of node histories, and pagination fields.
    #
    # Raises:
    #     DomainError: The ID is invalid or the execution does not exist.
    identifier = execution_id(identifier)
    async with context.db.tx(snapshot=True) as connection:
        execution = await load_execution(connection, identifier)
        node_records = await load_nodes(connection, identifier, offset, limit)
        nodes = await node_summaries(connection, identifier, node_records)
        total = await count_nodes(connection, identifier)
    return {
        "execution_id": str(execution["id"]),
        "state": execution["state"],
        "has_failure": execution["has_failure"],
        "created_at": execution["created_at"],
        "accepted_at": execution["accepted_at"],
        "deadline_at": execution["deadline_at"],
        "terminal_at": execution["terminal_at"],
        "nodes": nodes,
        "offset": offset,
        "limit": limit,
        "total_nodes": total,
    }


async def results(context: Context, identifier: str) -> dict[str, Any]:
    # Read current outcomes consistently before final or partial result assembly.
    #
    # Args:
    #     context: API settings and workflow database access.
    #     identifier: The execution UUID as text.
    #
    # Returns:
    #     Final selected outputs, or partial outputs and errors for an unfinished execution.
    #
    # Raises:
    #     DomainError: The ID is invalid, the execution is missing, or a completed result is incomplete.
    identifier = execution_id(identifier)
    async with context.db.tx(snapshot=True) as connection:
        execution = await load_execution(connection, identifier)
        nodes = await load_result_nodes(connection, identifier)
    return _result_payload(identifier, execution["state"], nodes)


def _result_payload(identifier: str, state: str, nodes: list[dict[str, Any]]) -> dict[str, Any]:
    # Select terminal outputs and represent unfinished workflows without hiding partial data.
    #
    # Args:
    #     identifier: The execution UUID as text.
    #     state: The saved execution state.
    #     nodes: Node definitions and their current output and error fields.
    #
    # Returns:
    #     Final output-node values, or leaf values when no output node exists.
    #     Unfinished states return partial_outputs and errors instead.
    #
    # Raises:
    #     DomainError: A completed workflow lacks a selected output.
    outputs = {node["node_id"]: node["output"] for node in nodes if node["has_output"]}
    if state != "COMPLETED":
        errors = {node["node_id"]: node["error"] for node in nodes if node["error"]}
        return {
            "execution_id": identifier,
            "state": state,
            "error": {
                "code": "RESULTS_" + state,
                # The executions.state CHECK covers every non-completed state here.
                "message": {
                    "PENDING": "The workflow has not started.",
                    "RUNNING": "The workflow is not complete.",
                    "FAILED": "The workflow failed.",
                    "EXPIRED": "The workflow expired.",
                }[state],
            },
            "errors": errors,
            "partial_outputs": outputs,
        }
    selected = [node["node_id"] for node in nodes if node["handler"] == "output"]
    if not selected:
        parents = {parent for node in nodes for parent in node["dependencies"]}
        selected = [node["node_id"] for node in nodes if node["node_id"] not in parents]
    if any(key not in outputs for key in selected):
        raise DomainError(500, "MISSING_RESULT", "A completed workflow is missing an output.")
    return {
        "execution_id": identifier,
        "state": state,
        "outputs": {key: outputs[key] for key in sorted(selected)},
    }


async def node_summaries(
    connection: Connection, identifier: str, node_records: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    # Attach try histories to node states for the status response.
    #
    # Args:
    #     connection: The caller's snapshot transaction.
    #     identifier: The execution UUID as text.
    #     node_records: The selected page of node state records.
    #
    # Returns:
    #     Node summaries with ordered try histories and explicit node IDs.
    attempts_by_node: dict[str, list[dict[str, Any]]] = {
        node["node_id"]: [] for node in node_records
    }
    attempts = await load_tries(connection, identifier, list(attempts_by_node))
    for attempt in attempts:
        attempts_by_node[attempt.pop("node_id")].append(attempt)
    return [
        {
            "id": node["node_id"],
            "state": node["state"],
            "reason": node["reason"],
            "try_number": node["try_number"],
            "starts": node["starts"],
            "error": node["error"],
            "tries": attempts_by_node[node["node_id"]],
        }
        for node in node_records
    ]


def audit_pool(dsn: str) -> AsyncConnectionPool:
    # Create a small independent pool so audit writes have bounded capacity.
    #
    # Args:
    #     dsn: The connection URL for the audit database.
    #
    # Returns:
    #     A closed connection pool with short connection, acquisition, and statement limits.
    return AsyncConnectionPool(
        dsn,
        min_size=0,
        max_size=config.AUDIT_POOL_SIZE,
        timeout=config.AUDIT_POOL_TIMEOUT_SECONDS,
        open=False,
        kwargs={
            "autocommit": True,
            "connect_timeout": config.AUDIT_CONNECT_TIMEOUT_SECONDS,
            "options": f"-c statement_timeout={config.AUDIT_STATEMENT_TIMEOUT_MS}",
        },
    )


async def write_audit(pool: AsyncConnectionPool, record: dict[str, Any]) -> None:
    # Write one audit record within a short budget to isolate diagnostic failures.
    #
    # Args:
    #     pool: The independent audit connection pool.
    #     record: The request audit fields to insert.
    #
    # Returns:
    #     None. Storage failures produce a warning and discard this diagnostic record.
    try:
        async with (
            asyncio.timeout(config.AUDIT_WRITE_TIMEOUT_SECONDS),
            pool.connection() as connection,
        ):
            await insert_audit(connection, record)
    except Exception as exc:
        logging.getLogger(__name__).warning(
            "audit_storage_unavailable request_id=%s type=%s",
            record["request_id"],
            type(exc).__name__,
        )


async def _consume_audit(pool: AsyncConnectionPool, queue: asyncio.Queue[dict[str, Any]]) -> None:
    # Drain queued audit records without making HTTP requests wait for storage.
    #
    # Args:
    #     pool: The independent audit connection pool.
    #     queue: The bounded queue of request audit records.
    #
    # Returns:
    #     This loop does not return normally. Cancellation stops the consumer.
    while True:
        record = await queue.get()
        try:
            await write_audit(pool, record)
        finally:
            queue.task_done()


async def prune_audit(pool: AsyncConnectionPool) -> int:
    # Delete one bounded expired batch without blocking another cleanup worker.
    #
    # Args:
    #     pool: The independent audit connection pool.
    #
    # Returns:
    #     The number of deleted records, or zero when storage failure defers cleanup.
    try:
        async with (
            asyncio.timeout(config.AUDIT_WRITE_TIMEOUT_SECONDS),
            pool.connection() as connection,
            connection.transaction(),
        ):
            deleted = await connection.execute(
                """WITH expired AS (
                    SELECT request_id FROM request_audit
                    WHERE created_at < clock_timestamp() - (%s * interval '1 second')
                    ORDER BY created_at LIMIT %s FOR UPDATE SKIP LOCKED
                ) DELETE FROM request_audit audit USING expired
                WHERE audit.request_id=expired.request_id""",
                (config.AUDIT_RETENTION_SECONDS, config.AUDIT_PRUNE_BATCH),
            )
            return deleted.rowcount
    except Exception as exc:
        logging.getLogger(__name__).warning("audit_retention_deferred type=%s", type(exc).__name__)
        return 0


async def _prune_audit(pool: AsyncConnectionPool) -> None:
    # Repeat bounded retention work to remove expired audit records periodically.
    #
    # Args:
    #     pool: The independent audit connection pool.
    #
    # Returns:
    #     This loop does not return normally. Cancellation stops retention work.
    while True:
        deleted = await prune_audit(pool)
        # Full batches drain old rows promptly. Partial batches keep the normal interval.
        delay = 1 if deleted >= config.AUDIT_PRUNE_BATCH else config.AUDIT_PRUNE_INTERVAL_SECONDS
        await asyncio.sleep(delay)


@asynccontextmanager
async def audit_writer(
    pool: AsyncConnectionPool, queue: asyncio.Queue[dict[str, Any]]
) -> AsyncGenerator[None]:
    # Own audit consumers and retention work for the API service lifetime.
    #
    # Args:
    #     pool: The independent audit connection pool to open and close.
    #     queue: The bounded queue whose records drain briefly during shutdown.
    #
    # Yields:
    #     None while audit consumers and retention work are active.
    #     Shutdown permits a bounded drain, then cancels the background tasks.
    async with pool:
        tasks = [
            asyncio.create_task(_consume_audit(pool, queue)) for _ in range(config.AUDIT_POOL_SIZE)
        ]
        tasks.append(asyncio.create_task(_prune_audit(pool)))
        try:
            yield
        finally:
            try:
                await asyncio.wait_for(queue.join(), timeout=config.AUDIT_DRAIN_TIMEOUT_SECONDS)
            except TimeoutError:
                logging.getLogger(__name__).warning(
                    "audit_shutdown_dropped queued=%s", queue.qsize()
                )
            finally:
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
