# Orchestrator database commands own locks, state changes, and transaction boundaries.

import json
import random
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

from psycopg.types.json import Jsonb

import shared.state as state
from orchestrator.config import Settings
from orchestrator.templates import canonical_json, required_output_ids, resolve_node
from shared.context import Context
from shared.contracts import node_value_limit, parse_node
from shared.cpu import BoundedProcessPool
from shared.database import Connection
from shared.errors import DomainError
from shared.identifiers import execution_id
from shared.output import (
    INVALID_OUTPUT,
    PreparedOutput,
    prepare_result,
    prepare_value,
    safe_runtime,
)
from shared.state import error, finish_execution

TERMINAL = {"COMPLETED", "FAILED", "BLOCKED", "EXPIRED"}


async def schedule_maintenance(
    connection: Connection, execution: dict[str, Any], repair_grace: float | None
) -> None:
    # Schedule only the next execution timer so idle executions need no periodic scan. The caller must hold the execution row lock.
    #
    # Args:
    #     connection: Connection inside the locked execution transaction.
    #     execution: Current execution row with its deadline and state.
    #     repair_grace: Seconds before control-event repair, or None to exclude repair timers.
    #
    # Returns:
    #     None. A running execution receives its next maintenance time.
    #
    # Raises:
    #     psycopg.Error: A database operation fails.
    if execution["state"] != "RUNNING":
        return
    await connection.execute(
        """UPDATE executions SET maintenance_due=timers.due FROM (
        SELECT LEAST(
            %s::timestamptz,
            (SELECT min(next_retry_at) FROM nodes
             WHERE execution_id=%s AND state='RETRY_WAIT'),
            (SELECT min(lease_until) FROM tries
             WHERE execution_id=%s AND token IS NOT NULL AND NOT has_outcome),
            (SELECT min(created_at) + (%s::double precision * interval '1 second') FROM outbox
             WHERE execution_id=%s AND kind IN ('start','completion') AND applied_at IS NULL)
        ) AS due) timers
        WHERE id=%s AND maintenance_due IS DISTINCT FROM timers.due""",
        (
            execution["deadline_at"],
            execution["id"],
            execution["id"],
            repair_grace,
            execution["id"],
            execution["id"],
        ),
    )


async def block_descendants(connection: Connection, identifier: Any) -> None:
    # Block unstarted descendants of failed nodes so impossible branches cannot dispatch. The caller must hold the execution row lock.
    #
    # Args:
    #     connection: Connection inside the locked execution transaction.
    #     identifier: Execution identifier for the nodes to examine.
    #
    # Returns:
    #     None. Dependent nodes and the execution failure flag can change.
    #
    # Raises:
    #     psycopg.Error: A database operation fails.
    node_records = await (
        await connection.execute(
            "SELECT node_id,state,dependencies FROM nodes WHERE execution_id=%s", (identifier,)
        )
    ).fetchall()
    failed = {
        record["node_id"]
        for record in node_records
        if record["state"] in ("FAILED", "BLOCKED", "EXPIRED")
    }
    changed = True
    while changed:
        changed = False
        for record in node_records:
            if record["state"] in ("PENDING", "RETRY_WAIT") and any(
                parent_id in failed for parent_id in record["dependencies"]
            ):
                record["state"] = "BLOCKED"
                failed.add(record["node_id"])
                changed = True
                await connection.execute(
                    "UPDATE nodes SET state='BLOCKED',reason='UPSTREAM_FAILED',error=%s WHERE execution_id=%s AND node_id=%s",
                    (
                        Jsonb(error("UPSTREAM_FAILED", "A dependency failed.")),
                        identifier,
                        record["node_id"],
                    ),
                )
    if failed:
        await connection.execute(
            "UPDATE executions SET has_failure=true WHERE id=%s", (identifier,)
        )


async def finalize_execution(
    connection: Connection, execution: dict[str, Any], now: datetime
) -> None:
    # Finish an execution only after all nodes become terminal. The caller must hold the execution row lock.
    #
    # Args:
    #     connection: Connection inside the locked execution transaction.
    #     execution: Execution row whose state and terminal metadata can change.
    #     now: Current database time for terminal state and retention.
    #
    # Returns:
    #     None. A terminal graph becomes COMPLETED or FAILED.
    #
    # Raises:
    #     psycopg.Error: A database operation fails.
    summary = await (
        await connection.execute(
            """SELECT bool_and(state='COMPLETED') AS completed,
        bool_and(state=ANY(%s)) AS terminal FROM nodes WHERE execution_id=%s""",
            (list(TERMINAL), execution["id"]),
        )
    ).fetchone()
    if execution["state"] != "RUNNING" or not summary or not summary["terminal"]:
        return
    await finish_execution(
        connection, execution, "COMPLETED" if summary["completed"] else "FAILED", now
    )


async def mark_roots_ready(connection: Connection, identifier: Any) -> None:
    # Release pending nodes without dependencies after the start event. The caller must hold the execution row lock.
    #
    # Args:
    #     connection: Connection inside the locked execution transaction.
    #     identifier: Execution identifier for the root nodes.
    #
    # Returns:
    #     None. Eligible root nodes receive the READY reason.
    #
    # Raises:
    #     psycopg.Error: A database operation fails.
    await connection.execute(
        """UPDATE nodes SET reason='READY' WHERE execution_id=%s
        AND state='PENDING' AND reason='WAITING_DEPENDENCIES' AND cardinality(dependencies)=0""",
        (identifier,),
    )


async def mark_children_ready(
    connection: Connection, identifier: Any, completed_parent: str
) -> None:
    # The execution lock makes the parent check and child update one decision.
    # Release children only when every parent is complete. The execution row lock makes fan-in one serialized decision.
    #
    # Args:
    #     connection: Connection inside the locked execution transaction.
    #     identifier: Execution identifier for the graph.
    #     completed_parent: Node ID of the parent whose completion was just applied.
    #
    # Returns:
    #     None. Eligible children receive the READY reason and timestamp.
    #
    # Raises:
    #     psycopg.Error: A database operation fails.
    await connection.execute(
        """UPDATE nodes child SET reason='READY',ready_at=clock_timestamp()
        WHERE child.execution_id=%s AND child.state='PENDING' AND child.reason='WAITING_DEPENDENCIES'
        AND EXISTS (SELECT 1 FROM edges changed WHERE changed.execution_id=child.execution_id
                    AND changed.parent_id=%s AND changed.child_id=child.node_id)
        AND NOT EXISTS (SELECT 1 FROM edges edge JOIN nodes parent ON parent.execution_id=edge.execution_id
                        AND parent.node_id=edge.parent_id WHERE edge.execution_id=child.execution_id
                        AND edge.child_id=child.node_id AND parent.state<>'COMPLETED')""",
        (identifier, completed_parent),
    )


async def dispatch_ready(connection: Connection, execution: dict[str, Any], now: datetime) -> None:
    # Fill available execution slots with ready nodes. The caller must hold the execution row lock to preserve fan-in and slot limits.
    #
    # Args:
    #     connection: Connection inside the locked execution transaction.
    #     execution: Locked execution row with its graph, trigger, saved policy, and byte counters.
    #     now: Current database time for expiry and terminal decisions.
    #
    # Returns:
    #     None. Eligible nodes receive tries and task events, or input failures become terminal.
    #
    # Raises:
    #     psycopg.Error: A database operation fails.
    if execution["state"] != "RUNNING" or await state.expire_execution(connection, execution, now):
        return
    await mark_roots_ready(connection, execution["id"])
    count = await (
        await connection.execute(
            "SELECT count(*) AS reserved FROM nodes WHERE execution_id=%s AND (state='RUNNING' OR (state='PENDING' AND reason='QUEUED'))",
            (execution["id"],),
        )
    ).fetchone()
    assert count is not None
    available = int(execution["policy"]["execution_slots"]) - count["reserved"]
    definitions = {node["id"]: node for node in execution["definition"]["dag"]["nodes"]}
    while available > 0:
        candidates = await (
            await connection.execute(
                "SELECT * FROM nodes WHERE execution_id=%s AND state='PENDING' AND reason='READY' ORDER BY ready_at,node_id LIMIT %s",
                (execution["id"], available),
            )
        ).fetchall()
        if not candidates:
            break
        outputs = await load_required_outputs(connection, execution, candidates, definitions)
        for node in candidates:
            frozen = await freeze_node_input(
                connection, execution, node, definitions[node["node_id"]], outputs
            )
            if frozen is None:
                continue
            await queue_node_try(connection, execution, node, frozen)
            available -= 1
    await finalize_execution(connection, execution, now)


async def load_required_outputs(
    connection: Connection,
    execution: dict[str, Any],
    nodes: list[dict[str, Any]],
    definitions: dict[str, Any],
) -> dict[str, Any]:
    # Load only outputs needed by nodes without frozen inputs to keep transaction memory and queries bounded.
    #
    # Args:
    #     connection: Connection inside the locked execution transaction.
    #     execution: Locked execution row with its ID and normalized trigger.
    #     nodes: Ready node rows considered for this dispatch pass.
    #     definitions: Stored node definitions indexed by node ID.
    #
    # Returns:
    #     A mapping from required node IDs to their saved output values. Returns an empty mapping if none are needed.
    #
    # Raises:
    #     psycopg.Error: A database operation fails.
    required = set()
    for node in nodes:
        if not node["has_input"]:
            required.update(required_output_ids(definitions[node["node_id"]], execution["trigger"]))
    if not required:
        return {}
    records = await (
        await connection.execute(
            """SELECT node.node_id,attempt.value FROM nodes node JOIN tries attempt
        ON attempt.execution_id=node.execution_id AND attempt.node_id=node.node_id AND attempt.try_number=node.try_number
        WHERE node.execution_id=%s AND node.has_output AND node.node_id=ANY(%s)""",
            (execution["id"], sorted(required)),
        )
    ).fetchall()
    return {record["node_id"]: record["value"] for record in records}


async def freeze_node_input(
    connection: Connection,
    execution: dict[str, Any],
    node: dict[str, Any],
    definition: dict[str, Any],
    outputs: dict[str, Any],
) -> dict[str, Any] | None:
    # Resolve a node once and charge its input budget. The caller must hold the execution row lock.
    #
    # Args:
    #     connection: Connection inside the locked execution transaction.
    #     execution: Locked row with trigger, policy, and mutable input_bytes counter.
    #     node: Ready node row with handler and optional saved resolved_input.
    #     definition: Stored definition of this node.
    #     outputs: Required saved ancestor outputs indexed by node ID.
    #
    # Returns:
    #     The saved or newly resolved worker request. Returns None after a resolution failure marks the node and dependent branches.
    #
    # Raises:
    #     psycopg.Error: A database operation fails.
    if node["has_input"]:
        return node["resolved_input"]
    policy = execution["policy"]
    limit = node_value_limit(policy, node["handler"])
    try:
        frozen = resolve_node(
            parse_node(definition),
            execution["trigger"],
            outputs,
            max_bytes=limit,
            max_depth=int(policy["max_depth"]),
        )
        size = len(canonical_json(frozen).encode())
        if execution["input_bytes"] + size > policy["max_execution_bytes"]:
            raise DomainError(422, "INPUT_BUDGET", "Execution input capacity is full.")
    except (DomainError, ValueError, KeyError) as exc:
        code = exc.code if isinstance(exc, DomainError) else "TEMPLATE_ERROR"
        await connection.execute(
            "UPDATE nodes SET state='FAILED',reason='TEMPLATE_ERROR',error=%s WHERE execution_id=%s AND node_id=%s",
            (Jsonb(error(code, "Node input resolution failed.")), execution["id"], node["node_id"]),
        )
        await block_descendants(connection, execution["id"])
        return None
    execution["input_bytes"] += size
    await connection.execute(
        "UPDATE executions SET input_bytes=%s WHERE id=%s",
        (execution["input_bytes"], execution["id"]),
    )
    return frozen


async def queue_node_try(
    connection: Connection, execution: dict[str, Any], node: dict[str, Any], frozen: dict[str, Any]
) -> None:
    # The try and task event commit together. A crash cannot leave a queued node without its task event.
    # Store a new try and its task event in one transaction so queued work always has a durable dispatch record.
    #
    # Args:
    #     connection: Connection inside the execution transaction. The caller holds its row lock.
    #     execution: Locked running execution row.
    #     node: PENDING node with reason READY, selected by dispatch_ready.
    #     frozen: Resolved worker request kept unchanged across retries.
    #
    # Returns:
    #     None. The node becomes QUEUED with its new try and outbox event.
    #
    # Raises:
    #     psycopg.Error: A database operation fails.
    number = node["try_number"] + 1
    event_id = str(uuid4())
    await connection.execute(
        "INSERT INTO tries(execution_id,node_id,try_number,event_id) VALUES (%s,%s,%s,%s)",
        (execution["id"], node["node_id"], number, event_id),
    )
    await connection.execute(
        "UPDATE nodes SET reason='QUEUED',try_number=%s,resolved_input=%s,has_input=true,next_retry_at=NULL WHERE execution_id=%s AND node_id=%s",
        (number, Jsonb(frozen), execution["id"], node["node_id"]),
    )
    await state.add_event(
        connection, str(execution["id"]), "task", node["node_id"], number, event_id
    )


async def apply_event(context: Context[Settings], envelope: Mapping[str, Any]) -> None:
    # Apply a trusted notification under the execution lock so duplicate deliveries cannot repeat state changes.
    #
    # Args:
    #     context: Database access and local repair settings.
    #     envelope: Broker notification whose identity must match a stored outbox row.
    #
    # Returns:
    #     None. Invalid identities, missing executions, expired work, and already-applied events cause no further action.
    #
    # Raises:
    #     DomainError: An unsupported message version or another non-404/non-422 domain failure occurs.
    #     psycopg.Error: The state transaction fails.
    try:
        identifier = execution_id(envelope.get("execution_id", ""))
        async with context.db.tx() as connection:
            execution = await state.lock_execution(connection, identifier)
            event = await state.trusted_event(connection, envelope)
            if not event or event["applied_at"] is not None:
                return
            now = await state.database_time(connection)
            if (
                await state.expire_execution(connection, execution, now)
                or execution["state"] != "RUNNING"
            ):
                return
            if not await apply_control_event(connection, execution, event, now):
                return
            await schedule_maintenance(
                connection,
                execution,
                context.settings.repair_interval if context.settings.enable_repair else None,
            )
    except DomainError as exc:
        if exc.status in (404, 422):
            return
        raise


async def apply_control_event(
    connection: Connection, execution: dict[str, Any], event: dict[str, Any], now: datetime
) -> bool:
    # Apply broker-delivered and repaired events through one path. The caller must hold the execution row lock.
    #
    # Args:
    #     connection: Connection inside the locked execution transaction.
    #     execution: Locked running execution row.
    #     event: Stored outbox start or completion event.
    #     now: Current database time for state changes and the applied marker.
    #
    # Returns:
    #     True after a supported event is applied. Returns False for applied, unsupported, or inactive work.
    #
    # Raises:
    #     psycopg.Error: A database operation fails.
    if event["applied_at"] is not None or execution["state"] != "RUNNING":
        return False
    if event["kind"] == "start":
        await dispatch_ready(connection, execution, now)
    elif event["kind"] == "completion":
        await apply_outcome(connection, execution, event, now)
    else:
        return False
    await connection.execute(
        "UPDATE outbox SET applied_at=%s WHERE event_id=%s", (now, event["event_id"])
    )
    return True


async def apply_outcome(
    connection: Connection, execution: dict[str, Any], event: dict[str, Any], now: datetime
) -> None:
    # Turn a saved current outcome into node state and release newly ready work. The caller must hold the execution row lock.
    #
    # Args:
    #     connection: Connection inside the locked execution transaction.
    #     execution: Locked running execution row and saved policy.
    #     event: Stored completion event with node ID and try number.
    #     now: Current database time for retry, expiry, and terminal decisions.
    #
    # Returns:
    #     None. Missing, stale, unfinished, or non-running tries cause no change.
    #
    # Raises:
    #     psycopg.Error: A database operation fails.
    attempt = await (
        await connection.execute(
            "SELECT * FROM tries WHERE execution_id=%s AND node_id=%s AND try_number=%s",
            (execution["id"], event["node_id"], event["try_number"]),
        )
    ).fetchone()
    node = await (
        await connection.execute(
            "SELECT * FROM nodes WHERE execution_id=%s AND node_id=%s",
            (execution["id"], event["node_id"]),
        )
    ).fetchone()
    if (
        not attempt
        or not node
        or (not attempt["has_outcome"])
        or (node["try_number"] != attempt["try_number"])
        or (node["state"] != "RUNNING")
    ):
        return
    if attempt["success"]:
        await connection.execute(
            "UPDATE nodes SET state='COMPLETED',reason='SUCCESS',has_output=true,error=NULL WHERE execution_id=%s AND node_id=%s",
            (execution["id"], node["node_id"]),
        )
        await mark_children_ready(connection, execution["id"], node["node_id"])
    else:
        await apply_failure(connection, execution, node, attempt, now)
    await dispatch_ready(connection, execution, now)


async def apply_failure(
    connection: Connection,
    execution: dict[str, Any],
    node: dict[str, Any],
    attempt: dict[str, Any],
    now: datetime,
) -> None:
    # Schedule a bounded retry or fail the node and block its descendants. The caller must hold the execution row lock.
    #
    # Args:
    #     connection: Connection inside the locked execution transaction.
    #     execution: Locked execution row with saved retry limits and deadline.
    #     node: Current running node row with its start count.
    #     attempt: Finished try with its error and retryable flag.
    #     now: Current database time used to calculate the jittered retry time.
    #
    # Returns:
    #     None. The node becomes RETRY_WAIT or FAILED.
    #
    # Raises:
    #     psycopg.Error: A database operation fails.
    failure = attempt["error"] or error("INTERNAL_HANDLER_ERROR", "The handler failed.", True)
    policy = execution["policy"]
    delay = random.uniform(
        0, min(policy["retry_max"], policy["retry_base"] * 2 ** max(0, node["starts"] - 1))
    )
    retry_at = now + timedelta(seconds=delay)
    can_retry = failure.get("retryable", False) and node["starts"] < policy["max_starts"]
    if can_retry and retry_at < execution["deadline_at"]:
        await connection.execute(
            "UPDATE nodes SET state='RETRY_WAIT',reason='RETRY_DELAY',next_retry_at=%s,error=%s WHERE execution_id=%s AND node_id=%s",
            (retry_at, Jsonb(failure), execution["id"], node["node_id"]),
        )
        return
    if can_retry:
        failure = error("RETRY_DEADLINE_EXCEEDED", "The retry exceeds the execution deadline.")
    await connection.execute(
        "UPDATE nodes SET state='FAILED',reason='HANDLER_FAILURE',error=%s WHERE execution_id=%s AND node_id=%s",
        (Jsonb(failure), execution["id"], node["node_id"]),
    )
    await block_descendants(connection, execution["id"])


async def maintain_once(context: Context[Settings], *, checked_claims: list[str]) -> None:
    # Process a bounded batch of due executions. Row locks and SKIP LOCKED let replicas divide maintenance work.
    #
    # Args:
    #     context: Database access and local batch, retry, and repair settings.
    #     checked_claims: Expired try event IDs whose Redis results the runtime examined.
    #
    # Returns:
    #     None after the batch completes or no due execution remains.
    #
    # Raises:
    #     psycopg.Error: A maintenance query or state change fails.
    #     PoolTimeout: No database connection becomes available within the pool timeout.
    processed: list[str] = []
    for _ in range(context.settings.maintenance_batch):
        async with context.db.tx() as connection:
            execution = await (
                await connection.execute(
                    "SELECT * FROM executions WHERE maintenance_due<=clock_timestamp() AND NOT (id=ANY(%s::uuid[])) ORDER BY maintenance_due FOR UPDATE SKIP LOCKED LIMIT 1",
                    (processed,),
                )
            ).fetchone()
            if execution is None:
                break
            now = await state.database_time(connection)
            await maintain_execution(context, connection, execution, now, checked_claims)
            processed.append(str(execution["id"]))


async def maintain_execution(
    context: Context[Settings],
    connection: Connection,
    execution: dict[str, Any],
    now: datetime,
    checked_claims: list[str],
) -> None:
    # Apply due expiry, retry, retention, and repair work. The caller must hold the execution row lock.
    #
    # Args:
    #     context: Database access and local repair settings.
    #     connection: Connection inside the locked execution transaction.
    #     execution: Execution row whose state and maintenance time can change.
    #     now: Current database time for all decisions in this pass.
    #     checked_claims: Expired try event IDs whose Redis results the runtime examined.
    #
    # Returns:
    #     None. The execution can advance, expire, or be deleted.
    #
    # Raises:
    #     psycopg.Error: A database operation fails.
    await state.expire_execution(connection, execution, now)
    if execution["state"] in ("COMPLETED", "FAILED", "EXPIRED"):
        await remove_expired_execution(connection, execution, now)
    elif execution["state"] == "PENDING":
        due = execution["created_at"] + timedelta(seconds=execution["policy"]["pending_seconds"])
        await connection.execute(
            "UPDATE executions SET maintenance_due=%s WHERE id=%s", (due, execution["id"])
        )
    elif execution["state"] == "RUNNING":
        await close_expired_claims(connection, execution, now, checked_claims)
        await release_due_retries(connection, execution, now)
        if context.settings.enable_repair:
            await repair_control_events(
                connection, execution, now, context.settings.repair_interval
            )
        await schedule_maintenance(
            connection,
            execution,
            context.settings.repair_interval if context.settings.enable_repair else None,
        )


async def remove_expired_execution(
    connection: Connection, execution: dict[str, Any], now: datetime
) -> None:
    # Delete terminal data after retention or schedule its deletion time. The caller must hold the execution row lock.
    #
    # Args:
    #     connection: Connection inside the locked execution transaction.
    #     execution: Terminal execution row with its saved retention policy.
    #     now: Current database time for the retention comparison.
    #
    # Returns:
    #     None. Expired records are deleted, or maintenance_due changes.
    #
    # Raises:
    #     psycopg.Error: A database operation fails.
    if execution["terminal_at"] is not None:
        due = execution["terminal_at"] + timedelta(seconds=execution["policy"]["retention_seconds"])
        if now >= due:
            await connection.execute("DELETE FROM executions WHERE id=%s", (execution["id"],))
        else:
            await connection.execute(
                "UPDATE executions SET maintenance_due=%s WHERE id=%s", (due, execution["id"])
            )


async def close_expired_claims(
    connection: Connection, execution: dict[str, Any], now: datetime, checked_claims: list[str]
) -> None:
    # Store retryable failures for expired worker leases so lost workers cannot leave active tries forever.
    #
    # Args:
    #     connection: Connection inside the locked execution transaction.
    #     execution: Locked execution row for the active tries.
    #     now: Current database time for lease expiry and outcome timestamps.
    #     checked_claims: Expired try event IDs whose Redis results the runtime examined.
    #
    # Returns:
    #     None. Expired tries receive timeout outcomes and completion events.
    #
    # Raises:
    #     psycopg.Error: A database operation fails.
    expired = await (
        await connection.execute(
            """SELECT * FROM tries WHERE execution_id=%s AND token IS NOT NULL
        AND NOT has_outcome AND lease_until<=%s AND event_id=ANY(%s::uuid[])""",
            (execution["id"], now, checked_claims),
        )
    ).fetchall()
    for attempt in expired:
        await connection.execute(
            "UPDATE tries SET has_outcome=true,success=false,error=%s,finished_at=%s WHERE event_id=%s",
            (
                Jsonb(state.error("HANDLER_TIMEOUT", "The handler lease elapsed.", True)),
                now,
                attempt["event_id"],
            ),
        )
        await state.add_event(
            connection,
            str(execution["id"]),
            "completion",
            attempt["node_id"],
            attempt["try_number"],
        )


async def release_due_retries(
    connection: Connection, execution: dict[str, Any], now: datetime
) -> None:
    # Make delayed retries ready and refill execution slots. The caller must hold the execution row lock.
    #
    # Args:
    #     connection: Connection inside the locked execution transaction.
    #     execution: Locked execution row and saved limits.
    #     now: Current database time for retry eligibility.
    #
    # Returns:
    #     None. Due retries can create new task events.
    #
    # Raises:
    #     psycopg.Error: A database operation fails.
    updated = await connection.execute(
        "UPDATE nodes SET state='PENDING',reason='READY',ready_at=%s,next_retry_at=NULL WHERE execution_id=%s AND state='RETRY_WAIT' AND next_retry_at<=%s",
        (now, execution["id"], now),
    )
    if updated.rowcount:
        await dispatch_ready(connection, execution, now)


async def repair_control_events(
    connection: Connection, execution: dict[str, Any], now: datetime, grace: float
) -> None:
    # Apply old unapplied control events from PostgreSQL so broker loss cannot strand an execution.
    #
    # Args:
    #     connection: Connection inside the locked execution transaction.
    #     execution: Locked running execution row.
    #     now: Current database time for event age and state changes.
    #     grace: Minimum event age in seconds before database repair.
    #
    # Returns:
    #     None. Eligible start and completion events can advance the execution.
    #
    # Raises:
    #     psycopg.Error: A database operation fails.
    events = await (
        await connection.execute(
            "SELECT * FROM outbox WHERE execution_id=%s AND kind IN ('start','completion') AND applied_at IS NULL AND created_at<=%s ORDER BY created_at",
            (execution["id"], now - timedelta(seconds=grace)),
        )
    ).fetchall()
    for event in events:
        if execution["state"] != "RUNNING":
            break
        await apply_control_event(connection, execution, event, now)


async def event_is_eligible(
    connection: Connection, execution: dict[str, Any], event: dict[str, Any]
) -> bool:
    # Examine current state before publication so obsolete tasks and completions stay unpublished.
    #
    # Args:
    #     connection: Connection inside the locked execution transaction.
    #     execution: Locked execution row with its current state.
    #     event: Outbox row whose node, try, kind, and applied marker need examination.
    #
    # Returns:
    #     True if the unapplied event still matches live execution state. Otherwise, False.
    #
    # Raises:
    #     psycopg.Error: A database operation fails.
    if execution["state"] != "RUNNING" or event["applied_at"] is not None:
        return False
    if event["kind"] == "start":
        return True
    current = await (
        await connection.execute(
            """SELECT n.state,n.reason,t.token,t.has_outcome
            FROM nodes n JOIN tries t USING (execution_id,node_id,try_number)
            WHERE n.execution_id=%s AND n.node_id=%s AND n.try_number=%s""",
            (execution["id"], event["node_id"], event["try_number"]),
        )
    ).fetchone()
    if not current:
        return False
    if event["kind"] == "task":
        return (
            current["state"] == "PENDING"
            and current["reason"] == "QUEUED"
            and current["token"] is None
            and not current["has_outcome"]
        )
    return current["state"] == "RUNNING" and current["has_outcome"]


async def reserve_publication(
    context: Context[Settings], item: dict[str, Any]
) -> tuple[dict[str, Any], str] | None:
    # Claim a short publication lease under the execution lock so relays coordinate broker I/O outside the transaction.
    #
    # Lease expiry can permit repeated publication of the same event.
    #
    # Args:
    #     context: Database access and publication lease duration.
    #     item: Candidate with event_id and execution_id from the outbox scan.
    #
    # Returns:
    #     The outbox row and new lease-owner token, or None if another publisher owns it or it is obsolete.
    #
    # Raises:
    #     psycopg.Error: The reservation transaction fails.
    #     DomainError: An execution error other than missing data occurs.
    owner = str(uuid4())
    async with context.db.tx() as connection:
        try:
            execution = await state.lock_execution(connection, str(item["execution_id"]))
        except DomainError as exc:
            if exc.status == 404:
                return None
            raise
        now = await state.database_time(connection)
        event = await (
            await connection.execute("SELECT * FROM outbox WHERE event_id=%s", (item["event_id"],))
        ).fetchone()
        if (
            not event
            or event["published_id"]
            or (event["lease_until"] and event["lease_until"] > now)
        ):
            return None
        if await state.expire_execution(connection, execution, now) or not await event_is_eligible(
            connection, execution, event
        ):
            return None
        await connection.execute(
            "UPDATE outbox SET lease_owner=%s,lease_until=%s WHERE event_id=%s",
            (owner, now + timedelta(seconds=context.settings.publication_lease), event["event_id"]),
        )
        if event["kind"] == "task":
            row = await (
                await connection.execute(
                    "SELECT resolved_input,handler FROM nodes WHERE execution_id=%s AND node_id=%s",
                    (event["execution_id"], event["node_id"]),
                )
            ).fetchone()
            assert row is not None
            event.update(
                input=row["resolved_input"],
                deadline_ms=_milliseconds(execution["deadline_at"]),
                handler_timeout=execution["policy"]["handler_timeout"],
                max_output_bytes=node_value_limit(execution["policy"], row["handler"]),
                max_depth=execution["policy"]["max_depth"],
            )
    return event, owner


async def record_publication(
    context: Context[Settings], event: dict[str, Any], owner: str, entry_id: str
) -> None:
    # Save a Redis receipt only while the publisher still owns its reservation. This prevents stale publishers from replacing newer receipts.
    #
    # Args:
    #     context: Database access for the receipt transaction.
    #     event: Reserved outbox row with event_id and execution_id.
    #     owner: Publication lease-owner token from reserve_publication.
    #     entry_id: Redis stream entry ID returned by publication.
    #
    # Returns:
    #     None. Missing executions and lost ownership cause no update.
    #
    # Raises:
    #     psycopg.Error: The receipt transaction fails.
    #     DomainError: An execution error other than missing data occurs.
    async with context.db.tx() as connection:
        try:
            await state.lock_execution(connection, str(event["execution_id"]))
        except DomainError as exc:
            if exc.status == 404:
                return
            raise
        now = await state.database_time(connection)
        await connection.execute(
            """UPDATE outbox SET published_id=%s,published_at=%s,checked_at=%s,
            lease_owner=NULL,lease_until=NULL,publications=publications+1 WHERE event_id=%s AND lease_owner=%s""",
            (entry_id, now, now, event["event_id"], owner),
        )


async def repair_publication(
    context: Context[Settings], item: dict[str, Any], present: bool
) -> None:
    # Apply a broker probe only if its receipt remains current. The execution lock protects against relay changes during the probe.
    #
    # Args:
    #     context: Database access for the repair transaction.
    #     item: Outbox snapshot used for the broker probe.
    #     present: Whether that published entry still contains the expected event ID.
    #
    # Returns:
    #     None. A live receipt gets a timestamp, or a missing eligible receipt becomes publishable again.
    #
    # Raises:
    #     psycopg.Error: The repair transaction fails.
    #     DomainError: An execution error other than missing data occurs.
    async with context.db.tx() as connection:
        try:
            execution = await state.lock_execution(connection, str(item["execution_id"]))
        except DomainError as exc:
            if exc.status == 404:
                return
            raise
        now = await state.database_time(connection)
        event = await (
            await connection.execute("SELECT * FROM outbox WHERE event_id=%s", (item["event_id"],))
        ).fetchone()
        # Another relay can change this event while Redis answers the probe.
        if (
            not event
            or event["published_id"] != item["published_id"]
            or (not await event_is_eligible(connection, execution, event))
        ):
            return
        if present:
            await connection.execute(
                "UPDATE outbox SET checked_at=%s WHERE event_id=%s", (now, event["event_id"])
            )
        elif event["lease_until"] is None or event["lease_until"] <= now:
            await connection.execute(
                "UPDATE outbox SET published_id=NULL,lease_owner=NULL,lease_until=NULL,checked_at=%s WHERE event_id=%s",
                (now, event["event_id"]),
            )


async def publication_candidates(context: Context[Settings]) -> list[dict[str, Any]]:
    # Read one bounded batch of unpublished events before broker I/O.
    #
    # Args:
    #     context: Database access, batch limit, and publication repair interval.
    #
    # Returns:
    #     Candidate event and execution IDs, oldest first.
    #
    # Raises:
    #     psycopg.Error: The candidate query fails.
    async with context.db.tx() as connection:
        candidates = await (
            await connection.execute(
                """SELECT event_id,execution_id FROM outbox
            WHERE applied_at IS NULL AND published_id IS NULL AND (lease_until IS NULL OR lease_until<=clock_timestamp())
            ORDER BY created_at LIMIT %s""",
                (context.settings.maintenance_batch,),
            )
        ).fetchall()
    return candidates


async def publication_checks(context: Context[Settings]) -> list[dict[str, Any]]:
    # Read old publication receipts before broker probes.
    #
    # Args:
    #     context: Database access, batch limit, and publication repair interval.
    #
    # Returns:
    #     Outbox snapshots with their current publication receipts.
    #
    # Raises:
    #     psycopg.Error: The candidate query fails.
    async with context.db.tx() as connection:
        candidates = await (
            await connection.execute(
                "SELECT * FROM outbox WHERE applied_at IS NULL AND published_id IS NOT NULL\n                AND (checked_at IS NULL OR checked_at<=clock_timestamp() - (%s * interval '1 second'))\n                ORDER BY checked_at NULLS FIRST LIMIT %s",
                (context.settings.repair_interval, context.settings.maintenance_batch),
            )
        ).fetchall()
    return candidates


async def _load_node(connection: Connection, event: dict[str, Any]) -> dict[str, Any] | None:
    # Read the event's node so claim guards use current database state.
    #
    # Args:
    #     connection: Connection in the caller's transaction.
    #     event: Trusted task record with execution and node IDs.
    #
    # Returns:
    #     The node row, or None if the node no longer exists.
    return await (
        await connection.execute(
            "SELECT * FROM nodes WHERE execution_id=%s AND node_id=%s",
            (event["execution_id"], event["node_id"]),
        )
    ).fetchone()


async def _load_attempt(connection: Connection, event: dict[str, Any]) -> dict[str, Any] | None:
    # Read claim and outcome metadata to avoid stored-result decoding.
    #
    # Args:
    #     connection: Connection in the caller's transaction.
    #     event: Trusted task record with its event ID.
    #
    # Returns:
    #     The try row, or None if no try matches the event.
    return await (
        await connection.execute(
            """SELECT event_id,try_number,token,has_outcome,success,started_at,lease_until
               FROM tries WHERE event_id=%s""",
            (event["event_id"],),
        )
    ).fetchone()


def _can_claim(
    execution: dict[str, Any], node: dict[str, Any] | None, attempt: dict[str, Any] | None
) -> bool:
    # Determine whether this queued try can receive its first owner.
    #
    # Args:
    #     execution: Locked execution row with its saved start limit.
    #     node: Current node row, or None if absent.
    #     attempt: Current try row, or None if absent.
    #
    # Returns:
    #     True for an unclaimed current try within its start limit. Otherwise False.
    return bool(
        node
        and attempt
        and node["state"] == "PENDING"
        and node["reason"] == "QUEUED"
        and node["try_number"] == attempt["try_number"]
        and not attempt["token"]
        and not attempt["has_outcome"]
        and node["starts"] < execution["policy"]["max_starts"]
    )


async def _matches_outcome(
    connection: Connection,
    attempt: dict[str, Any],
    prepared: PreparedOutput,
) -> bool:
    # Compare a repeated result with the accepted result through JSONB equality.
    #
    # Args:
    #     connection: Connection in the caller's locked transaction.
    #     attempt: Try metadata with an accepted outcome.
    #     prepared: Immutable candidate result or error JSON.
    #
    # Returns:
    #     True if success and JSON content match. Otherwise False.
    if attempt["success"] is not prepared.success or prepared.payload is None:
        return False
    # JSONB keeps booleans distinct from numbers and normalizes numeric notation.
    column = "value" if prepared.success else "error"
    result = await (
        await connection.execute(
            f"SELECT {column} = %s::jsonb AS matches FROM tries WHERE event_id=%s",
            (prepared.payload, attempt["event_id"]),
        )
    ).fetchone()
    assert result is not None
    return bool(result["matches"])


@dataclass(frozen=True)
class _Outcome:
    # Keep the database-ready outcome after policy and aggregate-budget decisions.
    #
    # Args:
    #     success: Whether the database accepts a successful result.
    #     value_json: Encoded result, or None for failure.
    #     error_json: Encoded failure, or None for success.
    #     output_bytes: Successful output bytes to add to the execution budget.
    #
    # Returns:
    #     An immutable outcome for one database write.

    success: bool
    value_json: str | None = None
    error_json: str | None = None
    output_bytes: int = 0


def _prepare_outcome(
    execution: dict[str, Any],
    node: dict[str, Any],
    prepared: PreparedOutput,
) -> _Outcome:
    # Apply saved limits to a prepared result while the execution lock protects its budget.
    #
    # Args:
    #     execution: Locked row with the saved policy and consumed output bytes.
    #     node: Node metadata that identifies its handler and value limit.
    #     prepared: Encoded output with its byte size and container depth.
    #
    # Returns:
    #     The accepted result or error. Invalid or excessive output becomes a permanent failure.
    if not prepared.success and prepared.payload is not None:
        return _Outcome(False, error_json=prepared.payload)
    policy = execution["policy"]
    if (
        prepared.payload is not None
        and prepared.output_bytes <= node_value_limit(policy, node["handler"])
        and prepared.depth <= policy["max_depth"]
        and execution["output_bytes"] + prepared.output_bytes <= policy["max_execution_bytes"]
    ):
        return _Outcome(True, prepared.payload, output_bytes=prepared.output_bytes)
    return _Outcome(False, error_json=_INVALID_OUTPUT_JSON)


async def _record_outcome(
    connection: Connection,
    event: dict[str, Any],
    outcome: _Outcome,
    runtime: dict[str, Any] | None,
    now: datetime,
) -> dict[str, Any]:
    # Save the outcome and completion event in the same locked transaction.
    #
    # Args:
    #     connection: Connection whose caller holds the execution lock.
    #     event: Trusted task identity for the owned try.
    #     outcome: Result after policy and aggregate-budget decisions.
    #     runtime: Optional process ID and start and finish times.
    #     now: Database time for the recorded finish.
    #
    # Returns:
    #     None. The caller must commit to make both writes durable.
    metadata = safe_runtime(runtime)
    await connection.execute(
        """UPDATE tries SET has_outcome=true,success=%s,value=%s::jsonb,error=%s::jsonb,
           runtime=%s,finished_at=%s WHERE event_id=%s""",
        (
            outcome.success,
            outcome.value_json,
            outcome.error_json,
            Jsonb(metadata) if metadata else None,
            now,
            event["event_id"],
        ),
    )
    await connection.execute(
        "UPDATE executions SET output_bytes=output_bytes+%s WHERE id=%s",
        (outcome.output_bytes, event["execution_id"]),
    )
    identifier = await state.add_event(
        connection, str(event["execution_id"]), "completion", event["node_id"], event["try_number"]
    )
    return {**event, "event_id": identifier, "kind": "completion", "applied_at": None}


async def store_outcome(
    context: Context[Settings],
    envelope: Mapping[str, Any],
    token: str,
    success: bool,
    value: Any = None,
    error: dict[str, Any] | None = None,
    runtime: dict[str, Any] | None = None,
    *,
    prepared: PreparedOutput | None = None,
) -> bool:
    # Store an owned outcome only while its lease, identity, and aggregate budget permit it.
    #
    # Args:
    #     context: Worker database and settings.
    #     envelope: Task identity that must match the stored event.
    #     token: Owner token returned by the committed claim.
    #     success: Whether the handler reports success.
    #     value: Native successful output, which can be JSON null.
    #     error: Failure details when success is False.
    #     runtime: Optional process ID and start and finish times.
    #     prepared: Existing immutable output. If absent, prepare it before the transaction.
    #
    # Returns:
    #     True if the outcome is stored or matches an accepted outcome. Otherwise False.
    #
    # Raises:
    #     ValueError: Prepared success disagrees with the supplied result.
    #     DomainError: An identifier or message version is invalid, or CPU capacity is unavailable.
    #     psycopg.Error: A database operation fails.
    identifier = execution_id(envelope.get("execution_id", ""))
    if prepared is None:
        prepared = await prepare_result((success, value, error, runtime), pool=output_pool(context))
    if prepared.success is not success:
        raise ValueError("Prepared outcome success does not match the result")
    async with context.db.tx() as connection:
        try:
            execution = await state.lock_execution(connection, identifier)
        except DomainError as exc:
            if exc.status == 404:
                return False
            raise
        event = await state.trusted_event(connection, {**envelope, "kind": "task"})
        if not event or event["kind"] != "task":
            return False
        attempt = await _load_attempt(connection, event)
        if attempt and not attempt["token"]:
            await _accept_started(connection, execution, event, envelope)
            attempt = await _load_attempt(connection, event)
        if not attempt or str(attempt["token"]) != token:
            return False
        if attempt["has_outcome"]:
            # A repeated result must match the stored result. It cannot replace that result.
            return await _matches_outcome(connection, attempt, prepared)
        now = await state.database_time(connection)
        node = await _live_outcome_node(connection, execution, event, attempt, now, envelope)
        if node is None:
            return False
        outcome = _prepare_outcome(execution, node, prepared)
        completion = await _record_outcome(connection, event, outcome, runtime, now)
        await apply_control_event(connection, execution, completion, now)
        await schedule_maintenance(
            connection,
            execution,
            context.settings.repair_interval if context.settings.enable_repair else None,
        )
    return True


_INVALID_OUTPUT_JSON = prepare_value(False, None, INVALID_OUTPUT).payload


def output_pool(context: Context[Settings]) -> BoundedProcessPool:
    # Reuse a bounded CPU pool for output validation during this orchestrator lifetime.
    # Args: context owns database cleanup and local service settings.
    # Returns: The existing pool, or a lazy pool registered for cleanup.
    pool = getattr(context.db, "_orchestrator_output_pool", None)
    if pool is None:
        pool = BoundedProcessPool()
        context.db._orchestrator_output_pool = pool  # type: ignore[attr-defined]
        context.db.add_cleanup(pool.close)
    return pool


def _prepare_report(success: bool, payload: str) -> PreparedOutput:
    # Decode and measure reported JSON before the execution transaction.
    # Args: success selects result or error content, and payload contains worker JSON.
    # Returns: An immutable snapshot. Invalid content produces an empty snapshot.
    try:
        value = json.loads(payload)
        if not success and not isinstance(value, dict):
            return PreparedOutput(success, None)
        return prepare_value(success, value if success else None, None if success else value)
    except ValueError, TypeError, RecursionError:
        return PreparedOutput(success, None)


def _report_identity(envelope: Mapping[str, Any]) -> bool:
    # Reject malformed worker ownership fields before SQL or timestamp conversion.
    # Args: envelope contains task identity, owner token, and Redis lease timestamps.
    # Returns: True for supported ownership fields. Otherwise False.
    try:
        UUID(envelope["token"])
        return (
            all(
                type(envelope.get(key)) is int and 0 < envelope[key] < 253402300799000
                for key in ("started_ms", "lease_ms")
            )
            and envelope["started_ms"] < envelope["lease_ms"]
        )
    except ValueError, TypeError, KeyError, AttributeError:
        return False


async def _accept_started(
    connection: Connection,
    execution: dict[str, Any],
    event: dict[str, Any],
    envelope: Mapping[str, Any],
) -> bool:
    # Record a Redis claim under the execution lock. A repeated claim cannot change its owner.
    # Args: connection holds the lock, execution and event identify current work, and envelope contains Redis ownership.
    # Returns: True for an accepted current claim. Otherwise False.
    node = await _load_node(connection, event)
    attempt = await _load_attempt(connection, event)
    if not attempt or not node or execution["state"] != "RUNNING":
        return False
    if attempt["token"]:
        return (
            str(attempt["token"]) == envelope["token"]
            and node["try_number"] == attempt["try_number"]
        )
    if not _can_claim(execution, node, attempt) or not _report_identity(envelope):
        return False
    started = datetime.fromtimestamp(envelope["started_ms"] / 1000, UTC)
    lease = datetime.fromtimestamp(envelope["lease_ms"] / 1000, UTC)
    if lease > execution["deadline_at"] or lease > started + timedelta(
        seconds=execution["policy"]["handler_timeout"]
    ):
        return False
    await connection.execute(
        "UPDATE tries SET token=%s,started_at=%s,lease_until=%s WHERE event_id=%s",
        (envelope["token"], started, lease, event["event_id"]),
    )
    await connection.execute(
        "UPDATE nodes SET state='RUNNING',reason='HANDLER',starts=starts+1 WHERE execution_id=%s AND node_id=%s",
        (execution["id"], node["node_id"]),
    )
    await connection.execute(
        "UPDATE outbox SET applied_at=%s WHERE event_id=%s", (started, event["event_id"])
    )
    await state.advance_maintenance(connection, execution["id"], lease)
    return True


async def record_started(context: Context[Settings], envelope: Mapping[str, Any]) -> bool:
    # Persist a worker start report without a worker database connection.
    # Args: context supplies database access, and envelope contains the Redis claim.
    # Returns: True for an accepted claim. Stale, expired, or malformed reports return False.
    if not _report_identity(envelope):
        return False
    try:
        identifier = execution_id(envelope.get("execution_id", ""))
        async with context.db.tx() as connection:
            execution = await state.lock_execution(connection, identifier)
            now = await state.database_time(connection)
            if await state.expire_execution(connection, execution, now):
                return False
            event = await state.trusted_event(connection, {**envelope, "kind": "task"})
            if not event:
                return False
            return await _accept_started(connection, execution, event, envelope)
    except DomainError as exc:
        if exc.status in (404, 422):
            return False
        raise


async def apply_worker_event(context: Context[Settings], envelope: Mapping[str, Any]) -> bool:
    # Turn Redis start and result reports into durable workflow state.
    # Args: context supplies database and CPU resources, and envelope contains a worker report.
    # Returns: True for an accepted report. Invalid or obsolete reports return False.
    if not _report_identity(envelope):
        return False
    if envelope["kind"] == "started":
        return await record_started(context, envelope)
    if type(envelope.get("success")) is not bool or not isinstance(envelope.get("payload"), str):
        return False
    reported = envelope.get("reported_ms")
    if type(reported) is not int or not envelope["started_ms"] <= reported < envelope["lease_ms"]:
        return False
    payload = envelope["payload"]
    prepared = (
        await output_pool(context).run(_prepare_report, envelope["success"], payload)
        if len(payload) > 16_384
        else _prepare_report(envelope["success"], payload)
    )
    try:
        return await store_outcome(
            context,
            envelope,
            envelope["token"],
            envelope["success"],
            runtime=envelope.get("runtime"),
            prepared=prepared,
        )
    except DomainError as exc:
        if exc.status in (404, 422):
            return False
        raise


async def expired_worker_claims(context: Context[Settings]) -> list[dict[str, Any]]:
    # Read expired claims before Redis result recovery so maintenance cannot discard a queued result.
    # Args: context supplies the database and bounded maintenance batch.
    # Returns: A bounded list of expired current tries without a stored outcome.
    async with context.db.tx() as connection:
        return await (
            await connection.execute(
                """SELECT t.* FROM tries t JOIN executions e ON e.id=t.execution_id
               WHERE e.state='RUNNING' AND t.token IS NOT NULL AND NOT t.has_outcome
               AND t.lease_until<=clock_timestamp() ORDER BY t.lease_until LIMIT %s""",
                (context.settings.maintenance_batch,),
            )
        ).fetchall()


async def _live_outcome_node(
    connection: Connection,
    execution: dict[str, Any],
    event: dict[str, Any],
    attempt: dict[str, Any],
    now: datetime,
    envelope: Mapping[str, Any],
) -> dict[str, Any] | None:
    # Accept an on-time Redis result even when orchestrator processing happens after its lease.
    # Args: connection holds the execution lock, current rows identify the try, now is database time, and envelope reports finish time.
    # Returns: The current running node, or None for expired, superseded, or late work.
    reported = envelope.get("reported_ms")
    if (
        execution["state"] != "RUNNING"
        or await state.expire_execution(connection, execution, now)
        or type(reported) is not int
        or not _milliseconds(attempt["started_at"])
        <= reported
        < _milliseconds(attempt["lease_until"])
    ):
        return None
    node = await _load_node(connection, event)
    if not node or node["try_number"] != attempt["try_number"] or node["state"] != "RUNNING":
        return None
    return node


def _milliseconds(value: datetime) -> int:
    # Convert a database timestamp without float rounding at lease boundaries.
    # Args: value is a timezone-aware database time.
    # Returns: Whole milliseconds since the Unix epoch.
    return (value - datetime(1970, 1, 1, tzinfo=UTC)) // timedelta(milliseconds=1)
