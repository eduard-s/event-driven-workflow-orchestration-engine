# Execution locks protect shared state changes.

from collections.abc import Mapping
from datetime import datetime, timedelta
from typing import Any, Literal
from uuid import UUID, uuid4

from psycopg.types.json import Jsonb

from shared.database import Connection
from shared.errors import DomainError


def error(code: str, message: str, retryable: bool = False) -> dict[str, Any]:
    # Create one failure shape so saved node errors use consistent fields.
    #
    # Args:
    #     code: Stable machine-readable failure code.
    #     message: Brief explanation for workflow status and results.
    #     retryable: True permits the orchestrator to consider a new try.
    #
    # Returns:
    #     A dictionary with code, message, and retryable fields.
    return {"code": code, "message": message, "retryable": retryable}


async def database_time(connection: Connection) -> datetime:
    # Read the database clock so replicas compare leases and deadlines against one clock.
    #
    # Args:
    #     connection: Connection in the caller's transaction.
    #
    # Returns:
    #     The current timezone-aware PostgreSQL timestamp.
    #
    # Raises:
    #     psycopg.Error: The clock query fails.
    row = await (await connection.execute("SELECT clock_timestamp() AS now")).fetchone()
    assert row is not None
    return row["now"]


async def lock_execution(connection: Connection, identifier: str) -> dict[str, Any]:
    # Lock an execution row so replicas cannot make conflicting changes to its nodes.
    #
    # Args:
    #     connection: Connection in a transaction that holds the lock until commit or rollback.
    #     identifier: Execution UUID as text.
    #
    # Returns:
    #     The locked execution row as a dictionary.
    #
    # Raises:
    #     DomainError: No execution exists for the identifier.
    #     psycopg.Error: The query or lock wait fails.
    record = await (
        await connection.execute("SELECT * FROM executions WHERE id=%s FOR UPDATE", (identifier,))
    ).fetchone()
    if record is None:
        raise DomainError(404, "NOT_FOUND", "Execution does not exist.")
    return record


async def advance_maintenance(connection: Connection, identifier: Any, due: datetime) -> None:
    # Move maintenance earlier when a new timer or event needs prompt recovery.
    #
    # Args:
    #     connection: Connection in the caller's locked transaction.
    #     identifier: Execution UUID accepted by PostgreSQL.
    #     due: Proposed maintenance time. A later time does not delay existing work.
    #
    # Returns:
    #     None. Only a running execution with a later due time changes.
    await connection.execute(
        "UPDATE executions SET maintenance_due=%s WHERE id=%s AND state='RUNNING' AND maintenance_due>%s",
        (due, identifier, due),
    )


async def add_event(
    connection: Connection,
    identifier: str,
    kind: str,
    node: str = "",
    number: int = 0,
    event: str | None = None,
) -> str:
    # Save a unique outbox identity with the state change so committed work can reach the broker after a crash.
    #
    # Args:
    #     connection: Connection in the caller's state transaction.
    #     identifier: Execution UUID as text.
    #     kind: Start, task, or completion event kind.
    #     node: Node ID, or empty text for a start event.
    #     number: Try number, or zero for a start event.
    #     event: Proposed event UUID, or None to create one.
    #
    # Returns:
    #     The stored event UUID. A repeated logical event returns its existing identity.
    event = event or str(uuid4())
    record = await (
        await connection.execute(
            "INSERT INTO outbox(event_id,execution_id,kind,node_id,try_number)\n               VALUES (%s,%s,%s,%s,%s) ON CONFLICT(execution_id,kind,node_id,try_number)\n               DO UPDATE SET kind=EXCLUDED.kind RETURNING event_id,created_at",
            (event, identifier, kind, node, number),
        )
    ).fetchone()
    assert record is not None
    if kind in ("start", "completion"):
        # Only the orchestrator knows its repair grace. Wake it to schedule this event.
        await advance_maintenance(connection, identifier, record["created_at"])
    return str(record["event_id"])


async def expire_execution(
    connection: Connection, execution: dict[str, Any], now: datetime
) -> bool:
    # Apply pending or running deadlines so expired work cannot restart through a late event.
    #
    # The caller must hold the execution lock.
    #
    # Args:
    #     connection: Connection in the caller's locked transaction.
    #     execution: Execution row with its saved deadlines and policy.
    #     now: Current database time.
    #
    # Returns:
    #     True if the execution is already expired or becomes expired. Otherwise False.
    due = (
        execution["state"] == "PENDING"
        and now
        >= execution["created_at"] + timedelta(seconds=execution["policy"]["pending_seconds"])
        or (execution["state"] == "RUNNING" and now >= execution["deadline_at"])
    )
    if not due:
        return execution["state"] == "EXPIRED"
    await finish_execution(connection, execution, "EXPIRED", now)
    await connection.execute(
        "UPDATE nodes SET state='EXPIRED',reason='EXECUTION_DEADLINE',error=%s WHERE execution_id=%s AND state NOT IN ('COMPLETED','FAILED','BLOCKED','EXPIRED')",
        (Jsonb(error("EXECUTION_DEADLINE", "The execution deadline elapsed.")), execution["id"]),
    )
    return True


async def finish_execution(
    connection: Connection,
    execution: dict[str, Any],
    status: Literal["COMPLETED", "FAILED", "EXPIRED"],
    now: datetime,
) -> None:
    # Save terminal state, retention time, and event markers together so cleanup sees one consistent result.
    #
    # The caller must hold the execution lock.
    #
    # Args:
    #     connection: Connection in the caller's locked transaction.
    #     execution: Execution row. This dictionary also receives the terminal values.
    #     status: COMPLETED, FAILED, or EXPIRED terminal state.
    #     now: Database time used for completion and retention.
    #
    # Returns:
    #     None after the execution and unapplied event markers change.
    due = now + timedelta(seconds=execution["policy"]["retention_seconds"])
    await connection.execute(
        "UPDATE executions SET state=%s,terminal_at=%s,has_failure=%s,maintenance_due=%s WHERE id=%s",
        (status, now, status != "COMPLETED", due, execution["id"]),
    )
    await connection.execute(
        "UPDATE outbox SET applied_at=%s WHERE execution_id=%s AND applied_at IS NULL",
        (now, execution["id"]),
    )
    execution.update(
        state=status, terminal_at=now, has_failure=status != "COMPLETED", maintenance_due=due
    )


async def trusted_event(
    connection: Connection, envelope: Mapping[str, Any]
) -> dict[str, Any] | None:
    # Match a broker envelope to its durable outbox record so messages cannot invent execution state changes.
    #
    # Args:
    #     connection: Connection in the caller's transaction.
    #     envelope: Proposed version, event identity, execution identity, and node try identity.
    #
    # Returns:
    #     The matching outbox row, or None for malformed or inconsistent identity fields.
    #
    # Raises:
    #     DomainError: The envelope declares an unsupported schema version.
    #     psycopg.Error: The durable event lookup fails.
    version = envelope.get("schema_version")
    if type(version) is not int:
        return None
    if version != 1:
        raise DomainError(503, "MESSAGE_VERSION", "Unsupported message version.")
    try:
        event = str(UUID(envelope["event_id"]))
        identifier = str(UUID(envelope["execution_id"]))
    except ValueError, TypeError, KeyError, AttributeError:
        return None
    record = await (
        await connection.execute(
            "SELECT * FROM outbox WHERE event_id=%s AND execution_id=%s", (event, identifier)
        )
    ).fetchone()
    if not record or record["kind"] != envelope.get("kind"):
        return None
    if record["kind"] != "start" and (
        type(envelope.get("try_number")) is not int
        or record["node_id"] != envelope.get("node_id")
        or record["try_number"] != envelope.get("try_number")
    ):
        return None
    return record
