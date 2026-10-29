"""Fixtures for Redis claims and orchestrator-owned result persistence."""

import time
from typing import Any

from orchestrator import storage
from orchestrator.broker import Broker as OrchestratorBroker
from orchestrator.runtime import Runtime
from shared.contracts import node_value_limit
from shared.output import prepare_result
from worker.broker import Broker


async def task_payload(context, envelope) -> dict[str, Any] | None:
    async with context.db.tx() as connection:
        row = await (
            await connection.execute(
                """SELECT n.resolved_input,n.handler,e.policy,e.deadline_at FROM nodes n
               JOIN executions e ON e.id=n.execution_id WHERE n.execution_id=%s AND n.node_id=%s""",
                (envelope["execution_id"], envelope["node_id"]),
            )
        ).fetchone()
    if row is None:
        return None
    return {
        **envelope,
        "input": row["resolved_input"],
        "deadline_ms": int(row["deadline_at"].timestamp() * 1000),
        "handler_timeout": row["policy"]["handler_timeout"],
        "max_output_bytes": node_value_limit(row["policy"], row["handler"]),
        "max_depth": row["policy"]["max_depth"],
    }


async def claim(context, envelope):
    broker = Broker(context.settings)
    try:
        task = await task_payload(context, envelope)
        if task is None:
            return None
        owner = await broker.claim(task)
        if owner is not None:
            assert await storage.record_started(context, {**envelope, **owner, "kind": "started"})
        return owner
    finally:
        await broker.close()


async def store_outcome(
    context, envelope, token, success, value=None, error=None, runtime=None, *, prepared=None
):
    if prepared is None:
        prepared = await prepare_result((success, value, error, runtime))
    async with context.db.tx() as connection:
        row = await (
            await connection.execute(
                "SELECT started_at,lease_until FROM tries WHERE event_id=%s",
                (envelope["event_id"],),
            )
        ).fetchone()
    if row is None:
        return False
    report = {
        **envelope,
        "kind": "outcome",
        "token": token,
        "started_ms": int(row["started_at"].timestamp() * 1000),
        "lease_ms": int(row["lease_until"].timestamp() * 1000),
        "reported_ms": int(time.time() * 1000),
    }
    return await storage.store_outcome(
        context, report, token, success, value, error, runtime, prepared=prepared
    )


async def maintain_once(context):
    broker = OrchestratorBroker(context.settings)
    try:
        await broker.start()
        await Runtime(context, broker).maintain()
    finally:
        await broker.close()


async def expire_claim(context, envelope):
    from shared.broker import task_claim_key

    broker = Broker(context.settings)
    try:
        await broker.client.hset(task_claim_key(context.settings, envelope), "lease_ms", 1)
    finally:
        await broker.close()
