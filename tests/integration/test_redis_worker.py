"""Stateless workers use Redis while the orchestrator owns all workflow state."""

import asyncio
import json
from uuid import uuid4

import pytest

from api import storage
from api.context import Context
from orchestrator import storage as orchestrator_storage
from orchestrator.broker import Broker as OrchestratorBroker
from orchestrator.runtime import Runtime
from shared.broker import StreamMessage, task_claim_key
from shared.output import prepare_value
from tests.integration.test_edges import _queued
from tests.integration.worker_events import task_payload
from worker.broker import Broker, outcome_envelope
from worker.config import Settings as WorkerSettings
from worker.service import Worker

pytestmark = pytest.mark.integration


async def test_redis_claim_retains_owner_after_handler_lease_expires(db, settings):
    settings = settings.model_copy(update={"handler_timeout": 0.1})
    context = Context(db, settings)
    _, identity = await _queued(context, db)
    task = await task_payload(context, identity)
    broker = Broker(settings)
    try:
        owner = await broker.claim(task)
        assert owner
        await asyncio.sleep(0.12)
        assert await broker.claim(task) is None
        key = task_claim_key(settings, task)
        assert await broker.client.hget(key, "token") == owner["token"].encode()
        assert await broker.client.pttl(key) > 1000
        assert not await broker.complete(
            task, owner["token"], outcome_envelope(task, owner, prepare_value(True, 7, None), None)
        )
    finally:
        await broker.close()


async def test_redis_result_is_immutable_and_notification_is_small(db, settings):
    context = Context(db, settings)
    _, identity = await _queued(context, db)
    task = await task_payload(context, identity)
    broker = Broker(settings)
    try:
        owner = await broker.claim(task)
        prepared = prepare_value(True, "x" * 100_000, None)
        encoded = outcome_envelope(task, owner, prepared, None)
        assert await broker.complete(task, owner["token"], encoded)
        assert await broker.complete(task, owner["token"], encoded)
        assert not await broker.complete(task, str(uuid4()), encoded)
        assert not await broker.complete(
            task,
            owner["token"],
            outcome_envelope(task, owner, prepare_value(True, "changed", None), None),
        )
        events = await broker.client.xrange(settings.event_stream)
        assert len(events) == 2
        assert all(len(fields[b"envelope"]) < 1024 for _, fields in events)
        saved = await broker.client.hget(task_claim_key(settings, task), "outcome")
        assert json.loads(json.loads(saved)["payload"]) == "x" * 100_000
    finally:
        await broker.close()


@pytest.mark.parametrize("delete_notification", [False, True])
async def test_orchestrator_recovers_on_time_result_after_lease_without_a_new_try(
    db, settings, delete_notification
):
    settings = settings.model_copy(update={"handler_timeout": 0.2})
    context = Context(db, settings)
    identifier, identity = await _queued(context, db)
    task = await task_payload(context, identity)
    worker_broker = Broker(settings)
    broker = OrchestratorBroker(settings)
    # The worker receives no Context, Database, PostgreSQL setting, or connection.
    worker = Worker(
        WorkerSettings(
            redis_url=settings.redis_url,
            task_stream=settings.task_stream,
            event_stream=settings.event_stream,
        ),
        worker_broker,
    )
    try:
        await worker_broker.start()
        await worker.process_message(StreamMessage("1-0", task))
        assert (await storage.status(context, identifier))["nodes"][0]["starts"] == 0
        reports = await worker_broker.client.xrange(settings.event_stream)
        started = json.loads(reports[0][1][b"envelope"])
        assert await orchestrator_storage.apply_worker_event(context, started)
        if delete_notification:
            await worker_broker.client.delete(settings.event_stream)
        await asyncio.sleep(0.22)
        runtime = Runtime(context, broker)
        await runtime.maintain()
        status = await storage.status(context, identifier)
        assert status["state"] == "COMPLETED"
        assert status["nodes"][0]["starts"] == 1
        assert len(status["nodes"][0]["tries"]) == 1
        key = task_claim_key(settings, task)
        assert await broker.client.hget(key, "outcome") is None
        assert await broker.client.hget(key, "token") is not None
    finally:
        await worker.output_pool.close()
        await worker_broker.close()
        await broker.close()


@pytest.mark.parametrize("completed", [False, True])
async def test_publication_repair_recovers_lost_worker_start_and_result_notifications(
    db, settings, completed
):
    from orchestrator.outbox import publish_once

    settings = settings.model_copy(
        update={"handler_timeout": 0.2, "enable_repair": True, "repair_interval": 0.01}
    )
    context = Context(db, settings)
    identifier, _ = await _queued(context, db)
    broker = OrchestratorBroker(settings)
    worker_broker = Broker(settings)
    worker = Worker(settings, worker_broker)
    try:
        await worker_broker.start()
        await publish_once(context, broker)
        message = (await worker_broker.read("task", "lost-notification"))[0]
        if completed:
            await worker.process_message(message)
        else:
            assert await worker_broker.claim(message.envelope)
            await worker_broker.ack("task", message.entry_id)
        await worker_broker.client.delete(settings.event_stream)
        await asyncio.sleep(0.02)
        runtime = Runtime(context, broker)
        await runtime.maintain()
        status = await storage.status(context, identifier)
        assert status["nodes"][0]["starts"] == 1
        if completed:
            assert status["state"] == "COMPLETED"
        else:
            assert status["nodes"][0]["state"] == "RUNNING"
            await asyncio.sleep(0.22)
            await runtime.maintain()
            await asyncio.sleep(0.02)
            await runtime.maintain()
            await runtime.maintain()
            async with db.tx() as connection:
                attempts = await (
                    await connection.execute(
                        "SELECT try_number FROM tries WHERE execution_id=%s ORDER BY try_number",
                        (identifier,),
                    )
                ).fetchall()
            assert [row["try_number"] for row in attempts] == [1, 2]
    finally:
        await worker.output_pool.close()
        await worker_broker.close()
        await broker.close()


async def test_database_clock_cannot_expire_a_live_redis_lease(db, settings):
    context = Context(db, settings)
    identifier, identity = await _queued(context, db)
    task = await task_payload(context, identity)
    broker = Broker(settings)
    orchestrator_broker = OrchestratorBroker(settings)
    try:
        owner = await broker.claim(task)
        assert await orchestrator_storage.record_started(
            context, {**identity, **owner, "kind": "started"}
        )
        async with db.tx() as connection:
            await connection.execute(
                "UPDATE tries SET lease_until=clock_timestamp()-interval '1 second' WHERE execution_id=%s",
                (identifier,),
            )
            await connection.execute(
                "UPDATE executions SET maintenance_due=clock_timestamp()-interval '1 second' WHERE id=%s",
                (identifier,),
            )
        await Runtime(context, orchestrator_broker).maintain()
        status = await storage.status(context, identifier)
        assert status["nodes"][0]["state"] == "RUNNING"
        assert status["nodes"][0]["tries"][0]["success"] is None
    finally:
        await broker.close()
        await orchestrator_broker.close()
