# Relay committed outbox records and probe broker receipts.

import os

import orchestrator.messages as messages
from orchestrator import storage
from orchestrator.broker import Broker
from orchestrator.config import Settings
from shared.broker import Kind
from shared.context import Context


async def publish_once(context: Context[Settings], broker: Broker) -> None:
    # Relay a bounded batch of committed outbox events. Storage reservations preserve logical identity across publication retries.
    #
    # Args:
    #     context: Database access, batch limit, publication lease, and optional test failpoint.
    #     broker: Orchestrator broker that accepts task and control notifications.
    #
    # Returns:
    #     None after eligible publications and their receipts. An enabled crash failpoint can exit before a receipt.
    #
    # Raises:
    #     RedisError: Broker publication fails and leaves durable work for retry.
    #     psycopg.Error: A storage query, reservation, or receipt fails.
    candidates = await storage.publication_candidates(context)
    for item in candidates:
        reservation = await storage.reserve_publication(context, item)
        if reservation is None:
            continue
        event, owner = reservation
        kind: Kind = "task" if event["kind"] == "task" else "event"
        # Redis publication and the database commit are separate. Stable event IDs make a repeated publication safe.
        entry_id = await broker.publish(kind, messages.event_envelope(event))
        if (
            context.settings.enable_test_fixtures
            and context.settings.orchestrator_failpoint == "after_publish_before_record"
        ):
            os._exit(81)
        await storage.record_publication(context, event, owner, entry_id)


async def repair_publications(context: Context[Settings], broker: Broker) -> None:
    # Probe old publication receipts and ask storage to restore missing notifications without new logical task identities.
    #
    # Args:
    #     context: Database access, probe interval, and batch limit.
    #     broker: Orchestrator broker for bounded entry-presence probes.
    #
    # Returns:
    #     None after the bounded probe batch.
    #
    # Raises:
    #     RedisError: A broker presence probe fails.
    #     psycopg.Error: A storage query or repair transaction fails.
    candidates = await storage.publication_checks(context)
    for item in candidates:
        present = await broker.exists(
            "task" if item["kind"] == "task" else "event",
            item["published_id"],
            str(item["event_id"]),
        )
        if not present and item["kind"] == "task":
            report = await broker.worker_report(item, include_start=True)
            if report is not None:
                await storage.apply_worker_event(context, report)
                if report["kind"] == "outcome":
                    await broker.forget_outcome(report)
                continue
        await storage.repair_publication(context, item, present)
