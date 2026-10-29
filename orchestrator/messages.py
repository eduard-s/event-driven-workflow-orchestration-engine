# The orchestrator formats notifications before broker publication.

from collections.abc import Mapping
from typing import Any, Literal, NotRequired, TypedDict


class EventEnvelope(TypedDict):
    # Describe broker notification fields so consumers share one versioned event identity.
    #
    # Args:
    #     schema_version: Protocol version. The current version is one.
    #     event_id: Stable identifier of the stored outbox event.
    #     kind: The start, task, or completion event kind.
    #     execution_id: Identifier of the workflow execution.
    #     node_id: Node identifier for task and completion events. Omit it for start events.
    #     try_number: Try number for task and completion events. Omit it for start events.
    #
    # Returns:
    #     A dictionary with the annotated fields. TypedDict does not enforce them at runtime.

    schema_version: int
    event_id: str
    kind: Literal["start", "task", "completion"]
    execution_id: str
    node_id: NotRequired[str]
    try_number: NotRequired[int]
    input: NotRequired[dict[str, Any]]
    deadline_ms: NotRequired[int]
    handler_timeout: NotRequired[float]
    max_output_bytes: NotRequired[int]
    max_depth: NotRequired[int]


def event_envelope(event: Mapping[str, Any]) -> EventEnvelope:
    # Convert a stored outbox row into a versioned notification with frozen input for worker tasks.
    #
    # Args:
    #     event: Outbox row with event_id, execution_id, kind, and node/try identity when needed.
    #
    # Returns:
    #     An EventEnvelope with string identifiers and the required fields.
    #
    # Raises:
    #     KeyError: The row lacks a required identity field.
    payload: EventEnvelope = {
        "schema_version": 1,
        "event_id": str(event["event_id"]),
        "kind": event["kind"],
        "execution_id": str(event["execution_id"]),
    }
    if event["kind"] != "start":
        payload["node_id"] = event["node_id"]
        payload["try_number"] = event["try_number"]
    if event["kind"] == "task" and "input" in event:
        for key in ("input", "deadline_ms", "handler_timeout", "max_output_bytes", "max_depth"):
            payload[key] = event[key]
    return payload
