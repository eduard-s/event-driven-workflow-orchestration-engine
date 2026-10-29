# Examine workflow input and coordinate storage before the API returns.

from dataclasses import dataclass
from typing import Any

from api import storage
from api.config import MAX_KEY_LENGTH
from api.context import Context
from api.validation import Workflow, normalize_trigger, validate_workflow
from shared.errors import DomainError
from shared.identifiers import execution_id


@dataclass(frozen=True)
class Acceptance:
    # Keep the receipt identity and duplicate flag for a committed submission.
    #
    # Args:
    #     execution_id: The accepted execution UUID as text.
    #     repeated: True when an existing submission key supplied this receipt.
    #
    # Returns:
    #     An immutable receipt that routes can return and audit.

    execution_id: str
    repeated: bool = False


def prepare_submission(payload: Workflow | dict[str, Any]) -> tuple[Workflow, dict[str, Any]]:
    # Examine a definition outside the transaction before workflow storage.
    #
    # Args:
    #     payload: A workflow model or native dictionary from a submission.
    #
    # Returns:
    #     The examined workflow model and its JSON-compatible definition dictionary.
    #
    # Raises:
    #     DomainError: The graph, schema, template references, or JSON values are invalid.
    if isinstance(payload, Workflow):
        payload = payload.model_dump(exclude_unset=True, mode="json")
    workflow = validate_workflow(payload)
    return workflow, workflow.model_dump(exclude_unset=True, mode="json")


async def submit(
    context: Context, payload: Workflow | dict[str, Any], key: str | None
) -> Acceptance:
    # Examine a submission and request atomic storage before returning its receipt.
    #
    # Args:
    #     context: API settings, database access, and bounded validation processes.
    #     payload: The workflow model or native definition dictionary.
    #     key: An optional submission key that identifies retries of the same receipt.
    #
    # Returns:
    #     An Acceptance after commit, with repeated set for an existing key.
    #
    # Raises:
    #     DomainError: The key, definition, CPU admission, or execution quota is invalid or unavailable.
    if key is not None and (not key or len(key) > MAX_KEY_LENGTH):
        raise DomainError(422, "INVALID_IDEMPOTENCY_KEY", f"Use 1 to {MAX_KEY_LENGTH} characters.")
    workflow, definition = await context.validation.run(prepare_submission, payload)
    identifier, repeated = await storage.store_submission(context, workflow, definition, key)
    return Acceptance(identifier, repeated)


async def trigger(context: Context, identifier: str, payload: Any) -> str:
    # Examine trigger overrides before storage accepts the trigger and its start event.
    #
    # Args:
    #     context: API settings, database access, and bounded validation processes.
    #     identifier: The execution UUID as text.
    #     payload: Literal input and allowed node parameter overrides.
    #
    # Returns:
    #     The execution ID after the accepted trigger commits.
    #
    # Raises:
    #     DomainError: The ID, overrides, capacity, execution state, or pending deadline prevents acceptance.
    identifier = execution_id(identifier)
    definition = await storage.load_definition(context, identifier)
    trigger = await context.validation.run(normalize_trigger, definition, payload)
    return await storage.store_trigger(context, identifier, trigger)
