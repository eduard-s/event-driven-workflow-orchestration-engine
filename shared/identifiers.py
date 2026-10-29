from uuid import UUID

from shared.errors import DomainError


def execution_id(value: str) -> str:
    # Normalize execution IDs before database access so malformed identifiers produce a controlled response.
    #
    # Args:
    #     value: Execution UUID text from a route or message.
    #
    # Returns:
    #     Canonical lowercase UUID text with hyphens.
    #
    # Raises:
    #     DomainError: The value is not valid UUID text.

    try:
        return str(UUID(value))
    except (ValueError, TypeError, AttributeError) as exc:
        raise DomainError(422, "INVALID_ID", "Use a UUID execution ID.") from exc
