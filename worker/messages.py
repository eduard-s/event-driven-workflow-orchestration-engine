# Worker claim and handler request types.

from typing import Any, NotRequired, TypedDict


class HandlerRequest(TypedDict):
    # Describe the frozen input that a worker sends to a handler.
    #
    # Args:
    #     handler: Handler name that selects the mock operation.
    #     data: Optional JSON input value.
    #     config: Optional handler configuration, such as a URL or prompt.
    #     fixture: Optional test-only delay, failure, crash, or output override.
    #
    # Returns:
    #     A typed dictionary. The type declaration does not examine values at runtime.

    handler: str
    data: NotRequired[Any]
    config: NotRequired[dict[str, Any]]
    fixture: NotRequired[dict[str, Any]]


class Claim(TypedDict):
    # Describe the authority and limits returned after a successful task claim.
    #
    # Args:
    #     token: Unique owner token needed to store the outcome.
    #     input: Frozen handler request from the node.
    #     deadline: Absolute monotonic deadline for handler work and result storage.
    #     max_output_bytes: Maximum encoded bytes for this node's output.
    #     max_depth: Maximum output container depth.
    #     started_ms: Redis start time in milliseconds.
    #     lease_ms: Redis lease deadline in milliseconds.
    #
    # Returns:
    #     A typed dictionary used only after the atomic Redis claim succeeds.

    token: str
    input: HandlerRequest
    deadline: float
    max_output_bytes: int
    max_depth: int
    started_ms: int
    lease_ms: int
