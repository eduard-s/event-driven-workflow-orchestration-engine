# Node models and JSON rules used across service boundaries.

from __future__ import annotations

import math
from collections.abc import Mapping
from typing import Annotated, Any, ClassVar, Literal
from urllib.parse import urlsplit

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    JsonValue,
    StringConstraints,
    TypeAdapter,
    field_validator,
)

from shared.errors import DomainError

NodeId = Annotated[str, StringConstraints(pattern=r"^[A-Za-z_][A-Za-z0-9_-]{0,63}$")]


class StrictModel(BaseModel):
    # Keep node schemas strict so unknown fields and implicit value conversions cannot change workflow meaning.
    #
    # Args:
    #     **data: Fields declared by a concrete schema.
    #
    # Returns:
    #     A schema instance with the supplied fields.
    #
    # Raises:
    #     ValidationError: A field is unknown, has an invalid type, or contains a non-finite number.

    model_config = ConfigDict(strict=True, extra="forbid", allow_inf_nan=False)


class NodeBase(StrictModel):
    # Define node identity and dependencies so API and orchestrator graph rules use the same fields.
    #
    # Args:
    #     id: Node identifier with the shared syntax and length limit.
    #     dependencies: IDs of the node's direct parents.
    #
    # Attributes:
    #     template_fields: Class-level paths that permit templates and trigger overrides.
    #
    # Returns:
    #     A base node instance for a concrete handler schema.

    id: NodeId
    dependencies: list[NodeId]
    template_fields: ClassVar[tuple[str, ...]] = ()


class ExternalConfig(StrictModel):
    # Keep the external mock URL separate from data so each node has a fixed HTTP destination.
    #
    # Args:
    #     url: Nonempty HTTP or HTTPS URL with a hostname and valid port.
    #
    # Returns:
    #     External-service configuration with a valid URL.
    #
    # Raises:
    #     ValidationError: The URL is absent, has an invalid type, or fails the URL rules.

    url: str = Field(min_length=1)

    @field_validator("url")
    @classmethod
    def http_url(cls, value: str) -> str:
        # Reject unsupported URL forms so external nodes share one HTTP destination rule.
        #
        # Args:
        #     value: URL text supplied to the configuration model.
        #
        # Returns:
        #     The unchanged URL after its syntax passes.
        #
        # Raises:
        #     ValueError: The URL has an unsupported scheme, whitespace, hostname, or port.

        if not _valid_url(value):
            raise ValueError("The external URL must use HTTP or HTTPS.")
        return value


class LLMConfig(StrictModel):
    # Define the LLM prompt so its template value has a shared string contract.
    #
    # Args:
    #     prompt: Text that the orchestrator resolves before handler dispatch.
    #
    # Returns:
    #     LLM configuration with the supplied prompt.
    #
    # Raises:
    #     ValidationError: The prompt is absent or is not text.

    prompt: str


class InputNode(NodeBase):
    # Define a root input handler so literal trigger data can enter the workflow.
    #
    # Args:
    #     id: Node identifier.
    #     dependencies: Direct parent IDs, normally empty for an input node.
    #     handler: The literal input handler name.
    #
    # Returns:
    #     An input node model.

    handler: Literal["input"]


class ExternalNode(NodeBase):
    # Define the external-service mock so the API and orchestrator agree on its configuration and data.
    #
    # Args:
    #     id: Node identifier.
    #     dependencies: Direct parent IDs.
    #     handler: The literal call_external_service handler name.
    #     config: Fixed external URL configuration.
    #     data: Optional JSON input that permits templates. The default is None.
    #
    # Returns:
    #     An external-service node model. Only data permits templates and trigger overrides.

    handler: Literal["call_external_service"]
    config: ExternalConfig
    data: JsonValue = None
    template_fields = ("data",)


class LLMNode(NodeBase):
    # Define the LLM mock so its prompt can receive text from ancestor outputs.
    #
    # Args:
    #     id: Node identifier.
    #     dependencies: Direct parent IDs.
    #     handler: The literal llm handler name.
    #     config: Prompt configuration that permits templates and trigger overrides.
    #
    # Returns:
    #     An LLM node model.

    handler: Literal["llm"]
    config: LLMConfig
    template_fields = ("config.prompt",)


class OutputNode(NodeBase):
    # Define a terminal result so a workflow can return explicit data or aggregate its direct parents.
    #
    # Args:
    #     id: Node identifier.
    #     dependencies: Direct parent IDs for the default aggregate.
    #     handler: The literal output handler name.
    #     data: Optional JSON result with template support. An absent field selects parent aggregation.
    #
    # Returns:
    #     An output node model that keeps absent data distinct from explicit None.

    handler: Literal["output"]
    data: JsonValue = None
    template_fields = ("data",)


Node = Annotated[InputNode | ExternalNode | LLMNode | OutputNode, Field(discriminator="handler")]
_NODE_ADAPTER = TypeAdapter(Node)


def parse_node(payload: Any) -> Node:
    # Select and examine the handler schema so stored definitions and overrides use the same node contract.
    #
    # Args:
    #     payload: Native node fields or a compatible node model.
    #
    # Returns:
    #     The input, external-service, LLM, or output node selected by handler.
    #
    # Raises:
    #     ValidationError: The node shape, handler, field type, or configuration is invalid.

    return _NODE_ADAPTER.validate_python(payload)


def node_value_limit(policy: Mapping[str, Any], handler: str) -> int:
    # Select the saved byte budget so terminal aggregation can exceed the ordinary per-node value limit.
    #
    # Args:
    #     policy: Saved execution policy with ordinary and aggregate byte limits.
    #     handler: Node handler name. Only output selects the aggregate limit.
    #
    # Returns:
    #     The selected byte limit as an integer.
    #
    # Raises:
    #     KeyError: The selected budget field is absent.
    #     TypeError, ValueError: The selected budget cannot convert to an integer.
    return int(policy["max_execution_bytes" if handler == "output" else "max_value_bytes"])


def node_templates(node: Node, params: dict[str, Any] | None = None) -> dict[str, Any]:
    # Collect declared template fields so API reference rules and runtime resolution inspect the same values.
    #
    # Args:
    #     node: Node model that declares supported template paths.
    #     params: Optional overrides for this node, keyed by each path's final field name.
    #
    # Returns:
    #     Present template paths and their values, with overrides applied. Absent data stays distinct from explicit None.
    definition = node.model_dump(exclude_unset=True)
    values = {}
    for path in node.template_fields:
        parts = path.split(".")
        if params is not None and parts[-1] in params:
            values[path] = params[parts[-1]]
            continue
        current = definition
        for part in parts[:-1]:
            current = current[part]
        if parts[-1] in current:
            values[path] = current[parts[-1]]
    return values


def _valid_url(value: str) -> bool:
    # Examine URL syntax so unsupported destinations fail before workflow storage.
    #
    # Args:
    #     value: Candidate URL text.
    #
    # Returns:
    #     True for an HTTP or HTTPS URL with a hostname, valid port, and no whitespace or control characters.
    #     Otherwise False.

    if not value or any(character.isspace() or ord(character) < 32 for character in value):
        return False
    try:
        parsed = urlsplit(value)
        return (
            parsed.scheme in {"http", "https"}
            and bool(parsed.hostname)
            and (parsed.port is None or 0 <= parsed.port <= 65535)
        )
    except ValueError:
        return False


def _valid_unicode(value: str) -> bool:
    # Determine whether text has a strict UTF-8 representation so JSON and storage encoders can use it.
    #
    # Args:
    #     value: Candidate Unicode text.
    #
    # Returns:
    #     True if UTF-8 encoding succeeds. False for unsupported surrogate values.

    try:
        value.encode("utf-8")
    except UnicodeEncodeError:
        return False
    return True


def examine_json(
    value: Any, max_depth: int = 32, *, status: int = 422, storage_safe: bool = False
) -> int:
    # Examine native JSON without recursive calls so values obey depth, Unicode, and storage rules.
    #
    # Args:
    #     value: Native JSON candidate, including containers and scalar values.
    #     max_depth: Maximum nesting of dictionaries and lists.
    #     status: Error status for invalid values. Depth-limit errors always use 422.
    #     storage_safe: If True, reject NUL characters that PostgreSQL JSONB cannot store.
    #
    # Returns:
    #     Maximum container depth, or zero for a scalar.
    #
    # Raises:
    #     DomainError: A value, key, number, Unicode sequence, cycle, or depth violates the JSON contract.
    stack: list[tuple[Any, int, bool]] = [(value, 0, False)]
    active: set[int] = set()
    maximum_depth = 0
    while stack:
        current, depth, leaving = stack.pop()
        if leaving:
            active.remove(id(current))
            continue
        if current is None or type(current) in {bool, int}:
            continue
        if type(current) is float:
            if not math.isfinite(current):
                raise DomainError(status, "INVALID_NUMBER", "JSON numbers must be finite.")
        elif type(current) is str:
            if not _valid_unicode(current) or (storage_safe and "\x00" in current):
                raise DomainError(
                    status, "INVALID_UNICODE", "JSON text contains an unsupported Unicode value."
                )
        elif type(current) in {dict, list}:
            if depth >= max_depth:
                raise DomainError(422, "JSON_DEPTH_LIMIT", "The JSON depth exceeds the limit.")
            if id(current) in active:
                raise DomainError(
                    status, "INVALID_JSON_VALUE", "JSON values cannot contain cycles."
                )
            active.add(id(current))
            maximum_depth = max(maximum_depth, depth + 1)
            stack.append((current, depth, True))
            if isinstance(current, dict):
                for key, item in current.items():
                    if (
                        type(key) is not str
                        or not _valid_unicode(key)
                        or (storage_safe and "\x00" in key)
                    ):
                        raise DomainError(
                            status,
                            "INVALID_JSON_KEY",
                            "JSON object keys must be valid Unicode text.",
                        )
                    stack.append((item, depth + 1, False))
            else:
                stack.extend((item, depth + 1, False) for item in current)
        else:
            raise DomainError(status, "INVALID_JSON_VALUE", "The value is not a JSON value.")
    return maximum_depth
