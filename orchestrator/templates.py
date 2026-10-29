# Resolve worker inputs from stored ancestor outputs within resource limits.

import json
import math
from dataclasses import dataclass
from typing import Any

from shared.contracts import InputNode, Node, OutputNode, examine_json, node_templates, parse_node
from shared.errors import DomainError
from shared.templates import Reference, iter_references, parse_template


def canonical_json(value: Any) -> str:
    # Encode stable JSON for input byte accounting and text substitution so key order cannot change the representation.
    #
    # Args:
    #     value: Native JSON value with finite numbers and valid Unicode.
    #
    # Returns:
    #     Compact Unicode JSON text with sorted object keys.
    #
    # Raises:
    #     DomainError: The value, number, Unicode, or container depth is invalid.
    # The request and node entry points apply their own byte and depth limits.
    # This encoder also accepts the protocol wrapper around those values.
    examine_json(value, max_depth=1_000)
    try:
        return json.dumps(
            value, ensure_ascii=False, allow_nan=False, sort_keys=True, separators=(",", ":")
        )
    except (ValueError, TypeError, RecursionError) as error:
        raise DomainError(
            422, "INVALID_JSON_VALUE", "The value cannot be encoded as JSON."
        ) from error


@dataclass
class _Budget:
    # Track allocation limits during input resolution so large references cannot allocate an unlimited result.
    #
    # Args:
    #     remaining: Number of UTF-8 JSON bytes still available.
    #     max_depth: Maximum number of nested JSON containers.
    #     path: Node or field path attached to resolution errors.
    #
    # Returns:
    #     A mutable budget shared by one resolution operation.

    remaining: int
    max_depth: int
    path: str

    def take(self, size: int) -> None:
        # Charge a known byte cost before allocation so resolution stops at the input limit.
        #
        # Args:
        #     size: Number of bytes to subtract from the remaining budget.
        #
        # Returns:
        #     None. The remaining byte count decreases by size.
        #
        # Raises:
        #     DomainError: The requested bytes exceed the remaining budget.
        if size > self.remaining:
            raise DomainError(
                422, "RESOLVED_INPUT_LIMIT", "The resolved input exceeds its byte limit.", self.path
            )
        self.remaining -= size

    def text(self, value: str, *, quotes: bool = True) -> None:
        # This lower bound rejects a large string before an encoded copy exists.
        # Charge escaped UTF-8 JSON text in bounded chunks so each temporary encoded chunk stays bounded.
        #
        # Args:
        #     value: Text whose encoded size counts against this budget.
        #     quotes: Whether to charge two surrounding JSON quotes. The default is True.
        #
        # Returns:
        #     None. The remaining byte budget decreases by the encoded size.
        #
        # Raises:
        #     DomainError: The encoded text exceeds the byte budget or contains invalid Unicode.
        if len(value) + (2 if quotes else 0) > self.remaining:
            self.take(self.remaining + 1)
        if quotes:
            self.take(2)
        for offset in range(0, len(value), 4096):
            try:
                chunk = value[offset : offset + 4096]
                encoded = json.dumps(chunk, ensure_ascii=False)[1:-1].encode("utf-8")
            except UnicodeError as error:
                raise DomainError(
                    422,
                    "INVALID_UNICODE",
                    "The resolved value contains invalid Unicode.",
                    self.path,
                ) from error
            self.take(len(encoded))

    def container(self, count: int, depth: int, *, mapping: bool) -> None:
        # Charge JSON container punctuation and enforce depth before a list or object allocates its children.
        #
        # Args:
        #     count: Number of items or key-value pairs in the container.
        #     depth: Number of containers outside the proposed container.
        #     mapping: True for an object, False for a list.
        #
        # Returns:
        #     None. The byte budget covers brackets, separators, and object colons.
        #
        # Raises:
        #     DomainError: The container exceeds the byte or depth budget.
        if depth + 1 > self.max_depth:
            raise DomainError(
                422,
                "RESOLVED_DEPTH_LIMIT",
                "The resolved input exceeds its depth limit.",
                self.path,
            )
        self.take(2 + max(0, count - 1) + (count if mapping else 0))


def _literal(value: Any, budget: _Budget, depth: int, *, copy: bool = True) -> Any:
    # Measure literal JSON before each allocation. Literal text never enters the template parser.
    #
    # Args:
    #     value: Native JSON value to measure and optionally copy.
    #     budget: Shared byte and depth budget for the resolved input.
    #     depth: Current number of outer JSON containers.
    #     copy: Whether to construct container copies. The default is True.
    #
    # Returns:
    #     The scalar or copied container. With copy=False, containers return None after measurement.
    #
    # Raises:
    #     DomainError: A value, object key, number, Unicode sequence, byte cost, or depth is invalid.
    if value is None:
        budget.take(4)
        return None
    if type(value) is bool:
        budget.take(4 if value else 5)
        return value
    if type(value) in {int, float}:
        if isinstance(value, float) and not math.isfinite(value):
            raise DomainError(
                422, "INVALID_NUMBER", "The resolved number must be finite.", budget.path
            )
        try:
            budget.take(len(str(value)))
        except ValueError as error:
            raise DomainError(
                422, "INVALID_NUMBER", "The resolved number cannot be encoded.", budget.path
            ) from error
        return value
    if type(value) is str:
        budget.text(value)
        return value
    if type(value) is list:
        budget.container(len(value), depth, mapping=False)
        array: list[Any] = []
        for item in value:
            child = _literal(item, budget, depth + 1, copy=copy)
            if copy:
                array.append(child)
        return array if copy else None
    if type(value) is dict:
        budget.container(len(value), depth, mapping=True)
        mapping: dict[str, Any] = {}
        for key, item in value.items():
            if type(key) is not str:
                raise DomainError(
                    422, "INVALID_JSON_KEY", "Resolved object keys must be text.", budget.path
                )
            budget.text(key)
            child = _literal(item, budget, depth + 1, copy=copy)
            if copy:
                mapping[key] = child
        return mapping if copy else None
    raise DomainError(422, "INVALID_JSON_VALUE", "The resolved value is not JSON.", budget.path)


def _lookup(reference: Reference, outputs: dict[str, Any], path: str) -> Any:
    # Read one parsed ancestor reference with explicit type and presence errors so template failures remain predictable.
    #
    # Args:
    #     reference: Parsed node ID and object-key or list-index steps.
    #     outputs: Available ancestor output values indexed by node ID.
    #     path: Input field path attached to lookup failures.
    #
    # Returns:
    #     The referenced native JSON value without a copy.
    #
    # Raises:
    #     DomainError: The node, field, index, or expected container type is unavailable.
    if reference.node_id not in outputs:
        raise DomainError(422, "MISSING_OUTPUT", "An ancestor output is unavailable.", path)
    current = outputs[reference.node_id]
    for step in reference.steps:
        if isinstance(step, str):
            if not isinstance(current, dict):
                raise DomainError(422, "REFERENCE_TYPE", "The reference needs a JSON object.", path)
            if step not in current:
                raise DomainError(422, "MISSING_FIELD", "The referenced field is absent.", path)
            current = current[step]
        else:
            if not isinstance(current, list):
                raise DomainError(422, "REFERENCE_TYPE", "The reference needs a JSON array.", path)
            if step >= len(current):
                raise DomainError(
                    422, "INVALID_INDEX", "The array index is outside the output.", path
                )
            current = current[step]
    return current


def _as_text(value: Any, budget: _Budget) -> str:
    # Convert a reference value to text after bounded measurement so string templates cannot create an unlimited temporary value.
    #
    # Args:
    #     value: Native JSON value for insertion into a text field.
    #     budget: Available byte budget used for the temporary-size examination.
    #
    # Returns:
    #     The original string or canonical JSON text. The caller charges the final text cost.
    #
    # Raises:
    #     DomainError: The value exceeds temporary byte/depth limits or is invalid JSON.
    if isinstance(value, str):
        return value
    # Bound the JSON text before json.dumps can allocate its complete string.
    measure = _Budget(budget.remaining, 32, budget.path)
    _literal(value, measure, 0, copy=False)
    return canonical_json(value)


def _string(
    text: str, outputs: dict[str, Any], budget: _Budget, depth: int, *, force_text: bool = False
) -> Any:
    # Resolve text references while preserving whole-value JSON types unless the target field needs text.
    #
    # Args:
    #     text: Template text with optional ancestor references.
    #     outputs: Available ancestor output values indexed by node ID.
    #     budget: Shared byte and depth budget for the resolved input.
    #     depth: Current number of outer JSON containers.
    #     force_text: Whether a whole-value reference must become text. The default is False.
    #
    # Returns:
    #     A native JSON value for a whole reference, or the final combined string.
    #
    # Raises:
    #     DomainError: Syntax, lookup, Unicode, or resource limits prevent resolution.
    tokens = parse_template(text, budget.path)
    significant = [token for token in tokens if not isinstance(token, str) or token.strip()]
    if len(significant) == 1 and isinstance(significant[0], Reference):
        value = _lookup(significant[0], outputs, budget.path)
        if not force_text:
            return _literal(value, budget, depth)
        result = _as_text(value, budget)
        budget.text(result)
        return result
    budget.take(2)
    pieces: list[str] = []
    for token in tokens:
        piece = (
            token
            if isinstance(token, str)
            else _as_text(_lookup(token, outputs, budget.path), budget)
        )
        budget.text(piece, quotes=False)
        pieces.append(piece)
    return "".join(pieces)


def _resolve(value: Any, outputs: dict[str, Any], budget: _Budget, depth: int) -> Any:
    # Resolve references in JSON values. Keep object keys literal and enforce limits before container allocation.
    #
    # Args:
    #     value: Template-bearing JSON scalar, list, or object.
    #     outputs: Available ancestor output values indexed by node ID.
    #     budget: Shared byte and depth budget for the resolved input.
    #     depth: Current number of outer JSON containers.
    #
    # Returns:
    #     The resolved native JSON value.
    #
    # Raises:
    #     DomainError: A template, value, reference, or resource limit is invalid.
    if isinstance(value, str):
        return _string(value, outputs, budget, depth)
    if isinstance(value, list):
        budget.container(len(value), depth, mapping=False)
        return [_resolve(item, outputs, budget, depth + 1) for item in value]
    if isinstance(value, dict):
        budget.container(len(value), depth, mapping=True)
        result: dict[str, Any] = {}
        for key, item in value.items():
            budget.text(key)
            result[key] = _resolve(item, outputs, budget, depth + 1)
        return result
    return _literal(value, budget, depth)


def resolve_node(
    node: Node,
    trigger: dict[str, Any],
    outputs: dict[str, Any],
    max_bytes: int = 262_144,
    max_depth: int = 32,
) -> dict[str, Any]:
    # Prepare one bounded worker request from the node, trigger, and ancestor outputs so dispatch uses actual data.
    #
    # Args:
    #     node: Parsed node contract with its handler and declared template fields.
    #     trigger: Normalized input and node_params overrides from the accepted trigger.
    #     outputs: Required ancestor outputs indexed by node ID.
    #     max_bytes: Maximum UTF-8 JSON size of the complete worker request.
    #     max_depth: Maximum nesting depth of the complete worker request.
    #
    # Returns:
    #     A dictionary with handler, data, and config. An implicit output node aggregates its direct parents.
    #
    # Raises:
    #     ValueError: A byte or depth limit is negative.
    #     DomainError: Resolution has invalid references, values, Unicode, or resource use.
    if max_bytes < 0 or max_depth < 0:
        raise ValueError("Resolution limits cannot be negative.")
    budget = _Budget(max_bytes, max_depth, f"nodes.{node.id}")
    budget.container(3, 0, mapping=True)
    for key in ("handler", "data", "config"):
        budget.text(key)
    handler = _literal(node.handler, budget, 1)
    params = trigger.get("node_params", {}).get(node.id, {})
    config: dict[str, Any] = {}

    values = node_templates(node, params)
    definition = node.model_dump(exclude_unset=True)
    raw_config = definition.get("config", {})
    budget.container(len(raw_config), 1, mapping=True)
    for field, value in raw_config.items():
        budget.text(field)
        path = f"config.{field}"
        config[field] = (
            _string(values[path], outputs, budget, 2, force_text=True)
            if path in values
            else _literal(value, budget, 2)
        )
    if isinstance(node, InputNode):
        data = _literal(trigger.get("input"), budget, 1)
    elif isinstance(node, OutputNode) and "data" not in values:
        for parent in node.dependencies:
            if parent not in outputs:
                raise DomainError(
                    422, "MISSING_OUTPUT", "A dependency output is unavailable.", budget.path
                )
        data = _literal(
            {parent: outputs[parent] for parent in sorted(node.dependencies)}, budget, 1
        )
    else:
        data = _resolve(values.get("data"), outputs, budget, 1)
    return {"handler": handler, "data": data, "config": config}


def required_output_ids(definition: dict[str, Any], trigger: dict[str, Any]) -> set[str]:
    # Find the ancestor outputs a node needs so dispatch does not load unrelated large results.
    #
    # Args:
    #     definition: Stored node definition with its handler and dependencies.
    #     trigger: Normalized trigger with node_params overrides.
    #
    # Returns:
    #     A set of referenced node IDs, or direct parent IDs for implicit output aggregation.
    #
    # Raises:
    #     ValidationError: The stored node does not match its contract.
    #     DomainError: A template reference has invalid syntax.
    node = parse_node(definition)
    values = node_templates(node, trigger["node_params"].get(node.id, {}))
    if isinstance(node, OutputNode) and "data" not in values:
        return set(node.dependencies)
    return {reference.node_id for reference, _ in iter_references(values)}
