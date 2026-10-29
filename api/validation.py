# Reject invalid workflow graphs and trigger overrides before storage.

from collections.abc import Container
from dataclasses import dataclass
from graphlib import CycleError, TopologicalSorter
from typing import Any

from pydantic import Field, JsonValue, ValidationError

from api.config import MAX_EDGES, MAX_NODES
from shared.contracts import Node, OutputNode, StrictModel, examine_json, node_templates, parse_node
from shared.errors import DomainError
from shared.templates import iter_references


class Dag(StrictModel):
    # Describe a nonempty node collection before graph-level examination.
    #
    # Args:
    #     nodes: Node definitions within the configured node-count bound.
    #
    # Returns:
    #     A strict DAG model. validate_workflow examines dependency and template rules.
    #
    # Raises:
    #     ValidationError: The collection or a node does not match its schema.

    nodes: list[Node] = Field(min_length=1, max_length=MAX_NODES)


class Workflow(StrictModel):
    # Keep the submitted name and graph in one strict definition.
    #
    # Args:
    #     name: A nonempty workflow name.
    #     dag: The node collection as a Dag model or matching dictionary.
    #
    # Returns:
    #     A workflow model for graph examination and storage.
    #
    # Raises:
    #     ValidationError: The name, graph, or supplied fields do not match the schema.

    name: str = Field(min_length=1)
    dag: Dag


class Trigger(StrictModel):
    # Describe literal input and node overrides without changing graph structure.
    #
    # Args:
    #     input: JSON input for input nodes, or None.
    #     node_params: Replacement template fields indexed by node ID.
    #
    # Returns:
    #     A strict trigger model. normalize_trigger examines override targets and references.
    #
    # Raises:
    #     ValidationError: Input fields or override shapes do not match the schema.

    input: JsonValue = None
    node_params: dict[str, dict[str, JsonValue]] = Field(default_factory=dict)


def _path(parts: tuple[str | int, ...]) -> str:
    # Format schema locations so error responses identify the failing JSON field.
    #
    # Args:
    #     parts: Field names and array indexes from a schema error location.
    #
    # Returns:
    #     A dotted path with array indexes, or $ for the document root.
    result = ""
    for part in parts:
        result += f"[{part}]" if isinstance(part, int) else ("." if result else "") + part
    return result or "$"


def _schema_error(error: ValidationError) -> DomainError:
    # Hide submitted values while exposing the first schema failure location.
    #
    # Args:
    #     error: A Pydantic ValidationError from a workflow or trigger model.
    #
    # Returns:
    #     A 422 DomainError with a safe schema message and JSON field path.
    first = error.errors(include_input=False, include_url=False)[0]
    path = first["loc"]
    if path[:2] == ("dag", "nodes") and len(path) > 3:
        path = path[:3] + path[4:]  # Remove Pydantic's union tag, which is not a JSON field.
    # Error text must not expose values from the supplied payload.
    return DomainError(422, "INVALID_SCHEMA", "The request does not match the schema.", _path(path))


def validate_references(value: Any, allowed_nodes: Container[str], path: str = "$") -> None:
    # Reject references outside the strict ancestors allowed for a node.
    #
    # Args:
    #     value: A JSON value whose string values can contain templates.
    #     allowed_nodes: A container of permitted ancestor IDs.
    #     path: The JSON location used in error paths.
    #
    # Returns:
    #     None when every template reference is permitted.
    #
    # Raises:
    #     DomainError: A template has invalid syntax or references a forbidden node.
    for reference, current_path in iter_references(value, path):
        if reference.node_id not in allowed_nodes:
            raise DomainError(
                422,
                "INVALID_REFERENCE",
                "A template must reference a strict ancestor.",
                current_path,
            )


@dataclass(frozen=True)
class _Ancestors:
    # Represent strict ancestors as bits for inexpensive membership queries.
    #
    # Args:
    #     indexes: The shared map from node IDs to one-bit masks.
    #     bits: The mask that contains this node's strict ancestors.
    #
    # Returns:
    #     An ancestor container with a fixed mask for template reference rules.

    indexes: dict[str, int]
    bits: int

    def __contains__(self, node_id: object) -> bool:
        # Examine ancestor membership without another graph traversal.
        #
        # Args:
        #     node_id: The candidate node ID, including possible non-string values.
        #
        # Returns:
        #     True for a known strict ancestor, otherwise False.
        return isinstance(node_id, str) and bool(self.bits & self.indexes.get(node_id, 0))


def _ancestors(parents: dict[str, list[str]]) -> dict[str, _Ancestors]:
    # Compute all ancestor masks once to avoid repeated depth-first traversals.
    #
    # Args:
    #     parents: Node IDs mapped to their direct dependency IDs.
    #
    # Returns:
    #     Each node ID mapped to its strict ancestor container.
    #
    # Raises:
    #     CycleError: The graph contains a dependency cycle.
    #     KeyError: A dependency is absent from the supplied graph.
    indexes = {node_id: 1 << index for index, node_id in enumerate(parents)}
    result: dict[str, _Ancestors] = {}
    for node_id in TopologicalSorter(parents).static_order():
        bits = 0
        for parent in parents[node_id]:
            bits |= result[parent].bits | indexes[parent]
        result[node_id] = _Ancestors(indexes, bits)
    return result


def _examine_templates(workflow: Workflow, ancestors: dict[str, _Ancestors]) -> None:
    # Apply ancestor rules to the declared template fields of each node.
    #
    # Args:
    #     workflow: The schema-valid workflow definition.
    #     ancestors: The strict ancestor container for each workflow node.
    #
    # Returns:
    #     None when all declared templates are valid.
    #
    # Raises:
    #     DomainError: A template has invalid syntax or a forbidden reference.
    for index, node in enumerate(workflow.dag.nodes):
        values = node_templates(node)
        if values:
            for path, value in values.items():
                validate_references(value, ancestors[node.id], f"dag.nodes[{index}].{path}")


def validate_workflow(payload: Any) -> Workflow:
    # Examine graph integrity before a definition can enter workflow storage.
    #
    # Args:
    #     payload: A native JSON-compatible definition dictionary.
    #
    # Returns:
    #     A strict Workflow model with valid dependencies and template references.
    #
    # Raises:
    #     DomainError: JSON, schema, IDs, graph bounds, dependencies, cycles, or templates are invalid.
    examine_json(payload, storage_safe=True)
    try:
        workflow = Workflow.model_validate(payload)
    except ValidationError as error:
        raise _schema_error(error) from error
    nodes = workflow.dag.nodes
    ids: set[str] = set()
    for index, node in enumerate(nodes):
        if node.id in ids:
            raise DomainError(
                422, "DUPLICATE_NODE", "Node IDs must be unique.", f"dag.nodes[{index}].id"
            )
        ids.add(node.id)
    if sum(len(node.dependencies) for node in nodes) > MAX_EDGES:
        raise DomainError(422, "EDGE_LIMIT", "The graph exceeds the edge limit.", "dag.nodes")
    for index, node in enumerate(nodes):
        path = f"dag.nodes[{index}].dependencies"
        if len(set(node.dependencies)) != len(node.dependencies):
            raise DomainError(422, "DUPLICATE_DEPENDENCY", "Dependencies must be unique.", path)
        if node.id in node.dependencies:
            raise DomainError(422, "SELF_DEPENDENCY", "A node cannot depend on itself.", path)
        if any(parent not in ids for parent in node.dependencies):
            raise DomainError(422, "MISSING_DEPENDENCY", "A dependency does not name a node.", path)
    try:
        ancestors = _ancestors({node.id: node.dependencies for node in nodes})
    except CycleError as error:
        raise DomainError(
            422, "GRAPH_CYCLE", "The workflow contains a dependency cycle.", "dag.nodes"
        ) from error
    used_as_parent = {parent for node in nodes for parent in node.dependencies}
    for index, node in enumerate(nodes):
        if isinstance(node, OutputNode) and node.id in used_as_parent:
            raise DomainError(
                422,
                "OUTPUT_NOT_TERMINAL",
                "Output nodes cannot have dependents.",
                f"dag.nodes[{index}].id",
            )
    _examine_templates(workflow, ancestors)
    return workflow


def _validate_node_params(node: Node, params: dict[str, JsonValue], path: str) -> None:
    # Examine replacement fields without permitting graph or handler changes.
    #
    # Args:
    #     node: The original typed node definition.
    #     params: Replacement values indexed by declared template field names.
    #     path: The override location used in error paths.
    #
    # Returns:
    #     None when replacements match the node schema.
    #
    # Raises:
    #     DomainError: A field is unsupported or its replacement has an invalid type or value.
    fields = {field.rsplit(".", 1)[-1]: field for field in node.template_fields}
    if set(params) - fields.keys():
        raise DomainError(
            422, "INVALID_OVERRIDE", "The node override contains an unsupported field.", path
        )
    replacement = node.model_dump(exclude_unset=True)
    for name, value in params.items():
        parts = fields[name].split(".")
        target = replacement
        for part in parts[:-1]:
            target = target[part]
        target[parts[-1]] = value
    try:
        parse_node(replacement)
    except ValidationError as error:
        raise DomainError(
            422, "INVALID_OVERRIDE", "The node override has an invalid value.", path
        ) from error


def normalize_trigger(workflow: Workflow | dict[str, Any], payload: Any) -> dict[str, Any]:
    # Normalize input and overrides so the orchestrator receives fixed trigger data.
    #
    # Args:
    #     workflow: An examined workflow model or its saved definition dictionary.
    #     payload: A Trigger model or native trigger dictionary.
    #
    # Returns:
    #     A dictionary with literal input and examined node_params replacements.
    #
    # Raises:
    #     DomainError: JSON, trigger fields, override targets, or template references are invalid.
    #     ValidationError: A supplied saved workflow does not match its schema.
    if not isinstance(workflow, Workflow):
        workflow = Workflow.model_validate(workflow)
    if isinstance(payload, Trigger):
        payload = payload.model_dump(exclude_unset=True)
    examine_json(payload, storage_safe=True)
    try:
        trigger = Trigger.model_validate(payload)
    except ValidationError as error:
        raise _schema_error(error) from error
    nodes = {node.id: node for node in workflow.dag.nodes}
    ancestors = _ancestors({node.id: node.dependencies for node in workflow.dag.nodes})
    for node_id, params in trigger.node_params.items():
        path = f"node_params.{node_id}"
        if node_id not in nodes:
            raise DomainError(
                422, "UNKNOWN_OVERRIDE_NODE", "The override does not name a node.", path
            )
        if not params:
            continue
        _validate_node_params(nodes[node_id], params, path)
        for name, value in params.items():
            validate_references(value, ancestors[node_id], f"{path}.{name}")
    return trigger.model_dump()
