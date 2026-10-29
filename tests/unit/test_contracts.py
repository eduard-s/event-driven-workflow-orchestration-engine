from __future__ import annotations

from collections.abc import Container
from typing import Any

import pytest

from api.validation import normalize_trigger, validate_workflow
from shared.errors import DomainError


def definition(*nodes: dict[str, Any]) -> dict[str, Any]:
    return {"name": "Contract test", "dag": {"nodes": list(nodes)}}


def node(node_id: str = "a", handler: str = "input", **fields: Any) -> dict[str, Any]:
    return {"id": node_id, "handler": handler, "dependencies": [], **fields}


def test_valid_parallel_workflow_keeps_its_definition() -> None:
    payload = definition(
        node("input"),
        node(
            "fetch",
            "call_external_service",
            dependencies=["input"],
            config={"url": "https://example.test/users"},
            data={"user_id": "{{ input.user.id }}"},
        ),
        node(
            "summary",
            "llm",
            dependencies=["input"],
            config={"prompt": "Summarize {{ input.user }}"},
        ),
        node("output", "output", dependencies=["fetch", "summary"]),
    )
    workflow = validate_workflow(payload)
    assert workflow.model_dump(exclude_unset=True) == payload


@pytest.mark.parametrize(
    "payload,code",
    [
        (definition(), "INVALID_SCHEMA"),
        (definition(node(), node()), "DUPLICATE_NODE"),
        (definition(node(dependencies=["missing"])), "MISSING_DEPENDENCY"),
        (definition(node(dependencies=["a"])), "SELF_DEPENDENCY"),
        (definition(node(), node("b", dependencies=["a", "a"])), "DUPLICATE_DEPENDENCY"),
        (definition(node("a", dependencies=["b"]), node("b", dependencies=["a"])), "GRAPH_CYCLE"),
        (
            definition(node("root"), node("a", dependencies=["b"]), node("b", dependencies=["a"])),
            "GRAPH_CYCLE",
        ),
        (definition(node(handler="unknown")), "INVALID_SCHEMA"),
        (definition(node("a", "output"), node("b", dependencies=["a"])), "OUTPUT_NOT_TERMINAL"),
        (definition(node(id=12)), "INVALID_SCHEMA"),
        (definition(node(id="a.b")), "INVALID_SCHEMA"),
        (definition(node(id="a\n")), "INVALID_SCHEMA"),
        (definition(node(dependencies="a")), "INVALID_SCHEMA"),
        (definition(node(extra=True)), "INVALID_SCHEMA"),
        (definition(node(data=None)), "INVALID_SCHEMA"),
        (definition(node(config={})), "INVALID_SCHEMA"),
        (definition(node(handler="output", config=None)), "INVALID_SCHEMA"),
        (definition(node(handler="llm", config={"prompt": 3})), "INVALID_SCHEMA"),
        (definition(node(handler="llm", config={"prompt": "x"}, data=None)), "INVALID_SCHEMA"),
        (
            definition(node(handler="call_external_service", config={"url": "file:///tmp/file"})),
            "INVALID_SCHEMA",
        ),
        (
            definition(node(handler="call_external_service", config={"url": "https://"})),
            "INVALID_SCHEMA",
        ),
        (
            definition(node(handler="call_external_service", config={"url": "https://x:99999"})),
            "INVALID_SCHEMA",
        ),
        (
            definition(
                node(handler="call_external_service", config={"url": "https://x", "timeout": 3})
            ),
            "INVALID_SCHEMA",
        ),
        ({"name": "x", "dag": {"nodes": [node()], "extra": 1}}, "INVALID_SCHEMA"),
        ({"name": 1, "dag": {"nodes": [node()]}}, "INVALID_SCHEMA"),
    ],
)
def test_workflow_rejections(payload: dict[str, Any], code: str) -> None:
    with pytest.raises(DomainError) as failure:
        validate_workflow(payload)
    assert failure.value.code == code
    assert failure.value.status == 422


def test_missing_dependency_does_not_become_implicit_graph_node() -> None:
    with pytest.raises(DomainError, match="dependency"):
        validate_workflow(definition(node(dependencies=["unknown"])))


def test_graph_limits_and_long_iterative_chain() -> None:
    nodes = [node(f"n{i}", dependencies=[f"n{i - 1}"] if i else []) for i in range(500)]
    assert len(validate_workflow(definition(*nodes)).dag.nodes) == 500
    with pytest.raises(DomainError):
        validate_workflow(definition(*nodes, node("overflow")))
    dense = [node(f"n{i}", dependencies=[f"n{j}" for j in range(i)]) for i in range(65)]
    with pytest.raises(DomainError) as failure:
        validate_workflow(definition(*dense))
    assert failure.value.code == "EDGE_LIMIT"


def test_shape_and_absence_survive_serialization() -> None:
    workflow = validate_workflow(definition(node("a", "output"), node("b", "output", data=None)))
    assert "data" not in workflow.dag.nodes[0].model_fields_set
    assert "data" in workflow.dag.nodes[1].model_fields_set
    persisted = workflow.model_dump(exclude_unset=True)
    restored = validate_workflow(persisted)
    assert "data" not in restored.dag.nodes[0].model_fields_set
    assert restored.dag.nodes[1].data is None


@pytest.mark.parametrize("stored_definition", [False, True])
def test_trigger_defaults_and_input_are_literal(stored_definition: bool) -> None:
    payload = definition(node())
    workflow = payload if stored_definition else validate_workflow(payload)
    assert normalize_trigger(workflow, {}) == {"input": None, "node_params": {}}
    assert normalize_trigger(workflow, {"input": None, "node_params": {}}) == normalize_trigger(
        workflow, {}
    )
    assert (
        normalize_trigger(workflow, {"input": "{{ invalid.expression( }}"})["input"]
        == "{{ invalid.expression( }}"
    )
    with pytest.raises(DomainError) as failure:
        normalize_trigger(workflow, None)
    assert failure.value.status == 422


def test_replacements_replace_complete_field() -> None:
    workflow = validate_workflow(
        definition(
            node(),
            node("b", "output", dependencies=["a"], data={"old": 1}),
        )
    )
    normalized = normalize_trigger(workflow, {"node_params": {"b": {"data": {"new": "{{ a }}"}}}})
    assert normalized["node_params"]["b"]["data"] == {"new": "{{ a }}"}


@pytest.mark.parametrize(
    "payload,code",
    [
        ({"node_params": {"missing": {"data": 1}}}, "UNKNOWN_OVERRIDE_NODE"),
        ({"node_params": {"a": {"data": 1}}}, "INVALID_OVERRIDE"),
        ({"node_params": {"b": {"url": "http://x"}}}, "INVALID_OVERRIDE"),
        ({"node_params": {"b": {"dependencies": []}}}, "INVALID_OVERRIDE"),
        ({"node_params": {"c": {"prompt": 42}}}, "INVALID_OVERRIDE"),
        ({"node_params": {"b": {"data": "{{ b }}"}}}, "INVALID_REFERENCE"),
        ({"node_params": {"b": {"data": "{{ c }}"}}}, "INVALID_REFERENCE"),
        ({"node_params": {"c": {"prompt": "{{ a[ }}"}}}, "INVALID_TEMPLATE"),
        ({"node_params": None}, "INVALID_SCHEMA"),
        ({"node_params": {"b": None}}, "INVALID_SCHEMA"),
        ({"unknown": 1}, "INVALID_SCHEMA"),
    ],
)
def test_invalid_trigger_replacements(payload: dict[str, Any], code: str) -> None:
    workflow = validate_workflow(
        definition(
            node(),
            node("b", "output", dependencies=["a"]),
            node("c", "llm", dependencies=["a"], config={"prompt": "{{ a }}"}),
        )
    )
    with pytest.raises(DomainError) as failure:
        normalize_trigger(workflow, payload)
    assert failure.value.code == code


def test_transitive_ancestor_is_valid_but_independent_node_is_not() -> None:
    payload = definition(
        node(),
        node("b", "call_external_service", dependencies=["a"], config={"url": "http://mock"}),
        node("c", "output", dependencies=["b"], data="{{ a }}"),
        node("independent"),
    )
    assert validate_workflow(payload).dag.nodes[2].data == "{{ a }}"
    payload["dag"]["nodes"][2]["data"] = "{{ independent }}"
    with pytest.raises(DomainError) as failure:
        validate_workflow(payload)
    assert failure.value.code == "INVALID_REFERENCE"


def test_error_text_does_not_echo_payload_values() -> None:
    payload = definition(node(handler="secret-bearer-token"))
    with pytest.raises(DomainError) as failure:
        validate_workflow(payload)
    assert "secret-bearer-token" not in failure.value.message


def test_trigger_only_scans_replacement_templates(monkeypatch: pytest.MonkeyPatch) -> None:
    import api.validation as validation

    workflow = validate_workflow(
        definition(
            node(),
            node("b", "llm", dependencies=["a"], config={"prompt": "Old {{ a }}"}),
            node("out", "output", dependencies=["b"]),
        )
    )
    examined = []
    validate = validation.validate_references

    def record(value: Any, ancestors: Container[str], path: str) -> None:
        allowed = {node.id for node in workflow.dag.nodes if node.id in ancestors}
        examined.append((value, allowed, path))
        validate(value, ancestors, path)

    monkeypatch.setattr(validation, "validate_references", record)
    normalize_trigger(workflow, {})
    normalize_trigger(workflow, {"node_params": {"b": {}}})
    assert examined == []
    normalize_trigger(workflow, {"node_params": {"b": {"prompt": "New {{ a }}"}}})
    assert examined == [("New {{ a }}", {"a"}, "node_params.b.prompt")]


def test_variant_error_path_names_the_json_field() -> None:
    with pytest.raises(DomainError) as failure:
        validate_workflow(definition(node(handler="llm", config={"prompt": 42})))
    assert failure.value.path == "dag.nodes[0].config.prompt"
