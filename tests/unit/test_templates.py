from __future__ import annotations

import json
from typing import Any

import pytest

import orchestrator.templates as templates
from api.validation import normalize_trigger, validate_references, validate_workflow
from orchestrator.templates import canonical_json, required_output_ids, resolve_node
from shared.contracts import (
    ExternalNode,
    InputNode,
    LLMNode,
    Node,
    OutputNode,
    parse_node,
)
from shared.errors import DomainError
from shared.templates import Reference, iter_references, parse_template


def output(data: Any = None, *, explicit: bool = True) -> Node:
    fields = {"id": "result", "handler": "output", "dependencies": ["a"]}
    if explicit:
        fields["data"] = data
    return parse_node(fields)


@pytest.mark.parametrize("text", ["", "plain", "é漢字😀", "{single}", "} {", "x" * 1_048_576])
def test_literal_text_is_unchanged(text: str) -> None:
    assert parse_template(text) == ([text] if text else [])


@pytest.mark.parametrize(
    "source,reference",
    [
        ("{{ a }}", Reference("a", ())),
        ("{{ a.items[0].name }}", Reference("a", ("items", 0, "name"))),
        ('{{ a["field-with-spaces"] }}', Reference("a", ("field-with-spaces",))),
        ('{{ a["a}}b"] }}', Reference("a", ("a}}b",))),
        ('{{ a["a\\"b"][0] }}', Reference("a", ('a"b', 0))),
        ('{{ a[""] }}', Reference("a", ("",))),
    ],
)
def test_reference_grammar(source: str, reference: Reference) -> None:
    assert parse_template(source) == [reference]


@pytest.mark.parametrize(
    "source",
    [
        "{{ }}",
        "{{ a",
        "a }}",
        "{{ a..x }}",
        "{{ a[-1] }}",
        "{{ a[01] }}",
        "{{ a[+1] }}",
        "{{ a[1.0] }}",
        "{{ a[1:2] }}",
        "{{ a() }}",
        "{{ a + b }}",
        "{{ a['x'] }}",
        "{{ a[ }}",
        "{{ a. }}",
        '{{ a["x] }}',
        '{{ a["\\ud800"] }}',
        "{{ a.b-c }}",
        "{{ a .x }}",
        "{{ a[0]tail }}",
        "{{ 1node }}",
        "{{ " + "a" * 65 + " }}",
    ],
)
def test_invalid_reference_grammar(source: str) -> None:
    with pytest.raises(DomainError) as failure:
        parse_template(source)
    assert failure.value.code == "INVALID_TEMPLATE"


def test_nested_reference_discovery_matches_validation_and_output_loading() -> None:
    value = {
        "{{ invalid( }}": "literal",
        "items": ["{{ a.name }}", "{{{{ missing }}}}", {"content": "{{ b[0] }} {{ a }}"}],
    }
    assert list(iter_references(value, "data")) == [
        (Reference("b", (0,)), "data.items[2].content"),
        (Reference("a", ()), "data.items[2].content"),
        (Reference("a", ("name",)), "data.items[0]"),
    ]
    validate_references(value, {"a", "b"}, "data")
    with pytest.raises(DomainError) as caught:
        validate_references(value, {"a"}, "data")
    assert caught.value.code == "INVALID_REFERENCE"
    assert caught.value.path == "data.items[2].content"
    definition = output(value).model_dump(exclude_unset=True)
    assert required_output_ids(definition, {"node_params": {}}) == {"a", "b"}
    assert required_output_ids(definition, {"node_params": {"result": {"data": None}}}) == set()


@pytest.mark.parametrize("value", [None, True, False, 12, 1.5, "text", {"id": 7}, [1, "x"]])
def test_exact_reference_preserves_type(value: Any) -> None:
    resolved = resolve_node(output("  {{ a }}\n"), {}, {"a": value})
    assert resolved["data"] == value
    assert type(resolved["data"]) is type(value)


def test_resolved_containers_are_frozen_copies() -> None:
    value = {"items": [{"id": 7}]}
    resolved = resolve_node(output("{{ a }}"), {}, {"a": value})
    value["items"][0]["id"] = 99
    assert resolved["data"] == {"items": [{"id": 7}]}


def test_interpolation_is_canonical_text() -> None:
    value = {"z": False, "a": [None, 2]}
    resolved = resolve_node(output("Value: {{ a }}."), {}, {"a": value})
    assert resolved["data"] == 'Value: {"a":[null,2],"z":false}.'


def test_nested_references_arrays_and_quoted_keys() -> None:
    source = {"user": "{{ a.items[0] }}", "items": ['{{ a["a}}b"] }}']}
    result = resolve_node(output(source), {}, {"a": {"items": [{"id": 7}], "a}}b": False}})
    assert result["data"] == {"user": {"id": 7}, "items": [False]}


def test_keys_and_substituted_values_are_not_scanned() -> None:
    source = {"{{ arbitrary }}": "{{ a }}"}
    result = resolve_node(output(source), {}, {"a": {"x": "{{ missing.bad( }}"}})
    assert result["data"] == {"{{ arbitrary }}": {"x": "{{ missing.bad( }}"}}


def test_escaped_delimiters_are_literal_and_not_rescanned() -> None:
    result = resolve_node(output("{{{{ a }}}} and {{ a }}"), {}, {"a": "value"})
    assert result["data"] == "{{ a }} and value"
    assert resolve_node(output("}}}}"), {}, {})["data"] == "}}"


@pytest.mark.parametrize(
    "source,outputs,code",
    [
        ("{{ a }}", {}, "MISSING_OUTPUT"),
        ("{{ a.absent }}", {"a": {}}, "MISSING_FIELD"),
        ("{{ a.field }}", {"a": []}, "REFERENCE_TYPE"),
        ("{{ a[0] }}", {"a": {}}, "REFERENCE_TYPE"),
        ("{{ a[1] }}", {"a": [0]}, "INVALID_INDEX"),
        ("{{ a.__class__ }}", {"a": "text"}, "REFERENCE_TYPE"),
    ],
)
def test_runtime_reference_failures(source: str, outputs: dict[str, Any], code: str) -> None:
    with pytest.raises(DomainError) as failure:
        resolve_node(output(source), {}, outputs)
    assert failure.value.code == code


def test_input_is_literal_and_defaults_to_null() -> None:
    node = InputNode(id="input", handler="input", dependencies=[])
    assert resolve_node(node, {}, {}) == {"handler": "input", "data": None, "config": {}}
    assert resolve_node(node, {"input": "{{ bad( }}"}, {})["data"] == "{{ bad( }}"


def test_output_absence_null_and_override_are_distinct() -> None:
    assert resolve_node(output(explicit=False), {}, {"a": 7})["data"] == {"a": 7}
    assert resolve_node(output(None), {}, {"a": 7})["data"] is None
    replacement = {"node_params": {"result": {"data": {"new": 1}}}}
    assert resolve_node(output({"old": 0}), replacement, {"a": 7})["data"] == {"new": 1}
    assert (
        resolve_node(output(explicit=False), {"node_params": {"result": {"data": None}}}, {"a": 7})[
            "data"
        ]
        is None
    )


def test_llm_object_reference_becomes_text() -> None:
    node = LLMNode(id="llm", handler="llm", dependencies=["a"], config={"prompt": "{{ a }}"})
    frozen = resolve_node(node, {}, {"a": {"z": True, "a": 1}})
    assert frozen == {"handler": "llm", "data": None, "config": {"prompt": '{"a":1,"z":true}'}}
    replacement = {"node_params": {"llm": {"prompt": "Say {{ a }}"}}}
    assert resolve_node(node, replacement, {"a": None})["config"]["prompt"] == "Say null"


def test_external_data_and_static_url() -> None:
    node = ExternalNode(
        id="remote",
        handler="call_external_service",
        dependencies=["a"],
        config={"url": "http://example.com/{{a}}"},
    )
    frozen = resolve_node(node, {}, {"a": 7})
    assert frozen == {
        "handler": "call_external_service",
        "data": None,
        "config": {"url": "http://example.com/{{a}}"},
    }
    replacement = {"node_params": {"remote": {"data": "{{ a }}"}}}
    assert resolve_node(node, replacement, {"a": 7})["data"] == 7


@pytest.mark.parametrize(
    "value",
    [
        "plain",
        "é😀",
        'quote" slash\\ tab\t control\x00',
        {"é": [None, False, 1e-7]},
        [1, 2, {"nested": "value"}],
        "",
        {},
        [],
        "x" * 4095 + '"\\\n😀é' + "x" * 4096,
        {"x" * 4096 + "😀": "\x00" * 8193},
    ],
)
def test_budget_matches_complete_canonical_envelope(value: Any) -> None:
    node = output("{{ a }}")
    result = resolve_node(node, {}, {"a": value})
    size = len(canonical_json(result).encode())
    assert resolve_node(node, {}, {"a": value}, max_bytes=size) == result
    with pytest.raises(DomainError) as failure:
        resolve_node(node, {}, {"a": value}, max_bytes=size - 1)
    assert failure.value.code == "RESOLVED_INPUT_LIMIT"


@pytest.mark.parametrize("offset", [4095, 4096, 8192])
def test_budget_rejects_invalid_unicode_at_chunk_boundary(offset: int) -> None:
    with pytest.raises(DomainError) as failure:
        resolve_node(output("{{ a }}"), {}, {"a": "x" * offset + "\ud800"})
    assert failure.value.code == "INVALID_UNICODE"


def test_budget_includes_escaped_interpolation_and_prompt_text() -> None:
    for node in [
        output("Text {{ a }}"),
        LLMNode(id="llm", handler="llm", dependencies=["a"], config={"prompt": "{{ a }}"}),
    ]:
        values = {"a": {"quote": '"\n\\'}}
        result = resolve_node(node, {}, values)
        size = len(canonical_json(result).encode())
        assert resolve_node(node, {}, values, max_bytes=size) == result
        with pytest.raises(DomainError):
            resolve_node(node, {}, values, max_bytes=size - 1)


def test_oversized_interpolation_fails_before_serialization(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def forbidden(_value: Any) -> str:
        pytest.fail("An excessive referenced value reached JSON serialization.")

    monkeypatch.setattr(templates, "canonical_json", forbidden)
    with pytest.raises(DomainError) as failure:
        resolve_node(output("value {{ a }}"), {}, {"a": {"large": "x" * 100_000}}, max_bytes=100)
    assert failure.value.code == "RESOLVED_INPUT_LIMIT"


def test_repeated_references_have_a_cumulative_budget() -> None:
    with pytest.raises(DomainError) as failure:
        resolve_node(output(["{{ a }}"] * 1_000), {}, {"a": "x" * 10_000}, max_bytes=20_000)
    assert failure.value.code == "RESOLVED_INPUT_LIMIT"


def test_output_aggregation_accepts_the_larger_execution_budget() -> None:
    node = OutputNode(id="out", handler="output", dependencies=["a", "b", "c"])
    parents = {key: "x" * 102_400 for key in node.dependencies}
    with pytest.raises(DomainError):
        resolve_node(node, {}, parents)
    assert resolve_node(node, {}, parents, max_bytes=8 * 1024 * 1024)["data"] == parents


def test_depth_limit_applies_to_resolved_containers() -> None:
    assert resolve_node(output("{{ a }}"), {}, {"a": {"nested": []}}, max_depth=3)
    with pytest.raises(DomainError) as failure:
        resolve_node(output("{{ a }}"), {}, {"a": {"nested": []}}, max_depth=2)
    assert failure.value.code == "RESOLVED_DEPTH_LIMIT"


def test_unchanged_sample_freezes_expected_inputs() -> None:
    url = "http://localhost:8911/document/policy/list"
    raw_nodes: list[dict[str, Any]] = [{"id": "input", "handler": "input", "dependencies": []}]
    branches = ["get_user", "get_posts", "get_comments"]
    raw_nodes.extend(
        {
            "id": name,
            "handler": "call_external_service",
            "dependencies": ["input"],
            "config": {"url": url},
        }
        for name in branches
    )
    raw_nodes.append({"id": "output", "handler": "output", "dependencies": branches})
    workflow = validate_workflow({"name": "Parallel API Fetcher", "dag": {"nodes": raw_nodes}})
    trigger = normalize_trigger(workflow, {})
    outputs: dict[str, Any] = {"input": resolve_node(workflow.dag.nodes[0], trigger, {})["data"]}
    for node in workflow.dag.nodes[1:4]:
        frozen = resolve_node(node, trigger, outputs)
        outputs[node.id] = {"url": frozen["config"]["url"], "data": frozen["data"]}
    assert resolve_node(workflow.dag.nodes[-1], trigger, outputs)["data"] == {
        key: {"url": url, "data": None} for key in branches
    }


@pytest.mark.parametrize("text", ["", "ASCII", "é漢字😀", "\ud7ff\ue000", "\U0010ffff"])
def test_unicode_round_trip(text: str) -> None:
    assert json.loads(canonical_json({text: text})) == {text: text}


@pytest.mark.parametrize("text", ["\ud800", "\udfff", "before\ud800after", "\ud800\udfff"])
def test_python_values_reject_surrogates(text: str) -> None:
    with pytest.raises(DomainError) as failure:
        canonical_json(text)
    assert failure.value.code == "INVALID_UNICODE"

    with pytest.raises(DomainError) as failure:
        canonical_json({text: "value"})
    assert failure.value.code == "INVALID_JSON_KEY"


def test_canonical_json_has_stable_keys_and_unicode() -> None:
    assert canonical_json({"z": "é", "a": [None, True, 1.5]}) == '{"a":[null,true,1.5],"z":"é"}'
    with pytest.raises(DomainError):
        canonical_json({"a": float("nan")})


def test_canonical_encoder_does_not_apply_request_depth_to_outer_envelopes() -> None:
    value: Any = None
    for _ in range(34):
        value = [value]
    assert canonical_json(value) == "[" * 34 + "null" + "]" * 34
