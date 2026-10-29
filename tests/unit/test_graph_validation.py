import pytest

from api.validation import normalize_trigger, validate_workflow
from shared.errors import DomainError


def branching_graph() -> dict:
    return {
        "name": "unordered fan-in",
        "dag": {
            "nodes": [
                {
                    "id": identifier,
                    "handler": "llm",
                    "dependencies": parents,
                    "config": {"prompt": prompt},
                }
                for identifier, parents, prompt in [
                    ("d", ["b", "c"], "{{ a }} {{ b }} {{ c }}"),
                    ("c", ["a"], "{{ a }}"),
                    ("b", ["a"], "{{ a }}"),
                    ("a", [], "root"),
                    ("x", [], "separate root"),
                ]
            ]
        },
    }


def test_unordered_graph_accepts_all_strict_ancestors_at_fan_in() -> None:
    workflow = validate_workflow(branching_graph())
    trigger = normalize_trigger(workflow, {"node_params": {"d": {"prompt": "{{ a }} {{ c }}"}}})
    assert trigger["node_params"]["d"]["prompt"] == "{{ a }} {{ c }}"


@pytest.mark.parametrize(("target", "reference"), [("d", "d"), ("b", "c"), ("d", "x")])
@pytest.mark.parametrize("override", [False, True])
def test_references_reject_self_siblings_and_disconnected_roots(
    target: str, reference: str, override: bool
) -> None:
    payload = branching_graph()
    with pytest.raises(DomainError) as failure:
        if override:
            normalize_trigger(
                validate_workflow(payload),
                {"node_params": {target: {"prompt": "{{ " + reference + " }}"}}},
            )
        else:
            node = next(node for node in payload["dag"]["nodes"] if node["id"] == target)
            node["config"]["prompt"] = "{{ " + reference + " }}"
            validate_workflow(payload)
    assert failure.value.code == "INVALID_REFERENCE"
