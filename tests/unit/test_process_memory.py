import json

import pytest

import tests.performance.test_memory as memory_test
from tests.performance.test_memory import PROCESS_MEMORY, process_kind


@pytest.mark.parametrize(
    "flags", [[], [b"-B"], [b"-I"], [b"-I", b"-B"], [b"-X", b"dev", b"-W", b"ignore"]]
)
@pytest.mark.parametrize(
    ("command", "kind"),
    [
        ([b"-m", b"api"], "service"),
        ([b"-m", b"orchestrator"], "service"),
        ([b"-m", b"worker"], "service"),
        ([b"-m", b"worker.handlers"], "handler"),
        ([b"-c", b"import urllib.request; urllib.request.urlopen('http://localhost')"], None),
        (
            [
                b"-c",
                b"from multiprocessing.spawn import spawn_main; spawn_main(tracker_fd=8, pipe_handle=12)",
                b"--multiprocessing-fork",
            ],
            "cpu_pool",
        ),
        (
            [b"-c", b"from multiprocessing.resource_tracker import main;main(7)"],
            "resource_tracker",
        ),
    ],
)
def test_python_flags_preserve_persistent_process_classification(
    flags: list[bytes], command: list[bytes], kind: str | None
) -> None:
    assert process_kind([b"/usr/local/bin/python", *flags, *command]) == kind


@pytest.mark.parametrize(
    "arguments",
    [
        [],
        [b"python", b"-B", b"-c"],
        [b"python", b"-I"],
        [b"/sbin/docker-init", b"--", b"python", b"-B", b"-m", b"api"],
        [b"python", b"script.py", b"-m", b"worker"],
        [b"python", b"--", b"-m", b"worker"],
        [b"python", b"-c", PROCESS_MEMORY.encode()],
    ],
)
def test_non_service_processes_and_probe_are_excluded(arguments: list[bytes]) -> None:
    assert process_kind(arguments) is None


@pytest.mark.parametrize("module", ["api", "worker", "orchestrator"])
@pytest.mark.parametrize(
    "children", [[], ["cpu_pool"], ["resource_tracker"], ["cpu_pool", "resource_tracker"]]
)
def test_warmed_services_require_pool_children_and_aggregate_their_rss(
    module: str, children: list[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    processes = [{"pid": 7, "kind": "service", "module": module, "rss_mib": 10}]
    processes.extend(
        {"pid": index + 8, "kind": kind, "rss_mib": 2} for index, kind in enumerate(children)
    )
    state = {"processes": processes, "handler_children": 0}
    monkeypatch.setattr(memory_test, "command", lambda *args: json.dumps(state))
    if module in ("api", "worker") and set(children) != {"cpu_pool", "resource_tracker"}:
        with pytest.raises(AssertionError):
            memory_test.memory(["container"])
    else:
        sample = memory_test.memory(["container"])["container"]
        assert sample["pid"] == 7 and sample["module"] == module
        assert sample["pids"] == list(range(7, 8 + len(children)))
        assert sample["rss_mib"] == 10 + 2 * len(children)
