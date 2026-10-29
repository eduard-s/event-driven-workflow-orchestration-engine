import pytest
from pydantic import ValidationError

from api.config import Settings as APISettings
from orchestrator.config import Settings as OrchestratorSettings
from shared.config import Settings as CommonSettings
from tests.settings import Settings
from worker.config import Settings as WorkerSettings

POLICY_DEFAULTS = {
    "execution_slots": 8,
    "handler_timeout": 10,
    "execution_timeout": 300,
    "max_starts": 3,
    "retry_base": 1,
    "retry_max": 10,
    "retention_seconds": 86400,
    "pending_seconds": 86400,
    "max_depth": 32,
    "max_value_bytes": 262144,
    "max_execution_bytes": 8388608,
}


def test_stream_groups_need_different_names() -> None:
    with pytest.raises(ValidationError):
        Settings(task_stream="same", event_stream="same")


@pytest.mark.parametrize("settings_type", [CommonSettings, WorkerSettings, OrchestratorSettings])
def test_only_api_settings_own_execution_policy_defaults(
    settings_type: type[CommonSettings],
) -> None:
    assert not POLICY_DEFAULTS.keys() & settings_type.model_fields.keys()
    assert POLICY_DEFAULTS.keys() <= APISettings.model_fields.keys()


def test_api_keeps_original_execution_policy_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    for field in POLICY_DEFAULTS:
        monkeypatch.delenv(f"WF_{field.upper()}", raising=False)
    assert APISettings().execution_policy() == POLICY_DEFAULTS


def test_api_saves_environment_policy_overrides(monkeypatch: pytest.MonkeyPatch) -> None:
    values = {**POLICY_DEFAULTS, "execution_slots": 6, "handler_timeout": 2, "retry_base": 0.25}
    for field, value in values.items():
        monkeypatch.setenv(f"WF_{field.upper()}", str(value))
    assert APISettings().execution_policy() == values


@pytest.mark.parametrize(
    "changes",
    [
        {"task_stream": "same", "event_stream": "same"},
        {"handler_timeout": 20, "execution_timeout": 10},
        {"retry_base": 2, "retry_max": 1},
    ],
)
def test_api_retains_stream_and_policy_validation(changes: dict) -> None:
    with pytest.raises(ValidationError):
        APISettings(**changes)


@pytest.mark.parametrize(
    "changes",
    [
        {"worker_concurrency": 0},
        {"worker_concurrency": 101},
        {"requests_per_second": 0},
        {"orchestrator_retries": -1},
        {"event_concurrency": 0},
        {"handler_timeout": 20, "execution_timeout": 10},
        {"worker_failpoint": "after_claim_before_call"},
        {"test_fixtures_json": '{"a":{}}'},
    ],
)
def test_invalid_settings_fail_at_startup(changes: dict) -> None:
    with pytest.raises(ValidationError):
        Settings(enable_test_fixtures=False, **changes)


@pytest.mark.parametrize(
    "field",
    [
        "handler_timeout",
        "execution_timeout",
        "retention_seconds",
        "repair_interval",
        "retry_base",
        "publication_lease",
    ],
)
@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_nonfinite_policies_fail_at_startup(field: str, value: float) -> None:
    with pytest.raises(ValidationError):
        Settings(**{field: value})


def test_worker_settings_have_no_database_options() -> None:
    assert not {"postgres_dsn", "pool_size"} & WorkerSettings.model_fields.keys()
    assert {"postgres_dsn", "pool_size"} <= OrchestratorSettings.model_fields.keys()
    assert {"postgres_dsn", "pool_size"} <= APISettings.model_fields.keys()
