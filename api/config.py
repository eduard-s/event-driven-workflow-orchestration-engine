# API admission, audit, and defaults for saved execution policies.

from pydantic import Field, NonNegativeFloat, PositiveFloat, PositiveInt, model_validator

from shared.config import DatabaseSettings as CommonSettings

# Fixed API limits and protocol defaults belong here, next to environment settings.
REQUEST_CONCURRENCY = 64
READ_CONCURRENCY = 2
VALIDATION_CONCURRENCY = 4
VALIDATION_WORKERS = 2
SLOT_WAIT_SECONDS = 0.1
MAX_NODES = 500
MAX_EDGES = 2_000
MAX_KEY_LENGTH = 128
MAX_STATUS_PAGE = 100
AUDIT_POOL_SIZE = 2
AUDIT_QUEUE_SIZE = 64
AUDIT_DRAIN_TIMEOUT_SECONDS = 1
AUDIT_POOL_TIMEOUT_SECONDS = 0.2
AUDIT_WRITE_TIMEOUT_SECONDS = 0.25
AUDIT_CONNECT_TIMEOUT_SECONDS = 1
AUDIT_STATEMENT_TIMEOUT_MS = 200
AUDIT_RETENTION_SECONDS = 7 * 86400
AUDIT_PRUNE_BATCH = 1000
AUDIT_PRUNE_INTERVAL_SECONDS = 60
HTTP_HOST = "0.0.0.0"
HTTP_PORT = 8000
HTTP_SHUTDOWN_SECONDS = 20
READINESS_TIMEOUT_SECONDS = 3


class Settings(CommonSettings):
    # Keep API limits and defaults for each saved execution policy.
    #
    # Workers and orchestrators read the saved policy, so later environment changes cannot change an accepted execution.
    #
    # Args:
    #     audit_postgres_dsn: The connection URL for the separate audit database.
    #     requests_per_second: The request rate for this API process.
    #     max_body_bytes: The largest upload body in bytes.
    #     body_timeout: The upload time limit in seconds.
    #     body_concurrency: The maximum number of active upload readers.
    #     max_executions: The shared quota for unfinished executions.
    #     max_records: The shared quota for all saved executions.
    #     execution_slots: The queued or active task limit for each execution.
    #     handler_timeout: The time limit for each handler try in seconds.
    #     execution_timeout: The execution time limit after trigger acceptance in seconds.
    #     max_starts: The largest number of authorized starts for each node.
    #     retry_base: The initial retry delay in seconds.
    #     retry_max: The largest retry delay in seconds.
    #     retention_seconds: The record lifetime after terminal state.
    #     pending_seconds: The allowed delay before trigger acceptance.
    #     max_depth: The maximum JSON container depth.
    #     max_value_bytes: The byte limit for an ordinary node value.
    #     max_execution_bytes: Separate execution budgets for input and output bytes.
    #
    # Returns:
    #     Immutable settings from explicit fields or WF_ environment variables.
    #     Inherited fields supply database, broker, logging, and fixture settings.
    #
    # Raises:
    #     ValidationError: A field or related policy limit is invalid.

    audit_postgres_dsn: str = "postgresql://workflow:workflow@localhost:15432/workflow_audit"
    requests_per_second: int = Field(default=10, gt=0)
    max_body_bytes: int = Field(default=1_048_576, ge=1024, le=16_777_216)
    body_timeout: float = Field(default=5, gt=0, le=120)
    body_concurrency: int = Field(default=4, ge=1, le=64)
    max_executions: int = Field(default=100, ge=1, le=100_000)
    max_records: int = Field(default=1000, ge=1, le=100_000)
    execution_slots: PositiveInt = 8
    handler_timeout: PositiveFloat = 10
    execution_timeout: PositiveFloat = 300
    max_starts: PositiveInt = 3
    retry_base: NonNegativeFloat = 1
    retry_max: NonNegativeFloat = 10
    retention_seconds: PositiveFloat = 86400
    pending_seconds: PositiveFloat = 86400
    max_depth: PositiveInt = 32
    max_value_bytes: int = Field(default=262144, gt=0, le=16_777_216)
    max_execution_bytes: int = Field(default=8388608, gt=0, le=16_777_216)

    @model_validator(mode="after")
    def policy_limits(self) -> Settings:
        # Reject inconsistent timeouts and retry ranges before the API starts.
        #
        # No arguments beyond the settings instance.
        #
        # Returns:
        #     This settings instance when the policy limits agree.
        #
        # Raises:
        #     ValueError: The handler timeout exceeds the execution timeout, or the retry range is reversed.
        if self.handler_timeout > self.execution_timeout or self.retry_max < self.retry_base:
            raise ValueError("Invalid timeout settings")
        return self

    def execution_policy(self) -> dict[str, int | float]:
        # Copy policy defaults so an execution keeps its original resource limits.
        #
        # No arguments beyond the settings instance.
        #
        # Returns:
        #     A dictionary of scheduling, retry, deadline, retention, and JSON limits.
        names = (
            "execution_slots",
            "handler_timeout",
            "execution_timeout",
            "max_starts",
            "retry_base",
            "retry_max",
            "max_depth",
            "max_value_bytes",
            "max_execution_bytes",
            "retention_seconds",
            "pending_seconds",
        )
        return {name: getattr(self, name) for name in names}
