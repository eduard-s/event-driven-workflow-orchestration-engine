# Connection, logging, and fixture settings shared across services.

import sys

from pydantic import PositiveInt, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from shared.logging_support import QueuedLogging


class Settings(BaseSettings):
    # Keep common connection and logging settings consistent across services.
    #
    # Attributes:
    #     redis_url: Redis connection URL.
    #     task_stream: Redis stream for worker tasks.
    #     event_stream: Redis stream for orchestrator events.
    #     enable_test_fixtures: Flag that permits explicit test behavior.
    #     log_level: Minimum log severity.
    #     log_queue_size: Maximum queued log records.
    #
    # Returns:
    #     Immutable settings from constructor values, WF_ variables, or defaults.
    #
    # Raises:
    #     ValidationError: A value is invalid or both streams have the same name.
    model_config = SettingsConfigDict(
        env_prefix="WF_",
        extra="ignore",
        frozen=True,
        allow_inf_nan=False,
        hide_input_in_errors=True,
    )
    redis_url: str = "redis://localhost:16379/0"
    task_stream: str = "workflow.tasks"
    event_stream: str = "workflow.events"
    enable_test_fixtures: bool = False
    log_level: str = "INFO"
    log_queue_size: PositiveInt = 1024

    @model_validator(mode="after")
    def bounded(self) -> Settings:
        # Reject equal stream names so task and control consumers stay separate.
        #
        # No arguments.
        #
        # Returns:
        #     This settings instance after the stream-name rule passes.
        #
        # Raises:
        #     ValueError: The task and event streams have the same name.
        if self.task_stream == self.event_stream:
            raise ValueError("Task and event streams must differ")
        return self


class DatabaseSettings(Settings):
    # Add PostgreSQL options for the API and orchestrator. Workers use Settings directly.
    # Args: postgres_dsn selects the database, pool_size bounds connections, and other values configure common resources.
    # Returns: Immutable database-service settings from arguments or WF_ variables.
    postgres_dsn: str = "postgresql://workflow:workflow@localhost:15432/workflow"
    pool_size: PositiveInt = 5


def configure_logging(settings: Settings) -> QueuedLogging:
    # Install bounded queued logging so slow output does not block service coroutines.
    #
    # Args:
    #     settings: Log level and maximum queue size.
    #
    # Returns:
    #     The logging controller. Its close method drains and stops output.
    #
    # Raises:
    #     Exception: Logging cannot start. The new controller closes before the error leaves.
    output = QueuedLogging(sys.stdout, settings.log_queue_size)
    try:
        output.install(settings.log_level)
    except BaseException:
        output.close()
        raise
    return output
