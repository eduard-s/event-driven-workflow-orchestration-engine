# Worker capacity and opt-in test fixtures.

from typing import Literal

from pydantic import Field, model_validator

from shared.config import Settings as CommonSettings


class Settings(CommonSettings):
    # Define worker capacity and execution mode. Each workflow keeps its saved policy.
    #
    # Args:
    #     worker_concurrency: Maximum active worker tasks, from 1 to 100.
    #     handler_mode: Automatic dispatch or forced process isolation.
    #     worker_failpoint: Optional named crash point for the test profile.
    #     test_fixtures_json: Per-node mock behavior for the test profile.
    #     **values: Common connection, logging, stream, and fixture settings.
    #
    # Returns:
    #     Immutable settings from arguments and WF_ environment variables.
    #
    # Raises:
    #     ValidationError: A field or fixture combination is invalid.

    worker_concurrency: int = Field(default=4, gt=0, le=100)
    handler_mode: Literal["auto", "process"] = "auto"
    worker_failpoint: str = ""
    test_fixtures_json: str = "{}"

    @model_validator(mode="after")
    def worker_limits(self) -> Settings:
        # Reject fixture settings unless the test profile explicitly permits them.
        #
        # No arguments beyond this settings instance.
        #
        # Returns:
        #     This settings instance after the fixture guard succeeds.
        #
        # Raises:
        #     ValueError: A failpoint or fixture exists outside the test profile.
        if (
            self.worker_failpoint or self.test_fixtures_json != "{}"
        ) and not self.enable_test_fixtures:
            raise ValueError("Test fixtures need the test profile")
        return self
