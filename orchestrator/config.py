# Control-loop retry, publication, and recovery settings.

from pydantic import Field, PositiveFloat, PositiveInt, model_validator

from shared.config import DatabaseSettings as CommonSettings


class Settings(CommonSettings):
    # Define local control-loop limits and recovery options. Common connection and logging fields come from CommonSettings.
    #
    # Args:
    #     orchestrator_retries: Dependency retries after the first call. The default is three.
    #     event_concurrency: Maximum simultaneous control events. The default is three.
    #     maintenance_interval: Seconds between maintenance passes. The default is one.
    #     repair_interval: Seconds before notification repair. The default is 15.
    #     maintenance_batch: Maximum records per maintenance or relay pass. The default is 100.
    #     publication_lease: Seconds reserved for one publisher. The default is three.
    #     enable_repair: Whether database and broker repair paths run. The default is True.
    #     orchestrator_failpoint: Optional crash location for an enabled test profile.
    #
    # Returns:
    #     Settings loaded from the supplied fields and WF_ environment variables.
    #
    # Raises:
    #     ValidationError: A value violates its type, range, or test-profile rule.

    orchestrator_retries: int = Field(default=3, ge=0)
    event_concurrency: PositiveInt = 3
    maintenance_interval: PositiveFloat = 1
    repair_interval: PositiveFloat = 15
    maintenance_batch: PositiveInt = 100
    publication_lease: PositiveFloat = 3
    enable_repair: bool = True
    orchestrator_failpoint: str = ""

    @model_validator(mode="after")
    def orchestrator_limits(self) -> Settings:
        # Reject a crash failpoint outside the test profile to prevent accidental service exits.
        #
        # No arguments.
        #
        # Returns:
        #     This settings instance when the profile permits its failpoint.
        #
        # Raises:
        #     ValueError: A failpoint is set while test fixtures are disabled.
        if self.orchestrator_failpoint and not self.enable_test_fixtures:
            raise ValueError("Test fixtures need the test profile")
        return self
