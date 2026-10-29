"""Integration tests run all three applications with one set of test options."""

from api.config import Settings as APISettings
from orchestrator.config import Settings as OrchestratorSettings
from worker.config import Settings as WorkerSettings


class Settings(APISettings, OrchestratorSettings, WorkerSettings):
    pass
