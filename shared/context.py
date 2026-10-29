# API and orchestrator contexts hold their database and settings.

from dataclasses import dataclass

from shared.config import DatabaseSettings as Settings
from shared.database import Database


@dataclass(frozen=True)
class Context[S: Settings = Settings]:
    # Keep a service's database and settings together for explicit dependency access.
    #
    # Args:
    #     db: Database pool and transaction wrapper.
    #     settings: Settings for the service that owns this context.
    #
    # Returns:
    #     An immutable context with shared database and settings references.
    db: Database
    settings: S
