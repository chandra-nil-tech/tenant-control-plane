"""Runtime configuration, read from CP_* environment variables.

Defaults point at the local docker-compose services; nothing secret lives here.
"""

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="CP_", extra="ignore")

    database_url: str = (
        "postgresql+asyncpg://control_plane:control_plane@localhost:5432/control_plane"
    )
    rabbitmq_url: str = "amqp://guest:guest@localhost:5672/"

    # How often the outbox relay looks for committed, unpublished events.
    outbox_poll_interval_s: float = Field(default=0.5, gt=0)

    # Tests run the API without the broker loops.
    enable_messaging: bool = True

    log_level: str = "INFO"


@lru_cache
def get_settings() -> Settings:
    return Settings()
