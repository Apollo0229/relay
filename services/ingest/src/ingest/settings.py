from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Configuration from RELAY_* environment variables."""

    model_config = SettingsConfigDict(env_prefix="RELAY_")

    database_url: str = "postgresql://relay:relay@localhost:5432/relay"
    pool_size: int = 10
    topic: str = "relay.events"
