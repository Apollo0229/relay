from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="WORKER_")

    database_url: str = "postgresql://relay:relay@localhost:5432/relay"
    kafka_brokers: str = "localhost:9092"
    group_id: str = "relay-workers"
    # Recorded in processed_events; another consumer of the same topic gets its own name.
    consumer: str = "event-counter"
    topic: str = "relay.events"
    # Seconds to wait before each retry, as JSON: [5, 30] means two retries.
    retry_delays: list[float] = [5, 30]

    @property
    def retry_topics(self) -> list[str]:
        return [f"{self.topic}.retry.{n}" for n in range(1, len(self.retry_delays) + 1)]

    @property
    def dead_letter_topic(self) -> str:
        return f"{self.topic}.dlq"
