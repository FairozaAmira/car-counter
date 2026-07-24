from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime settings loaded from environment variables or a local .env file."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    app_name: str = "AIPS Traffic Counter"
    app_version: str = "1.0.0"
    log_level: str = "INFO"
    batch_concurrency: int = Field(default=8, ge=1)

    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_request_topic: str = "traffic.analysis.requests"
    kafka_result_topic: str = "traffic.analysis.results"
    kafka_consumer_group: str = "traffic-analysis-workers"
    kafka_request_timeout_ms: int = Field(default=120_000, ge=1)
    kafka_consumer_poll_timeout_ms: int = Field(default=1_000, ge=1)
    kafka_consumer_max_records: int = Field(default=100, ge=1)


@lru_cache
def get_settings() -> Settings:
    return Settings()
