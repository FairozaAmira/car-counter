from functools import lru_cache
from typing import Annotated

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict


class Settings(BaseSettings):
    """Provide validated runtime settings.

    Args:
        Values are loaded from environment variables and the local ``.env`` file.

    Returns:
        A validated settings instance.

    Raises:
        pydantic.ValidationError: If an environment value is invalid.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    app_name: str = "AIPS Traffic Counter"
    app_version: str = "1.0.0"
    environment: str = "local"
    app_host: str = "0.0.0.0"
    app_port: int = Field(default=8000, ge=1, le=65_535)
    app_workers: int = Field(default=4, ge=1)
    app_reload: bool = False
    graceful_shutdown_timeout_seconds: int = Field(default=30, ge=1)
    keep_alive_timeout_seconds: int = Field(default=5, ge=1)
    log_level: str = "INFO"
    batch_concurrency: int = Field(default=8, ge=1)
    cors_origins: Annotated[tuple[str, ...], NoDecode] = ()
    trusted_proxy_hosts: Annotated[tuple[str, ...], NoDecode] = ()
    api_key: str | None = None
    upload_max_bytes: int = Field(default=1_048_576, ge=1)
    upload_allowed_extensions: Annotated[tuple[str, ...], NoDecode] = (".txt",)
    upload_allowed_mime_types: Annotated[tuple[str, ...], NoDecode] = (
        "text/plain",
        "application/octet-stream",
    )

    rate_limit_enabled: bool = False
    rate_limit_redis_url: str | None = None
    rate_limit_upload_requests: int = Field(default=10, ge=1)
    rate_limit_window_seconds: int = Field(default=60, ge=1)
    rate_limit_fail_open: bool = True

    database_url: str | None = None
    database_pool_size: int = Field(default=5, ge=1)
    database_max_overflow: int = Field(default=10, ge=0)
    database_pool_timeout_seconds: float = Field(default=30.0, gt=0)
    database_pool_recycle_seconds: int = Field(default=1_800, ge=1)
    database_connect_timeout_seconds: float = Field(default=10.0, gt=0)
    database_command_timeout_seconds: float = Field(default=30.0, gt=0)

    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_request_topic: str = "traffic.analysis.requests"
    kafka_result_topic: str = "traffic.analysis.results"
    kafka_consumer_group: str = "traffic-analysis-workers"
    kafka_request_timeout_ms: int = Field(default=120_000, ge=1)
    kafka_consumer_poll_timeout_ms: int = Field(default=1_000, ge=1)
    kafka_consumer_max_records: int = Field(default=100, ge=1)

    @field_validator(
        "cors_origins",
        "trusted_proxy_hosts",
        "upload_allowed_extensions",
        "upload_allowed_mime_types",
        mode="before",
    )
    @classmethod
    def parse_comma_separated_values(cls, value: object) -> object:
        """Convert comma-separated environment values into tuples.

        Args:
            value: Raw value supplied by Pydantic Settings.

        Returns:
            A tuple for comma-separated strings, or the original value.

        Raises:
            None.
        """
        if not isinstance(value, str):
            return value
        parsed = tuple(item.strip() for item in value.split(",") if item.strip())
        return parsed

    @field_validator("api_key", "rate_limit_redis_url", "database_url", mode="before")
    @classmethod
    def empty_string_to_none(cls, value: object) -> object:
        """Treat empty optional environment values as disabled.

        Args:
            value: Raw optional setting value.

        Returns:
            ``None`` for an empty string, otherwise the original value.

        Raises:
            None.
        """
        if isinstance(value, str) and not value.strip():
            return None
        return value

    @field_validator("database_url")
    @classmethod
    def validate_database_url(cls, value: str | None) -> str | None:
        """Require the asynchronous PostgreSQL SQLAlchemy driver.

        Args:
            value: Configured database connection URL.

        Returns:
            The validated URL or ``None`` when startup is not being performed.

        Raises:
            ValueError: If a non-PostgreSQL or synchronous URL is configured.
        """
        if value is not None and not value.startswith("postgresql+asyncpg://"):
            raise ValueError("DATABASE_URL must start with postgresql+asyncpg://.")
        return value

    @field_validator("log_level")
    @classmethod
    def normalize_log_level(cls, value: str) -> str:
        """Normalize and validate the configured log level.

        Args:
            value: Raw logging level.

        Returns:
            An uppercase standard logging level.

        Raises:
            ValueError: If the level is unsupported.
        """
        normalized = value.upper()
        if normalized not in {"CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"}:
            raise ValueError("LOG_LEVEL must be a standard Python logging level.")
        return normalized

    @model_validator(mode="after")
    def validate_runtime_contract(self) -> "Settings":
        """Validate settings whose rules depend on multiple fields.

        Args:
            None.

        Returns:
            The validated settings instance.

        Raises:
            ValueError: If rate limiting or reload settings are inconsistent.
        """
        if self.rate_limit_enabled and not self.rate_limit_redis_url:
            raise ValueError("RATE_LIMIT_REDIS_URL is required when rate limiting is enabled.")
        if self.app_reload and self.app_workers != 1:
            raise ValueError("APP_WORKERS must be 1 when APP_RELOAD is enabled.")
        if "*" in self.cors_origins:
            raise ValueError("CORS_ORIGINS must list explicit origins; wildcard is not allowed.")
        return self


@lru_cache
def get_settings() -> Settings:
    """Return the process-wide immutable settings instance.

    Args:
        None.

    Returns:
        The cached runtime settings.

    Raises:
        pydantic.ValidationError: If configuration is invalid.
    """
    return Settings()
