from functools import lru_cache
from typing import Annotated

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict


def toEnvironmentName(fieldName: str) -> str:
    """Convert a camelCase setting name to its uppercase environment name.

    Args:
        fieldName: Camel-case Pydantic field name.

    Returns:
        The equivalent uppercase snake-case environment variable name.

    Raises:
        Exception: Re-raises unexpected conversion failures.
    """
    try:
        characters: list[str] = []
        for character in fieldName:
            if character.isupper():
                characters.extend(("_", character))
            else:
                characters.append(character.upper())
        return "".join(characters)
    except Exception as e:
        print(f"Error in toEnvironmentName: {e}")
        raise


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
        alias_generator=toEnvironmentName,
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        populate_by_name=True,
    )

    appName: str = "AIPS Traffic Counter"
    appVersion: str = "1.0.0"
    environment: str = "local"
    appHost: str = "0.0.0.0"
    appPort: int = Field(default=8000, ge=1, le=65_535)
    appWorkers: int = Field(default=4, ge=1)
    appReload: bool = False
    gracefulShutdownTimeoutSeconds: int = Field(default=30, ge=1)
    keepAliveTimeoutSeconds: int = Field(default=5, ge=1)
    logLevel: str = "INFO"
    batchConcurrency: int = Field(default=8, ge=1)
    corsOrigins: Annotated[tuple[str, ...], NoDecode] = ()
    trustedProxyHosts: Annotated[tuple[str, ...], NoDecode] = ()
    apiKey: str | None = None
    uploadMaxBytes: int = Field(default=1_048_576, ge=1)
    uploadAllowedExtensions: Annotated[tuple[str, ...], NoDecode] = (".txt",)
    uploadAllowedMimeTypes: Annotated[tuple[str, ...], NoDecode] = (
        "text/plain",
        "application/octet-stream",
    )

    rateLimitEnabled: bool = False
    rateLimitRedisUrl: str | None = None
    rateLimitUploadRequests: int = Field(default=10, ge=1)
    rateLimitWindowSeconds: int = Field(default=60, ge=1)
    rateLimitFailOpen: bool = True

    databaseUrl: str | None = None
    databasePoolSize: int = Field(default=5, ge=1)
    databaseMaxOverflow: int = Field(default=10, ge=0)
    databasePoolTimeoutSeconds: float = Field(default=30.0, gt=0)
    databasePoolRecycleSeconds: int = Field(default=1_800, ge=1)
    databaseConnectTimeoutSeconds: float = Field(default=10.0, gt=0)
    databaseCommandTimeoutSeconds: float = Field(default=30.0, gt=0)

    kafkaBootstrapServers: str = "localhost:9092"
    kafkaRequestTopic: str = "traffic.analysis.requests"
    kafkaResultTopic: str = "traffic.analysis.results"
    kafkaConsumerGroup: str = "traffic-analysis-workers"
    kafkaRequestTimeoutMs: int = Field(default=120_000, ge=1)
    kafkaConsumerPollTimeoutMs: int = Field(default=1_000, ge=1)
    kafkaConsumerMaxRecords: int = Field(default=100, ge=1)

    @field_validator(
        "corsOrigins",
        "trustedProxyHosts",
        "uploadAllowedExtensions",
        "uploadAllowedMimeTypes",
        mode="before",
    )
    @classmethod
    def parseCommaSeparatedValues(cls, value: object) -> object:
        """Convert comma-separated environment values into tuples.

        Args:
            value: Raw value supplied by Pydantic Settings.

        Returns:
            A tuple for comma-separated strings, or the original value.

        Raises:
            None.
        """
        try:
            if not isinstance(value, str):
                return value
            parsed = tuple(item.strip() for item in value.split(",") if item.strip())
            return parsed
        except Exception as e:  # pragma: no cover - diagnostic boundary
            print(f"Error in parseCommaSeparatedValues: {e}")
            raise

    @field_validator("apiKey", "rateLimitRedisUrl", "databaseUrl", mode="before")
    @classmethod
    def emptyStringToNone(cls, value: object) -> object:
        """Treat empty optional environment values as disabled.

        Args:
            value: Raw optional setting value.

        Returns:
            ``None`` for an empty string, otherwise the original value.

        Raises:
            None.
        """
        try:
            if isinstance(value, str) and not value.strip():
                return None
            return value
        except Exception as e:  # pragma: no cover - diagnostic boundary
            print(f"Error in emptyStringToNone: {e}")
            raise

    @field_validator("databaseUrl")
    @classmethod
    def validateDatabaseUrl(cls, value: str | None) -> str | None:
        """Require the asynchronous PostgreSQL SQLAlchemy driver.

        Args:
            value: Configured database connection URL.

        Returns:
            The validated URL or ``None`` when startup is not being performed.

        Raises:
            ValueError: If a non-PostgreSQL or synchronous URL is configured.
        """
        try:
            if value is not None and not value.startswith("postgresql+asyncpg://"):
                raise ValueError("DATABASE_URL must start with postgresql+asyncpg://.")
            return value
        except Exception as e:  # pragma: no cover - diagnostic boundary
            print(f"Error in validateDatabaseUrl: {e}")
            raise

    @field_validator("logLevel")
    @classmethod
    def normalizeLogLevel(cls, value: str) -> str:
        """Normalize and validate the configured log level.

        Args:
            value: Raw logging level.

        Returns:
            An uppercase standard logging level.

        Raises:
            ValueError: If the level is unsupported.
        """
        try:
            normalized = value.upper()
            if normalized not in {"CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"}:
                raise ValueError("LOG_LEVEL must be a standard Python logging level.")
            return normalized
        except Exception as e:  # pragma: no cover - diagnostic boundary
            print(f"Error in normalizeLogLevel: {e}")
            raise

    @model_validator(mode="after")
    def validateRuntimeContract(self) -> "Settings":
        """Validate settings whose rules depend on multiple fields.

        Args:
            None.

        Returns:
            The validated settings instance.

        Raises:
            ValueError: If rate limiting or reload settings are inconsistent.
        """
        if self.rateLimitEnabled and not self.rateLimitRedisUrl:
            raise ValueError("RATE_LIMIT_REDIS_URL is required when rate limiting is enabled.")
        if self.appReload and self.appWorkers != 1:
            raise ValueError("APP_WORKERS must be 1 when APP_RELOAD is enabled.")
        if "*" in self.corsOrigins:
            raise ValueError("CORS_ORIGINS must list explicit origins; wildcard is not allowed.")
        return self


@lru_cache
def getSettings() -> Settings:
    """Return the process-wide immutable settings instance.

    Args:
        None.

    Returns:
        The cached runtime settings.

    Raises:
        pydantic.ValidationError: If configuration is invalid.
    """
    return Settings()
