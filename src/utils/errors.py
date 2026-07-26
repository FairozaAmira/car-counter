"""Application exception hierarchy and stable error catalog."""

from dataclasses import dataclass
from enum import StrEnum
from typing import Final


class ErrorCode(StrEnum):
    """Define stable machine-readable application error codes."""

    KAFKA_PRODUCER_INITIALIZATION = "ERR00010"
    KAFKA_CONSUMER_INITIALIZATION = "ERR00011"
    KAFKA_CONSUMER_ACTION = "ERR00012"
    KAFKA_PUBLISH_ERROR = "ERR00013"
    DATABASE_WRITE_ERROR = "ERR00020"
    AUTHENTICATION_FAILED = "ERR00021"
    RATE_LIMIT_EXCEEDED = "ERR00022"
    INVALID_JSON_REQUEST_BODY = "ERR00030"
    INVALID_REQUEST_BODY = "ERR00031"
    INVALID_RECORD = "ERR00032"
    INVALID_TIMESTAMP = "ERR00033"
    INVALID_CAR_COUNT = "ERR00034"
    DUPLICATE_TIMESTAMP = "ERR00035"
    EMPTY_INPUT = "ERR00036"
    NO_CONTIGUOUS_PERIOD = "ERR00037"
    INVALID_FILENAME = "ERR00040"
    UNSUPPORTED_FILE_EXTENSION = "ERR00041"
    UNSUPPORTED_MEDIA_TYPE = "ERR00042"
    FILE_TOO_LARGE = "ERR00043"
    INVALID_FILE_CONTENT = "ERR00044"
    INVALID_ENCODING = "ERR00045"
    FILE_READ_ERROR = "ERR00046"


@dataclass(frozen=True)
class ErrorDefinition:
    """Define the safe defaults for one stable application error."""

    message: str
    statusCode: int


ERROR_DEFINITIONS: Final[dict[ErrorCode, ErrorDefinition]] = {
    ErrorCode.KAFKA_PRODUCER_INITIALIZATION: ErrorDefinition(
        "Kafka Producer Initialization Error", 500
    ),
    ErrorCode.KAFKA_CONSUMER_INITIALIZATION: ErrorDefinition(
        "Kafka Consumer Initialization Error", 500
    ),
    ErrorCode.KAFKA_CONSUMER_ACTION: ErrorDefinition(
        "Error while executing Kafka consumer action", 500
    ),
    ErrorCode.KAFKA_PUBLISH_ERROR: ErrorDefinition("Kafka publish error", 500),
    ErrorCode.DATABASE_WRITE_ERROR: ErrorDefinition(
        "The analysis result could not be stored.", 503
    ),
    ErrorCode.AUTHENTICATION_FAILED: ErrorDefinition("A valid API key is required.", 401),
    ErrorCode.RATE_LIMIT_EXCEEDED: ErrorDefinition("Too many requests.", 429),
    ErrorCode.INVALID_JSON_REQUEST_BODY: ErrorDefinition(
        "Invalid or missing JSON request body", 400
    ),
    ErrorCode.INVALID_REQUEST_BODY: ErrorDefinition("Invalid request body", 422),
    ErrorCode.INVALID_RECORD: ErrorDefinition("Invalid traffic record", 422),
    ErrorCode.INVALID_TIMESTAMP: ErrorDefinition("Invalid timestamp", 422),
    ErrorCode.INVALID_CAR_COUNT: ErrorDefinition("Invalid car count", 422),
    ErrorCode.DUPLICATE_TIMESTAMP: ErrorDefinition("Duplicate timestamp", 422),
    ErrorCode.EMPTY_INPUT: ErrorDefinition("Input is empty", 422),
    ErrorCode.NO_CONTIGUOUS_PERIOD: ErrorDefinition("No contiguous period", 422),
    ErrorCode.INVALID_FILENAME: ErrorDefinition("Invalid filename", 400),
    ErrorCode.UNSUPPORTED_FILE_EXTENSION: ErrorDefinition("Unsupported file extension", 415),
    ErrorCode.UNSUPPORTED_MEDIA_TYPE: ErrorDefinition("Unsupported media type", 415),
    ErrorCode.FILE_TOO_LARGE: ErrorDefinition("File is too large", 413),
    ErrorCode.INVALID_FILE_CONTENT: ErrorDefinition("Invalid file content", 415),
    ErrorCode.INVALID_ENCODING: ErrorDefinition("Invalid file encoding", 415),
    ErrorCode.FILE_READ_ERROR: ErrorDefinition("File read error", 500),
}


class TrafficCounterError(Exception):
    """Carry a stable, externally safe error code.

    Args:
        code: Stable machine-readable error code.
        message: Safe user-facing error message.
        statusCode: HTTP status used at the API boundary.

    Returns:
        A domain exception instance.

    Raises:
        None.
    """

    def __init__(
        self,
        code: ErrorCode,
        message: str | None = None,
        statusCode: int | None = None,
    ) -> None:
        try:
            definition = ERROR_DEFINITIONS[code]
            resolvedMessage = message or definition.message
            resolvedStatusCode = statusCode or definition.statusCode
            super().__init__(resolvedMessage)
            self.code = code
            self.message = resolvedMessage
            self.statusCode = resolvedStatusCode
        except Exception as e:  # pragma: no cover - diagnostic boundary
            print(f"Error in __init__: {e}")
            raise


class InputValidationError(TrafficCounterError):
    """Raised when traffic input cannot be parsed or validated."""


class AnalysisError(TrafficCounterError):
    """Raised when valid records cannot produce a required analysis."""


class UploadValidationError(InputValidationError):
    """Represent a rejected uploaded file.

    Args:
        code: Stable machine-readable error code.
        message: Safe user-facing error message.
        statusCode: HTTP status used at the API boundary.

    Returns:
        An upload validation exception.

    Raises:
        None.
    """

    def __init__(
        self,
        code: ErrorCode,
        message: str,
        statusCode: int | None = None,
    ) -> None:
        try:
            super().__init__(code, message, statusCode)
        except Exception as e:  # pragma: no cover - diagnostic boundary
            print(f"Error in __init__: {e}")
            raise


class RateLimitExceededError(TrafficCounterError):
    """Represent an exceeded API rate limit."""

    def __init__(self, retryAfter: int) -> None:
        """Create a rate-limit exception.

        Args:
            retryAfter: Seconds until the current window resets.

        Returns:
            A rate-limit exception.

        Raises:
            None.
        """
        try:
            super().__init__(ErrorCode.RATE_LIMIT_EXCEEDED)
            self.retryAfter = retryAfter
        except Exception as e:  # pragma: no cover - diagnostic boundary
            print(f"Error in __init__: {e}")
            raise


class AuthenticationError(TrafficCounterError):
    """Represent missing or invalid API credentials."""

    def __init__(self) -> None:
        """Create a safe authentication exception.

        Args:
            None.

        Returns:
            An authentication exception.

        Raises:
            None.
        """
        super().__init__(ErrorCode.AUTHENTICATION_FAILED)


class KafkaProducerInitializationError(TrafficCounterError):
    """Represent failure to initialize the shared Kafka producer."""

    def __init__(self) -> None:
        """Create a producer initialization exception.

        Args:
            None.

        Returns:
            A standardized producer initialization exception.

        Raises:
            None.
        """
        super().__init__(ErrorCode.KAFKA_PRODUCER_INITIALIZATION)


class KafkaConsumerInitializationError(TrafficCounterError):
    """Represent failure to initialize the Kafka consumer."""

    def __init__(self) -> None:
        """Create a consumer initialization exception.

        Args:
            None.

        Returns:
            A standardized consumer initialization exception.

        Raises:
            None.
        """
        super().__init__(ErrorCode.KAFKA_CONSUMER_INITIALIZATION)


class KafkaConsumerActionError(TrafficCounterError):
    """Represent failure while executing a Kafka consumer action."""

    def __init__(self) -> None:
        """Create a consumer action exception.

        Args:
            None.

        Returns:
            A standardized consumer action exception.

        Raises:
            None.
        """
        super().__init__(ErrorCode.KAFKA_CONSUMER_ACTION)


class InvalidJsonRequestError(TrafficCounterError):
    """Represent a missing or malformed JSON request body."""

    def __init__(self) -> None:
        """Create an invalid JSON request exception.

        Args:
            None.

        Returns:
            A standardized invalid JSON request exception.

        Raises:
            None.
        """
        super().__init__(ErrorCode.INVALID_JSON_REQUEST_BODY)


class InvalidRequestBodyError(TrafficCounterError):
    """Represent a request body that fails schema validation."""

    def __init__(self) -> None:
        """Create an invalid request-body exception.

        Args:
            None.

        Returns:
            A standardized invalid request-body exception.

        Raises:
            None.
        """
        super().__init__(ErrorCode.INVALID_REQUEST_BODY)


class DatabasePersistenceError(TrafficCounterError):
    """Represent a failed database write."""

    def __init__(self) -> None:
        """Create a safe persistence exception.

        Args:
            None.

        Returns:
            A database persistence exception.

        Raises:
            None.
        """
        super().__init__(ErrorCode.DATABASE_WRITE_ERROR)
