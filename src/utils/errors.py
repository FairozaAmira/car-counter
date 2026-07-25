"""Application exception hierarchy and stable error catalog."""

from enum import StrEnum
from typing import Final


class ErrorCode(StrEnum):
    """Define stable machine-readable application error codes."""

    KAFKA_PRODUCER_INITIALIZATION = "ERR00010"
    KAFKA_CONSUMER_INITIALIZATION = "ERR00011"
    KAFKA_CONSUMER_ACTION = "ERR00012"
    INVALID_JSON_REQUEST_BODY = "ERR00030"
    INVALID_REQUEST_BODY = "ERR00031"
    AUTHENTICATION_FAILED = "authentication_failed"
    RATE_LIMIT_EXCEEDED = "rate_limit_exceeded"
    INVALID_RECORD = "invalid_record"
    INVALID_TIMESTAMP = "invalid_timestamp"
    INVALID_CAR_COUNT = "invalid_car_count"
    DUPLICATE_TIMESTAMP = "duplicate_timestamp"
    EMPTY_INPUT = "empty_input"
    NO_CONTIGUOUS_PERIOD = "no_contiguous_period"
    INVALID_FILENAME = "invalid_filename"
    UNSUPPORTED_FILE_EXTENSION = "unsupported_file_extension"
    UNSUPPORTED_MEDIA_TYPE = "unsupported_media_type"
    FILE_TOO_LARGE = "file_too_large"
    INVALID_FILE_CONTENT = "invalid_file_content"
    INVALID_ENCODING = "invalid_encoding"
    FILE_READ_ERROR = "file_read_error"
    KAFKA_PUBLISH_ERROR = "kafka_publish_error"


STANDARD_ERROR_MESSAGES: Final[dict[ErrorCode, str]] = {
    ErrorCode.KAFKA_PRODUCER_INITIALIZATION: "Kafka Producer Initialization Error",
    ErrorCode.KAFKA_CONSUMER_INITIALIZATION: "Kafka Consumer Initialization Error",
    ErrorCode.KAFKA_CONSUMER_ACTION: "Error while executing Kafka consumer action",
    ErrorCode.INVALID_JSON_REQUEST_BODY: "Invalid or missing JSON request body",
    ErrorCode.INVALID_REQUEST_BODY: "Invalid request body",
}


class TrafficCounterError(Exception):
    """Carry a stable, externally safe error code.

    Args:
        code: Stable machine-readable error code.
        message: Safe user-facing error message.
        status_code: HTTP status used at the API boundary.

    Returns:
        A domain exception instance.

    Raises:
        None.
    """

    def __init__(self, code: str, message: str, status_code: int = 422) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.status_code = status_code


class InputValidationError(TrafficCounterError):
    """Raised when traffic input cannot be parsed or validated."""


class AnalysisError(TrafficCounterError):
    """Raised when valid records cannot produce a required analysis."""


class UploadValidationError(InputValidationError):
    """Represent a rejected uploaded file.

    Args:
        code: Stable machine-readable error code.
        message: Safe user-facing error message.
        status_code: HTTP status used at the API boundary.

    Returns:
        An upload validation exception.

    Raises:
        None.
    """

    def __init__(self, code: str, message: str, status_code: int) -> None:
        super().__init__(code, message, status_code)


class RateLimitExceededError(TrafficCounterError):
    """Represent an exceeded API rate limit."""

    def __init__(self, retry_after: int) -> None:
        """Create a rate-limit exception.

        Args:
            retry_after: Seconds until the current window resets.

        Returns:
            A rate-limit exception.

        Raises:
            None.
        """
        super().__init__(ErrorCode.RATE_LIMIT_EXCEEDED, "Too many requests.", 429)
        self.retry_after = retry_after


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
        super().__init__(
            ErrorCode.AUTHENTICATION_FAILED,
            "A valid API key is required.",
            401,
        )


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
        code = ErrorCode.KAFKA_PRODUCER_INITIALIZATION
        super().__init__(code, STANDARD_ERROR_MESSAGES[code], 500)


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
        code = ErrorCode.KAFKA_CONSUMER_INITIALIZATION
        super().__init__(code, STANDARD_ERROR_MESSAGES[code], 500)


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
        code = ErrorCode.KAFKA_CONSUMER_ACTION
        super().__init__(code, STANDARD_ERROR_MESSAGES[code], 500)


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
        code = ErrorCode.INVALID_JSON_REQUEST_BODY
        super().__init__(code, STANDARD_ERROR_MESSAGES[code], 400)


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
        code = ErrorCode.INVALID_REQUEST_BODY
        super().__init__(code, STANDARD_ERROR_MESSAGES[code], 422)
