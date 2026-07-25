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
        super().__init__("rate_limit_exceeded", "Too many requests.", 429)
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
        super().__init__("authentication_failed", "A valid API key is required.", 401)
