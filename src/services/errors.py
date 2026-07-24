class TrafficCounterError(Exception):
    """Base exception carrying a stable, externally safe error code."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


class InputValidationError(TrafficCounterError):
    """Raised when traffic input cannot be parsed or validated."""


class AnalysisError(TrafficCounterError):
    """Raised when valid records cannot produce a required analysis."""
