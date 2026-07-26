"""String-formatting utilities for public API responses."""

from datetime import date, datetime

DATE_RESPONSE_FORMAT = "%d-%m-%Y"
DATETIME_RESPONSE_FORMAT = "%d-%m-%Y %H:%M:%S"


def formatResponseDate(value: date) -> str:
    """Format a date using the public API convention.

    Args:
        value: Date to format.

    Returns:
        The date formatted as ``DD-MM-YYYY``.

    Raises:
        None.
    """
    try:
        return value.strftime(DATE_RESPONSE_FORMAT)
    except Exception as e:  # pragma: no cover - diagnostic boundary
        print(f"Error in formatResponseDate: {e}")
        raise


def formatResponseDatetime(value: datetime) -> str:
    """Format a date-time using the public API convention.

    Args:
        value: Date-time to format.

    Returns:
        The timestamp formatted as ``DD-MM-YYYY HH:MM:SS``.

    Raises:
        None.
    """
    try:
        return value.strftime(DATETIME_RESPONSE_FORMAT)
    except Exception as e:  # pragma: no cover - diagnostic boundary
        print(f"Error in formatResponseDatetime: {e}")
        raise
