from datetime import datetime

from pydantic import ValidationError

from src.schemas.traffic import TrafficRecord
from src.utils.errors import ErrorCode, InputValidationError

TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S"


def parseTrafficText(content: str) -> list[TrafficRecord]:
    """Parse machine-generated traffic records from whitespace-delimited text."""

    try:
        records: list[TrafficRecord] = []
        timestamps: set[datetime] = set()

        for lineNumber, rawLine in enumerate(content.splitlines(), start=1):
            line = rawLine.strip()
            if not line:
                continue

            fields = line.split()
            if len(fields) != 2:
                raise InputValidationError(
                    ErrorCode.INVALID_RECORD,
                    f"Line {lineNumber} must contain a timestamp and car count.",
                )

            timestampText, countText = fields
            try:
                timestamp = datetime.strptime(timestampText, TIMESTAMP_FORMAT)
            except ValueError as exc:
                raise InputValidationError(
                    ErrorCode.INVALID_TIMESTAMP,
                    f"Line {lineNumber} has an invalid timestamp.",
                ) from exc

            try:
                carCount = int(countText)
            except ValueError as exc:
                raise InputValidationError(
                    ErrorCode.INVALID_CAR_COUNT,
                    f"Line {lineNumber} has an invalid car count.",
                ) from exc

            if timestamp in timestamps:
                raise InputValidationError(
                    ErrorCode.DUPLICATE_TIMESTAMP,
                    f"Line {lineNumber} duplicates timestamp {timestampText}.",
                )

            try:
                record = TrafficRecord(timestamp=timestamp, carCount=carCount)
            except ValidationError as exc:
                raise InputValidationError(
                    ErrorCode.INVALID_CAR_COUNT,
                    f"Line {lineNumber} has a negative car count.",
                ) from exc

            timestamps.add(timestamp)
            records.append(record)

        if not records:
            raise InputValidationError(
                ErrorCode.EMPTY_INPUT,
                "The input file contains no traffic records.",
            )

        return sorted(records, key=lambda record: record.timestamp)
    except Exception as e:  # pragma: no cover - diagnostic boundary
        print(f"Error in parseTrafficText: {e}")
        raise
