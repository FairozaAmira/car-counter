from datetime import datetime

from pydantic import ValidationError

from src.schemas.traffic import TrafficRecord
from src.services.errors import InputValidationError

TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S"


def parse_traffic_text(content: str) -> list[TrafficRecord]:
    """Parse machine-generated traffic records from whitespace-delimited text."""

    records: list[TrafficRecord] = []
    timestamps: set[datetime] = set()

    for line_number, raw_line in enumerate(content.splitlines(), start=1):
        line = raw_line.strip()
        if not line:
            continue

        fields = line.split()
        if len(fields) != 2:
            raise InputValidationError(
                "invalid_record",
                f"Line {line_number} must contain a timestamp and car count.",
            )

        timestamp_text, count_text = fields
        try:
            timestamp = datetime.strptime(timestamp_text, TIMESTAMP_FORMAT)
        except ValueError as exc:
            raise InputValidationError(
                "invalid_timestamp",
                f"Line {line_number} has an invalid timestamp.",
            ) from exc

        try:
            car_count = int(count_text)
        except ValueError as exc:
            raise InputValidationError(
                "invalid_car_count",
                f"Line {line_number} has an invalid car count.",
            ) from exc

        if timestamp in timestamps:
            raise InputValidationError(
                "duplicate_timestamp",
                f"Line {line_number} duplicates timestamp {timestamp_text}.",
            )

        try:
            record = TrafficRecord(timestamp=timestamp, car_count=car_count)
        except ValidationError as exc:
            raise InputValidationError(
                "invalid_car_count",
                f"Line {line_number} has a negative car count.",
            ) from exc

        timestamps.add(timestamp)
        records.append(record)

    if not records:
        raise InputValidationError("empty_input", "The input file contains no traffic records.")

    return sorted(records, key=lambda record: record.timestamp)
