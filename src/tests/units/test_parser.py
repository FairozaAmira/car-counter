from datetime import datetime

import pytest

from src.services.errors import InputValidationError
from src.services.parser import parse_traffic_text


def test_parse_sorts_records_and_accepts_whitespace() -> None:
    records = parse_traffic_text(
        "2021-12-01T05:30:00   12\n2021-12-01T05:00:00 5\n",
    )

    assert [record.timestamp for record in records] == [
        datetime(2021, 12, 1, 5, 0),
        datetime(2021, 12, 1, 5, 30),
    ]
    assert [record.car_count for record in records] == [5, 12]


@pytest.mark.parametrize(
    ("content", "code"),
    [
        ("", "empty_input"),
        ("not-a-time 1", "invalid_timestamp"),
        ("2021-12-01T05:00:00 cars", "invalid_car_count"),
        ("2021-12-01T05:00:00 -1", "invalid_car_count"),
        ("2021-12-01T05:00:00 1 extra", "invalid_record"),
        (
            "2021-12-01T05:00:00 1\n2021-12-01T05:00:00 2",
            "duplicate_timestamp",
        ),
    ],
)
def test_parse_rejects_invalid_input(content: str, code: str) -> None:
    with pytest.raises(InputValidationError) as error:
        parse_traffic_text(content)

    assert error.value.code == code
