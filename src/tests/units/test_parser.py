from datetime import datetime

import pytest

from src.services.parser import parseTrafficText
from src.utils.errors import InputValidationError


def test_parse_sorts_records_and_accepts_whitespace() -> None:
    records = parseTrafficText(
        "2021-12-01T05:30:00   12\n\n2021-12-01T05:00:00 5\n",
    )

    assert [record.timestamp for record in records] == [
        datetime(2021, 12, 1, 5, 0),
        datetime(2021, 12, 1, 5, 30),
    ]
    assert [record.carCount for record in records] == [5, 12]


@pytest.mark.parametrize(
    ("content", "code"),
    [
        ("", "ERR00036"),
        ("not-a-time 1", "ERR00033"),
        ("2021-12-01T05:00:00 cars", "ERR00034"),
        ("2021-12-01T05:00:00 -1", "ERR00034"),
        ("2021-12-01T05:00:00 1 extra", "ERR00032"),
        (
            "2021-12-01T05:00:00 1\n2021-12-01T05:00:00 2",
            "ERR00035",
        ),
    ],
)
def test_parse_rejects_invalid_input(content: str, code: str) -> None:
    with pytest.raises(InputValidationError) as error:
        parseTrafficText(content)

    assert error.value.code == code
