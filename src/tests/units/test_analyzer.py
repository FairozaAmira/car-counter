from datetime import datetime

import pytest

from src.schemas.traffic import TrafficRecord
from src.services.analyzer import analyze_traffic
from src.services.errors import AnalysisError


def record(timestamp: str, count: int) -> TrafficRecord:
    return TrafficRecord(timestamp=datetime.fromisoformat(timestamp), car_count=count)


def test_analyze_calculates_totals_rankings_and_period() -> None:
    result = analyze_traffic(
        [
            record("2021-12-02T00:00:00", 2),
            record("2021-12-01T06:00:00", 1),
            record("2021-12-01T05:00:00", 3),
            record("2021-12-01T05:30:00", 2),
            record("2021-12-01T06:30:00", 8),
        ],
    )

    assert result.total_cars == 16
    assert [(item.date.isoformat(), item.car_count) for item in result.daily_totals] == [
        ("2021-12-01", 14),
        ("2021-12-02", 2),
    ]
    assert [item.car_count for item in result.top_half_hours] == [8, 3, 2]
    assert result.least_cars_period.start == datetime(2021, 12, 1, 5, 0)
    assert result.least_cars_period.end == datetime(2021, 12, 1, 6, 30)
    assert result.least_cars_period.total_cars == 6


def test_ties_choose_earliest_timestamp_and_period() -> None:
    result = analyze_traffic(
        [
            record("2021-12-01T05:00:00", 1),
            record("2021-12-01T05:30:00", 1),
            record("2021-12-01T06:00:00", 1),
            record("2021-12-01T06:30:00", 1),
        ],
    )

    assert result.top_half_hours[0].timestamp == datetime(2021, 12, 1, 5, 0)
    assert result.least_cars_period.start == datetime(2021, 12, 1, 5, 0)


def test_cross_midnight_records_form_a_contiguous_period() -> None:
    result = analyze_traffic(
        [
            record("2021-12-01T23:30:00", 1),
            record("2021-12-02T00:00:00", 2),
            record("2021-12-02T00:30:00", 3),
        ],
    )

    assert result.least_cars_period.total_cars == 6
    assert [total.car_count for total in result.daily_totals] == [1, 5]


def test_gap_does_not_form_a_period() -> None:
    with pytest.raises(AnalysisError) as error:
        analyze_traffic(
            [
                record("2021-12-01T05:00:00", 1),
                record("2021-12-01T05:30:00", 2),
                record("2021-12-01T06:30:00", 3),
            ],
        )

    assert error.value.code == "no_contiguous_period"


def test_analyze_rejects_empty_record_list() -> None:
    """Verify direct service use rejects an empty record collection."""
    with pytest.raises(AnalysisError) as error:
        analyze_traffic([])

    assert error.value.code == "empty_input"
