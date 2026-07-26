from datetime import datetime

import pytest

from src.schemas.traffic import TrafficRecord
from src.services.analyzer import analyzeTraffic
from src.utils.errors import AnalysisError


def record(timestamp: str, count: int) -> TrafficRecord:
    return TrafficRecord(timestamp=datetime.fromisoformat(timestamp), carCount=count)


def test_analyze_calculates_totals_rankings_and_period() -> None:
    result = analyzeTraffic(
        [
            record("2021-12-02T00:00:00", 2),
            record("2021-12-01T06:00:00", 1),
            record("2021-12-01T05:00:00", 3),
            record("2021-12-01T05:30:00", 2),
            record("2021-12-01T06:30:00", 8),
        ],
    )

    assert result.totalCars == 16
    assert [(item.date.isoformat(), item.carCount) for item in result.dailyTotals] == [
        ("2021-12-01", 14),
        ("2021-12-02", 2),
    ]
    assert [item.carCount for item in result.topHalfHours] == [8, 3, 2]
    assert result.leastCarsPeriod.start == datetime(2021, 12, 1, 5, 0)
    assert result.leastCarsPeriod.end == datetime(2021, 12, 1, 6, 30)
    assert result.leastCarsPeriod.totalCars == 6


def test_ties_choose_earliest_timestamp_and_period() -> None:
    result = analyzeTraffic(
        [
            record("2021-12-01T05:00:00", 1),
            record("2021-12-01T05:30:00", 1),
            record("2021-12-01T06:00:00", 1),
            record("2021-12-01T06:30:00", 1),
        ],
    )

    assert result.topHalfHours[0].timestamp == datetime(2021, 12, 1, 5, 0)
    assert result.leastCarsPeriod.start == datetime(2021, 12, 1, 5, 0)


def test_cross_midnight_records_form_a_contiguous_period() -> None:
    result = analyzeTraffic(
        [
            record("2021-12-01T23:30:00", 1),
            record("2021-12-02T00:00:00", 2),
            record("2021-12-02T00:30:00", 3),
        ],
    )

    assert result.leastCarsPeriod.totalCars == 6
    assert [total.carCount for total in result.dailyTotals] == [1, 5]


def test_gap_does_not_form_a_period() -> None:
    with pytest.raises(AnalysisError) as error:
        analyzeTraffic(
            [
                record("2021-12-01T05:00:00", 1),
                record("2021-12-01T05:30:00", 2),
                record("2021-12-01T06:30:00", 3),
            ],
        )

    assert error.value.code == "ERR00037"


def test_analyze_rejects_empty_record_list() -> None:
    """Verify direct service use rejects an empty record collection."""
    with pytest.raises(AnalysisError) as error:
        analyzeTraffic([])

    assert error.value.code == "ERR00036"
