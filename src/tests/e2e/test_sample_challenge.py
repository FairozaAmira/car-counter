from pathlib import Path

from src.services.traffic import TrafficAnalysisService

FIXTURE = Path(__file__).parents[1] / "data" / "sample_traffic.txt"


def test_challenge_sample_output() -> None:
    result = TrafficAnalysisService().analyze_text(FIXTURE.read_text())

    assert result.total_cars == 398
    assert [(item.date.isoformat(), item.car_count) for item in result.daily_totals] == [
        ("2021-12-01", 179),
        ("2021-12-05", 81),
        ("2021-12-08", 134),
        ("2021-12-09", 4),
    ]
    assert [item.car_count for item in result.top_half_hours] == [46, 42, 33]
    assert result.least_cars_period.start.isoformat() == "2021-12-01T05:00:00"
    assert result.least_cars_period.total_cars == 31
