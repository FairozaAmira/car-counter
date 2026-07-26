from pathlib import Path

from src.services.traffic import TrafficAnalysisService

FIXTURE = Path(__file__).parents[1] / "data" / "sample_traffic.txt"


def test_challenge_sample_output() -> None:
    result = TrafficAnalysisService().analyzeText(FIXTURE.read_text())

    assert result.totalCars == 398
    assert [(item.date.isoformat(), item.carCount) for item in result.dailyTotals] == [
        ("2021-12-01", 179),
        ("2021-12-05", 81),
        ("2021-12-08", 134),
        ("2021-12-09", 4),
    ]
    assert [item.carCount for item in result.topHalfHours] == [46, 42, 33]
    assert result.leastCarsPeriod.start.isoformat() == "2021-12-01T05:00:00"
    assert result.leastCarsPeriod.totalCars == 31
