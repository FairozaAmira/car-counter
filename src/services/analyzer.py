from collections import defaultdict
from datetime import date, timedelta

from src.schemas.traffic import AnalysisResult, DailyTotal, LeastCarsPeriod, TrafficRecord
from src.utils.errors import AnalysisError, ErrorCode

HALF_HOUR = timedelta(minutes=30)
PERIOD_LENGTH = timedelta(minutes=90)


def analyzeTraffic(records: list[TrafficRecord]) -> AnalysisResult:
    """Calculate all outputs required by the coding challenge."""

    try:
        if not records:
            raise AnalysisError(
                ErrorCode.EMPTY_INPUT,
                "At least one traffic record is required.",
            )

        ordered = sorted(records, key=lambda record: record.timestamp)
        totalCars = sum(record.carCount for record in ordered)

        totalsByDate: defaultdict[date, int] = defaultdict(int)
        for record in ordered:
            totalsByDate[record.timestamp.date()] += record.carCount

        dailyTotals = [
            DailyTotal(date=day, carCount=totalsByDate[day]) for day in sorted(totalsByDate)
        ]
        topHalfHours = sorted(
            ordered,
            key=lambda record: (-record.carCount, record.timestamp),
        )[:3]

        windows: list[tuple[int, TrafficRecord, list[TrafficRecord]]] = []
        for index in range(len(ordered) - 2):
            window = ordered[index : index + 3]
            if (
                window[1].timestamp - window[0].timestamp == HALF_HOUR
                and window[2].timestamp - window[1].timestamp == HALF_HOUR
            ):
                windows.append((sum(item.carCount for item in window), window[0], window))

        if not windows:
            raise AnalysisError(
                ErrorCode.NO_CONTIGUOUS_PERIOD,
                "No three records form a contiguous 1.5-hour period.",
            )

        periodTotal, firstRecord, periodRecords = min(
            windows,
            key=lambda item: (item[0], item[1].timestamp),
        )
        leastPeriod = LeastCarsPeriod(
            start=firstRecord.timestamp,
            end=firstRecord.timestamp + PERIOD_LENGTH,
            totalCars=periodTotal,
            records=periodRecords,
        )

        return AnalysisResult(
            totalCars=totalCars,
            dailyTotals=dailyTotals,
            topHalfHours=topHalfHours,
            leastCarsPeriod=leastPeriod,
        )
    except Exception as e:  # pragma: no cover - diagnostic boundary
        print(f"Error in analyzeTraffic: {e}")
        raise
