from collections import defaultdict
from datetime import date, timedelta

from src.schemas.traffic import AnalysisResult, DailyTotal, LeastCarsPeriod, TrafficRecord
from src.utils.errors import AnalysisError, ErrorCode

HALF_HOUR = timedelta(minutes=30)
PERIOD_LENGTH = timedelta(minutes=90)


def analyze_traffic(records: list[TrafficRecord]) -> AnalysisResult:
    """Calculate all outputs required by the coding challenge."""

    if not records:
        raise AnalysisError(
            ErrorCode.EMPTY_INPUT,
            "At least one traffic record is required.",
        )

    ordered = sorted(records, key=lambda record: record.timestamp)
    total_cars = sum(record.car_count for record in ordered)

    totals_by_date: defaultdict[date, int] = defaultdict(int)
    for record in ordered:
        totals_by_date[record.timestamp.date()] += record.car_count

    daily_totals = [
        DailyTotal(date=day, car_count=totals_by_date[day]) for day in sorted(totals_by_date)
    ]
    top_half_hours = sorted(
        ordered,
        key=lambda record: (-record.car_count, record.timestamp),
    )[:3]

    windows: list[tuple[int, TrafficRecord, list[TrafficRecord]]] = []
    for index in range(len(ordered) - 2):
        window = ordered[index : index + 3]
        if (
            window[1].timestamp - window[0].timestamp == HALF_HOUR
            and window[2].timestamp - window[1].timestamp == HALF_HOUR
        ):
            windows.append((sum(item.car_count for item in window), window[0], window))

    if not windows:
        raise AnalysisError(
            ErrorCode.NO_CONTIGUOUS_PERIOD,
            "No three records form a contiguous 1.5-hour period.",
        )

    period_total, first_record, period_records = min(
        windows,
        key=lambda item: (item[0], item[1].timestamp),
    )
    least_period = LeastCarsPeriod(
        start=first_record.timestamp,
        end=first_record.timestamp + PERIOD_LENGTH,
        total_cars=period_total,
        records=period_records,
    )

    return AnalysisResult(
        total_cars=total_cars,
        daily_totals=daily_totals,
        top_half_hours=top_half_hours,
        least_cars_period=least_period,
    )
