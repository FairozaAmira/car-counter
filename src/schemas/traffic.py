from datetime import date, datetime
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field


class TrafficRecord(BaseModel):
    """Represent one half-hour traffic observation."""

    model_config = ConfigDict(frozen=True)

    timestamp: datetime
    car_count: int = Field(ge=0)


class DailyTotal(BaseModel):
    """Represent the aggregated car count for one calendar day."""

    model_config = ConfigDict(frozen=True)

    date: date
    car_count: int = Field(ge=0)


class LeastCarsPeriod(BaseModel):
    """Represent the quietest contiguous 90-minute period."""

    model_config = ConfigDict(frozen=True)

    start: datetime
    end: datetime
    total_cars: int = Field(ge=0)
    records: list[TrafficRecord] = Field(min_length=3, max_length=3)


class AnalysisResult(BaseModel):
    """Represent all calculated traffic statistics."""

    model_config = ConfigDict(frozen=True)

    total_cars: int = Field(ge=0)
    daily_totals: list[DailyTotal]
    top_half_hours: list[TrafficRecord]
    least_cars_period: LeastCarsPeriod


class ProcessingStatus(StrEnum):
    """Describe whether an independently processed item succeeded."""

    COMPLETED = "completed"
    FAILED = "failed"


class ErrorDetail(BaseModel):
    """Represent a stable, safe error returned to a client."""

    model_config = ConfigDict(frozen=True)

    code: str
    message: str


class BatchAnalysisItem(BaseModel):
    """Represent one independently processed batch upload."""

    filename: str
    status: ProcessingStatus
    result: AnalysisResult | None = None
    error: ErrorDetail | None = None


class BatchAnalysisResponse(BaseModel):
    """Represent ordered results for a batch request."""

    items: list[BatchAnalysisItem]
