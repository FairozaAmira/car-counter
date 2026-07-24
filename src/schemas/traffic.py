from datetime import date, datetime
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field


class TrafficRecord(BaseModel):
    model_config = ConfigDict(frozen=True)

    timestamp: datetime
    car_count: int = Field(ge=0)


class DailyTotal(BaseModel):
    model_config = ConfigDict(frozen=True)

    date: date
    car_count: int = Field(ge=0)


class LeastCarsPeriod(BaseModel):
    model_config = ConfigDict(frozen=True)

    start: datetime
    end: datetime
    total_cars: int = Field(ge=0)
    records: list[TrafficRecord] = Field(min_length=3, max_length=3)


class AnalysisResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    total_cars: int = Field(ge=0)
    daily_totals: list[DailyTotal]
    top_half_hours: list[TrafficRecord]
    least_cars_period: LeastCarsPeriod


class ProcessingStatus(StrEnum):
    COMPLETED = "completed"
    FAILED = "failed"


class ErrorDetail(BaseModel):
    model_config = ConfigDict(frozen=True)

    code: str
    message: str


class BatchAnalysisItem(BaseModel):
    filename: str
    status: ProcessingStatus
    result: AnalysisResult | None = None
    error: ErrorDetail | None = None


class BatchAnalysisResponse(BaseModel):
    items: list[BatchAnalysisItem]
