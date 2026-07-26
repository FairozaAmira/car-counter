from datetime import date, datetime
from enum import StrEnum
from typing import Annotated
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, PlainSerializer

from src.utils.errors import ErrorCode
from src.utils.formatting import formatResponseDate, formatResponseDatetime

ResponseDate = Annotated[
    date,
    PlainSerializer(formatResponseDate, return_type=str, when_used="json"),
]
ResponseDateTime = Annotated[
    datetime,
    PlainSerializer(formatResponseDatetime, return_type=str, when_used="json"),
]
CreatedAtDate = Annotated[
    datetime,
    PlainSerializer(formatResponseDate, return_type=str, when_used="json"),
]


class TrafficRecord(BaseModel):
    """Represent one half-hour traffic observation."""

    model_config = ConfigDict(frozen=True)

    timestamp: datetime
    carCount: int = Field(ge=0)


class DailyTotal(BaseModel):
    """Represent the aggregated car count for one calendar day."""

    model_config = ConfigDict(frozen=True)

    date: date
    carCount: int = Field(ge=0)


class LeastCarsPeriod(BaseModel):
    """Represent the quietest contiguous 90-minute period."""

    model_config = ConfigDict(frozen=True)

    start: datetime
    end: datetime
    totalCars: int = Field(ge=0)
    records: list[TrafficRecord] = Field(min_length=3, max_length=3)


class AnalysisResult(BaseModel):
    """Represent all calculated traffic statistics."""

    model_config = ConfigDict(frozen=True)

    totalCars: int = Field(ge=0)
    dailyTotals: list[DailyTotal]
    topHalfHours: list[TrafficRecord]
    leastCarsPeriod: LeastCarsPeriod


class TrafficRecordResponse(BaseModel):
    """Represent one formatted traffic observation in an API response."""

    model_config = ConfigDict(frozen=True)

    timestamp: ResponseDateTime
    carCount: int = Field(ge=0)


class DailyTotalResponse(BaseModel):
    """Represent one formatted daily total in an API response."""

    model_config = ConfigDict(frozen=True)

    date: ResponseDate
    carCount: int = Field(ge=0)


class LeastCarsPeriodResponse(BaseModel):
    """Represent one formatted quietest period in an API response."""

    model_config = ConfigDict(frozen=True)

    start: ResponseDateTime
    end: ResponseDateTime
    totalCars: int = Field(ge=0)
    records: list[TrafficRecordResponse] = Field(min_length=3, max_length=3)


class AnalysisDataResponse(BaseModel):
    """Represent formatted traffic statistics in an API response."""

    model_config = ConfigDict(frozen=True)

    totalCars: int = Field(ge=0)
    dailyTotals: list[DailyTotalResponse]
    topHalfHours: list[TrafficRecordResponse]
    leastCarsPeriod: LeastCarsPeriodResponse


class ResponseMetadata(BaseModel):
    """Represent common metadata returned by successful POST operations."""

    model_config = ConfigDict(frozen=True, populate_by_name=True)

    id: UUID = Field(description="Unique identifier for this API operation")
    createdAt: CreatedAtDate = Field(
        alias="createdAt",
        description="UTC creation date formatted as DD-MM-YYYY",
    )
    timeTaken: float = Field(
        alias="timeTaken",
        ge=0,
        description="Operation duration in milliseconds, rounded to two decimal places",
        examples=[12.34],
    )


class AnalysisResponse(ResponseMetadata, AnalysisDataResponse):
    """Represent a successful single-file analysis API response."""


class ProcessingStatus(StrEnum):
    """Describe whether an independently processed item succeeded."""

    COMPLETED = "completed"
    FAILED = "failed"


class ErrorDetail(BaseModel):
    """Represent a stable, safe error returned to a client."""

    model_config = ConfigDict(frozen=True)

    code: ErrorCode
    message: str


class BatchAnalysisItem(BaseModel):
    """Represent one independently processed batch upload."""

    filename: str
    status: ProcessingStatus
    result: AnalysisResult | None = None
    error: ErrorDetail | None = None


class BatchAnalysisResult(BaseModel):
    """Represent ordered results from batch analysis."""

    items: list[BatchAnalysisItem]


class BatchAnalysisItemResponse(BaseModel):
    """Represent one formatted batch item in an API response."""

    filename: str
    status: ProcessingStatus
    result: AnalysisDataResponse | None = None
    error: ErrorDetail | None = None


class BatchAnalysisResponse(ResponseMetadata):
    """Represent a successful batch-analysis API response."""

    items: list[BatchAnalysisItemResponse]
