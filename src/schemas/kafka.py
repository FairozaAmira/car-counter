from datetime import UTC, datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from src.schemas.traffic import AnalysisResult, ErrorDetail, ProcessingStatus, TrafficRecord


class KafkaAnalysisRequest(BaseModel):
    model_config = ConfigDict(frozen=True)

    schema_version: str = "1.0"
    request_id: UUID
    filename: str
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    records: list[TrafficRecord]


class KafkaAnalysisResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    schema_version: str = "1.0"
    request_id: UUID
    filename: str
    completed_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    status: ProcessingStatus
    result: AnalysisResult | None = None
    error: ErrorDetail | None = None


class KafkaPublishItem(BaseModel):
    filename: str
    status: ProcessingStatus
    request_id: UUID | None = None
    error: ErrorDetail | None = None
