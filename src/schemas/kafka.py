from datetime import UTC, datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from src.schemas.traffic import AnalysisResult, ErrorDetail, ProcessingStatus, TrafficRecord


class KafkaAnalysisRequest(BaseModel):
    """Represent a versioned traffic-analysis request event."""

    model_config = ConfigDict(frozen=True)

    schemaVersion: str = "1.0"
    requestId: UUID
    filename: str
    createdAt: datetime = Field(default_factory=lambda: datetime.now(UTC))
    records: list[TrafficRecord]


class KafkaAnalysisResult(BaseModel):
    """Represent a versioned traffic-analysis result event."""

    model_config = ConfigDict(frozen=True)

    schemaVersion: str = "1.0"
    requestId: UUID
    filename: str
    completedAt: datetime = Field(default_factory=lambda: datetime.now(UTC))
    status: ProcessingStatus
    result: AnalysisResult | None = None
    error: ErrorDetail | None = None


class KafkaPublishItem(BaseModel):
    """Represent the outcome of publishing one local file."""

    filename: str
    status: ProcessingStatus
    requestId: UUID | None = None
    error: ErrorDetail | None = None
