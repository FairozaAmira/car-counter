"""Persisted traffic analysis response model."""

from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any
from uuid import UUID

from sqlalchemy import CheckConstraint, DateTime, Index, Numeric, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PostgreSQLUUID
from sqlalchemy.orm import Mapped, mapped_column

from src.db.base import Base


class AnalysisKind(StrEnum):
    """Identify which POST operation produced a stored result."""

    SINGLE = "single"
    BATCH = "batch"


class TrafficAnalysisResult(Base):
    """Store one complete POST API response.

    Args:
        None.

    Returns:
        A mapped traffic analysis result.

    Raises:
        None.
    """

    __tablename__ = "traffic_analysis_results"
    __table_args__ = (
        CheckConstraint("time_taken >= 0", name="ck_traffic_analysis_results_time_taken"),
        CheckConstraint(
            "analysis_kind IN ('single', 'batch')",
            name="ck_traffic_analysis_results_kind",
        ),
        Index(
            "ix_traffic_analysis_results_kind_created_at",
            "analysis_kind",
            "created_at",
        ),
    )

    id: Mapped[UUID] = mapped_column(PostgreSQLUUID(as_uuid=True), primary_key=True)
    analysis_kind: Mapped[str] = mapped_column(String(16), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    time_taken: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False)
    response_payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
