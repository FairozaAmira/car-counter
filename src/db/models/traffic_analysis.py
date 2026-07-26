"""Persisted traffic analysis response model."""

from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any
from uuid import UUID

from sqlalchemy import CheckConstraint, DateTime, Index, Integer, Numeric, String, Text
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
        CheckConstraint("timeTaken >= 0", name="ck_traffic_analysis_results_time_taken"),
        CheckConstraint(
            "analysisKind IN ('single', 'batch')",
            name="ck_traffic_analysis_results_kind",
        ),
        Index(
            "ix_traffic_analysis_results_kind_created_at",
            "analysisKind",
            "createdAt",
        ),
    )

    id: Mapped[UUID] = mapped_column(PostgreSQLUUID(as_uuid=True), primary_key=True)
    analysisKind: Mapped[str] = mapped_column(String(16), nullable=False)
    createdAt: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    timeTaken: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False)
    totalCars: Mapped[int | None] = mapped_column(Integer, nullable=True)
    dailyTotals: Mapped[list[dict[str, Any]] | None] = mapped_column(JSONB, nullable=True)
    topHalfHours: Mapped[list[dict[str, Any]] | None] = mapped_column(JSONB, nullable=True)
    leastCarsPeriodStart: Mapped[str | None] = mapped_column(Text, nullable=True)
    leastCarsPeriodEnd: Mapped[str | None] = mapped_column(Text, nullable=True)
    leastCarsPeriodTotalCars: Mapped[int | None] = mapped_column(Integer, nullable=True)
    leastCarsPeriodRecords: Mapped[list[dict[str, Any]] | None] = mapped_column(
        JSONB,
        nullable=True,
    )
    items: Mapped[list[dict[str, Any]] | None] = mapped_column(JSONB, nullable=True)
