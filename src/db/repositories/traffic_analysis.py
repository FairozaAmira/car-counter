"""Traffic analysis persistence repository."""

import logging
from collections.abc import Mapping
from datetime import datetime
from decimal import Decimal
from typing import Protocol, cast
from uuid import UUID

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from src.db.models import AnalysisKind, TrafficAnalysisResult
from src.utils.errors import DatabasePersistenceError

logger = logging.getLogger(__name__)


class AnalysisResultRepository(Protocol):
    """Define persistence required by the traffic controller."""

    async def save(
        self,
        *,
        resultId: UUID,
        analysisKind: AnalysisKind,
        createdAt: datetime,
        timeTaken: float,
        responsePayload: Mapping[str, object],
    ) -> None:
        """Persist one completed POST response.

        Args:
            resultId: Database primary key and response identifier.
            analysisKind: POST operation that produced the result.
            createdAt: Timezone-aware UTC creation timestamp.
            timeTaken: Total request processing time in milliseconds.
            responsePayload: JSON-compatible complete API response.

        Returns:
            None.

        Raises:
            DatabasePersistenceError: If the transaction fails.
        """
        try:
            ...  # pragma: no cover - protocol declaration
        except Exception as e:  # pragma: no cover - diagnostic boundary
            print(f"Error in save: {e}")
            raise


class SqlAlchemyAnalysisResultRepository:
    """Persist traffic analysis results with an async SQLAlchemy session."""

    def __init__(self, session: AsyncSession) -> None:
        """Create a repository.

        Args:
            session: Request-scoped asynchronous database session.

        Returns:
            A configured repository.

        Raises:
            None.
        """
        try:
            self._session = session
        except Exception as e:  # pragma: no cover - diagnostic boundary
            print(f"Error in __init__: {e}")
            raise

    async def save(
        self,
        *,
        resultId: UUID,
        analysisKind: AnalysisKind,
        createdAt: datetime,
        timeTaken: float,
        responsePayload: Mapping[str, object],
    ) -> None:
        """Persist one completed POST response in a transaction.

        Args:
            resultId: Database primary key and response identifier.
            analysisKind: POST operation that produced the result.
            createdAt: Timezone-aware UTC creation timestamp.
            timeTaken: Total request processing time in milliseconds.
            responsePayload: JSON-compatible complete API response.

        Returns:
            None.

        Raises:
            DatabasePersistenceError: If SQLAlchemy cannot commit the result.
        """
        try:
            leastCarsPeriod = cast(
                Mapping[str, object],
                responsePayload.get("leastCarsPeriod") or {},
            )
            model = TrafficAnalysisResult(
                id=resultId,
                analysisKind=analysisKind.value,
                createdAt=createdAt,
                timeTaken=Decimal(str(timeTaken)),
                totalCars=cast(int | None, responsePayload.get("totalCars")),
                dailyTotals=cast(
                    list[dict[str, object]] | None,
                    responsePayload.get("dailyTotals"),
                ),
                topHalfHours=cast(
                    list[dict[str, object]] | None,
                    responsePayload.get("topHalfHours"),
                ),
                leastCarsPeriodStart=cast(str | None, leastCarsPeriod.get("start")),
                leastCarsPeriodEnd=cast(str | None, leastCarsPeriod.get("end")),
                leastCarsPeriodTotalCars=cast(int | None, leastCarsPeriod.get("totalCars")),
                leastCarsPeriodRecords=cast(
                    list[dict[str, object]] | None,
                    leastCarsPeriod.get("records"),
                ),
                items=cast(list[dict[str, object]] | None, responsePayload.get("items")),
            )
            try:
                async with self._session.begin():
                    self._session.add(model)
            except SQLAlchemyError as exc:
                await self._session.rollback()
                logger.exception("Failed to persist traffic analysis result")
                raise DatabasePersistenceError() from exc
        except Exception as e:  # pragma: no cover - diagnostic boundary
            print(f"Error in save: {e}")
            raise
