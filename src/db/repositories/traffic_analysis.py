"""Traffic analysis persistence repository."""

import logging
from collections.abc import Mapping
from datetime import datetime
from decimal import Decimal
from typing import Protocol
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
        result_id: UUID,
        analysis_kind: AnalysisKind,
        created_at: datetime,
        time_taken: float,
        response_payload: Mapping[str, object],
    ) -> None:
        """Persist one completed POST response.

        Args:
            result_id: Database primary key and response identifier.
            analysis_kind: POST operation that produced the result.
            created_at: Timezone-aware UTC creation timestamp.
            time_taken: Total request processing time in milliseconds.
            response_payload: JSON-compatible complete API response.

        Returns:
            None.

        Raises:
            DatabasePersistenceError: If the transaction fails.
        """
        ...


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
        self._session = session

    async def save(
        self,
        *,
        result_id: UUID,
        analysis_kind: AnalysisKind,
        created_at: datetime,
        time_taken: float,
        response_payload: Mapping[str, object],
    ) -> None:
        """Persist one completed POST response in a transaction.

        Args:
            result_id: Database primary key and response identifier.
            analysis_kind: POST operation that produced the result.
            created_at: Timezone-aware UTC creation timestamp.
            time_taken: Total request processing time in milliseconds.
            response_payload: JSON-compatible complete API response.

        Returns:
            None.

        Raises:
            DatabasePersistenceError: If SQLAlchemy cannot commit the result.
        """
        model = TrafficAnalysisResult(
            id=result_id,
            analysis_kind=analysis_kind.value,
            created_at=created_at,
            time_taken=Decimal(str(time_taken)),
            response_payload=dict(response_payload),
        )
        try:
            async with self._session.begin():
                self._session.add(model)
        except SQLAlchemyError as exc:
            await self._session.rollback()
            logger.exception("Failed to persist traffic analysis result")
            raise DatabasePersistenceError() from exc
