from datetime import UTC, datetime
from time import perf_counter
from uuid import uuid4

from fastapi import UploadFile

from src.schemas.traffic import (
    AnalysisDataResponse,
    AnalysisResponse,
    AnalysisResult,
    BatchAnalysisItem,
    BatchAnalysisItemResponse,
    BatchAnalysisResponse,
)
from src.services.traffic import TrafficAnalysisService


def _format_analysis_result(result: AnalysisResult) -> AnalysisDataResponse:
    """Convert domain analysis data to its API response representation."""
    return AnalysisDataResponse.model_validate(result.model_dump())


def _format_batch_item(item: BatchAnalysisItem) -> BatchAnalysisItemResponse:
    """Convert one domain batch item to its API response representation."""
    result = _format_analysis_result(item.result) if item.result is not None else None
    return BatchAnalysisItemResponse(
        filename=item.filename,
        status=item.status,
        result=result,
        error=item.error,
    )


class TrafficController:
    """Coordinate HTTP use cases with the traffic analysis service."""

    def __init__(self, service: TrafficAnalysisService, batch_concurrency: int) -> None:
        """Create a controller.

        Args:
            service: Traffic analysis service.
            batch_concurrency: Maximum concurrent batch operations.

        Returns:
            A configured controller.

        Raises:
            ValueError: If batch concurrency is invalid.
        """
        if batch_concurrency < 1:
            raise ValueError("batch_concurrency must be positive.")
        self._service = service
        self._batch_concurrency = batch_concurrency

    async def analyze(self, file: UploadFile) -> AnalysisResponse:
        """Analyze one uploaded traffic file.

        Args:
            file: Uploaded traffic text file.

        Returns:
            Calculated traffic statistics.

        Raises:
            TrafficCounterError: If validation or analysis fails.
        """
        started_at = perf_counter()
        created_at = datetime.now(UTC)
        result = await self._service.analyze_upload(file)
        return AnalysisResponse(
            id=uuid4(),
            created_at=created_at,
            time_taken=round((perf_counter() - started_at) * 1_000, 2),
            **_format_analysis_result(result).model_dump(),
        )

    async def analyze_batch(self, files: list[UploadFile]) -> BatchAnalysisResponse:
        """Analyze multiple uploaded files with bounded concurrency.

        Args:
            files: Uploaded traffic text files.

        Returns:
            Ordered per-file analysis results.

        Raises:
            ValueError: If controller configuration is invalid.
        """
        started_at = perf_counter()
        created_at = datetime.now(UTC)
        result = await self._service.analyze_uploads(files, self._batch_concurrency)
        return BatchAnalysisResponse(
            id=uuid4(),
            created_at=created_at,
            time_taken=round((perf_counter() - started_at) * 1_000, 2),
            items=[_format_batch_item(item) for item in result.items],
        )
