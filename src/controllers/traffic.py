from datetime import UTC, datetime
from time import perf_counter
from uuid import uuid4

from fastapi import UploadFile

from src.db.models import AnalysisKind
from src.db.repositories import AnalysisResultRepository
from src.schemas.traffic import (
    AnalysisDataResponse,
    AnalysisResponse,
    AnalysisResult,
    BatchAnalysisItem,
    BatchAnalysisItemResponse,
    BatchAnalysisResponse,
)
from src.services.traffic import TrafficAnalysisService


def _formatAnalysisResult(result: AnalysisResult) -> AnalysisDataResponse:
    """Convert domain analysis data to its API response representation."""
    try:
        return AnalysisDataResponse.model_validate(result.model_dump())
    except Exception as e:  # pragma: no cover - diagnostic boundary
        print(f"Error in _formatAnalysisResult: {e}")
        raise


def _formatBatchItem(item: BatchAnalysisItem) -> BatchAnalysisItemResponse:
    """Convert one domain batch item to its API response representation."""
    try:
        result = _formatAnalysisResult(item.result) if item.result is not None else None
        return BatchAnalysisItemResponse(
            filename=item.filename,
            status=item.status,
            result=result,
            error=item.error,
        )
    except Exception as e:  # pragma: no cover - diagnostic boundary
        print(f"Error in _formatBatchItem: {e}")
        raise


class TrafficController:
    """Coordinate HTTP use cases with the traffic analysis service."""

    def __init__(
        self,
        service: TrafficAnalysisService,
        repository: AnalysisResultRepository,
        batchConcurrency: int,
    ) -> None:
        """Create a controller.

        Args:
            service: Traffic analysis service.
            repository: Traffic analysis result repository.
            batchConcurrency: Maximum concurrent batch operations.

        Returns:
            A configured controller.

        Raises:
            ValueError: If batch concurrency is invalid.
        """
        try:
            if batchConcurrency < 1:
                raise ValueError("batchConcurrency must be positive.")
            self._service = service
            self._repository = repository
            self._batchConcurrency = batchConcurrency
        except Exception as e:  # pragma: no cover - diagnostic boundary
            print(f"Error in __init__: {e}")
            raise

    async def analyze(self, file: UploadFile) -> AnalysisResponse:
        """Analyze one uploaded traffic file.

        Args:
            file: Uploaded traffic text file.

        Returns:
            Calculated traffic statistics.

        Raises:
            TrafficCounterError: If validation or analysis fails.
        """
        try:
            startedAt = perf_counter()
            createdAt = datetime.now(UTC)
            result = await self._service.analyzeUpload(file)
            response = AnalysisResponse(
                id=uuid4(),
                createdAt=createdAt,
                timeTaken=round((perf_counter() - startedAt) * 1_000, 2),
                **_formatAnalysisResult(result).model_dump(),
            )
            await self._repository.save(
                resultId=response.id,
                analysisKind=AnalysisKind.SINGLE,
                createdAt=response.createdAt,
                timeTaken=response.timeTaken,
                responsePayload=response.model_dump(mode="json", by_alias=True),
            )
            return response
        except Exception as e:  # pragma: no cover - diagnostic boundary
            print(f"Error in analyze: {e}")
            raise

    async def analyzeBatch(self, files: list[UploadFile]) -> BatchAnalysisResponse:
        """Analyze multiple uploaded files with bounded concurrency.

        Args:
            files: Uploaded traffic text files.

        Returns:
            Ordered per-file analysis results.

        Raises:
            ValueError: If controller configuration is invalid.
        """
        try:
            startedAt = perf_counter()
            createdAt = datetime.now(UTC)
            result = await self._service.analyzeUploads(files, self._batchConcurrency)
            response = BatchAnalysisResponse(
                id=uuid4(),
                createdAt=createdAt,
                timeTaken=round((perf_counter() - startedAt) * 1_000, 2),
                items=[_formatBatchItem(item) for item in result.items],
            )
            await self._repository.save(
                resultId=response.id,
                analysisKind=AnalysisKind.BATCH,
                createdAt=response.createdAt,
                timeTaken=response.timeTaken,
                responsePayload=response.model_dump(mode="json", by_alias=True),
            )
            return response
        except Exception as e:  # pragma: no cover - diagnostic boundary
            print(f"Error in analyzeBatch: {e}")
            raise
