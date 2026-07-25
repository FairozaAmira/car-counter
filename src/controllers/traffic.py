from fastapi import UploadFile

from src.schemas.traffic import AnalysisResult, BatchAnalysisResponse
from src.services.traffic import TrafficAnalysisService


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

    async def analyze(self, file: UploadFile) -> AnalysisResult:
        """Analyze one uploaded traffic file.

        Args:
            file: Uploaded traffic text file.

        Returns:
            Calculated traffic statistics.

        Raises:
            TrafficCounterError: If validation or analysis fails.
        """
        return await self._service.analyze_upload(file)

    async def analyze_batch(self, files: list[UploadFile]) -> BatchAnalysisResponse:
        """Analyze multiple uploaded files with bounded concurrency.

        Args:
            files: Uploaded traffic text files.

        Returns:
            Ordered per-file analysis results.

        Raises:
            ValueError: If controller configuration is invalid.
        """
        return await self._service.analyze_uploads(files, self._batch_concurrency)
