import asyncio

from fastapi import UploadFile

from src.schemas.traffic import (
    AnalysisResult,
    BatchAnalysisItem,
    BatchAnalysisResult,
    ErrorDetail,
    ProcessingStatus,
)
from src.services.analyzer import analyzeTraffic
from src.services.parser import parseTrafficText
from src.utils.errors import TrafficCounterError
from src.utils.files import readUploadText, safeUploadFilename


class TrafficAnalysisService:
    """Coordinate file validation, parsing, and traffic analysis."""

    def __init__(
        self,
        uploadMaxBytes: int = 1_048_576,
        allowedExtensions: tuple[str, ...] = (".txt",),
        allowedMimeTypes: tuple[str, ...] = ("text/plain", "application/octet-stream"),
    ) -> None:
        """Configure secure upload validation.

        Args:
            uploadMaxBytes: Maximum accepted upload size.
            allowedExtensions: Permitted lowercase filename extensions.
            allowedMimeTypes: Permitted client content types.

        Returns:
            A configured analysis service.

        Raises:
            ValueError: If the maximum upload size is not positive.
        """
        if uploadMaxBytes < 1:
            raise ValueError("uploadMaxBytes must be positive.")
        self._uploadMaxBytes = uploadMaxBytes
        self._allowedExtensions = {extension.lower() for extension in allowedExtensions}
        self._allowedMimeTypes = {mimeType.lower() for mimeType in allowedMimeTypes}

    def analyzeText(self, content: str) -> AnalysisResult:
        """Parse and analyze traffic text.

        Args:
            content: UTF-8 traffic records as text.

        Returns:
            Calculated traffic statistics.

        Raises:
            TrafficCounterError: If the content cannot be parsed or analyzed.
        """
        try:
            return analyzeTraffic(parseTrafficText(content))
        except Exception as e:  # pragma: no cover - diagnostic boundary
            print(f"Error in analyzeText: {e}")
            raise

    async def analyzeUpload(self, upload: UploadFile) -> AnalysisResult:
        """Validate, read, and analyze one upload.

        Args:
            upload: FastAPI upload object.

        Returns:
            Calculated traffic statistics.

        Raises:
            UploadValidationError: If filename, MIME type, size, or content is unsafe.
            TrafficCounterError: If traffic records are invalid.
        """
        try:
            content = await readUploadText(
                upload,
                self._uploadMaxBytes,
                self._allowedExtensions,
                self._allowedMimeTypes,
            )
            return self.analyzeText(content)
        except Exception as e:  # pragma: no cover - diagnostic boundary
            print(f"Error in analyzeUpload: {e}")
            raise

    async def analyzeUploads(
        self,
        uploads: list[UploadFile],
        concurrency: int,
    ) -> BatchAnalysisResult:
        """Analyze uploads concurrently while preserving input order.

        Args:
            uploads: Files to validate and analyze.
            concurrency: Maximum simultaneous file operations.

        Returns:
            One success or failure item per input upload.

        Raises:
            ValueError: If concurrency is less than one.
        """
        try:
            if concurrency < 1:
                raise ValueError("concurrency must be positive.")
            semaphore = asyncio.Semaphore(concurrency)
            results: list[BatchAnalysisItem | None] = [None] * len(uploads)

            async def analyzeOne(index: int, upload: UploadFile) -> None:
                """Store one upload outcome at its original input index."""
                try:
                    filename = f"upload-{index + 1}.txt"
                    async with semaphore:
                        try:
                            filename = safeUploadFilename(upload, index)
                            result = await self.analyzeUpload(upload)
                            results[index] = BatchAnalysisItem(
                                filename=filename,
                                status=ProcessingStatus.COMPLETED,
                                result=result,
                            )
                        except TrafficCounterError as exc:
                            results[index] = BatchAnalysisItem(
                                filename=filename,
                                status=ProcessingStatus.FAILED,
                                error=ErrorDetail(code=exc.code, message=exc.message),
                            )
                except Exception as e:  # pragma: no cover - diagnostic boundary
                    print(f"Error in analyzeOne: {e}")
                    raise

            await asyncio.gather(
                *(analyzeOne(index, upload) for index, upload in enumerate(uploads)),
            )

            return BatchAnalysisResult(items=[item for item in results if item is not None])
        except Exception as e:  # pragma: no cover - diagnostic boundary
            print(f"Error in analyzeUploads: {e}")
            raise
