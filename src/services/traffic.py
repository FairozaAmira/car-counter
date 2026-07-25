import asyncio

from fastapi import UploadFile

from src.schemas.traffic import (
    AnalysisResult,
    BatchAnalysisItem,
    BatchAnalysisResult,
    ErrorDetail,
    ProcessingStatus,
)
from src.services.analyzer import analyze_traffic
from src.services.parser import parse_traffic_text
from src.utils.errors import TrafficCounterError
from src.utils.files import read_upload_text, safe_upload_filename


class TrafficAnalysisService:
    """Coordinate file validation, parsing, and traffic analysis."""

    def __init__(
        self,
        upload_max_bytes: int = 1_048_576,
        allowed_extensions: tuple[str, ...] = (".txt",),
        allowed_mime_types: tuple[str, ...] = ("text/plain", "application/octet-stream"),
    ) -> None:
        """Configure secure upload validation.

        Args:
            upload_max_bytes: Maximum accepted upload size.
            allowed_extensions: Permitted lowercase filename extensions.
            allowed_mime_types: Permitted client content types.

        Returns:
            A configured analysis service.

        Raises:
            ValueError: If the maximum upload size is not positive.
        """
        if upload_max_bytes < 1:
            raise ValueError("upload_max_bytes must be positive.")
        self._upload_max_bytes = upload_max_bytes
        self._allowed_extensions = {extension.lower() for extension in allowed_extensions}
        self._allowed_mime_types = {mime_type.lower() for mime_type in allowed_mime_types}

    def analyze_text(self, content: str) -> AnalysisResult:
        """Parse and analyze traffic text.

        Args:
            content: UTF-8 traffic records as text.

        Returns:
            Calculated traffic statistics.

        Raises:
            TrafficCounterError: If the content cannot be parsed or analyzed.
        """
        return analyze_traffic(parse_traffic_text(content))

    async def analyze_upload(self, upload: UploadFile) -> AnalysisResult:
        """Validate, read, and analyze one upload.

        Args:
            upload: FastAPI upload object.

        Returns:
            Calculated traffic statistics.

        Raises:
            UploadValidationError: If filename, MIME type, size, or content is unsafe.
            TrafficCounterError: If traffic records are invalid.
        """
        content = await read_upload_text(
            upload,
            self._upload_max_bytes,
            self._allowed_extensions,
            self._allowed_mime_types,
        )
        return self.analyze_text(content)

    async def analyze_uploads(
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
        if concurrency < 1:
            raise ValueError("concurrency must be positive.")
        semaphore = asyncio.Semaphore(concurrency)
        results: list[BatchAnalysisItem | None] = [None] * len(uploads)

        async def analyze_one(index: int, upload: UploadFile) -> None:
            """Store one upload outcome at its original input index."""
            filename = f"upload-{index + 1}.txt"
            async with semaphore:
                try:
                    filename = safe_upload_filename(upload, index)
                    result = await self.analyze_upload(upload)
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

        await asyncio.gather(
            *(analyze_one(index, upload) for index, upload in enumerate(uploads)),
        )

        return BatchAnalysisResult(items=[item for item in results if item is not None])
