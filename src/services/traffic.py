import asyncio
from pathlib import PurePath

from fastapi import UploadFile
from werkzeug.utils import secure_filename

from src.schemas.traffic import (
    AnalysisResult,
    BatchAnalysisItem,
    BatchAnalysisResponse,
    ErrorDetail,
    ProcessingStatus,
)
from src.services.analyzer import analyze_traffic
from src.services.errors import (
    TrafficCounterError,
    UploadValidationError,
)
from src.services.parser import parse_traffic_text


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
        self._validate_upload_metadata(upload)
        try:
            raw_content = await upload.read(self._upload_max_bytes + 1)
            if len(raw_content) > self._upload_max_bytes:
                raise UploadValidationError(
                    "file_too_large",
                    f"The input file must not exceed {self._upload_max_bytes} bytes.",
                    413,
                )
            if b"\x00" in raw_content:
                raise UploadValidationError(
                    "invalid_file_content",
                    "The input file must contain plain text.",
                    415,
                )
            content = raw_content.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise UploadValidationError(
                "invalid_encoding",
                "The input file must be UTF-8 encoded text.",
                415,
            ) from exc
        finally:
            await upload.close()

        return self.analyze_text(content)

    async def analyze_uploads(
        self,
        uploads: list[UploadFile],
        concurrency: int,
    ) -> BatchAnalysisResponse:
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
                    filename = self.safe_filename(upload, index)
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

        return BatchAnalysisResponse(items=[item for item in results if item is not None])

    def safe_filename(self, upload: UploadFile, index: int = 0) -> str:
        """Return a sanitized filename suitable for a response.

        Args:
            upload: Uploaded file whose filename is untrusted.
            index: Input position used for the fallback name.

        Returns:
            A non-empty sanitized filename.

        Raises:
            UploadValidationError: If an explicitly supplied filename is unsafe.
        """
        original = upload.filename
        if not original:
            return f"upload-{index + 1}.txt"
        sanitized = secure_filename(PurePath(original).name)
        if not sanitized:
            raise UploadValidationError("invalid_filename", "The filename is invalid.", 400)
        return sanitized

    def _validate_upload_metadata(self, upload: UploadFile) -> None:
        """Validate untrusted upload metadata before reading content."""
        filename = self.safe_filename(upload)
        extension = PurePath(filename).suffix.lower()
        if extension not in self._allowed_extensions:
            raise UploadValidationError(
                "unsupported_file_extension",
                "The input file extension is not supported.",
                415,
            )
        content_type = (upload.content_type or "").lower()
        if content_type not in self._allowed_mime_types:
            raise UploadValidationError(
                "unsupported_media_type",
                "The input file content type is not supported.",
                415,
            )
