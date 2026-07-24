import asyncio

from fastapi import UploadFile

from src.schemas.traffic import (
    AnalysisResult,
    BatchAnalysisItem,
    BatchAnalysisResponse,
    ErrorDetail,
    ProcessingStatus,
)
from src.services.analyzer import analyze_traffic
from src.services.errors import InputValidationError, TrafficCounterError
from src.services.parser import parse_traffic_text


class TrafficAnalysisService:
    """Coordinates file decoding, parsing, and traffic analysis."""

    def analyze_text(self, content: str) -> AnalysisResult:
        return analyze_traffic(parse_traffic_text(content))

    async def analyze_upload(self, upload: UploadFile) -> AnalysisResult:
        try:
            content = (await upload.read()).decode("utf-8")
        except UnicodeDecodeError as exc:
            raise InputValidationError(
                "invalid_encoding",
                "The input file must be UTF-8 encoded text.",
            ) from exc
        finally:
            await upload.close()

        return self.analyze_text(content)

    async def analyze_uploads(
        self,
        uploads: list[UploadFile],
        concurrency: int,
    ) -> BatchAnalysisResponse:
        semaphore = asyncio.Semaphore(concurrency)
        results: list[BatchAnalysisItem | None] = [None] * len(uploads)

        async def analyze_one(index: int, upload: UploadFile) -> None:
            filename = upload.filename or f"upload-{index + 1}.txt"
            async with semaphore:
                try:
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

        async with asyncio.TaskGroup() as task_group:
            for index, upload in enumerate(uploads):
                task_group.create_task(analyze_one(index, upload))

        return BatchAnalysisResponse(items=[item for item in results if item is not None])
