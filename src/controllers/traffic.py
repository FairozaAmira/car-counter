from fastapi import UploadFile

from src.schemas.traffic import AnalysisResult, BatchAnalysisResponse
from src.services.traffic import TrafficAnalysisService


class TrafficController:
    def __init__(self, service: TrafficAnalysisService, batch_concurrency: int) -> None:
        self._service = service
        self._batch_concurrency = batch_concurrency

    async def analyze(self, file: UploadFile) -> AnalysisResult:
        return await self._service.analyze_upload(file)

    async def analyze_batch(self, files: list[UploadFile]) -> BatchAnalysisResponse:
        return await self._service.analyze_uploads(files, self._batch_concurrency)
