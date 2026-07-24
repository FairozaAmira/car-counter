from typing import Annotated

from fastapi import APIRouter, Depends, File, UploadFile

from src.config import Settings, get_settings
from src.controllers.traffic import TrafficController
from src.schemas.traffic import AnalysisResult, BatchAnalysisResponse
from src.services.traffic import TrafficAnalysisService

router = APIRouter(prefix="/api/v1/traffic", tags=["traffic"])


def get_traffic_controller(
    settings: Annotated[Settings, Depends(get_settings)],
) -> TrafficController:
    return TrafficController(TrafficAnalysisService(), settings.batch_concurrency)


@router.post("/analyze", response_model=AnalysisResult)
async def analyze_file(
    file: Annotated[UploadFile, File(description="Traffic counter text file")],
    controller: Annotated[TrafficController, Depends(get_traffic_controller)],
) -> AnalysisResult:
    return await controller.analyze(file)


@router.post("/analyze/batch", response_model=BatchAnalysisResponse)
async def analyze_files(
    files: Annotated[list[UploadFile], File(description="Traffic counter text files")],
    controller: Annotated[TrafficController, Depends(get_traffic_controller)],
) -> BatchAnalysisResponse:
    return await controller.analyze_batch(files)
