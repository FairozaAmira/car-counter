from typing import Annotated

from fastapi import APIRouter, Depends, File, UploadFile

from src.config import Settings, get_settings
from src.controllers.traffic import TrafficController
from src.dependencies.security import enforce_upload_rate_limit
from src.schemas.traffic import AnalysisResult, BatchAnalysisResponse
from src.services.traffic import TrafficAnalysisService

router = APIRouter(prefix="/api/v1/traffic", tags=["traffic"])


def get_traffic_controller(
    settings: Annotated[Settings, Depends(get_settings)],
) -> TrafficController:
    """Build a request-scoped traffic controller.

    Args:
        settings: Validated runtime settings.

    Returns:
        A configured traffic controller.

    Raises:
        ValueError: If service or concurrency settings are invalid.
    """
    service = TrafficAnalysisService(
        upload_max_bytes=settings.upload_max_bytes,
        allowed_extensions=settings.upload_allowed_extensions,
        allowed_mime_types=settings.upload_allowed_mime_types,
    )
    return TrafficController(service, settings.batch_concurrency)


@router.post(
    "/analyze",
    response_model=AnalysisResult,
    summary="Analyze one traffic file",
    description="Validates and analyzes one UTF-8 traffic counter text file.",
    responses={
        401: {"description": "Invalid API key"},
        413: {"description": "File too large"},
        415: {"description": "Unsupported file"},
        422: {"description": "Invalid traffic data"},
        429: {"description": "Rate limit exceeded"},
    },
    dependencies=[Depends(enforce_upload_rate_limit)],
)
async def analyze_file(
    file: Annotated[UploadFile, File(description="Traffic counter text file")],
    controller: Annotated[TrafficController, Depends(get_traffic_controller)],
) -> AnalysisResult:
    """Analyze one uploaded traffic file.

    Args:
        file: Traffic counter text file.
        controller: Injected traffic controller.

    Returns:
        Calculated traffic statistics.

    Raises:
        TrafficCounterError: If validation or analysis fails.
    """
    return await controller.analyze(file)


@router.post(
    "/analyze/batch",
    response_model=BatchAnalysisResponse,
    summary="Analyze traffic files in a batch",
    description="Processes files concurrently and returns ordered item-level results.",
    responses={
        401: {"description": "Invalid API key"},
        429: {"description": "Rate limit exceeded"},
    },
    dependencies=[Depends(enforce_upload_rate_limit)],
)
async def analyze_files(
    files: Annotated[list[UploadFile], File(description="Traffic counter text files")],
    controller: Annotated[TrafficController, Depends(get_traffic_controller)],
) -> BatchAnalysisResponse:
    """Analyze multiple uploaded traffic files.

    Args:
        files: Traffic counter text files.
        controller: Injected traffic controller.

    Returns:
        Ordered success and failure items.

    Raises:
        TrafficCounterError: If request-level validation fails.
    """
    return await controller.analyze_batch(files)
