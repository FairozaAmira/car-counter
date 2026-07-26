from typing import Annotated

from fastapi import APIRouter, Depends, File, UploadFile
from sqlalchemy.ext.asyncio import AsyncSession

from src.config import Settings, getSettings
from src.controllers.traffic import TrafficController
from src.db.repositories import AnalysisResultRepository, SqlAlchemyAnalysisResultRepository
from src.db.session import getDbSession
from src.dependencies.security import enforceUploadRateLimit
from src.schemas.traffic import AnalysisResponse, BatchAnalysisResponse
from src.services.traffic import TrafficAnalysisService

router = APIRouter(prefix="/api/v1/traffic", tags=["traffic"])


def getAnalysisRepository(
    session: Annotated[AsyncSession, Depends(getDbSession)],
) -> AnalysisResultRepository:
    """Build a request-scoped analysis result repository.

    Args:
        session: Request-scoped asynchronous database session.

    Returns:
        A SQLAlchemy-backed result repository.

    Raises:
        None.
    """
    try:
        return SqlAlchemyAnalysisResultRepository(session)
    except Exception as e:  # pragma: no cover - diagnostic boundary
        print(f"Error in getAnalysisRepository: {e}")
        raise


def getTrafficController(
    settings: Annotated[Settings, Depends(getSettings)],
    repository: Annotated[AnalysisResultRepository, Depends(getAnalysisRepository)],
) -> TrafficController:
    """Build a request-scoped traffic controller.

    Args:
        settings: Validated runtime settings.
        repository: Request-scoped analysis result repository.

    Returns:
        A configured traffic controller.

    Raises:
        ValueError: If service or concurrency settings are invalid.
    """
    try:
        service = TrafficAnalysisService(
            uploadMaxBytes=settings.uploadMaxBytes,
            allowedExtensions=settings.uploadAllowedExtensions,
            allowedMimeTypes=settings.uploadAllowedMimeTypes,
        )
        return TrafficController(service, repository, settings.batchConcurrency)
    except Exception as e:  # pragma: no cover - diagnostic boundary
        print(f"Error in getTrafficController: {e}")
        raise


@router.post(
    "/analyze",
    response_model=AnalysisResponse,
    summary="Analyze one traffic file",
    description="Validates and analyzes one UTF-8 traffic counter text file.",
    responses={
        401: {"description": "Invalid API key"},
        413: {"description": "File too large"},
        415: {"description": "Unsupported file"},
        422: {"description": "Invalid traffic data"},
        429: {"description": "Rate limit exceeded"},
    },
    dependencies=[Depends(enforceUploadRateLimit)],
)
async def analyzeFile(
    file: Annotated[UploadFile, File(description="Traffic counter text file")],
    controller: Annotated[TrafficController, Depends(getTrafficController)],
) -> AnalysisResponse:
    """Analyze one uploaded traffic file.

    Args:
        file: Traffic counter text file.
        controller: Injected traffic controller.

    Returns:
        Calculated traffic statistics.

    Raises:
        TrafficCounterError: If validation or analysis fails.
    """
    try:
        return await controller.analyze(file)
    except Exception as e:  # pragma: no cover - diagnostic boundary
        print(f"Error in analyzeFile: {e}")
        raise


@router.post(
    "/analyze/batch",
    response_model=BatchAnalysisResponse,
    summary="Analyze traffic files in a batch",
    description="Processes files concurrently and returns ordered item-level results.",
    responses={
        401: {"description": "Invalid API key"},
        429: {"description": "Rate limit exceeded"},
    },
    dependencies=[Depends(enforceUploadRateLimit)],
)
async def analyzeFiles(
    files: Annotated[
        list[UploadFile],
        File(
            description="Traffic counter text files",
            json_schema_extra={"items": {"type": "string", "format": "binary"}},
        ),
    ],
    controller: Annotated[TrafficController, Depends(getTrafficController)],
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
    try:
        return await controller.analyzeBatch(files)
    except Exception as e:  # pragma: no cover - diagnostic boundary
        print(f"Error in analyzeFiles: {e}")
        raise
