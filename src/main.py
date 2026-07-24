from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from src.config import get_settings
from src.routers.traffic import router as traffic_router
from src.schemas.traffic import ErrorDetail
from src.services.errors import TrafficCounterError

settings = get_settings()
app = FastAPI(title=settings.app_name, version=settings.app_version)
app.include_router(traffic_router)


@app.get("/health", tags=["operations"])
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.exception_handler(TrafficCounterError)
async def traffic_error_handler(
    _request: Request,
    exception: TrafficCounterError,
) -> JSONResponse:
    detail = ErrorDetail(code=exception.code, message=exception.message)
    return JSONResponse(status_code=422, content={"detail": detail.model_dump()})
