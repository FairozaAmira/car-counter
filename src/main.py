import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from redis.asyncio import Redis
from redis.exceptions import RedisError

from src.config import Settings, get_settings
from src.middleware.request_context import RequestContextMiddleware
from src.routers.traffic import router as traffic_router
from src.schemas.traffic import ErrorDetail
from src.services.errors import RateLimitExceededError, TrafficCounterError
from src.services.rate_limit import RedisRateLimiter

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(application: FastAPI) -> AsyncIterator[None]:
    """Initialize and close process-local application resources.

    Args:
        application: FastAPI application instance.

    Yields:
        Control while the application is serving requests.

    Raises:
        RedisError: If the rate-limit backend is required and unavailable.
    """
    settings: Settings = application.state.settings
    redis_client: Redis | None = None
    if settings.rate_limit_enabled:
        assert settings.rate_limit_redis_url is not None
        redis_client = Redis.from_url(settings.rate_limit_redis_url, decode_responses=True)
        try:
            await redis_client.ping()
        except RedisError:
            if not settings.rate_limit_fail_open:
                await redis_client.aclose()
                raise
            logger.exception("Rate-limit backend unavailable during startup")
        application.state.redis_client = redis_client
        application.state.rate_limiter = RedisRateLimiter(
            redis_client,
            settings.rate_limit_window_seconds,
            settings.rate_limit_fail_open,
        )
    logger.info("Application started", extra={"environment": settings.environment})
    try:
        yield
    finally:
        if redis_client is not None:
            await redis_client.aclose()
        logger.info("Application stopped", extra={"environment": settings.environment})


def create_app(settings: Settings | None = None) -> FastAPI:
    """Create and configure the FastAPI application.

    Args:
        settings: Optional settings override used by tests.

    Returns:
        A configured FastAPI application.

    Raises:
        ValueError: If the supplied settings are inconsistent.
    """
    runtime_settings = settings or get_settings()
    application = FastAPI(
        title=runtime_settings.app_name,
        version=runtime_settings.app_version,
        description="Analyze half-hour traffic counter files.",
        lifespan=lifespan,
    )
    application.state.settings = runtime_settings

    def settings_dependency() -> Settings:
        """Return settings bound to this application instance.

        Args:
            None.

        Returns:
            The application's validated runtime settings.

        Raises:
            None.
        """
        return runtime_settings

    application.dependency_overrides[get_settings] = settings_dependency
    application.add_middleware(RequestContextMiddleware)
    if runtime_settings.cors_origins:
        application.add_middleware(
            CORSMiddleware,
            allow_origins=list(runtime_settings.cors_origins),
            allow_credentials=False,
            allow_methods=["GET", "POST"],
            allow_headers=["Accept", "Content-Type", "X-API-Key", "X-Request-ID"],
        )
    application.include_router(traffic_router)

    @application.get(
        "/health/live",
        tags=["operations"],
        summary="Liveness check",
        description="Confirms that the API process can serve requests.",
    )
    async def health_live() -> dict[str, str]:
        """Return the process liveness status.

        Args:
            None.

        Returns:
            A healthy liveness status.

        Raises:
            None.
        """
        return {"status": "ok"}

    @application.get(
        "/health/ready",
        tags=["operations"],
        summary="Readiness check",
        description="Checks the shared rate-limit backend when it is enabled.",
    )
    async def health_ready(request: Request) -> JSONResponse:
        """Return whether critical dependencies are ready.

        Args:
            request: Current HTTP request.

        Returns:
            A 200 response when ready, otherwise a 503 response.

        Raises:
            None.
        """
        current_settings: Settings = request.app.state.settings
        redis_client: Redis | None = getattr(request.app.state, "redis_client", None)
        if current_settings.rate_limit_enabled and redis_client is not None:
            try:
                await redis_client.ping()
            except RedisError:
                if current_settings.rate_limit_fail_open:
                    return JSONResponse(
                        status_code=200,
                        content={"status": "ready", "rate_limit": "degraded"},
                    )
                return JSONResponse(status_code=503, content={"status": "not_ready"})
        return JSONResponse(status_code=200, content={"status": "ready"})

    @application.exception_handler(TrafficCounterError)
    async def traffic_error_handler(
        _request: Request,
        exception: TrafficCounterError,
    ) -> JSONResponse:
        """Map safe domain failures to HTTP responses.

        Args:
            _request: Current HTTP request.
            exception: Domain failure to serialize.

        Returns:
            A safe structured error response.

        Raises:
            None.
        """
        detail = ErrorDetail(code=exception.code, message=exception.message)
        headers = None
        if isinstance(exception, RateLimitExceededError):
            headers = {"Retry-After": str(exception.retry_after)}
        return JSONResponse(
            status_code=exception.status_code,
            content={"detail": detail.model_dump()},
            headers=headers,
        )

    return application


app = create_app()
