import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from redis.asyncio import Redis
from redis.exceptions import RedisError
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine

from src.config import Settings, getSettings
from src.db.session import (
    checkDatabaseConnection,
    createDatabaseEngine,
    createSessionFactory,
)
from src.middleware.request_context import RequestContextMiddleware
from src.routers.traffic import router as trafficRouter
from src.schemas.traffic import ErrorDetail
from src.services.rate_limit import RedisRateLimiter
from src.utils.errors import (
    InvalidRequestBodyError,
    RateLimitExceededError,
    TrafficCounterError,
)

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
    try:
        settings: Settings = application.state.settings
        databaseEngine = createDatabaseEngine(settings)
        redisClient: Redis | None = None
        try:
            await checkDatabaseConnection(databaseEngine)
            application.state.databaseEngine = databaseEngine
            application.state.dbSessionFactory = createSessionFactory(databaseEngine)
            if settings.rateLimitEnabled:
                assert settings.rateLimitRedisUrl is not None
                redisClient = Redis.from_url(settings.rateLimitRedisUrl, decode_responses=True)
                try:
                    await redisClient.ping()
                except RedisError:
                    if not settings.rateLimitFailOpen:
                        await redisClient.aclose()
                        redisClient = None
                        raise
                    logger.exception("Rate-limit backend unavailable during startup")
                application.state.redisClient = redisClient
                application.state.rateLimiter = RedisRateLimiter(
                    redisClient,
                    settings.rateLimitWindowSeconds,
                    settings.rateLimitFailOpen,
                )
            logger.info("Application started", extra={"environment": settings.environment})
            yield
        finally:
            if redisClient is not None:
                await redisClient.aclose()
            await databaseEngine.dispose()
            logger.info("Application stopped", extra={"environment": settings.environment})
    except Exception as e:  # pragma: no cover - diagnostic boundary
        print(f"Error in lifespan: {e}")
        raise


def createApp(settings: Settings | None = None) -> FastAPI:
    """Create and configure the FastAPI application.

    Args:
        settings: Optional settings override used by tests.

    Returns:
        A configured FastAPI application.

    Raises:
        ValueError: If the supplied settings are inconsistent.
    """
    runtimeSettings = settings or getSettings()
    application = FastAPI(
        title=runtimeSettings.appName,
        version=runtimeSettings.appVersion,
        description="Analyze half-hour traffic counter files.",
        lifespan=lifespan,
    )
    application.state.settings = runtimeSettings

    def settingsDependency() -> Settings:
        """Return settings bound to this application instance.

        Args:
            None.

        Returns:
            The application's validated runtime settings.

        Raises:
            None.
        """
        return runtimeSettings

    application.dependency_overrides[getSettings] = settingsDependency
    application.add_middleware(RequestContextMiddleware)
    if runtimeSettings.corsOrigins:
        application.add_middleware(
            CORSMiddleware,
            allow_origins=list(runtimeSettings.corsOrigins),
            allow_credentials=False,
            allow_methods=["GET", "POST"],
            allow_headers=["Accept", "Content-Type", "X-API-Key", "X-Request-ID"],
        )
    application.include_router(trafficRouter)

    @application.get(
        "/health/live",
        tags=["operations"],
        summary="Liveness check",
        description="Confirms that the API process can serve requests.",
    )
    async def healthLive() -> dict[str, str]:
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
        description="Checks PostgreSQL and the shared rate-limit backend when enabled.",
    )
    async def healthReady(request: Request) -> JSONResponse:
        """Return whether critical dependencies are ready.

        Args:
            request: Current HTTP request.

        Returns:
            A 200 response when ready, otherwise a 503 response.

        Raises:
            None.
        """
        try:
            currentSettings: Settings = request.app.state.settings
            databaseEngine: AsyncEngine | None = getattr(
                request.app.state,
                "databaseEngine",
                None,
            )
            if databaseEngine is None:
                if currentSettings.databaseUrl is not None:
                    return JSONResponse(status_code=503, content={"status": "not_ready"})
            else:
                try:
                    await checkDatabaseConnection(databaseEngine)
                except SQLAlchemyError:
                    return JSONResponse(status_code=503, content={"status": "not_ready"})
            redisClient: Redis | None = getattr(request.app.state, "redisClient", None)
            if currentSettings.rateLimitEnabled and redisClient is not None:
                try:
                    await redisClient.ping()
                except RedisError:
                    if currentSettings.rateLimitFailOpen:
                        return JSONResponse(
                            status_code=200,
                            content={"status": "ready", "rateLimit": "degraded"},
                        )
                    return JSONResponse(status_code=503, content={"status": "not_ready"})
            return JSONResponse(status_code=200, content={"status": "ready"})
        except Exception as e:  # pragma: no cover - diagnostic boundary
            print(f"Error in healthReady: {e}")
            raise

    @application.exception_handler(TrafficCounterError)
    async def trafficErrorHandler(
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
        try:
            detail = ErrorDetail(code=exception.code, message=exception.message)
            headers = None
            if isinstance(exception, RateLimitExceededError):
                headers = {"Retry-After": str(exception.retryAfter)}
            return JSONResponse(
                status_code=exception.statusCode,
                content={"detail": detail.model_dump()},
                headers=headers,
            )
        except Exception as e:  # pragma: no cover - diagnostic boundary
            print(f"Error in trafficErrorHandler: {e}")
            raise

    @application.exception_handler(RequestValidationError)
    async def requestValidationErrorHandler(
        _request: Request,
        _exception: RequestValidationError,
    ) -> JSONResponse:
        """Map invalid request bodies to the standard error response.

        Args:
            _request: Current HTTP request.
            _exception: FastAPI request validation failure.

        Returns:
            A standardized HTTP 422 response.

        Raises:
            None.
        """
        try:
            exception = InvalidRequestBodyError()
            detail = ErrorDetail(code=exception.code, message=exception.message)
            return JSONResponse(
                status_code=exception.statusCode,
                content={"detail": detail.model_dump()},
            )
        except Exception as e:  # pragma: no cover - diagnostic boundary
            print(f"Error in requestValidationErrorHandler: {e}")
            raise

    return application


app = createApp()
