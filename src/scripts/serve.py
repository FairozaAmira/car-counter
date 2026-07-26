import uvicorn

from src.config import getSettings


def main() -> None:
    """Run the FastAPI service using validated environment settings.

    Args:
        None.

    Returns:
        None.

    Raises:
        pydantic.ValidationError: If runtime configuration is invalid.
        RuntimeError: If Uvicorn cannot start the application.
    """
    settings = getSettings()
    uvicorn.run(
        "src.main:app",
        host=settings.appHost,
        port=settings.appPort,
        workers=settings.appWorkers,
        reload=settings.appReload,
        timeout_keep_alive=settings.keepAliveTimeoutSeconds,
        timeout_graceful_shutdown=settings.gracefulShutdownTimeoutSeconds,
        log_level=settings.logLevel.lower(),
    )


if __name__ == "__main__":  # pragma: no cover - exercised through main()
    main()
