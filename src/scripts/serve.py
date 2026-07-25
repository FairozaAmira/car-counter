import uvicorn

from src.config import get_settings


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
    settings = get_settings()
    uvicorn.run(
        "src.main:app",
        host=settings.app_host,
        port=settings.app_port,
        workers=settings.app_workers,
        reload=settings.app_reload,
        timeout_keep_alive=settings.keep_alive_timeout_seconds,
        timeout_graceful_shutdown=settings.graceful_shutdown_timeout_seconds,
        log_level=settings.log_level.lower(),
    )


if __name__ == "__main__":  # pragma: no cover - exercised through main()
    main()
