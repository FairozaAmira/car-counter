"""Async PostgreSQL engine and request-session management."""

from collections.abc import AsyncIterator

from fastapi import Request
from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from src.config import Settings

AsyncSessionFactory = async_sessionmaker[AsyncSession]


def createDatabaseEngine(settings: Settings) -> AsyncEngine:
    """Create the process-local PostgreSQL connection pool.

    Args:
        settings: Validated database and pool settings.

    Returns:
        A configured asynchronous SQLAlchemy engine.

    Raises:
        RuntimeError: If ``DATABASE_URL`` is not configured.
    """
    try:
        if settings.databaseUrl is None:
            raise RuntimeError("DATABASE_URL is required to start the API.")
        return create_async_engine(
            settings.databaseUrl,
            pool_size=settings.databasePoolSize,
            max_overflow=settings.databaseMaxOverflow,
            pool_timeout=settings.databasePoolTimeoutSeconds,
            pool_recycle=settings.databasePoolRecycleSeconds,
            pool_pre_ping=True,
            connect_args={
                "timeout": settings.databaseConnectTimeoutSeconds,
                "command_timeout": settings.databaseCommandTimeoutSeconds,
            },
        )
    except Exception as e:  # pragma: no cover - diagnostic boundary
        print(f"Error in createDatabaseEngine: {e}")
        raise


def createSessionFactory(engine: AsyncEngine) -> AsyncSessionFactory:
    """Create request-scoped async sessions.

    Args:
        engine: Process-local asynchronous SQLAlchemy engine.

    Returns:
        An asynchronous session factory.

    Raises:
        None.
    """
    try:
        return async_sessionmaker(engine, expire_on_commit=False)
    except Exception as e:  # pragma: no cover - diagnostic boundary
        print(f"Error in createSessionFactory: {e}")
        raise


async def checkDatabaseConnection(engine: AsyncEngine) -> None:
    """Verify that PostgreSQL accepts a simple query.

    Args:
        engine: Asynchronous SQLAlchemy engine to check.

    Returns:
        None.

    Raises:
        SQLAlchemyError: If a connection or query fails.
    """
    try:
        async with engine.connect() as connection:
            await connection.execute(text("SELECT 1"))
    except Exception as e:  # pragma: no cover - diagnostic boundary
        print(f"Error in checkDatabaseConnection: {e}")
        raise


async def getDbSession(request: Request) -> AsyncIterator[AsyncSession]:
    """Yield one request-scoped database session.

    Args:
        request: Current FastAPI request.

    Yields:
        A database session bound to the application connection pool.

    Raises:
        RuntimeError: If application database startup did not complete.
    """
    try:
        factory: AsyncSessionFactory | None = getattr(
            request.app.state,
            "dbSessionFactory",
            None,
        )
        if factory is None:
            raise RuntimeError("Database session factory is unavailable.")
        async with factory() as session:
            yield session
    except Exception as e:  # pragma: no cover - diagnostic boundary
        print(f"Error in getDbSession: {e}")
        raise
