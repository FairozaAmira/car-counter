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


def create_database_engine(settings: Settings) -> AsyncEngine:
    """Create the process-local PostgreSQL connection pool.

    Args:
        settings: Validated database and pool settings.

    Returns:
        A configured asynchronous SQLAlchemy engine.

    Raises:
        RuntimeError: If ``DATABASE_URL`` is not configured.
    """
    if settings.database_url is None:
        raise RuntimeError("DATABASE_URL is required to start the API.")
    return create_async_engine(
        settings.database_url,
        pool_size=settings.database_pool_size,
        max_overflow=settings.database_max_overflow,
        pool_timeout=settings.database_pool_timeout_seconds,
        pool_recycle=settings.database_pool_recycle_seconds,
        pool_pre_ping=True,
        connect_args={
            "timeout": settings.database_connect_timeout_seconds,
            "command_timeout": settings.database_command_timeout_seconds,
        },
    )


def create_session_factory(engine: AsyncEngine) -> AsyncSessionFactory:
    """Create request-scoped async sessions.

    Args:
        engine: Process-local asynchronous SQLAlchemy engine.

    Returns:
        An asynchronous session factory.

    Raises:
        None.
    """
    return async_sessionmaker(engine, expire_on_commit=False)


async def check_database_connection(engine: AsyncEngine) -> None:
    """Verify that PostgreSQL accepts a simple query.

    Args:
        engine: Asynchronous SQLAlchemy engine to check.

    Returns:
        None.

    Raises:
        SQLAlchemyError: If a connection or query fails.
    """
    async with engine.connect() as connection:
        await connection.execute(text("SELECT 1"))


async def get_db_session(request: Request) -> AsyncIterator[AsyncSession]:
    """Yield one request-scoped database session.

    Args:
        request: Current FastAPI request.

    Yields:
        A database session bound to the application connection pool.

    Raises:
        RuntimeError: If application database startup did not complete.
    """
    factory: AsyncSessionFactory | None = getattr(
        request.app.state,
        "db_session_factory",
        None,
    )
    if factory is None:
        raise RuntimeError("Database session factory is unavailable.")
    async with factory() as session:
        yield session
