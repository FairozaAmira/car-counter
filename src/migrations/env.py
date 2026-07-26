"""Alembic environment for async PostgreSQL migrations."""

import asyncio
from logging.config import fileConfig

from alembic import context
from sqlalchemy import Connection, pool
from sqlalchemy.ext.asyncio import async_engine_from_config

from src.config import getSettings
from src.db.models import TrafficAnalysisResult

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

targetMetadata = TrafficAnalysisResult.metadata


def getDatabaseUrl() -> str:
    """Return the configured Alembic database URL.

    Args:
        None.

    Returns:
        The async PostgreSQL SQLAlchemy URL.

    Raises:
        RuntimeError: If ``DATABASE_URL`` is not configured.
    """
    databaseUrl = getSettings().databaseUrl
    if databaseUrl is None:
        raise RuntimeError("DATABASE_URL is required for Alembic commands.")
    return databaseUrl


def runMigrationsOffline() -> None:
    """Run migrations without creating a database connection.

    Args:
        None.

    Returns:
        None.

    Raises:
        RuntimeError: If database configuration is missing.
    """
    context.configure(
        url=getDatabaseUrl(),
        target_metadata=targetMetadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
    )
    with context.begin_transaction():
        context.run_migrations()


def runSyncMigrations(connection: Connection) -> None:
    """Run migrations with a synchronous connection adapter.

    Args:
        connection: SQLAlchemy connection adapted from the async engine.

    Returns:
        None.

    Raises:
        Exception: If migration execution fails.
    """
    try:
        context.configure(
            connection=connection,
            target_metadata=targetMetadata,
            compare_type=True,
        )
        with context.begin_transaction():
            context.run_migrations()
    except Exception as e:  # pragma: no cover - diagnostic boundary
        print(f"Error in runSyncMigrations: {e}")
        raise


async def runAsyncMigrations() -> None:
    """Create an async engine and run online migrations.

    Args:
        None.

    Returns:
        None.

    Raises:
        Exception: If connection or migration execution fails.
    """
    configuration = config.get_section(config.config_ini_section, {})
    configuration["sqlalchemy.url"] = getDatabaseUrl()
    connectable = async_engine_from_config(
        configuration,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    try:
        async with connectable.connect() as connection:
            await connection.run_sync(runSyncMigrations)
    finally:
        await connectable.dispose()


def runMigrationsOnline() -> None:
    """Run migrations against the configured PostgreSQL database.

    Args:
        None.

    Returns:
        None.

    Raises:
        Exception: If migration execution fails.
    """
    asyncio.run(runAsyncMigrations())


if context.is_offline_mode():
    runMigrationsOffline()
else:
    runMigrationsOnline()
