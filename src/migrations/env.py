"""Alembic environment for async PostgreSQL migrations."""

import asyncio
from logging.config import fileConfig

from alembic import context
from sqlalchemy import Connection, pool
from sqlalchemy.ext.asyncio import async_engine_from_config

from src.config import get_settings
from src.db.models import TrafficAnalysisResult

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = TrafficAnalysisResult.metadata


def get_database_url() -> str:
    """Return the configured Alembic database URL.

    Args:
        None.

    Returns:
        The async PostgreSQL SQLAlchemy URL.

    Raises:
        RuntimeError: If ``DATABASE_URL`` is not configured.
    """
    database_url = get_settings().database_url
    if database_url is None:
        raise RuntimeError("DATABASE_URL is required for Alembic commands.")
    return database_url


def run_migrations_offline() -> None:
    """Run migrations without creating a database connection.

    Args:
        None.

    Returns:
        None.

    Raises:
        RuntimeError: If database configuration is missing.
    """
    context.configure(
        url=get_database_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
    )
    with context.begin_transaction():
        context.run_migrations()


def run_sync_migrations(connection: Connection) -> None:
    """Run migrations with a synchronous connection adapter.

    Args:
        connection: SQLAlchemy connection adapted from the async engine.

    Returns:
        None.

    Raises:
        Exception: If migration execution fails.
    """
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        compare_type=True,
    )
    with context.begin_transaction():
        context.run_migrations()


async def run_async_migrations() -> None:
    """Create an async engine and run online migrations.

    Args:
        None.

    Returns:
        None.

    Raises:
        Exception: If connection or migration execution fails.
    """
    configuration = config.get_section(config.config_ini_section, {})
    configuration["sqlalchemy.url"] = get_database_url()
    connectable = async_engine_from_config(
        configuration,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    try:
        async with connectable.connect() as connection:
            await connection.run_sync(run_sync_migrations)
    finally:
        await connectable.dispose()


def run_migrations_online() -> None:
    """Run migrations against the configured PostgreSQL database.

    Args:
        None.

    Returns:
        None.

    Raises:
        Exception: If migration execution fails.
    """
    asyncio.run(run_async_migrations())


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
