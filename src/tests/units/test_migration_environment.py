"""Alembic environment execution tests."""

import logging.config
import runpy
from types import SimpleNamespace
from typing import Any

import alembic
import pytest
import sqlalchemy.ext.asyncio

from src.config import Settings

DATABASE_URL = "postgresql+asyncpg://application:application@localhost:5432/application_test"


class Transaction:
    """Provide a synchronous Alembic transaction context."""

    def __enter__(self) -> None:
        """Enter the transaction."""

    def __exit__(self, *_args: object) -> None:
        """Exit the transaction."""


class FakeAlembicContext:
    """Capture Alembic environment calls."""

    def __init__(self, *, offline: bool) -> None:
        """Create a context for one execution mode."""
        self.offline = offline
        self.config = SimpleNamespace(
            config_file_name="unused.ini",
            config_ini_section="alembic",
            get_section=lambda _section, _default: {},
        )
        self.configurations: list[dict[str, object]] = []
        self.migration_runs = 0

    def is_offline_mode(self) -> bool:
        """Return the configured execution mode."""
        return self.offline

    def configure(self, **options: object) -> None:
        """Capture migration configuration."""
        self.configurations.append(options)

    def begin_transaction(self) -> Transaction:
        """Create a transaction context."""
        return Transaction()

    def run_migrations(self) -> None:
        """Capture a migration run."""
        self.migration_runs += 1


class AsyncContext:
    """Provide an async context manager."""

    def __init__(self, value: object) -> None:
        """Create a context."""
        self.value = value

    async def __aenter__(self) -> object:
        """Return the configured value."""
        return self.value

    async def __aexit__(self, *_args: object) -> None:
        """Exit the context."""


class FakeAsyncConnection:
    """Adapt a synchronous migration callback."""

    async def run_sync(self, callback: Any) -> None:
        """Execute the migration callback."""
        callback(object())


class FakeAsyncEngine:
    """Capture async migration engine disposal."""

    def __init__(self) -> None:
        """Create an active engine."""
        self.disposed = False

    def connect(self) -> AsyncContext:
        """Create a fake connection context."""
        return AsyncContext(FakeAsyncConnection())

    async def dispose(self) -> None:
        """Capture engine disposal."""
        self.disposed = True


@pytest.mark.parametrize(
    ("offline", "has_config_file"),
    [(True, True), (False, True), (True, False)],
)
def test_migration_environment_modes(
    monkeypatch: pytest.MonkeyPatch,
    offline: bool,
    has_config_file: bool,
) -> None:
    """Verify offline SQL generation and online async migration execution."""
    context = FakeAlembicContext(offline=offline)
    if not has_config_file:
        context.config.config_file_name = None
    engine = FakeAsyncEngine()
    monkeypatch.setattr(alembic, "context", context)
    monkeypatch.setattr(logging.config, "fileConfig", lambda _path: None)
    monkeypatch.setattr(
        sqlalchemy.ext.asyncio,
        "async_engine_from_config",
        lambda *_args, **_kwargs: engine,
    )
    monkeypatch.setattr(
        "src.config.get_settings",
        lambda: Settings(database_url=DATABASE_URL),
    )

    namespace = runpy.run_module("src.migrations.env", run_name=f"__migration_{offline}__")

    assert context.migration_runs == 1
    if offline:
        assert context.configurations[0]["url"] == DATABASE_URL
    else:
        assert context.configurations[0]["connection"] is not None
        assert engine.disposed

    namespace["get_settings"] = lambda: Settings(database_url=None)
    with pytest.raises(RuntimeError, match="DATABASE_URL"):
        namespace["get_database_url"]()
