"""Database model, repository, and session tests."""

from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy.exc import SQLAlchemyError

from src.config import Settings
from src.db.models import AnalysisKind, TrafficAnalysisResult
from src.db.repositories import SqlAlchemyAnalysisResultRepository
from src.db.session import (
    check_database_connection,
    create_database_engine,
    create_session_factory,
    get_db_session,
)
from src.routers.traffic import get_analysis_repository
from src.utils.errors import DatabasePersistenceError

DATABASE_URL = "postgresql+asyncpg://application:application@localhost:5432/application_test"


class AsyncContext:
    """Provide a configurable asynchronous context manager."""

    def __init__(self, value: object, error: Exception | None = None) -> None:
        """Create an async context."""
        self.value = value
        self.error = error

    async def __aenter__(self) -> object:
        """Return the configured value or raise the configured error."""
        if self.error is not None:
            raise self.error
        return self.value

    async def __aexit__(self, *_args: object) -> None:
        """Exit the context."""


class FakeSession:
    """Capture repository transaction operations."""

    def __init__(self, error: Exception | None = None) -> None:
        """Create a session."""
        self.error = error
        self.added: TrafficAnalysisResult | None = None
        self.rolled_back = False

    def begin(self) -> AsyncContext:
        """Create the transaction context."""
        return AsyncContext(self, self.error)

    def add(self, model: TrafficAnalysisResult) -> None:
        """Capture an inserted model."""
        self.added = model

    async def rollback(self) -> None:
        """Capture an explicit rollback."""
        self.rolled_back = True


async def test_repository_persists_complete_result() -> None:
    """Verify model mapping and transaction use."""
    session = FakeSession()
    repository = SqlAlchemyAnalysisResultRepository(session)  # type: ignore[arg-type]
    result_id = uuid4()
    created_at = datetime.now(UTC)
    payload = {"id": str(result_id), "total_cars": 6}

    await repository.save(
        result_id=result_id,
        analysis_kind=AnalysisKind.SINGLE,
        created_at=created_at,
        time_taken=4.25,
        response_payload=payload,
    )

    assert session.added is not None
    assert session.added.id == result_id
    assert session.added.analysis_kind == "single"
    assert session.added.created_at == created_at
    assert str(session.added.time_taken) == "4.25"
    assert session.added.response_payload == payload
    assert not session.rolled_back


async def test_repository_rolls_back_and_maps_database_failure(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Verify SQLAlchemy failures are rolled back and safely translated."""
    session = FakeSession(SQLAlchemyError("database unavailable"))
    repository = SqlAlchemyAnalysisResultRepository(session)  # type: ignore[arg-type]

    with pytest.raises(DatabasePersistenceError) as error:
        await repository.save(
            result_id=uuid4(),
            analysis_kind=AnalysisKind.BATCH,
            created_at=datetime.now(UTC),
            time_taken=1.0,
            response_payload={"items": []},
        )

    assert error.value.__cause__ is not None
    assert session.rolled_back
    assert "Failed to persist traffic analysis result" in caplog.text


def test_create_database_engine_uses_configured_pool(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify every pool and timeout setting reaches SQLAlchemy."""
    captured: dict[str, object] = {}
    expected_engine = object()

    def fake_create_engine(url: str, **options: object) -> object:
        captured["url"] = url
        captured.update(options)
        return expected_engine

    monkeypatch.setattr("src.db.session.create_async_engine", fake_create_engine)
    settings = Settings(database_url=DATABASE_URL)

    engine = create_database_engine(settings)

    assert engine is expected_engine
    assert captured["url"] == DATABASE_URL
    assert captured["pool_size"] == settings.database_pool_size
    assert captured["max_overflow"] == settings.database_max_overflow
    assert captured["pool_timeout"] == settings.database_pool_timeout_seconds
    assert captured["pool_recycle"] == settings.database_pool_recycle_seconds
    assert captured["pool_pre_ping"] is True
    assert captured["connect_args"] == {
        "timeout": settings.database_connect_timeout_seconds,
        "command_timeout": settings.database_command_timeout_seconds,
    }


def test_create_database_engine_requires_url() -> None:
    """Verify API startup fails fast without database configuration."""
    with pytest.raises(RuntimeError, match="DATABASE_URL"):
        create_database_engine(Settings(database_url=None))


def test_create_session_factory(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify request sessions retain loaded state after commits."""
    expected_factory = object()
    captured: dict[str, object] = {}

    def fake_sessionmaker(engine: object, **options: object) -> object:
        captured["engine"] = engine
        captured.update(options)
        return expected_factory

    monkeypatch.setattr("src.db.session.async_sessionmaker", fake_sessionmaker)
    engine = object()

    factory = create_session_factory(engine)  # type: ignore[arg-type]

    assert factory is expected_factory
    assert captured == {"engine": engine, "expire_on_commit": False}


async def test_check_database_connection_executes_probe() -> None:
    """Verify readiness executes a lightweight query."""

    class Connection:
        """Capture executed statements."""

        def __init__(self) -> None:
            self.statement = ""

        async def execute(self, statement: object) -> None:
            self.statement = str(statement)

    connection = Connection()
    engine = SimpleNamespace(connect=lambda: AsyncContext(connection))

    await check_database_connection(engine)  # type: ignore[arg-type]

    assert connection.statement == "SELECT 1"


async def test_get_db_session_yields_and_closes_request_session() -> None:
    """Verify the FastAPI dependency obtains a request-scoped session."""
    session = object()
    request = SimpleNamespace(
        app=SimpleNamespace(
            state=SimpleNamespace(db_session_factory=lambda: AsyncContext(session)),
        ),
    )
    dependency = get_db_session(request)  # type: ignore[arg-type]

    assert await anext(dependency) is session
    await dependency.aclose()


async def test_get_db_session_requires_initialized_factory() -> None:
    """Verify requests cannot proceed before database startup."""
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace()))
    dependency = get_db_session(request)  # type: ignore[arg-type]

    with pytest.raises(RuntimeError, match="session factory"):
        await anext(dependency)


def test_router_builds_sqlalchemy_repository() -> None:
    """Verify the route dependency preserves repository separation."""
    session = object()

    repository = get_analysis_repository(session)  # type: ignore[arg-type]

    assert isinstance(repository, SqlAlchemyAnalysisResultRepository)
