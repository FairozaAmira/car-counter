"""Database model, repository, and session tests."""

from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy.exc import SQLAlchemyError

from src.config import Settings
from src.db.models import AnalysisKind, TrafficAnalysisResult
from src.db.repositories import AnalysisResultRepository, SqlAlchemyAnalysisResultRepository
from src.db.session import (
    checkDatabaseConnection,
    createDatabaseEngine,
    createSessionFactory,
    getDbSession,
)
from src.routers.traffic import getAnalysisRepository
from src.utils.errors import DatabasePersistenceError

DATABASE_URL = "postgresql+asyncpg://application:application@localhost:5432/application_test"


async def test_repository_protocol_save_is_a_declaration() -> None:
    """Cover the structural protocol declaration without persistence."""
    await AnalysisResultRepository.save(  # type: ignore[arg-type]
        object(),
        resultId=uuid4(),
        analysisKind=AnalysisKind.SINGLE,
        createdAt=datetime.now(UTC),
        timeTaken=0.0,
        responsePayload={},
    )


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
    resultId = uuid4()
    createdAt = datetime.now(UTC)
    payload = {
        "id": str(resultId),
        "totalCars": 6,
        "dailyTotals": [{"date": "01-12-2021", "carCount": 6}],
        "topHalfHours": [{"timestamp": "01-12-2021 05:00:00", "carCount": 6}],
        "leastCarsPeriod": {
            "start": "01-12-2021 05:00:00",
            "end": "01-12-2021 06:30:00",
            "totalCars": 6,
            "records": [{"timestamp": "01-12-2021 05:00:00", "carCount": 6}],
        },
    }

    await repository.save(
        resultId=resultId,
        analysisKind=AnalysisKind.SINGLE,
        createdAt=createdAt,
        timeTaken=4.25,
        responsePayload=payload,
    )

    assert session.added is not None
    assert session.added.id == resultId
    assert session.added.analysisKind == "single"
    assert session.added.createdAt == createdAt
    assert str(session.added.timeTaken) == "4.25"
    assert session.added.totalCars == 6
    assert session.added.dailyTotals == payload["dailyTotals"]
    assert session.added.topHalfHours == payload["topHalfHours"]
    assert session.added.leastCarsPeriodStart == "01-12-2021 05:00:00"
    assert session.added.leastCarsPeriodEnd == "01-12-2021 06:30:00"
    assert session.added.leastCarsPeriodTotalCars == 6
    assert session.added.leastCarsPeriodRecords == payload["leastCarsPeriod"]["records"]
    assert session.added.items is None
    assert not session.rolled_back


async def test_repository_rolls_back_and_maps_database_failure(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Verify SQLAlchemy failures are rolled back and safely translated."""
    session = FakeSession(SQLAlchemyError("database unavailable"))
    repository = SqlAlchemyAnalysisResultRepository(session)  # type: ignore[arg-type]

    with pytest.raises(DatabasePersistenceError) as error:
        await repository.save(
            resultId=uuid4(),
            analysisKind=AnalysisKind.BATCH,
            createdAt=datetime.now(UTC),
            timeTaken=1.0,
            responsePayload={"items": []},
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
    settings = Settings(databaseUrl=DATABASE_URL)

    engine = createDatabaseEngine(settings)

    assert engine is expected_engine
    assert captured["url"] == DATABASE_URL
    assert captured["pool_size"] == settings.databasePoolSize
    assert captured["max_overflow"] == settings.databaseMaxOverflow
    assert captured["pool_timeout"] == settings.databasePoolTimeoutSeconds
    assert captured["pool_recycle"] == settings.databasePoolRecycleSeconds
    assert captured["pool_pre_ping"] is True
    assert captured["connect_args"] == {
        "timeout": settings.databaseConnectTimeoutSeconds,
        "command_timeout": settings.databaseCommandTimeoutSeconds,
    }


def test_create_database_engine_requires_url() -> None:
    """Verify API startup fails fast without database configuration."""
    with pytest.raises(RuntimeError, match="DATABASE_URL"):
        createDatabaseEngine(Settings(databaseUrl=None))


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

    factory = createSessionFactory(engine)  # type: ignore[arg-type]

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

    await checkDatabaseConnection(engine)  # type: ignore[arg-type]

    assert connection.statement == "SELECT 1"


async def test_get_db_session_yields_and_closes_request_session() -> None:
    """Verify the FastAPI dependency obtains a request-scoped session."""
    session = object()
    request = SimpleNamespace(
        app=SimpleNamespace(
            state=SimpleNamespace(dbSessionFactory=lambda: AsyncContext(session)),
        ),
    )
    dependency = getDbSession(request)  # type: ignore[arg-type]

    assert await anext(dependency) is session
    await dependency.aclose()


async def test_get_db_session_requires_initialized_factory() -> None:
    """Verify requests cannot proceed before database startup."""
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace()))
    dependency = getDbSession(request)  # type: ignore[arg-type]

    with pytest.raises(RuntimeError, match="session factory"):
        await anext(dependency)


def test_router_builds_sqlalchemy_repository() -> None:
    """Verify the route dependency preserves repository separation."""
    session = object()

    repository = getAnalysisRepository(session)  # type: ignore[arg-type]

    assert isinstance(repository, SqlAlchemyAnalysisResultRepository)
