"""API database readiness and failure integration tests."""

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.exc import SQLAlchemyError

from src.config import Settings
from src.main import create_app
from src.routers.traffic import get_analysis_repository
from src.utils.errors import DatabasePersistenceError

VALID = """\
2021-12-01T05:00:00 1
2021-12-01T05:30:00 2
2021-12-01T06:00:00 3
"""


async def test_readiness_requires_initialized_database() -> None:
    """Verify a configured database must have completed application startup."""
    database_app = create_app(
        Settings(
            database_url=(
                "postgresql+asyncpg://application:application@localhost:5432/application_test"
            ),
        ),
    )
    async with AsyncClient(
        transport=ASGITransport(app=database_app),
        base_url="http://test",
    ) as client:
        response = await client.get("/health/ready")

    assert response.status_code == 503
    assert response.json() == {"status": "not_ready"}


async def test_readiness_reports_database_probe_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify a failed PostgreSQL probe makes the application unavailable."""

    async def failed_probe(_engine: object) -> None:
        """Raise a deterministic SQLAlchemy connection failure."""
        raise SQLAlchemyError("database unavailable")

    database_app = create_app(Settings(database_url=None))
    database_app.state.database_engine = object()
    monkeypatch.setattr("src.main.check_database_connection", failed_probe)
    async with AsyncClient(
        transport=ASGITransport(app=database_app),
        base_url="http://test",
    ) as client:
        response = await client.get("/health/ready")

    assert response.status_code == 503
    assert response.json() == {"status": "not_ready"}


async def test_database_failure_returns_safe_service_error() -> None:
    """Verify failed persistence never returns an unstored result identifier."""

    class FailingRepository:
        """Raise a deterministic persistence failure."""

        async def save(self, **_record: object) -> None:
            """Reject every attempted write."""
            raise DatabasePersistenceError

    failing_app = create_app(Settings(database_url=None))
    failing_app.dependency_overrides[get_analysis_repository] = FailingRepository
    async with AsyncClient(
        transport=ASGITransport(app=failing_app),
        base_url="http://test",
    ) as client:
        response = await client.post(
            "/api/v1/traffic/analyze",
            files={"file": ("traffic.txt", VALID, "text/plain")},
        )

    assert response.status_code == 503
    assert response.json()["detail"] == {
        "code": "database_write_error",
        "message": "The analysis result could not be stored.",
    }
