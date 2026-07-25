import pytest
from fastapi import FastAPI
from redis.exceptions import ConnectionError

from src.config import Settings
from src.main import lifespan


class FakeEngine:
    """Capture database engine disposal."""

    def __init__(self) -> None:
        """Create an undisposed engine."""
        self.disposed = False

    async def dispose(self) -> None:
        """Record pool disposal."""
        self.disposed = True


@pytest.fixture
def database_engine(monkeypatch: pytest.MonkeyPatch) -> FakeEngine:
    """Replace PostgreSQL resources with deterministic test doubles."""
    engine = FakeEngine()

    async def check_database(_engine: FakeEngine) -> None:
        """Accept the fake database connection."""

    monkeypatch.setattr("src.main.create_database_engine", lambda _settings: engine)
    monkeypatch.setattr("src.main.check_database_connection", check_database)
    monkeypatch.setattr("src.main.create_session_factory", lambda _engine: object())
    return engine


class FakeRedis:
    """Provide a controllable async Redis lifecycle."""

    def __init__(self, *, fail_ping: bool = False) -> None:
        """Create the fake client.

        Args:
            fail_ping: Whether readiness probes should fail.

        Returns:
            A fake Redis client.

        Raises:
            None.
        """
        self.fail_ping = fail_ping
        self.closed = False

    async def ping(self) -> bool:
        """Return backend health or raise a connection failure."""
        if self.fail_ping:
            raise ConnectionError("Redis unavailable")
        return True

    async def aclose(self) -> None:
        """Record that the client was closed."""
        self.closed = True


async def test_lifespan_initializes_and_closes_rate_limiter(
    monkeypatch: pytest.MonkeyPatch,
    database_engine: FakeEngine,
) -> None:
    """Verify enabled shared state is initialized once per API process."""
    redis = FakeRedis()
    monkeypatch.setattr("src.main.Redis.from_url", lambda *_args, **_kwargs: redis)
    application = FastAPI()
    application.state.settings = Settings(
        rate_limit_enabled=True,
        rate_limit_redis_url="redis://test",
    )

    async with lifespan(application):
        assert application.state.rate_limiter is not None

    assert redis.closed
    assert database_engine.disposed


async def test_lifespan_fails_closed_when_redis_is_unavailable(
    monkeypatch: pytest.MonkeyPatch,
    database_engine: FakeEngine,
) -> None:
    """Verify mandatory rate limiting prevents unsafe startup."""
    redis = FakeRedis(fail_ping=True)
    monkeypatch.setattr("src.main.Redis.from_url", lambda *_args, **_kwargs: redis)
    application = FastAPI()
    application.state.settings = Settings(
        rate_limit_enabled=True,
        rate_limit_redis_url="redis://test",
        rate_limit_fail_open=False,
    )

    with pytest.raises(ConnectionError):
        async with lifespan(application):
            raise AssertionError("Application should not start.")

    assert redis.closed
    assert database_engine.disposed


async def test_lifespan_fails_open_when_redis_is_unavailable(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    database_engine: FakeEngine,
) -> None:
    """Verify optional limiting permits startup while logging degradation."""
    redis = FakeRedis(fail_ping=True)
    monkeypatch.setattr("src.main.Redis.from_url", lambda *_args, **_kwargs: redis)
    application = FastAPI()
    application.state.settings = Settings(
        rate_limit_enabled=True,
        rate_limit_redis_url="redis://test",
        rate_limit_fail_open=True,
    )

    async with lifespan(application):
        assert application.state.rate_limiter is not None

    assert redis.closed
    assert database_engine.disposed
    assert "Rate-limit backend unavailable during startup" in caplog.text


async def test_lifespan_without_rate_limiting(database_engine: FakeEngine) -> None:
    """Verify startup and shutdown need no Redis resource when disabled."""
    application = FastAPI()
    application.state.settings = Settings(rate_limit_enabled=False)

    async with lifespan(application):
        assert not hasattr(application.state, "redis_client")

    assert database_engine.disposed
