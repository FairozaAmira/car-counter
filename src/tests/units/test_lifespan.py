import pytest
from fastapi import FastAPI
from redis.exceptions import ConnectionError

from src.config import Settings
from src.main import lifespan


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


async def test_lifespan_fails_closed_when_redis_is_unavailable(
    monkeypatch: pytest.MonkeyPatch,
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
