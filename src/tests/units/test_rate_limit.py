import pytest
from redis.exceptions import ConnectionError

from src.services.rate_limit import RateLimitBackend, RedisRateLimiter
from src.utils.errors import RateLimitExceededError


class FakeRateLimitBackend:
    """Provide a deterministic Redis-compatible counter for tests."""

    def __init__(self, *, fail: bool = False) -> None:
        """Create the fake backend.

        Args:
            fail: Whether increment operations should fail.

        Returns:
            A fake backend instance.

        Raises:
            None.
        """
        self.count = 0
        self.fail = fail
        self.expirations: list[int] = []

    async def incr(self, _key: str) -> int:
        """Increment the in-memory counter.

        Args:
            _key: Ignored Redis key.

        Returns:
            The incremented value.

        Raises:
            ConnectionError: When failure mode is enabled.
        """
        if self.fail:
            raise ConnectionError("Redis unavailable")
        self.count += 1
        return self.count

    async def expire(self, _key: str, seconds: int) -> bool:
        """Record a requested expiration.

        Args:
            _key: Ignored Redis key.
            seconds: Requested lifetime.

        Returns:
            Always ``True``.

        Raises:
            None.
        """
        self.expirations.append(seconds)
        return True


def test_rate_limit_protocol_methods_are_declarations() -> None:
    """Verify protocol declarations cannot execute as concrete operations."""
    with pytest.raises(NotImplementedError):
        RateLimitBackend.incr(object(), "key")  # type: ignore[arg-type]
    with pytest.raises(NotImplementedError):
        RateLimitBackend.expire(object(), "key", 1)  # type: ignore[arg-type]


async def test_rate_limit_allows_then_rejects_requests() -> None:
    """Verify shared counters reject requests above the configured limit."""
    backend = FakeRateLimitBackend()
    limiter = RedisRateLimiter(backend, windowSeconds=60, failOpen=False)

    await limiter.enforce("ip:test", limit=1)

    try:
        await limiter.enforce("ip:test", limit=1)
    except RateLimitExceededError as exc:
        assert exc.retryAfter >= 1
    else:
        raise AssertionError("Expected the second request to be rate limited.")


async def test_rate_limit_can_fail_open() -> None:
    """Verify backend failures can be configured to allow requests."""
    limiter = RedisRateLimiter(
        FakeRateLimitBackend(fail=True),
        windowSeconds=60,
        failOpen=True,
    )

    await limiter.enforce("ip:test", limit=1)


def test_rate_limit_rejects_invalid_window() -> None:
    """Verify fixed windows must have a positive duration."""
    with pytest.raises(ValueError, match="windowSeconds"):
        RedisRateLimiter(FakeRateLimitBackend(), windowSeconds=0, failOpen=True)


async def test_rate_limit_rejects_invalid_limit() -> None:
    """Verify request limits must be positive."""
    limiter = RedisRateLimiter(FakeRateLimitBackend(), windowSeconds=60, failOpen=True)

    with pytest.raises(ValueError, match="limit"):
        await limiter.enforce("ip:test", limit=0)


async def test_rate_limit_can_fail_closed() -> None:
    """Verify backend failures propagate in fail-closed mode."""
    limiter = RedisRateLimiter(
        FakeRateLimitBackend(fail=True),
        windowSeconds=60,
        failOpen=False,
    )

    with pytest.raises(ConnectionError, match="Redis unavailable"):
        await limiter.enforce("ip:test", limit=1)
