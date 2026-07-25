from redis.exceptions import ConnectionError

from src.services.errors import RateLimitExceededError
from src.services.rate_limit import RedisRateLimiter


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


async def test_rate_limit_allows_then_rejects_requests() -> None:
    """Verify shared counters reject requests above the configured limit."""
    backend = FakeRateLimitBackend()
    limiter = RedisRateLimiter(backend, window_seconds=60, fail_open=False)

    await limiter.enforce("ip:test", limit=1)

    try:
        await limiter.enforce("ip:test", limit=1)
    except RateLimitExceededError as exc:
        assert exc.retry_after >= 1
    else:
        raise AssertionError("Expected the second request to be rate limited.")


async def test_rate_limit_can_fail_open() -> None:
    """Verify backend failures can be configured to allow requests."""
    limiter = RedisRateLimiter(
        FakeRateLimitBackend(fail=True),
        window_seconds=60,
        fail_open=True,
    )

    await limiter.enforce("ip:test", limit=1)
