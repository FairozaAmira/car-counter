import logging
import time
from collections.abc import Awaitable
from typing import Protocol

from redis.exceptions import RedisError

from src.services.errors import RateLimitExceededError

logger = logging.getLogger(__name__)


class RateLimitBackend(Protocol):
    """Define the Redis operations required by the rate limiter."""

    def incr(self, key: str) -> Awaitable[int]:
        """Increment a counter.

        Args:
            key: Redis key to increment.

        Returns:
            The incremented counter value.

        Raises:
            RedisError: If Redis is unavailable.
        """

    def expire(self, key: str, seconds: int) -> Awaitable[bool]:
        """Set a key expiration.

        Args:
            key: Redis key to expire.
            seconds: Expiration duration.

        Returns:
            Whether the timeout was applied.

        Raises:
            RedisError: If Redis is unavailable.
        """


class RedisRateLimiter:
    """Enforce fixed-window limits using a shared Redis backend."""

    def __init__(
        self,
        backend: RateLimitBackend,
        window_seconds: int,
        fail_open: bool,
    ) -> None:
        """Create a shared rate limiter.

        Args:
            backend: Redis-compatible async backend.
            window_seconds: Fixed-window size.
            fail_open: Whether requests continue when Redis is unavailable.

        Returns:
            A configured rate limiter.

        Raises:
            ValueError: If the window is not positive.
        """
        if window_seconds < 1:
            raise ValueError("window_seconds must be positive.")
        self._backend = backend
        self._window_seconds = window_seconds
        self._fail_open = fail_open

    async def enforce(self, identity: str, limit: int) -> None:
        """Reject an identity after it exceeds its current window.

        Args:
            identity: Stable authenticated or network identity.
            limit: Maximum requests per window.

        Returns:
            None.

        Raises:
            RateLimitExceededError: If the identity exceeds the limit.
            RedisError: If the backend fails and fail-open is disabled.
            ValueError: If the limit is not positive.
        """
        if limit < 1:
            raise ValueError("limit must be positive.")
        window = int(time.time()) // self._window_seconds
        key = f"traffic-api:rate-limit:{identity}:{window}"
        try:
            count = await self._backend.incr(key)
            if count == 1:
                await self._backend.expire(key, self._window_seconds + 1)
        except RedisError:
            logger.exception("Rate-limit backend failed", extra={"operation": "rate_limit"})
            if self._fail_open:
                return
            raise
        if count > limit:
            retry_after = self._window_seconds - (int(time.time()) % self._window_seconds)
            raise RateLimitExceededError(retry_after=max(retry_after, 1))
