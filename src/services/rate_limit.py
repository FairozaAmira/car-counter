import logging
import time
from collections.abc import Awaitable
from typing import Protocol

from redis.exceptions import RedisError

from src.utils.errors import RateLimitExceededError

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
        try:
            raise NotImplementedError  # pragma: no cover - protocol declaration
        except Exception as e:  # pragma: no cover - diagnostic boundary
            print(f"Error in incr: {e}")
            raise

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
        try:
            raise NotImplementedError  # pragma: no cover - protocol declaration
        except Exception as e:  # pragma: no cover - diagnostic boundary
            print(f"Error in expire: {e}")
            raise


class RedisRateLimiter:
    """Enforce fixed-window limits using a shared Redis backend."""

    def __init__(
        self,
        backend: RateLimitBackend,
        windowSeconds: int,
        failOpen: bool,
    ) -> None:
        """Create a shared rate limiter.

        Args:
            backend: Redis-compatible async backend.
            windowSeconds: Fixed-window size.
            failOpen: Whether requests continue when Redis is unavailable.

        Returns:
            A configured rate limiter.

        Raises:
            ValueError: If the window is not positive.
        """
        try:
            if windowSeconds < 1:
                raise ValueError("windowSeconds must be positive.")
            self._backend = backend
            self._windowSeconds = windowSeconds
            self._failOpen = failOpen
        except Exception as e:  # pragma: no cover - diagnostic boundary
            print(f"Error in __init__: {e}")
            raise

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
        try:
            if limit < 1:
                raise ValueError("limit must be positive.")
            window = int(time.time()) // self._windowSeconds
            key = f"traffic-api:rate-limit:{identity}:{window}"
            try:
                count = await self._backend.incr(key)
                if count == 1:
                    await self._backend.expire(key, self._windowSeconds + 1)
            except RedisError:
                logger.exception("Rate-limit backend failed", extra={"operation": "rate_limit"})
                if self._failOpen:
                    return
                raise
            if count > limit:
                retryAfter = self._windowSeconds - (int(time.time()) % self._windowSeconds)
                raise RateLimitExceededError(retryAfter=max(retryAfter, 1))
        except Exception as e:  # pragma: no cover - diagnostic boundary
            print(f"Error in enforce: {e}")
            raise
