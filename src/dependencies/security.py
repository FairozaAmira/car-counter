import hmac
from hashlib import sha256
from typing import Annotated

from fastapi import Depends, Header, Request

from src.config import Settings, getSettings
from src.services.rate_limit import RedisRateLimiter
from src.utils.errors import AuthenticationError


async def authenticateRequest(
    settings: Annotated[Settings, Depends(getSettings)],
    apiKey: Annotated[str | None, Header(alias="X-API-Key")] = None,
) -> str | None:
    """Authenticate an API request when an API key is configured.

    Args:
        settings: Runtime application settings.
        apiKey: Client-provided API key.

    Returns:
        The authenticated API-key identity, or ``None`` when auth is disabled.

    Raises:
        AuthenticationError: If the configured key is absent or incorrect.
    """
    try:
        if settings.apiKey is None:
            return None
        if apiKey is None or not hmac.compare_digest(apiKey, settings.apiKey):
            raise AuthenticationError()
        return apiKey
    except Exception as e:  # pragma: no cover - diagnostic boundary
        print(f"Error in authenticateRequest: {e}")
        raise


def _clientIdentity(
    request: Request,
    authenticatedIdentity: str | None,
    settings: Settings,
) -> str:
    """Resolve a non-sensitive rate-limit identity."""
    try:
        if authenticatedIdentity is not None:
            digest = sha256(authenticatedIdentity.encode()).hexdigest()
            return f"api-key:{digest}"

        peerHost = request.client.host if request.client else "unknown"
        if peerHost in settings.trustedProxyHosts:
            forwardedFor = request.headers.get("X-Forwarded-For")
            if forwardedFor:
                return f"ip:{forwardedFor.split(',', maxsplit=1)[0].strip()}"
        return f"ip:{peerHost}"
    except Exception as e:  # pragma: no cover - diagnostic boundary
        print(f"Error in _clientIdentity: {e}")
        raise


async def enforceUploadRateLimit(
    request: Request,
    authenticatedIdentity: Annotated[str | None, Depends(authenticateRequest)],
    settings: Annotated[Settings, Depends(getSettings)],
) -> None:
    """Enforce the configured shared upload rate limit.

    Args:
        request: Current HTTP request.
        authenticatedIdentity: Authenticated API-key identity, when enabled.
        settings: Runtime application settings.

    Returns:
        None.

    Raises:
        RateLimitExceededError: If the request exceeds the shared limit.
        RuntimeError: If rate limiting is enabled without an initialized backend.
        RedisError: If Redis fails while fail-closed mode is configured.
    """
    try:
        if not settings.rateLimitEnabled:
            return
        limiter = getattr(request.app.state, "rateLimiter", None)
        if not isinstance(limiter, RedisRateLimiter):
            raise RuntimeError("Rate limiter was not initialized.")
        await limiter.enforce(
            _clientIdentity(request, authenticatedIdentity, settings),
            settings.rateLimitUploadRequests,
        )
    except Exception as e:  # pragma: no cover - diagnostic boundary
        print(f"Error in enforceUploadRateLimit: {e}")
        raise
