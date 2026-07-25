import hmac
from hashlib import sha256
from typing import Annotated

from fastapi import Depends, Header, Request

from src.config import Settings, get_settings
from src.services.errors import AuthenticationError
from src.services.rate_limit import RedisRateLimiter


async def authenticate_request(
    settings: Annotated[Settings, Depends(get_settings)],
    api_key: Annotated[str | None, Header(alias="X-API-Key")] = None,
) -> str | None:
    """Authenticate an API request when an API key is configured.

    Args:
        settings: Runtime application settings.
        api_key: Client-provided API key.

    Returns:
        The authenticated API-key identity, or ``None`` when auth is disabled.

    Raises:
        AuthenticationError: If the configured key is absent or incorrect.
    """
    if settings.api_key is None:
        return None
    if api_key is None or not hmac.compare_digest(api_key, settings.api_key):
        raise AuthenticationError()
    return api_key


def _client_identity(
    request: Request,
    authenticated_identity: str | None,
    settings: Settings,
) -> str:
    """Resolve a non-sensitive rate-limit identity."""
    if authenticated_identity is not None:
        digest = sha256(authenticated_identity.encode()).hexdigest()
        return f"api-key:{digest}"

    peer_host = request.client.host if request.client else "unknown"
    if peer_host in settings.trusted_proxy_hosts:
        forwarded_for = request.headers.get("X-Forwarded-For")
        if forwarded_for:
            return f"ip:{forwarded_for.split(',', maxsplit=1)[0].strip()}"
    return f"ip:{peer_host}"


async def enforce_upload_rate_limit(
    request: Request,
    authenticated_identity: Annotated[str | None, Depends(authenticate_request)],
    settings: Annotated[Settings, Depends(get_settings)],
) -> None:
    """Enforce the configured shared upload rate limit.

    Args:
        request: Current HTTP request.
        authenticated_identity: Authenticated API-key identity, when enabled.
        settings: Runtime application settings.

    Returns:
        None.

    Raises:
        RateLimitExceededError: If the request exceeds the shared limit.
        RuntimeError: If rate limiting is enabled without an initialized backend.
        RedisError: If Redis fails while fail-closed mode is configured.
    """
    if not settings.rate_limit_enabled:
        return
    limiter = getattr(request.app.state, "rate_limiter", None)
    if not isinstance(limiter, RedisRateLimiter):
        raise RuntimeError("Rate limiter was not initialized.")
    await limiter.enforce(
        _client_identity(request, authenticated_identity, settings),
        settings.rate_limit_upload_requests,
    )
