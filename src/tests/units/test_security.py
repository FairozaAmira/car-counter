import pytest
from fastapi import FastAPI
from starlette.requests import Request

from src.config import Settings
from src.dependencies.security import (
    _clientIdentity,
    authenticateRequest,
    enforceUploadRateLimit,
)
from src.services.rate_limit import RedisRateLimiter
from src.utils.errors import AuthenticationError


def request_for(
    peerHost: str | None,
    forwardedFor: str | None = None,
    app: FastAPI | None = None,
) -> Request:
    """Create a minimal request for identity tests.

    Args:
        peerHost: Direct network peer address.
        forwardedFor: Optional proxy-provided address.

    Returns:
        A Starlette request.

    Raises:
        None.
    """
    headers = []
    if forwardedFor is not None:
        headers.append((b"x-forwarded-for", forwardedFor.encode()))
    return Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/",
            "headers": headers,
            "client": (peerHost, 1234) if peerHost is not None else None,
            "server": ("test", 80),
            "scheme": "http",
            "app": app,
        },
    )


async def test_authentication_accepts_disabled_and_valid_keys() -> None:
    """Verify optional API-key authentication behavior."""
    assert await authenticateRequest(Settings(apiKey=None), None) is None
    assert await authenticateRequest(Settings(apiKey="secret"), "secret") == "secret"


async def test_authentication_rejects_invalid_key() -> None:
    """Verify invalid credentials produce a safe domain error."""
    try:
        await authenticateRequest(Settings(apiKey="secret"), "wrong")
    except AuthenticationError as exc:
        assert exc.statusCode == 401
    else:
        raise AssertionError("Expected invalid authentication to fail.")


def test_identity_prefers_hashed_api_key() -> None:
    """Verify rate-limit storage never contains a raw API key."""
    identity = _clientIdentity(request_for("127.0.0.1"), "secret", Settings())

    assert identity.startswith("api-key:")
    assert "secret" not in identity


def test_identity_trusts_forwarding_only_from_configured_proxy() -> None:
    """Verify spoofed forwarding headers are ignored for untrusted peers."""
    untrusted = _clientIdentity(
        request_for("203.0.113.1", "198.51.100.1"),
        None,
        Settings(trustedProxyHosts=("127.0.0.1",)),
    )
    trusted = _clientIdentity(
        request_for("127.0.0.1", "198.51.100.1, 203.0.113.1"),
        None,
        Settings(trustedProxyHosts=("127.0.0.1",)),
    )

    assert untrusted == "ip:203.0.113.1"
    assert trusted == "ip:198.51.100.1"


def test_identity_handles_missing_peer_and_forwarding_header() -> None:
    """Verify identity fallback works without client or forwarding data."""
    settings = Settings(trustedProxyHosts=("127.0.0.1",))

    assert _clientIdentity(request_for(None), None, settings) == "ip:unknown"
    assert _clientIdentity(request_for("127.0.0.1"), None, settings) == "ip:127.0.0.1"


async def test_enabled_rate_limit_requires_initialized_backend() -> None:
    """Verify enabled limiting rejects a missing shared backend."""
    settings = Settings(
        rateLimitEnabled=True,
        rateLimitRedisUrl="redis://test",
    )
    request = request_for("127.0.0.1", app=FastAPI())

    with pytest.raises(RuntimeError, match="not initialized"):
        await enforceUploadRateLimit(request, None, settings)


async def test_enabled_rate_limit_delegates_to_shared_backend() -> None:
    """Verify enabled limiting delegates using the resolved identity."""

    class Backend:
        """Record fixed-window operations."""

        def __init__(self) -> None:
            self.keys: list[str] = []

        async def incr(self, key: str) -> int:
            self.keys.append(key)
            return 1

        async def expire(self, _key: str, _seconds: int) -> bool:
            return True

    backend = Backend()
    application = FastAPI()
    application.state.rateLimiter = RedisRateLimiter(backend, 60, False)
    settings = Settings(
        rateLimitEnabled=True,
        rateLimitRedisUrl="redis://test",
    )

    await enforceUploadRateLimit(
        request_for("127.0.0.1", app=application),
        None,
        settings,
    )

    assert "ip:127.0.0.1" in backend.keys[0]
