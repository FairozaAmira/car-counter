import pytest
from fastapi import FastAPI
from starlette.requests import Request

from src.config import Settings
from src.dependencies.security import (
    _client_identity,
    authenticate_request,
    enforce_upload_rate_limit,
)
from src.services.errors import AuthenticationError
from src.services.rate_limit import RedisRateLimiter


def request_for(
    peer_host: str | None,
    forwarded_for: str | None = None,
    app: FastAPI | None = None,
) -> Request:
    """Create a minimal request for identity tests.

    Args:
        peer_host: Direct network peer address.
        forwarded_for: Optional proxy-provided address.

    Returns:
        A Starlette request.

    Raises:
        None.
    """
    headers = []
    if forwarded_for is not None:
        headers.append((b"x-forwarded-for", forwarded_for.encode()))
    return Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/",
            "headers": headers,
            "client": (peer_host, 1234) if peer_host is not None else None,
            "server": ("test", 80),
            "scheme": "http",
            "app": app,
        },
    )


async def test_authentication_accepts_disabled_and_valid_keys() -> None:
    """Verify optional API-key authentication behavior."""
    assert await authenticate_request(Settings(api_key=None), None) is None
    assert await authenticate_request(Settings(api_key="secret"), "secret") == "secret"


async def test_authentication_rejects_invalid_key() -> None:
    """Verify invalid credentials produce a safe domain error."""
    try:
        await authenticate_request(Settings(api_key="secret"), "wrong")
    except AuthenticationError as exc:
        assert exc.status_code == 401
    else:
        raise AssertionError("Expected invalid authentication to fail.")


def test_identity_prefers_hashed_api_key() -> None:
    """Verify rate-limit storage never contains a raw API key."""
    identity = _client_identity(request_for("127.0.0.1"), "secret", Settings())

    assert identity.startswith("api-key:")
    assert "secret" not in identity


def test_identity_trusts_forwarding_only_from_configured_proxy() -> None:
    """Verify spoofed forwarding headers are ignored for untrusted peers."""
    untrusted = _client_identity(
        request_for("203.0.113.1", "198.51.100.1"),
        None,
        Settings(trusted_proxy_hosts=("127.0.0.1",)),
    )
    trusted = _client_identity(
        request_for("127.0.0.1", "198.51.100.1, 203.0.113.1"),
        None,
        Settings(trusted_proxy_hosts=("127.0.0.1",)),
    )

    assert untrusted == "ip:203.0.113.1"
    assert trusted == "ip:198.51.100.1"


def test_identity_handles_missing_peer_and_forwarding_header() -> None:
    """Verify identity fallback works without client or forwarding data."""
    settings = Settings(trusted_proxy_hosts=("127.0.0.1",))

    assert _client_identity(request_for(None), None, settings) == "ip:unknown"
    assert _client_identity(request_for("127.0.0.1"), None, settings) == "ip:127.0.0.1"


async def test_enabled_rate_limit_requires_initialized_backend() -> None:
    """Verify enabled limiting rejects a missing shared backend."""
    settings = Settings(
        rate_limit_enabled=True,
        rate_limit_redis_url="redis://test",
    )
    request = request_for("127.0.0.1", app=FastAPI())

    with pytest.raises(RuntimeError, match="not initialized"):
        await enforce_upload_rate_limit(request, None, settings)


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
    application.state.rate_limiter = RedisRateLimiter(backend, 60, False)
    settings = Settings(
        rate_limit_enabled=True,
        rate_limit_redis_url="redis://test",
    )

    await enforce_upload_rate_limit(
        request_for("127.0.0.1", app=application),
        None,
        settings,
    )

    assert "ip:127.0.0.1" in backend.keys[0]
