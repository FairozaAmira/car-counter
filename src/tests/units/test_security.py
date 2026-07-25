from starlette.requests import Request

from src.config import Settings
from src.dependencies.security import _client_identity, authenticate_request
from src.services.errors import AuthenticationError


def request_for(peer_host: str, forwarded_for: str | None = None) -> Request:
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
            "client": (peer_host, 1234),
            "server": ("test", 80),
            "scheme": "http",
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
