import pytest
from fastapi import FastAPI, Request, Response

from src.middleware.request_context import RequestContextMiddleware


async def test_request_context_logs_and_reraises_downstream_error(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Verify middleware retains context when downstream handling fails."""
    middleware = RequestContextMiddleware(FastAPI())
    request = Request(
        {
            "type": "http",
            "method": "GET",
            "path": "/failure",
            "headers": [(b"x-request-id", b"request-123")],
            "query_string": b"",
            "client": ("127.0.0.1", 1234),
            "server": ("test", 80),
            "scheme": "http",
        },
    )

    async def fail(_request: Request) -> Response:
        raise RuntimeError("downstream failed")

    with pytest.raises(RuntimeError, match="downstream failed"):
        await middleware.dispatch(request, fail)

    assert request.state.request_id == "request-123"
    assert "Request failed" in caplog.text
