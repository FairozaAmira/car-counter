from fastapi.middleware.cors import CORSMiddleware
from httpx import ASGITransport, AsyncClient
from redis.exceptions import ConnectionError

from src.config import Settings
from src.main import app, create_app
from src.services.rate_limit import RedisRateLimiter

VALID = """\
2021-12-01T05:00:00 1
2021-12-01T05:30:00 2
2021-12-01T06:00:00 3
"""


async def test_single_file_endpoint() -> None:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post(
            "/api/v1/traffic/analyze",
            files={"file": ("traffic.txt", VALID, "text/plain")},
        )

    assert response.status_code == 200
    assert response.json()["total_cars"] == 6


async def test_single_file_endpoint_returns_domain_error() -> None:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post(
            "/api/v1/traffic/analyze",
            files={"file": ("traffic.txt", "bad data", "text/plain")},
        )

    assert response.status_code == 422
    assert response.json()["detail"]["code"] == "invalid_timestamp"


async def test_batch_endpoint_returns_partial_results() -> None:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post(
            "/api/v1/traffic/analyze/batch",
            files=[
                ("files", ("good.txt", VALID, "text/plain")),
                ("files", ("bad.txt", "bad data", "text/plain")),
            ],
        )

    assert response.status_code == 200
    payload = response.json()
    assert [item["status"] for item in payload["items"]] == ["completed", "failed"]


def test_batch_openapi_schema_declares_binary_file_items() -> None:
    """Verify Swagger UI renders a file picker for every batch item."""
    schema = app.openapi()
    request_schema = schema["paths"]["/api/v1/traffic/analyze/batch"]["post"]["requestBody"][
        "content"
    ]["multipart/form-data"]["schema"]
    component_name = request_schema["$ref"].rsplit("/", maxsplit=1)[-1]
    files_schema = schema["components"]["schemas"][component_name]["properties"]["files"]

    assert files_schema["type"] == "array"
    assert files_schema["items"] == {"type": "string", "format": "binary"}


async def test_health_endpoints() -> None:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        live_response = await client.get("/health/live")
        ready_response = await client.get("/health/ready")

    assert live_response.status_code == 200
    assert live_response.json() == {"status": "ok"}
    assert ready_response.status_code == 200
    assert ready_response.json() == {"status": "ready"}


async def test_api_key_authentication() -> None:
    protected_app = create_app(Settings(api_key="test-key"))
    async with AsyncClient(
        transport=ASGITransport(app=protected_app),
        base_url="http://test",
    ) as client:
        rejected = await client.post(
            "/api/v1/traffic/analyze",
            files={"file": ("traffic.txt", VALID, "text/plain")},
        )
        accepted = await client.post(
            "/api/v1/traffic/analyze",
            headers={"X-API-Key": "test-key"},
            files={"file": ("traffic.txt", VALID, "text/plain")},
        )

    assert rejected.status_code == 401
    assert accepted.status_code == 200


async def test_readiness_respects_rate_limit_failure_policy() -> None:
    """Verify Redis failure is degraded or unavailable according to configuration."""

    class UnavailableRedis:
        """Provide a failing readiness dependency."""

        async def ping(self) -> bool:
            """Raise a deterministic connection error."""
            raise ConnectionError("Redis unavailable")

    fail_open_app = create_app(
        Settings(
            rate_limit_enabled=True,
            rate_limit_redis_url="redis://test",
            rate_limit_fail_open=True,
        ),
    )
    fail_open_app.state.redis_client = UnavailableRedis()
    fail_closed_app = create_app(
        Settings(
            rate_limit_enabled=True,
            rate_limit_redis_url="redis://test",
            rate_limit_fail_open=False,
        ),
    )
    fail_closed_app.state.redis_client = UnavailableRedis()

    async with AsyncClient(
        transport=ASGITransport(app=fail_open_app),
        base_url="http://test",
    ) as client:
        degraded = await client.get("/health/ready")
    async with AsyncClient(
        transport=ASGITransport(app=fail_closed_app),
        base_url="http://test",
    ) as client:
        unavailable = await client.get("/health/ready")

    assert degraded.status_code == 200
    assert degraded.json()["rate_limit"] == "degraded"
    assert unavailable.status_code == 503


def test_create_app_configures_explicit_cors() -> None:
    """Verify configured origins install the CORS middleware."""
    cors_app = create_app(Settings(cors_origins=("https://client.example",)))
    no_cors_app = create_app(Settings(cors_origins=()))

    assert any(middleware.cls is CORSMiddleware for middleware in cors_app.user_middleware)
    assert all(middleware.cls is not CORSMiddleware for middleware in no_cors_app.user_middleware)


async def test_upload_rate_limit_returns_retry_after_header() -> None:
    """Verify API rate-limit failures use the documented HTTP contract."""

    class Backend:
        """Provide a deterministic shared counter."""

        def __init__(self) -> None:
            self.count = 0

        async def incr(self, _key: str) -> int:
            self.count += 1
            return self.count

        async def expire(self, _key: str, _seconds: int) -> bool:
            return True

    limited_app = create_app(
        Settings(
            rate_limit_enabled=True,
            rate_limit_redis_url="redis://test",
            rate_limit_upload_requests=1,
        ),
    )
    limited_app.state.rate_limiter = RedisRateLimiter(Backend(), 60, False)
    transport = ASGITransport(app=limited_app)

    async with AsyncClient(transport=transport, base_url="http://test") as client:
        accepted = await client.post(
            "/api/v1/traffic/analyze",
            files={"file": ("traffic.txt", VALID, "text/plain")},
        )
        rejected = await client.post(
            "/api/v1/traffic/analyze",
            files={"file": ("traffic.txt", VALID, "text/plain")},
        )

    assert accepted.status_code == 200
    assert rejected.status_code == 429
    assert int(rejected.headers["Retry-After"]) >= 1
