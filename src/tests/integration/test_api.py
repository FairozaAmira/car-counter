from httpx import ASGITransport, AsyncClient
from redis.exceptions import ConnectionError

from src.config import Settings
from src.main import app, create_app

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
