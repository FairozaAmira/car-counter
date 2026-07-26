from datetime import datetime
from typing import Any
from uuid import UUID

from fastapi.middleware.cors import CORSMiddleware
from httpx import ASGITransport, AsyncClient
from redis.exceptions import ConnectionError

from src.config import Settings
from src.db.models import AnalysisKind
from src.main import createApp
from src.routers.traffic import getAnalysisRepository
from src.services.rate_limit import RedisRateLimiter

VALID = """\
2021-12-01T05:00:00 1
2021-12-01T05:30:00 2
2021-12-01T06:00:00 3
"""


class InMemoryAnalysisRepository:
    """Capture persisted API results without using a production database."""

    def __init__(self) -> None:
        """Create an empty repository."""
        self.records: list[dict[str, Any]] = []

    async def save(self, **record: Any) -> None:
        """Capture one stored response."""
        self.records.append(record)


repository = InMemoryAnalysisRepository()
app = createApp(Settings(databaseUrl=None))
app.dependency_overrides[getAnalysisRepository] = lambda: repository


def configure_repository(application: Any) -> InMemoryAnalysisRepository:
    """Configure isolated API persistence for a test application."""
    test_repository = InMemoryAnalysisRepository()
    application.dependency_overrides[getAnalysisRepository] = lambda: test_repository
    return test_repository


def assert_post_response_metadata(payload: dict[str, object]) -> None:
    """Verify common POST response identifiers and timing metadata."""
    assert UUID(str(payload["id"])).version == 4
    datetime.strptime(str(payload["createdAt"]), "%d-%m-%Y")
    timeTaken = payload["timeTaken"]
    assert isinstance(timeTaken, float)
    assert timeTaken >= 0
    assert timeTaken == round(timeTaken, 2)


async def test_single_file_endpoint() -> None:
    record_count = len(repository.records)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post(
            "/api/v1/traffic/analyze",
            files={"file": ("traffic.txt", VALID, "text/plain")},
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["totalCars"] == 6
    assert payload["dailyTotals"][0]["date"] == "01-12-2021"
    assert payload["topHalfHours"][0]["timestamp"] == "01-12-2021 06:00:00"
    assert payload["leastCarsPeriod"]["start"] == "01-12-2021 05:00:00"
    assert payload["leastCarsPeriod"]["end"] == "01-12-2021 06:30:00"
    assert_post_response_metadata(payload)
    assert len(repository.records) == record_count + 1
    stored = repository.records[-1]
    assert stored["resultId"] == UUID(payload["id"])
    assert stored["analysisKind"] is AnalysisKind.SINGLE
    assert stored["responsePayload"] == payload


async def test_single_file_endpoint_returns_domain_error() -> None:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post(
            "/api/v1/traffic/analyze",
            files={"file": ("traffic.txt", "bad data", "text/plain")},
        )

    assert response.status_code == 422
    assert response.json()["detail"]["code"] == "ERR00033"


async def test_missing_upload_uses_standard_request_body_error() -> None:
    """Verify request validation failures use the standard error catalog."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post("/api/v1/traffic/analyze")

    assert response.status_code == 422
    assert response.json()["detail"] == {
        "code": "ERR00031",
        "message": "Invalid request body",
    }


async def test_batch_endpoint_returns_partial_results() -> None:
    record_count = len(repository.records)
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
    assert payload["items"][0]["result"]["topHalfHours"][0]["timestamp"] == ("01-12-2021 06:00:00")
    assert_post_response_metadata(payload)
    assert len(repository.records) == record_count + 1
    assert repository.records[-1]["analysisKind"] is AnalysisKind.BATCH
    assert repository.records[-1]["responsePayload"] == payload


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
    protected_app = createApp(Settings(apiKey="test-key", databaseUrl=None))
    configure_repository(protected_app)
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

    fail_open_app = createApp(
        Settings(
            rateLimitEnabled=True,
            rateLimitRedisUrl="redis://test",
            rateLimitFailOpen=True,
            databaseUrl=None,
        ),
    )
    configure_repository(fail_open_app)
    fail_open_app.state.redisClient = UnavailableRedis()
    fail_closed_app = createApp(
        Settings(
            rateLimitEnabled=True,
            rateLimitRedisUrl="redis://test",
            rateLimitFailOpen=False,
            databaseUrl=None,
        ),
    )
    configure_repository(fail_closed_app)
    fail_closed_app.state.redisClient = UnavailableRedis()

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
    assert degraded.json()["rateLimit"] == "degraded"
    assert unavailable.status_code == 503


def test_create_app_configures_explicit_cors() -> None:
    """Verify configured origins install the CORS middleware."""
    cors_app = createApp(
        Settings(corsOrigins=("https://client.example",), databaseUrl=None),
    )
    no_cors_app = createApp(Settings(corsOrigins=(), databaseUrl=None))

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

    limited_app = createApp(
        Settings(
            rateLimitEnabled=True,
            rateLimitRedisUrl="redis://test",
            rateLimitUploadRequests=1,
            databaseUrl=None,
        ),
    )
    configure_repository(limited_app)
    limited_app.state.rateLimiter = RedisRateLimiter(Backend(), 60, False)
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
