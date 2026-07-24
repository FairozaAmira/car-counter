from httpx import ASGITransport, AsyncClient

from src.main import app

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


async def test_health_endpoint() -> None:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
