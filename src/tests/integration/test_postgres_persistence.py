"""Live PostgreSQL persistence integration test."""

import os
from uuid import UUID

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from src.config import Settings
from src.db.models import TrafficAnalysisResult
from src.main import createApp

VALID = """\
2021-12-01T05:00:00 1
2021-12-01T05:30:00 2
2021-12-01T06:00:00 3
"""


@pytest.mark.database
async def test_post_response_is_committed_to_disposable_postgres() -> None:
    """Verify a real POST response is queryable by its returned primary key."""
    if os.getenv("RUN_DATABASE_TESTS") != "1":
        pytest.skip("Set RUN_DATABASE_TESTS=1 to run PostgreSQL integration tests.")
    databaseUrl = os.environ["DATABASE_URL"]
    database_name = databaseUrl.rsplit("/", maxsplit=1)[-1].split("?", maxsplit=1)[0]
    assert database_name.endswith("_test"), "Database tests require a disposable *_test database."
    application = createApp(Settings(databaseUrl=databaseUrl))

    async with application.router.lifespan_context(application):
        async with AsyncClient(
            transport=ASGITransport(app=application),
            base_url="http://test",
        ) as client:
            response = await client.post(
                "/api/v1/traffic/analyze",
                files={"file": ("traffic.txt", VALID, "text/plain")},
            )

        assert response.status_code == 200
        payload = response.json()
        async with application.state.dbSessionFactory() as session:
            session: AsyncSession
            async with session.begin():
                stored = await session.get(TrafficAnalysisResult, UUID(payload["id"]))
                assert stored is not None
                assert stored.totalCars == payload["totalCars"]
                assert stored.dailyTotals == payload["dailyTotals"]
                assert stored.topHalfHours == payload["topHalfHours"]
                assert stored.leastCarsPeriodStart == payload["leastCarsPeriod"]["start"]
                assert stored.leastCarsPeriodEnd == payload["leastCarsPeriod"]["end"]
                assert stored.leastCarsPeriodTotalCars == payload["leastCarsPeriod"]["totalCars"]
                assert stored.leastCarsPeriodRecords == payload["leastCarsPeriod"]["records"]
                assert stored.items is None
                await session.delete(stored)
