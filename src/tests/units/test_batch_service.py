from io import BytesIO

from fastapi import UploadFile

from src.schemas.traffic import ProcessingStatus
from src.services.traffic import TrafficAnalysisService

VALID = b"""\
2021-12-01T05:00:00 1
2021-12-01T05:30:00 2
2021-12-01T06:00:00 3
"""


async def test_batch_preserves_order_and_isolates_failures() -> None:
    uploads = [
        UploadFile(filename="first.txt", file=BytesIO(VALID)),
        UploadFile(filename="bad.txt", file=BytesIO(b"bad data")),
        UploadFile(filename="third.txt", file=BytesIO(VALID)),
    ]

    response = await TrafficAnalysisService().analyze_uploads(uploads, concurrency=2)

    assert [item.filename for item in response.items] == [
        "first.txt",
        "bad.txt",
        "third.txt",
    ]
    assert [item.status for item in response.items] == [
        ProcessingStatus.COMPLETED,
        ProcessingStatus.FAILED,
        ProcessingStatus.COMPLETED,
    ]
    assert response.items[1].error is not None
    assert response.items[1].error.code == "invalid_timestamp"
