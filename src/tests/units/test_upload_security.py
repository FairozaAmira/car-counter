from io import BytesIO

import pytest
from fastapi import UploadFile

from src.services.errors import UploadValidationError
from src.services.traffic import TrafficAnalysisService


def upload(filename: str, content: bytes, content_type: str = "text/plain") -> UploadFile:
    """Create an upload fixture.

    Args:
        filename: Client-provided filename.
        content: Raw file bytes.
        content_type: Client-provided MIME type.

    Returns:
        A FastAPI upload object.

    Raises:
        None.
    """
    return UploadFile(
        filename=filename,
        file=BytesIO(content),
        headers={"content-type": content_type},
    )


async def test_upload_rejects_unsupported_extension() -> None:
    """Verify executable extensions are rejected before reading."""
    service = TrafficAnalysisService()

    with pytest.raises(UploadValidationError, match="extension"):
        await service.analyze_upload(upload("traffic.exe", b"content"))


async def test_upload_rejects_oversized_content() -> None:
    """Verify content larger than the configured limit returns a safe error."""
    service = TrafficAnalysisService(upload_max_bytes=3)

    with pytest.raises(UploadValidationError) as captured:
        await service.analyze_upload(upload("traffic.txt", b"1234"))

    assert captured.value.status_code == 413


async def test_upload_rejects_binary_content() -> None:
    """Verify NUL-containing content is not accepted as plain text."""
    service = TrafficAnalysisService()

    with pytest.raises(UploadValidationError) as captured:
        await service.analyze_upload(upload("traffic.txt", b"bad\x00data"))

    assert captured.value.status_code == 415
