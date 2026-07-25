from io import BytesIO

import pytest
from fastapi import UploadFile

from src.services.traffic import TrafficAnalysisService
from src.utils.errors import UploadValidationError
from src.utils.files import safe_upload_filename


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


def test_upload_service_rejects_invalid_size_configuration() -> None:
    """Verify maximum upload size must be positive."""
    with pytest.raises(ValueError, match="upload_max_bytes"):
        TrafficAnalysisService(upload_max_bytes=0)


async def test_upload_rejects_invalid_utf8() -> None:
    """Verify undecodable content produces the stable encoding error."""
    service = TrafficAnalysisService()

    with pytest.raises(UploadValidationError) as captured:
        await service.analyze_upload(upload("traffic.txt", b"\xff"))

    assert captured.value.code == "invalid_encoding"


async def test_batch_rejects_invalid_concurrency() -> None:
    """Verify batch concurrency must be positive."""
    with pytest.raises(ValueError, match="concurrency"):
        await TrafficAnalysisService().analyze_uploads([], concurrency=0)


def test_safe_filename_handles_missing_and_invalid_names() -> None:
    """Verify response filenames always have a safe non-empty value."""
    missing = UploadFile(
        filename=None,
        file=BytesIO(b""),
        headers={"content-type": "text/plain"},
    )
    invalid = upload("???", b"")

    assert safe_upload_filename(missing, index=2) == "upload-3.txt"
    with pytest.raises(UploadValidationError) as captured:
        safe_upload_filename(invalid)

    assert captured.value.code == "invalid_filename"


async def test_upload_rejects_unsupported_mime_type() -> None:
    """Verify a permitted extension cannot bypass MIME validation."""
    service = TrafficAnalysisService()

    with pytest.raises(UploadValidationError) as captured:
        await service.analyze_upload(upload("traffic.txt", b"content", "application/pdf"))

    assert captured.value.code == "unsupported_media_type"
