"""Secure upload validation and text-file reading utilities."""

import asyncio
from pathlib import Path, PurePath

from fastapi import UploadFile
from werkzeug.utils import secure_filename

from src.utils.errors import ErrorCode, UploadValidationError


def safe_upload_filename(upload: UploadFile, index: int = 0) -> str:
    """Return a sanitized filename suitable for a response.

    Args:
        upload: Uploaded file whose filename is untrusted.
        index: Input position used for the fallback name.

    Returns:
        A non-empty sanitized filename.

    Raises:
        UploadValidationError: If an explicitly supplied filename is unsafe.
    """
    original = upload.filename
    if not original:
        return f"upload-{index + 1}.txt"
    sanitized = secure_filename(PurePath(original).name)
    if not sanitized:
        raise UploadValidationError(
            ErrorCode.INVALID_FILENAME,
            "The filename is invalid.",
            400,
        )
    return sanitized


def validate_upload_metadata(
    upload: UploadFile,
    allowed_extensions: set[str],
    allowed_mime_types: set[str],
) -> None:
    """Validate untrusted upload metadata before reading content.

    Args:
        upload: Uploaded file to validate.
        allowed_extensions: Permitted lowercase file extensions.
        allowed_mime_types: Permitted lowercase MIME types.

    Returns:
        None.

    Raises:
        UploadValidationError: If the file extension or MIME type is unsupported.
    """
    filename = safe_upload_filename(upload)
    extension = PurePath(filename).suffix.lower()
    if extension not in allowed_extensions:
        raise UploadValidationError(
            ErrorCode.UNSUPPORTED_FILE_EXTENSION,
            "The input file extension is not supported.",
            415,
        )
    content_type = (upload.content_type or "").lower()
    if content_type not in allowed_mime_types:
        raise UploadValidationError(
            ErrorCode.UNSUPPORTED_MEDIA_TYPE,
            "The input file content type is not supported.",
            415,
        )


async def read_upload_text(
    upload: UploadFile,
    max_bytes: int,
    allowed_extensions: set[str],
    allowed_mime_types: set[str],
) -> str:
    """Validate and read one uploaded UTF-8 text file.

    Args:
        upload: Uploaded file to read.
        max_bytes: Maximum accepted payload size.
        allowed_extensions: Permitted lowercase file extensions.
        allowed_mime_types: Permitted lowercase MIME types.

    Returns:
        Decoded UTF-8 content.

    Raises:
        UploadValidationError: If metadata, size, or content is invalid.
    """
    validate_upload_metadata(upload, allowed_extensions, allowed_mime_types)
    try:
        raw_content = await upload.read(max_bytes + 1)
        if len(raw_content) > max_bytes:
            raise UploadValidationError(
                ErrorCode.FILE_TOO_LARGE,
                f"The input file must not exceed {max_bytes} bytes.",
                413,
            )
        if b"\x00" in raw_content:
            raise UploadValidationError(
                ErrorCode.INVALID_FILE_CONTENT,
                "The input file must contain plain text.",
                415,
            )
        return raw_content.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise UploadValidationError(
            ErrorCode.INVALID_ENCODING,
            "The input file must be UTF-8 encoded text.",
            415,
        ) from exc
    finally:
        await upload.close()


def read_text_file(path: Path) -> str:
    """Read a local UTF-8 text file.

    Args:
        path: File path to read.

    Returns:
        Decoded UTF-8 content.

    Raises:
        OSError: If the file cannot be read.
        UnicodeError: If the file is not valid UTF-8.
    """
    return path.read_text(encoding="utf-8")


async def read_text_file_async(path: Path) -> str:
    """Read a local UTF-8 text file without blocking the event loop.

    Args:
        path: File path to read.

    Returns:
        Decoded UTF-8 content.

    Raises:
        OSError: If the file cannot be read.
        UnicodeError: If the file is not valid UTF-8.
    """
    return await asyncio.to_thread(read_text_file, path)
