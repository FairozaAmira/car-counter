"""Secure upload validation and text-file reading utilities."""

import asyncio
from pathlib import Path, PurePath

from fastapi import UploadFile
from werkzeug.utils import secure_filename

from src.utils.errors import ErrorCode, UploadValidationError


def safeUploadFilename(upload: UploadFile, index: int = 0) -> str:
    """Return a sanitized filename suitable for a response.

    Args:
        upload: Uploaded file whose filename is untrusted.
        index: Input position used for the fallback name.

    Returns:
        A non-empty sanitized filename.

    Raises:
        UploadValidationError: If an explicitly supplied filename is unsafe.
    """
    try:
        original = upload.filename
        if not original:
            return f"upload-{index + 1}.txt"
        sanitized = secure_filename(PurePath(original).name)
        if not sanitized:
            raise UploadValidationError(
                ErrorCode.INVALID_FILENAME,
                "The filename is invalid.",
            )
        return sanitized
    except Exception as e:  # pragma: no cover - diagnostic boundary
        print(f"Error in safeUploadFilename: {e}")
        raise


def validateUploadMetadata(
    upload: UploadFile,
    allowedExtensions: set[str],
    allowedMimeTypes: set[str],
) -> None:
    """Validate untrusted upload metadata before reading content.

    Args:
        upload: Uploaded file to validate.
        allowedExtensions: Permitted lowercase file extensions.
        allowedMimeTypes: Permitted lowercase MIME types.

    Returns:
        None.

    Raises:
        UploadValidationError: If the file extension or MIME type is unsupported.
    """
    try:
        filename = safeUploadFilename(upload)
        extension = PurePath(filename).suffix.lower()
        if extension not in allowedExtensions:
            raise UploadValidationError(
                ErrorCode.UNSUPPORTED_FILE_EXTENSION,
                "The input file extension is not supported.",
            )
        contentType = (upload.content_type or "").lower()
        if contentType not in allowedMimeTypes:
            raise UploadValidationError(
                ErrorCode.UNSUPPORTED_MEDIA_TYPE,
                "The input file content type is not supported.",
            )
    except Exception as e:  # pragma: no cover - diagnostic boundary
        print(f"Error in validateUploadMetadata: {e}")
        raise


async def readUploadText(
    upload: UploadFile,
    maxBytes: int,
    allowedExtensions: set[str],
    allowedMimeTypes: set[str],
) -> str:
    """Validate and read one uploaded UTF-8 text file.

    Args:
        upload: Uploaded file to read.
        maxBytes: Maximum accepted payload size.
        allowedExtensions: Permitted lowercase file extensions.
        allowedMimeTypes: Permitted lowercase MIME types.

    Returns:
        Decoded UTF-8 content.

    Raises:
        UploadValidationError: If metadata, size, or content is invalid.
    """
    try:
        validateUploadMetadata(upload, allowedExtensions, allowedMimeTypes)
        try:
            rawContent = await upload.read(maxBytes + 1)
            if len(rawContent) > maxBytes:
                raise UploadValidationError(
                    ErrorCode.FILE_TOO_LARGE,
                    f"The input file must not exceed {maxBytes} bytes.",
                )
            if b"\x00" in rawContent:
                raise UploadValidationError(
                    ErrorCode.INVALID_FILE_CONTENT,
                    "The input file must contain plain text.",
                )
            return rawContent.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise UploadValidationError(
                ErrorCode.INVALID_ENCODING,
                "The input file must be UTF-8 encoded text.",
            ) from exc
        finally:
            await upload.close()
    except Exception as e:  # pragma: no cover - diagnostic boundary
        print(f"Error in readUploadText: {e}")
        raise


def readTextFile(path: Path) -> str:
    """Read a local UTF-8 text file.

    Args:
        path: File path to read.

    Returns:
        Decoded UTF-8 content.

    Raises:
        OSError: If the file cannot be read.
        UnicodeError: If the file is not valid UTF-8.
    """
    try:
        return path.read_text(encoding="utf-8")
    except Exception as e:  # pragma: no cover - diagnostic boundary
        print(f"Error in readTextFile: {e}")
        raise


async def readTextFileAsync(path: Path) -> str:
    """Read a local UTF-8 text file without blocking the event loop.

    Args:
        path: File path to read.

    Returns:
        Decoded UTF-8 content.

    Raises:
        OSError: If the file cannot be read.
        UnicodeError: If the file is not valid UTF-8.
    """
    try:
        return await asyncio.to_thread(readTextFile, path)
    except Exception as e:  # pragma: no cover - diagnostic boundary
        print(f"Error in readTextFileAsync: {e}")
        raise
