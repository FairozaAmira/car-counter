"""Stable error-code catalog tests."""

import re

from src.utils.errors import ERROR_DEFINITIONS, ErrorCode


def test_every_error_has_a_stable_code_and_http_status() -> None:
    """Verify every declared error has a safe message and explicit HTTP status."""
    assert set(ERROR_DEFINITIONS) == set(ErrorCode)
    assert all(re.fullmatch(r"ERR\d{5}", code.value) for code in ErrorCode)
    assert all(definition.message for definition in ERROR_DEFINITIONS.values())
    assert {definition.statusCode for definition in ERROR_DEFINITIONS.values()} == {
        400,
        401,
        413,
        415,
        422,
        429,
        500,
        503,
    }
