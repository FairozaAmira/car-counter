import pytest
from pydantic import ValidationError

from src.config import Settings


def test_settings_parse_structured_and_optional_values() -> None:
    """Verify settings normalize environment-shaped values."""
    settings = Settings(
        cors_origins="https://one.example, https://two.example",
        trusted_proxy_hosts=("127.0.0.1",),
        api_key="",
        rate_limit_redis_url=" ",
        log_level="debug",
    )

    assert settings.cors_origins == ("https://one.example", "https://two.example")
    assert settings.trusted_proxy_hosts == ("127.0.0.1",)
    assert settings.api_key is None
    assert settings.rate_limit_redis_url is None
    assert settings.log_level == "DEBUG"


@pytest.mark.parametrize(
    "values",
    [
        {"log_level": "verbose"},
        {"rate_limit_enabled": True, "rate_limit_redis_url": None},
        {"app_reload": True, "app_workers": 2},
        {"cors_origins": ("*",)},
        {"database_url": "postgresql://application:application@localhost/application"},
    ],
)
def test_settings_reject_invalid_runtime_contracts(values: dict[str, object]) -> None:
    """Verify invalid cross-field and logging settings fail fast."""
    with pytest.raises(ValidationError):
        Settings(**values)
