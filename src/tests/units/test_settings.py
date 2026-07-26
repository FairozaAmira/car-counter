import pytest
from pydantic import ValidationError

from src.config import Settings
from src.config.settings import toEnvironmentName


def test_environment_name_conversion_reports_unexpected_errors(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Verify the required function diagnostic includes its camelCase name."""
    with pytest.raises(TypeError):
        toEnvironmentName(None)  # type: ignore[arg-type]

    assert "Error in toEnvironmentName:" in capsys.readouterr().out


def test_settings_parse_structured_and_optional_values() -> None:
    """Verify settings normalize environment-shaped values."""
    settings = Settings(
        corsOrigins="https://one.example, https://two.example",
        trustedProxyHosts=("127.0.0.1",),
        apiKey="",
        rateLimitRedisUrl=" ",
        logLevel="debug",
    )

    assert settings.corsOrigins == ("https://one.example", "https://two.example")
    assert settings.trustedProxyHosts == ("127.0.0.1",)
    assert settings.apiKey is None
    assert settings.rateLimitRedisUrl is None
    assert settings.logLevel == "DEBUG"


@pytest.mark.parametrize(
    "values",
    [
        {"logLevel": "verbose"},
        {"rateLimitEnabled": True, "rateLimitRedisUrl": None},
        {"appReload": True, "appWorkers": 2},
        {"corsOrigins": ("*",)},
        {"databaseUrl": "postgresql://application:application@localhost/application"},
    ],
)
def test_settings_reject_invalid_runtime_contracts(values: dict[str, object]) -> None:
    """Verify invalid cross-field and logging settings fail fast."""
    with pytest.raises(ValidationError):
        Settings(**values)
