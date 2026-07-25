from pathlib import Path

import pytest

from src.config import Settings
from src.schemas.kafka import KafkaPublishItem
from src.schemas.traffic import ProcessingStatus
from src.scripts import analyze, kafka_consumer, kafka_producer, serve

VALID = """\
2021-12-01T05:00:00 1
2021-12-01T05:30:00 2
2021-12-01T06:00:00 3
"""


def test_analyze_cli_outputs_json(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Verify the analysis CLI reads a file and prints structured JSON."""
    input_path = tmp_path / "traffic.txt"
    input_path.write_text(VALID, encoding="utf-8")
    monkeypatch.setattr("sys.argv", ["analyze", str(input_path)])

    analyze.main()

    assert '"total_cars": 6' in capsys.readouterr().out


async def test_kafka_producer_run_starts_and_stops_service(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Verify producer CLI lifecycle and success exit code."""
    events: list[str] = []

    class FakeProducerService:
        """Provide a deterministic producer service."""

        def __init__(self, _settings: Settings) -> None:
            """Create the fake service."""

        async def start(self) -> None:
            """Record startup."""
            events.append("start")

        async def publish_files(
            self,
            paths: list[Path],
            _concurrency: int,
        ) -> list[KafkaPublishItem]:
            """Return one successful publish item."""
            return [
                KafkaPublishItem(
                    filename=paths[0].name,
                    status=ProcessingStatus.COMPLETED,
                ),
            ]

        async def stop(self) -> None:
            """Record shutdown."""
            events.append("stop")

    monkeypatch.setattr(kafka_producer, "KafkaProducerService", FakeProducerService)

    result = await kafka_producer.run([Path("traffic.txt")])

    assert result == 0
    assert events == ["start", "stop"]
    assert '"status":"completed"' in capsys.readouterr().out


async def test_kafka_consumer_run_starts_and_stops_service(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify consumer CLI lifecycle closes its service."""
    events: list[str] = []

    class FakeConsumerService:
        """Provide a non-blocking consumer service."""

        def __init__(self, _settings: Settings) -> None:
            """Create the fake service."""

        async def start(self) -> None:
            """Record startup."""
            events.append("start")

        async def run(self, _stop_event: object) -> None:
            """Record the worker loop."""
            events.append("run")

        async def stop(self) -> None:
            """Record shutdown."""
            events.append("stop")

    monkeypatch.setattr(kafka_consumer, "KafkaConsumerService", FakeConsumerService)

    await kafka_consumer.run()

    assert events == ["start", "run", "stop"]


def test_serve_cli_uses_runtime_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify Uvicorn receives validated environment-driven settings."""
    calls: list[dict[str, object]] = []
    monkeypatch.setattr(
        serve,
        "get_settings",
        lambda: Settings(app_host="127.0.0.1", app_port=9000, app_workers=1),
    )
    monkeypatch.setattr(
        serve.uvicorn,
        "run",
        lambda *_args, **kwargs: calls.append(kwargs),
    )

    serve.main()

    assert calls[0]["host"] == "127.0.0.1"
    assert calls[0]["port"] == 9000
