from pathlib import Path
from uuid import uuid4

import pytest

from src.config import Settings
from src.schemas.kafka import KafkaAnalysisResult
from src.schemas.traffic import ErrorDetail, ProcessingStatus, TrafficRecord
from src.services.kafka_producer import KafkaProducerService

VALID_TRAFFIC = """\
2021-01-01T00:00:00 1
2021-01-01T00:30:00 2
2021-01-01T01:00:00 3
"""


class RecordingProducer:
    """Record producer lifecycle and optionally fail when publishing."""

    def __init__(self, *, fail_publish: bool = False) -> None:
        self.events: list[str] = []
        self.sent: list[tuple[str, bytes, bytes]] = []
        self.fail_publish = fail_publish

    async def start(self) -> None:
        """Record startup."""
        self.events.append("start")

    async def stop(self) -> None:
        """Record shutdown."""
        self.events.append("stop")

    async def send_and_wait(self, topic: str, *, key: bytes, value: bytes) -> None:
        """Record a publication or raise the configured failure."""
        if self.fail_publish:
            raise RuntimeError("broker unavailable")
        self.sent.append((topic, key, value))


async def test_producer_lifecycle_is_idempotent_and_requires_start() -> None:
    """Verify duplicate lifecycle calls and the pre-start guard."""
    producer = RecordingProducer()
    service = KafkaProducerService(Settings(), producer_factory=lambda **_: producer)
    records = [TrafficRecord(timestamp="2021-01-01T00:00:00", car_count=1)]

    with pytest.raises(RuntimeError, match="has not been started"):
        await service.publish_records("traffic.txt", records)

    await service.start()
    await service.start()
    await service.stop()
    await service.stop()

    assert producer.events == ["start", "stop"]


async def test_producer_publishes_result_event() -> None:
    """Verify result events use the configured result topic."""
    producer = RecordingProducer()
    settings = Settings()
    service = KafkaProducerService(settings, producer_factory=lambda **_: producer)
    event = KafkaAnalysisResult(
        request_id=uuid4(),
        filename="traffic.txt",
        status=ProcessingStatus.FAILED,
        error=ErrorDetail(code="invalid_event", message="Invalid event."),
    )

    await service.start()
    await service.publish_result(event)
    await service.stop()

    assert producer.sent[0][0] == settings.kafka_result_topic


@pytest.mark.parametrize(
    ("filename", "content", "expected_code"),
    [
        ("invalid.txt", "not traffic data", "invalid_record"),
        ("invalid-utf8.txt", b"\xff", "file_read_error"),
    ],
)
async def test_publish_file_returns_safe_input_failures(
    tmp_path: Path,
    filename: str,
    content: str | bytes,
    expected_code: str,
) -> None:
    """Verify parsing and file-decoding failures remain item-local."""
    path = tmp_path / filename
    if isinstance(content, bytes):
        path.write_bytes(content)
    else:
        path.write_text(content, encoding="utf-8")
    service = KafkaProducerService(Settings())

    result = await service.publish_file(path)

    assert result.status is ProcessingStatus.FAILED
    assert result.error is not None
    assert result.error.code == expected_code


async def test_publish_file_returns_missing_file_failure(tmp_path: Path) -> None:
    """Verify missing files return a safe read failure."""
    service = KafkaProducerService(Settings())

    result = await service.publish_file(tmp_path / "missing.txt")

    assert result.error is not None
    assert result.error.code == "file_read_error"


async def test_publish_file_returns_broker_failure(tmp_path: Path) -> None:
    """Verify unexpected broker failures remain item-local."""
    path = tmp_path / "traffic.txt"
    path.write_text(VALID_TRAFFIC, encoding="utf-8")
    producer = RecordingProducer(fail_publish=True)
    service = KafkaProducerService(Settings(), producer_factory=lambda **_: producer)

    await service.start()
    result = await service.publish_file(path)
    await service.stop()

    assert result.error is not None
    assert result.error.code == "kafka_publish_error"


async def test_publish_files_rejects_non_positive_concurrency() -> None:
    """Verify bounded concurrency must be positive."""
    service = KafkaProducerService(Settings())

    with pytest.raises(ValueError, match="positive"):
        await service.publish_files([], concurrency=0)
