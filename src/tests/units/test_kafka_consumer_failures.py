import asyncio
from types import SimpleNamespace

import pytest
from aiokafka.structs import TopicPartition

from src.config import Settings
from src.schemas.kafka import KafkaAnalysisRequest
from src.schemas.traffic import ProcessingStatus, TrafficRecord
from src.services.kafka_consumer import KafkaConsumerService
from src.utils.errors import (
    ErrorCode,
    KafkaConsumerActionError,
    KafkaConsumerInitializationError,
)


class RecordingResultProducer:
    """Record result-producer lifecycle and published events."""

    def __init__(self) -> None:
        self.events: list[str] = []
        self.results: list[object] = []

    async def start(self) -> None:
        """Record startup."""
        self.events.append("start")

    async def stop(self) -> None:
        """Record shutdown."""
        self.events.append("stop")

    async def publishResult(self, event: object) -> None:
        """Record a published result."""
        self.results.append(event)


class PollingConsumer:
    """Return an empty poll followed by one message."""

    def __init__(self, stopEvent: asyncio.Event, rawValue: bytes) -> None:
        self.stopEvent = stopEvent
        self.rawValue = rawValue
        self.poll_count = 0
        self.events: list[str] = []

    async def start(self) -> None:
        """Record startup."""
        self.events.append("start")

    async def stop(self) -> None:
        """Record shutdown."""
        self.events.append("stop")

    async def getmany(self, **_kwargs: object) -> dict[TopicPartition, list[object]]:
        """Return an empty batch before the configured message."""
        self.poll_count += 1
        if self.poll_count == 1:
            return {}
        self.stopEvent.set()
        partition = TopicPartition("traffic.analysis.requests", 0)
        return {partition: [SimpleNamespace(value=self.rawValue, offset=0)]}

    async def commit(self, _offsets: object) -> None:
        """Record a committed message."""
        self.events.append("commit")


class FailingConsumer:
    """Fail during Kafka consumer startup."""

    async def start(self) -> None:
        """Raise a deterministic startup failure."""
        raise RuntimeError("consumer unavailable")


class FailingPollingConsumer:
    """Fail while polling after a successful startup."""

    async def start(self) -> None:
        """Complete startup."""

    async def stop(self) -> None:
        """Complete shutdown."""

    async def getmany(self, **_kwargs: object) -> dict[object, object]:
        """Raise a deterministic polling failure."""
        raise RuntimeError("poll unavailable")


async def test_consumer_start_failure_cleans_up_result_producer() -> None:
    """Verify partial startup is rolled back."""
    resultProducer = RecordingResultProducer()
    service = KafkaConsumerService(
        Settings(),
        consumerFactory=lambda *_args, **_kwargs: FailingConsumer(),
        resultProducer=resultProducer,  # type: ignore[arg-type]
    )

    with pytest.raises(KafkaConsumerInitializationError) as captured:
        await service.start()

    assert captured.value.code == ErrorCode.KAFKA_CONSUMER_INITIALIZATION
    assert resultProducer.events == ["start", "stop"]


async def test_consumer_factory_failure_uses_initialization_error() -> None:
    """Verify client-construction failures use the standard initialization error."""

    def fail_factory(*_args: object, **_kwargs: object) -> object:
        """Raise a deterministic client-construction failure."""
        raise RuntimeError("factory unavailable")

    service = KafkaConsumerService(Settings(), consumerFactory=fail_factory)

    with pytest.raises(KafkaConsumerInitializationError) as captured:
        await service.start()

    assert captured.value.code == ErrorCode.KAFKA_CONSUMER_INITIALIZATION


async def test_consumer_requires_start_and_stop_is_idempotent() -> None:
    """Verify the worker guard and repeated shutdown behavior."""
    resultProducer = RecordingResultProducer()
    service = KafkaConsumerService(
        Settings(),
        resultProducer=resultProducer,  # type: ignore[arg-type]
    )

    with pytest.raises(RuntimeError, match="has not been started"):
        await service.run(asyncio.Event())

    await service.stop()
    assert resultProducer.events == ["stop"]


async def test_consumer_action_uses_standard_error() -> None:
    """Verify polling failures use the standard consumer-action error."""
    resultProducer = RecordingResultProducer()
    service = KafkaConsumerService(
        Settings(),
        consumerFactory=lambda *_args, **_kwargs: FailingPollingConsumer(),
        resultProducer=resultProducer,  # type: ignore[arg-type]
    )

    await service.start()
    with pytest.raises(KafkaConsumerActionError) as captured:
        await service.run(asyncio.Event())
    await service.stop()

    assert captured.value.code == ErrorCode.KAFKA_CONSUMER_ACTION


async def test_consumer_polls_until_stopped_and_processes_messages() -> None:
    """Verify empty polls continue and later messages are processed."""
    request = KafkaAnalysisRequest(
        requestId="d768e416-7cb7-419e-b11d-b6e94f813944",
        filename="traffic.txt",
        records=[
            TrafficRecord(timestamp="2021-01-01T00:00:00", carCount=1),
            TrafficRecord(timestamp="2021-01-01T00:30:00", carCount=2),
            TrafficRecord(timestamp="2021-01-01T01:00:00", carCount=3),
        ],
    )
    stopEvent = asyncio.Event()
    consumer = PollingConsumer(stopEvent, request.model_dump_json().encode())
    resultProducer = RecordingResultProducer()
    service = KafkaConsumerService(
        Settings(),
        consumerFactory=lambda *_args, **_kwargs: consumer,
        resultProducer=resultProducer,  # type: ignore[arg-type]
    )

    await service.start()
    await service.start()
    await service.run(stopEvent)
    await service.stop()
    await service.stop()

    assert consumer.events == ["start", "commit", "stop"]
    assert consumer.poll_count == 2


@pytest.mark.parametrize(
    ("rawValue", "expected_code"),
    [
        (b"not-json", ErrorCode.INVALID_JSON_REQUEST_BODY),
        (
            KafkaAnalysisRequest(
                requestId="d768e416-7cb7-419e-b11d-b6e94f813944",
                filename="traffic.txt",
                records=[TrafficRecord(timestamp="2021-01-01T00:00:00", carCount=1)],
            )
            .model_dump_json()
            .encode(),
            ErrorCode.INVALID_REQUEST_BODY,
        ),
    ],
)
async def test_consumer_publishes_safe_failure_results(
    rawValue: bytes,
    expected_code: str,
) -> None:
    """Verify invalid and unanalyzable requests publish failed results."""
    resultProducer = RecordingResultProducer()
    service = KafkaConsumerService(
        Settings(),
        resultProducer=resultProducer,  # type: ignore[arg-type]
    )

    result = await service.processMessage(rawValue)

    assert result.status is ProcessingStatus.FAILED
    assert result.error is not None
    assert result.error.code == expected_code
