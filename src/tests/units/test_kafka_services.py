from pathlib import Path
from types import SimpleNamespace

from aiokafka.structs import TopicPartition

from src.config import Settings
from src.schemas.kafka import KafkaAnalysisRequest
from src.schemas.traffic import ProcessingStatus, TrafficRecord
from src.services.kafka_consumer import KafkaConsumerService
from src.services.kafka_producer import KafkaProducerService


class FakeProducer:
    def __init__(self, events: list[str] | None = None) -> None:
        self.sent: list[tuple[str, bytes, bytes]] = []
        self.events = events if events is not None else []

    async def start(self) -> None:
        self.events.append("producer_started")

    async def stop(self) -> None:
        self.events.append("producer_stopped")

    async def send_and_wait(self, topic: str, *, key: bytes, value: bytes) -> None:
        self.events.append("published")
        self.sent.append((topic, key, value))


class FakeResultProducer:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.results = []

    async def start(self) -> None:
        self.events.append("result_producer_started")

    async def stop(self) -> None:
        self.events.append("result_producer_stopped")

    async def publishResult(self, event: object) -> None:
        self.events.append("published")
        self.results.append(event)


class FakeConsumer:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.commits = []

    async def start(self) -> None:
        self.events.append("consumer_started")

    async def stop(self) -> None:
        self.events.append("consumer_stopped")

    async def commit(self, offsets: object) -> None:
        self.events.append("committed")
        self.commits.append(offsets)


async def test_producer_publishes_multiple_files_in_input_order(tmp_path: Path) -> None:
    valid = "2021-01-01T00:00:00 1\n2021-01-01T00:30:00 2\n2021-01-01T01:00:00 3"
    first = tmp_path / "first.txt"
    second = tmp_path / "second.txt"
    first.write_text(valid)
    second.write_text(valid)
    fake = FakeProducer()
    service = KafkaProducerService(Settings(), producerFactory=lambda **_: fake)

    await service.start()
    results = await service.publishFiles([first, second], concurrency=2)
    await service.stop()

    assert [result.filename for result in results] == ["first.txt", "second.txt"]
    assert all(result.status is ProcessingStatus.COMPLETED for result in results)
    assert len(fake.sent) == 2


async def test_consumer_publishes_before_committing() -> None:
    events: list[str] = []
    fake_consumer = FakeConsumer(events)
    fake_result_producer = FakeResultProducer(events)
    service = KafkaConsumerService(
        Settings(),
        consumerFactory=lambda *_args, **_kwargs: fake_consumer,
        resultProducer=fake_result_producer,  # type: ignore[arg-type]
    )
    request = KafkaAnalysisRequest(
        requestId="d768e416-7cb7-419e-b11d-b6e94f813944",
        filename="traffic.txt",
        records=[
            TrafficRecord(timestamp="2021-01-01T00:00:00", carCount=1),
            TrafficRecord(timestamp="2021-01-01T00:30:00", carCount=2),
            TrafficRecord(timestamp="2021-01-01T01:00:00", carCount=3),
        ],
    )
    message = SimpleNamespace(value=request.model_dump_json().encode(), offset=4)

    await service.start()
    await service._processPartition(
        TopicPartition("traffic.analysis.requests", 0),
        [message],
    )
    await service.stop()

    assert events.index("published") < events.index("committed")
    assert fake_result_producer.results[0].status is ProcessingStatus.COMPLETED
