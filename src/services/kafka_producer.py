import asyncio
from collections.abc import Callable
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

from aiokafka import AIOKafkaProducer

from src.config import Settings
from src.schemas.kafka import KafkaAnalysisRequest, KafkaAnalysisResult, KafkaPublishItem
from src.schemas.traffic import ErrorDetail, ProcessingStatus, TrafficRecord
from src.services.errors import TrafficCounterError
from src.services.parser import parse_traffic_text


class KafkaProducerService:
    """Asynchronous publisher for traffic analysis requests and results."""

    def __init__(
        self,
        settings: Settings,
        producer_factory: Callable[..., Any] = AIOKafkaProducer,
    ) -> None:
        self._settings = settings
        self._producer_factory = producer_factory
        self._producer: Any | None = None

    async def start(self) -> None:
        if self._producer is not None:
            return
        self._producer = self._producer_factory(
            bootstrap_servers=self._settings.kafka_bootstrap_servers,
            enable_idempotence=True,
            request_timeout_ms=self._settings.kafka_request_timeout_ms,
        )
        await self._producer.start()

    async def stop(self) -> None:
        if self._producer is not None:
            await self._producer.stop()
            self._producer = None

    def _require_started(self) -> Any:
        if self._producer is None:
            raise RuntimeError("Kafka producer service has not been started.")
        return self._producer

    async def publish_records(
        self,
        filename: str,
        records: list[TrafficRecord],
        request_id: UUID | None = None,
    ) -> UUID:
        event = KafkaAnalysisRequest(
            request_id=request_id or uuid4(),
            filename=filename,
            records=records,
        )
        producer = self._require_started()
        await producer.send_and_wait(
            self._settings.kafka_request_topic,
            key=str(event.request_id).encode(),
            value=event.model_dump_json().encode(),
        )
        return event.request_id

    async def publish_result(self, event: KafkaAnalysisResult) -> None:
        producer = self._require_started()
        await producer.send_and_wait(
            self._settings.kafka_result_topic,
            key=str(event.request_id).encode(),
            value=event.model_dump_json().encode(),
        )

    async def publish_file(self, path: Path) -> KafkaPublishItem:
        try:
            content = await asyncio.to_thread(path.read_text, encoding="utf-8")
            records = parse_traffic_text(content)
            request_id = await self.publish_records(path.name, records)
            return KafkaPublishItem(
                filename=path.name,
                status=ProcessingStatus.COMPLETED,
                request_id=request_id,
            )
        except TrafficCounterError as exc:
            return KafkaPublishItem(
                filename=path.name,
                status=ProcessingStatus.FAILED,
                error=ErrorDetail(code=exc.code, message=exc.message),
            )
        except (OSError, UnicodeError) as exc:
            return KafkaPublishItem(
                filename=path.name,
                status=ProcessingStatus.FAILED,
                error=ErrorDetail(code="file_read_error", message=str(exc)),
            )
        except Exception as exc:
            return KafkaPublishItem(
                filename=path.name,
                status=ProcessingStatus.FAILED,
                error=ErrorDetail(code="kafka_publish_error", message=str(exc)),
            )

    async def publish_files(
        self,
        paths: list[Path],
        concurrency: int,
    ) -> list[KafkaPublishItem]:
        semaphore = asyncio.Semaphore(concurrency)
        results: list[KafkaPublishItem | None] = [None] * len(paths)

        async def publish_one(index: int, path: Path) -> None:
            async with semaphore:
                results[index] = await self.publish_file(path)

        async with asyncio.TaskGroup() as task_group:
            for index, path in enumerate(paths):
                task_group.create_task(publish_one(index, path))

        return [result for result in results if result is not None]
