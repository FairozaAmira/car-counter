"""Kafka consumer worker service implementation."""

import asyncio
from collections.abc import Callable
from typing import Any
from uuid import uuid4

from aiokafka import AIOKafkaConsumer
from aiokafka.structs import ConsumerRecord, OffsetAndMetadata, TopicPartition
from pydantic import ValidationError

from src.config import Settings
from src.schemas.kafka import KafkaAnalysisRequest, KafkaAnalysisResult
from src.schemas.traffic import ErrorDetail, ProcessingStatus
from src.services.analyzer import analyze_traffic
from src.services.errors import TrafficCounterError
from src.services.kafka_producer import KafkaProducerService


class KafkaConsumerService:
    """Consumes analysis requests and publishes results partition-safely."""

    def __init__(
        self,
        settings: Settings,
        consumer_factory: Callable[..., Any] = AIOKafkaConsumer,
        result_producer: KafkaProducerService | None = None,
    ) -> None:
        self._settings = settings
        self._consumer_factory = consumer_factory
        self._consumer: Any | None = None
        self._result_producer = result_producer or KafkaProducerService(settings)

    async def start(self) -> None:
        """Start the result producer and request consumer.

        Args:
            None.

        Returns:
            None.

        Raises:
            KafkaError: If either Kafka client cannot start.
        """
        if self._consumer is not None:
            return
        self._consumer = self._consumer_factory(
            self._settings.kafka_request_topic,
            bootstrap_servers=self._settings.kafka_bootstrap_servers,
            group_id=self._settings.kafka_consumer_group,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
        )
        await self._result_producer.start()
        try:
            await self._consumer.start()
        except Exception:
            await self._result_producer.stop()
            self._consumer = None
            raise

    async def stop(self) -> None:
        """Close the request consumer and result producer.

        Args:
            None.

        Returns:
            None.

        Raises:
            KafkaError: If either Kafka client cannot close.
        """
        if self._consumer is not None:
            await self._consumer.stop()
            self._consumer = None
        await self._result_producer.stop()

    def _require_started(self) -> Any:
        if self._consumer is None:
            raise RuntimeError("Kafka consumer service has not been started.")
        return self._consumer

    async def run(self, stop_event: asyncio.Event) -> None:
        """Poll and process partitions until shutdown is requested.

        Args:
            stop_event: Cooperative shutdown signal.

        Returns:
            None.

        Raises:
            RuntimeError: If the service has not started.
            KafkaError: If polling or message processing fails.
        """
        consumer = self._require_started()
        while not stop_event.is_set():
            messages = await consumer.getmany(
                timeout_ms=self._settings.kafka_consumer_poll_timeout_ms,
                max_records=self._settings.kafka_consumer_max_records,
            )
            if not messages:
                continue
            await asyncio.gather(
                *(
                    self._process_partition(partition, records)
                    for partition, records in messages.items()
                ),
            )

    async def _process_partition(
        self,
        partition: TopicPartition,
        records: list[ConsumerRecord],
    ) -> None:
        consumer = self._require_started()
        for message in records:
            await self.process_message(message.value)
            await consumer.commit(
                {partition: OffsetAndMetadata(message.offset + 1, "")},
            )

    async def process_message(self, raw_value: bytes) -> KafkaAnalysisResult:
        """Validate, analyze, and publish one request event.

        Args:
            raw_value: Serialized request event.

        Returns:
            The published analysis outcome.

        Raises:
            KafkaError: If the result cannot be published.
        """
        try:
            request = KafkaAnalysisRequest.model_validate_json(raw_value)
        except (ValidationError, ValueError) as exc:
            result = KafkaAnalysisResult(
                request_id=uuid4(),
                filename="unknown",
                status=ProcessingStatus.FAILED,
                error=ErrorDetail(code="invalid_event", message=str(exc)),
            )
        else:
            try:
                analysis = analyze_traffic(request.records)
                result = KafkaAnalysisResult(
                    request_id=request.request_id,
                    filename=request.filename,
                    status=ProcessingStatus.COMPLETED,
                    result=analysis,
                )
            except TrafficCounterError as exc:
                result = KafkaAnalysisResult(
                    request_id=request.request_id,
                    filename=request.filename,
                    status=ProcessingStatus.FAILED,
                    error=ErrorDetail(code=exc.code, message=exc.message),
                )

        await self._result_producer.publish_result(result)
        return result
