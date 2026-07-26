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
from src.services.analyzer import analyzeTraffic
from src.services.kafka_producer import KafkaProducerService
from src.utils.errors import (
    InvalidJsonRequestError,
    InvalidRequestBodyError,
    KafkaConsumerActionError,
    KafkaConsumerInitializationError,
    TrafficCounterError,
)


class KafkaConsumerService:
    """Consumes analysis requests and publishes results partition-safely."""

    def __init__(
        self,
        settings: Settings,
        consumerFactory: Callable[..., Any] = AIOKafkaConsumer,
        resultProducer: KafkaProducerService | None = None,
    ) -> None:
        try:
            self._settings = settings
            self._consumerFactory = consumerFactory
            self._consumer: Any | None = None
            self._resultProducer = resultProducer or KafkaProducerService(settings)
        except Exception as e:  # pragma: no cover - diagnostic boundary
            print(f"Error in __init__: {e}")
            raise

    async def start(self) -> None:
        """Start the result producer and request consumer.

        Args:
            None.

        Returns:
            None.

        Raises:
            KafkaProducerInitializationError: If the result producer cannot start.
            KafkaConsumerInitializationError: If the consumer cannot initialize.
        """
        if self._consumer is not None:
            return
        try:
            self._consumer = self._consumerFactory(
                self._settings.kafkaRequestTopic,
                bootstrap_servers=self._settings.kafkaBootstrapServers,
                group_id=self._settings.kafkaConsumerGroup,
                enable_auto_commit=False,
                auto_offset_reset="earliest",
            )
        except Exception as exc:
            raise KafkaConsumerInitializationError() from exc
        await self._resultProducer.start()
        try:
            await self._consumer.start()
        except Exception as exc:
            await self._resultProducer.stop()
            self._consumer = None
            raise KafkaConsumerInitializationError() from exc

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
        await self._resultProducer.stop()

    def _requireStarted(self) -> Any:
        if self._consumer is None:
            raise RuntimeError("Kafka consumer service has not been started.")
        return self._consumer

    async def run(self, stopEvent: asyncio.Event) -> None:
        """Poll and process partitions until shutdown is requested.

        Args:
            stopEvent: Cooperative shutdown signal.

        Returns:
            None.

        Raises:
            RuntimeError: If the service has not started.
            KafkaConsumerActionError: If polling or message processing fails.
        """
        try:
            consumer = self._requireStarted()
            try:
                while not stopEvent.is_set():
                    messages = await consumer.getmany(
                        timeout_ms=self._settings.kafkaConsumerPollTimeoutMs,
                        max_records=self._settings.kafkaConsumerMaxRecords,
                    )
                    if not messages:
                        continue
                    await asyncio.gather(
                        *(
                            self._processPartition(partition, records)
                            for partition, records in messages.items()
                        ),
                    )
            except Exception as exc:
                raise KafkaConsumerActionError() from exc
        except Exception as e:  # pragma: no cover - diagnostic boundary
            print(f"Error in run: {e}")
            raise

    async def _processPartition(
        self,
        partition: TopicPartition,
        records: list[ConsumerRecord],
    ) -> None:
        try:
            consumer = self._requireStarted()
            for message in records:
                await self.processMessage(message.value)
                await consumer.commit(
                    {partition: OffsetAndMetadata(message.offset + 1, "")},
                )
        except Exception as e:  # pragma: no cover - diagnostic boundary
            print(f"Error in _processPartition: {e}")
            raise

    async def processMessage(self, rawValue: bytes) -> KafkaAnalysisResult:
        """Validate, analyze, and publish one request event.

        Args:
            rawValue: Serialized request event.

        Returns:
            The published analysis outcome.

        Raises:
            KafkaError: If the result cannot be published.
        """
        try:
            try:
                request = KafkaAnalysisRequest.model_validate_json(rawValue)
            except (ValidationError, ValueError):
                invalidJsonError = InvalidJsonRequestError()
                result = KafkaAnalysisResult(
                    requestId=uuid4(),
                    filename="unknown",
                    status=ProcessingStatus.FAILED,
                    error=ErrorDetail(
                        code=invalidJsonError.code,
                        message=invalidJsonError.message,
                    ),
                )
            else:
                try:
                    analysis = analyzeTraffic(request.records)
                    result = KafkaAnalysisResult(
                        requestId=request.requestId,
                        filename=request.filename,
                        status=ProcessingStatus.COMPLETED,
                        result=analysis,
                    )
                except TrafficCounterError:
                    invalidRequestError = InvalidRequestBodyError()
                    result = KafkaAnalysisResult(
                        requestId=request.requestId,
                        filename=request.filename,
                        status=ProcessingStatus.FAILED,
                        error=ErrorDetail(
                            code=invalidRequestError.code,
                            message=invalidRequestError.message,
                        ),
                    )

            await self._resultProducer.publishResult(result)
            return result
        except Exception as e:  # pragma: no cover - diagnostic boundary
            print(f"Error in processMessage: {e}")
            raise
