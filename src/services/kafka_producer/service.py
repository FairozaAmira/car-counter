"""Kafka producer service implementation."""

import asyncio
from collections.abc import Callable
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

from aiokafka import AIOKafkaProducer

from src.config import Settings
from src.schemas.kafka import KafkaAnalysisRequest, KafkaAnalysisResult, KafkaPublishItem
from src.schemas.traffic import ErrorDetail, ProcessingStatus, TrafficRecord
from src.services.parser import parseTrafficText
from src.utils.errors import ErrorCode, KafkaProducerInitializationError, TrafficCounterError
from src.utils.files import readTextFileAsync


class KafkaProducerService:
    """Asynchronous publisher for traffic analysis requests and results."""

    def __init__(
        self,
        settings: Settings,
        producerFactory: Callable[..., Any] = AIOKafkaProducer,
    ) -> None:
        try:
            self._settings = settings
            self._producerFactory = producerFactory
            self._producer: Any | None = None
        except Exception as e:  # pragma: no cover - diagnostic boundary
            print(f"Error in __init__: {e}")
            raise

    async def start(self) -> None:
        """Start and retain the shared Kafka producer.

        Args:
            None.

        Returns:
            None.

        Raises:
            KafkaProducerInitializationError: If the producer cannot connect.
        """
        if self._producer is not None:
            return
        try:
            self._producer = self._producerFactory(
                bootstrap_servers=self._settings.kafkaBootstrapServers,
                enable_idempotence=True,
                request_timeout_ms=self._settings.kafkaRequestTimeoutMs,
            )
            await self._producer.start()
        except Exception as exc:
            self._producer = None
            raise KafkaProducerInitializationError() from exc

    async def stop(self) -> None:
        """Close the shared Kafka producer.

        Args:
            None.

        Returns:
            None.

        Raises:
            KafkaError: If the producer cannot close cleanly.
        """
        if self._producer is not None:
            await self._producer.stop()
            self._producer = None

    def _requireStarted(self) -> Any:
        if self._producer is None:
            raise RuntimeError("Kafka producer service has not been started.")
        return self._producer

    async def publishRecords(
        self,
        filename: str,
        records: list[TrafficRecord],
        requestId: UUID | None = None,
    ) -> UUID:
        """Publish parsed records as a versioned request.

        Args:
            filename: Safe source filename.
            records: Parsed traffic observations.
            requestId: Optional caller-provided event identifier.

        Returns:
            The published request identifier.

        Raises:
            RuntimeError: If the service has not started.
            KafkaError: If Kafka rejects the event.
        """
        try:
            event = KafkaAnalysisRequest(
                requestId=requestId or uuid4(),
                filename=filename,
                records=records,
            )
            producer = self._requireStarted()
            await producer.send_and_wait(
                self._settings.kafkaRequestTopic,
                key=str(event.requestId).encode(),
                value=event.model_dump_json().encode(),
            )
            return event.requestId
        except Exception as e:  # pragma: no cover - diagnostic boundary
            print(f"Error in publishRecords: {e}")
            raise

    async def publishResult(self, event: KafkaAnalysisResult) -> None:
        """Publish a versioned analysis result.

        Args:
            event: Result event to publish.

        Returns:
            None.

        Raises:
            RuntimeError: If the service has not started.
            KafkaError: If Kafka rejects the event.
        """
        try:
            producer = self._requireStarted()
            await producer.send_and_wait(
                self._settings.kafkaResultTopic,
                key=str(event.requestId).encode(),
                value=event.model_dump_json().encode(),
            )
        except Exception as e:  # pragma: no cover - diagnostic boundary
            print(f"Error in publishResult: {e}")
            raise

    async def publishFile(self, path: Path) -> KafkaPublishItem:
        """Parse and publish one local traffic file.

        Args:
            path: Local file path.

        Returns:
            A completed or failed item with a safe error.

        Raises:
            None.
        """
        try:
            try:
                content = await readTextFileAsync(path)
                records = parseTrafficText(content)
                requestId = await self.publishRecords(path.name, records)
                return KafkaPublishItem(
                    filename=path.name,
                    status=ProcessingStatus.COMPLETED,
                    requestId=requestId,
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
                    error=ErrorDetail(code=ErrorCode.FILE_READ_ERROR, message=str(exc)),
                )
            except Exception as exc:
                return KafkaPublishItem(
                    filename=path.name,
                    status=ProcessingStatus.FAILED,
                    error=ErrorDetail(code=ErrorCode.KAFKA_PUBLISH_ERROR, message=str(exc)),
                )
        except Exception as e:  # pragma: no cover - diagnostic boundary
            print(f"Error in publishFile: {e}")
            raise

    async def publishFiles(
        self,
        paths: list[Path],
        concurrency: int,
    ) -> list[KafkaPublishItem]:
        """Publish files concurrently while preserving input order.

        Args:
            paths: Local traffic file paths.
            concurrency: Maximum simultaneous operations.

        Returns:
            Ordered item-level publish outcomes.

        Raises:
            ValueError: If concurrency is not positive.
        """
        try:
            if concurrency < 1:
                raise ValueError("concurrency must be positive.")
            semaphore = asyncio.Semaphore(concurrency)
            results: list[KafkaPublishItem | None] = [None] * len(paths)

            async def publishOne(index: int, path: Path) -> None:
                """Store one publish outcome at its original input index."""
                try:
                    async with semaphore:
                        results[index] = await self.publishFile(path)
                except Exception as e:  # pragma: no cover - diagnostic boundary
                    print(f"Error in publishOne: {e}")
                    raise

            await asyncio.gather(
                *(publishOne(index, path) for index, path in enumerate(paths)),
            )

            return [result for result in results if result is not None]
        except Exception as e:  # pragma: no cover - diagnostic boundary
            print(f"Error in publishFiles: {e}")
            raise
