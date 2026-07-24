import asyncio
import os
from pathlib import Path
from uuid import uuid4

import pytest
from aiokafka import AIOKafkaConsumer

from src.config import Settings
from src.schemas.kafka import KafkaAnalysisResult
from src.services.kafka_consumer import KafkaConsumerService
from src.services.kafka_producer import KafkaProducerService

pytestmark = [
    pytest.mark.broker,
    pytest.mark.skipif(
        os.getenv("RUN_KAFKA_TESTS") != "1",
        reason="Set RUN_KAFKA_TESTS=1 with a local Kafka broker to run.",
    ),
]


async def test_real_broker_processes_multiple_files() -> None:
    settings = Settings(kafka_consumer_group=f"test-workers-{uuid4()}")
    result_consumer = AIOKafkaConsumer(
        settings.kafka_result_topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=f"test-results-{uuid4()}",
        auto_offset_reset="latest",
    )
    worker = KafkaConsumerService(settings)
    producer = KafkaProducerService(settings)
    stop_event = asyncio.Event()
    worker_task: asyncio.Task[None] | None = None

    await result_consumer.start()
    await worker.start()
    await producer.start()
    try:
        worker_task = asyncio.create_task(worker.run(stop_event))
        fixture = Path(__file__).parents[1] / "data" / "sample_traffic.txt"
        published = await producer.publish_files([fixture, fixture], concurrency=2)
        expected_ids = {str(item.request_id) for item in published}

        received: set[str] = set()
        async with asyncio.timeout(20):
            while received != expected_ids:
                message = await result_consumer.getone()
                event = KafkaAnalysisResult.model_validate_json(message.value)
                if str(event.request_id) in expected_ids:
                    received.add(str(event.request_id))

        assert received == expected_ids
    finally:
        stop_event.set()
        if worker_task is not None:
            await worker_task
        await producer.stop()
        await worker.stop()
        await result_consumer.stop()
