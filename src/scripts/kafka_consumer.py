import asyncio
import signal

from src.config import get_settings
from src.services.kafka_consumer import KafkaConsumerService


async def run() -> None:
    """Run the consumer until an operating-system stop signal arrives.

    Args:
        None.

    Returns:
        None.

    Raises:
        KafkaError: If consumer startup, processing, or shutdown fails.
    """
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signal_name in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(signal_name, stop_event.set)

    service = KafkaConsumerService(get_settings())
    await service.start()
    try:
        await service.run(stop_event)
    finally:
        await service.stop()


def main() -> None:
    """Run the asynchronous Kafka consumer worker.

    Args:
        None.

    Returns:
        None.

    Raises:
        RuntimeError: If an event loop cannot be created.
    """
    asyncio.run(run())


if __name__ == "__main__":  # pragma: no cover - exercised through main()
    main()
