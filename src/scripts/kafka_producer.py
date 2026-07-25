import argparse
import asyncio
from pathlib import Path

from src.config import get_settings
from src.services.kafka_producer import KafkaProducerService


def build_parser() -> argparse.ArgumentParser:
    """Build the Kafka producer CLI parser.

    Args:
        None.

    Returns:
        The configured argument parser.

    Raises:
        None.
    """
    parser = argparse.ArgumentParser(description="Publish traffic files to Kafka.")
    parser.add_argument("files", nargs="+", type=Path, help="Traffic counter text files")
    return parser


async def run(paths: list[Path]) -> int:
    """Publish files and return a process exit code.

    Args:
        paths: Local traffic files to publish.

    Returns:
        Zero when every publish succeeds, otherwise one.

    Raises:
        KafkaError: If producer startup or shutdown fails.
    """
    settings = get_settings()
    service = KafkaProducerService(settings)
    await service.start()
    try:
        results = await service.publish_files(paths, settings.batch_concurrency)
    finally:
        await service.stop()

    for result in results:
        print(result.model_dump_json())
    return 1 if any(result.status == "failed" for result in results) else 0


def main() -> None:
    """Run the async Kafka producer command.

    Args:
        None.

    Returns:
        None.

    Raises:
        SystemExit: Always, using the producer result as the exit code.
    """
    args = build_parser().parse_args()
    raise SystemExit(asyncio.run(run(args.files)))


if __name__ == "__main__":  # pragma: no cover - exercised through main()
    main()
