import argparse
import asyncio
from pathlib import Path

from src.config import getSettings
from src.services.kafka_producer import KafkaProducerService


def buildParser() -> argparse.ArgumentParser:
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
    try:
        settings = getSettings()
        service = KafkaProducerService(settings)
        await service.start()
        try:
            results = await service.publishFiles(paths, settings.batchConcurrency)
        finally:
            await service.stop()

        for result in results:
            print(result.model_dump_json())
        return 1 if any(result.status == "failed" for result in results) else 0
    except Exception as e:  # pragma: no cover - diagnostic boundary
        print(f"Error in run: {e}")
        raise


def main() -> None:
    """Run the async Kafka producer command.

    Args:
        None.

    Returns:
        None.

    Raises:
        SystemExit: Always, using the producer result as the exit code.
    """
    args = buildParser().parse_args()
    raise SystemExit(asyncio.run(run(args.files)))


if __name__ == "__main__":  # pragma: no cover - exercised through main()
    main()
