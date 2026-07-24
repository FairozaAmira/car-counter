import argparse
import asyncio
from pathlib import Path

from src.config import get_settings
from src.services.kafka_producer import KafkaProducerService


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Publish traffic files to Kafka.")
    parser.add_argument("files", nargs="+", type=Path, help="Traffic counter text files")
    return parser


async def run(paths: list[Path]) -> int:
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
    args = build_parser().parse_args()
    raise SystemExit(asyncio.run(run(args.files)))


if __name__ == "__main__":
    main()
