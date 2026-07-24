import argparse
from pathlib import Path

from src.services.traffic import TrafficAnalysisService


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Analyze an AIPS traffic counter file.")
    parser.add_argument("file", type=Path, help="Path to the traffic counter text file")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    content = args.file.read_text(encoding="utf-8")
    result = TrafficAnalysisService().analyze_text(content)
    print(result.model_dump_json(indent=2))


if __name__ == "__main__":
    main()
