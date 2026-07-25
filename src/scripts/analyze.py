import argparse
from pathlib import Path

from src.services.traffic import TrafficAnalysisService
from src.utils.files import read_text_file


def build_parser() -> argparse.ArgumentParser:
    """Build the traffic analysis CLI parser.

    Args:
        None.

    Returns:
        The configured argument parser.

    Raises:
        None.
    """
    parser = argparse.ArgumentParser(description="Analyze an AIPS traffic counter file.")
    parser.add_argument("file", type=Path, help="Path to the traffic counter text file")
    return parser


def main() -> None:
    """Analyze a CLI-selected file and print structured JSON.

    Args:
        None.

    Returns:
        None.

    Raises:
        OSError: If the selected file cannot be read.
        TrafficCounterError: If its traffic records are invalid.
    """
    args = build_parser().parse_args()
    content = read_text_file(args.file)
    result = TrafficAnalysisService().analyze_text(content)
    print(result.model_dump_json(indent=2))


if __name__ == "__main__":  # pragma: no cover - exercised through main()
    main()
