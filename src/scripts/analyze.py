import argparse
from pathlib import Path

from src.services.traffic import TrafficAnalysisService
from src.utils.files import readTextFile


def buildParser() -> argparse.ArgumentParser:
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
    args = buildParser().parse_args()
    content = readTextFile(args.file)
    result = TrafficAnalysisService().analyzeText(content)
    print(result.model_dump_json(indent=2))


if __name__ == "__main__":  # pragma: no cover - exercised through main()
    main()
