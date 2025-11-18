from pathlib import Path
import csv
import logging

logger = logging.getLogger("main")


def read_raw_files(directory: Path):
    for file in directory.glob("**/*"):
        if file.suffix.lower() in {".csv", ".json"}:
            yield file


def extract(input_path: Path) -> list[dict]:
    """
    High-level extract step.
    For now, assumes a single CSV file.
    Later you can dispatch on suffix / provider here.
    """
    return extract_csv(input_path)


def extract_csv(path: Path) -> list[dict]:
    """
    Very simple CSV -> list[dict] extractor.
    No types, no validation, just raw records.
    """
    rows: list[dict] = []
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    logger.info(f"Extracted {len(rows)} rows from {path}")
    logger.info(f"First row: {rows[0] if rows else 'No rows found'}")
    return rows
