# src/readers.py
from pathlib import Path
import csv
import json
import logging

logger = logging.getLogger("main")


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


def extract_json(path: Path) -> list[dict]:
    """
    Simple JSON -> list[dict] extractor.

    Assumes either:
    - a list[dict]
    - or a dict that contains a list[dict] as one of its values.
    """
    with path.open(encoding="utf-8") as f:
        data = json.load(f)

    if isinstance(data, list):
        rows = data
    elif isinstance(data, dict):
        # pick the first list value
        for value in data.values():
            if isinstance(value, list):
                rows = value
                break
        else:
            raise ValueError(f"JSON in {path} does not contain a list of records")
    else:
        raise ValueError(f"Unsupported JSON structure in {path}")

    logger.info(f"Extracted {len(rows)} rows from {path}")
    logger.info(f"First row: {rows[0] if rows else 'No rows found'}")
    return rows


def extract_from_path(path: Path) -> list[dict]:
    """
    Dispatch to the right extractor based on file suffix.
    """
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return extract_csv(path)
    if suffix == ".json":
        return extract_json(path)
    raise ValueError(f"Unsupported file type for {path}")
