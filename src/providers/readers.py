from pathlib import Path
import csv, json

from utils.logutils import (
    get_logger, color, bold, indent, CYAN, GREEN, YELLOW, RED, ICONS
)

logger = get_logger(__name__)


def extract_csv(path: Path) -> list[dict]:
    logger.info(indent(color(
        f"{ICONS['dispatch']} Reading CSV file...",
        CYAN,
    ), 2))
    rows = []
    with path.open(newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            rows.append(r)
    return rows


def extract_json(path: Path) -> list[dict]:
    logger.info(indent(color(
        f"{ICONS['dispatch']} Reading JSON file...",
        CYAN,
    ), 2))

    with path.open(encoding="utf-8") as f:
        data = json.load(f)

    if isinstance(data, list):
        rows = data
    else:
        rows = next((v for v in data.values() if isinstance(v, list)), None)

    if rows is None:
        logger.error(color(f"{ICONS['err']} No list found in JSON", RED))
        raise ValueError(f"Invalid JSON structure in {path}")
    return rows


def extract_from_path(path: Path) -> list[dict]:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return extract_csv(path)
    if suffix == ".json":
        return extract_json(path)

    logger.error(color(f"{ICONS['err']} Unsupported file type {suffix}", RED))
    raise ValueError(f"Unsupported file type: {suffix}")
