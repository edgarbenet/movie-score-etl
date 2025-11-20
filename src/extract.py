from pathlib import Path
import csv
import logging
from src.readers import extract_from_path
from src.utils.logutils import get_logger, color, bold, indent, CYAN, GREEN, RED, ICONS

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


def extract_all_providers(input_data: Path) -> list[dict]:
    logger.info(color(f"{ICONS['scan']} Scanning providers in: {input_data}", CYAN))

    all_rows: list[dict] = []

    for provider_path in input_data.iterdir():
        if not provider_path.is_file():
            continue

        if provider_path.suffix.lower() not in {".csv", ".json"}:
            logger.info(indent(f"⚪ Skipping non-data file: {provider_path.name}"))
            continue

        logger.info(indent(f"🔹 Found {bold(provider_path.name)}"))

        try:
            logger.info(indent(color(f"{ICONS['extract']} Extracting rows...", CYAN), 2))
            rows = extract_from_path(provider_path)
            logger.info(
                indent(
                    f"{ICONS['ok']} Extracted {color(str(len(rows)), GREEN)} rows",
                    2,
                )
            )
        except Exception as exc:
            logger.error(indent(color(f"{ICONS['err']} Failed: {exc}", RED), 2))
            continue

        for row in rows:
            row["provider"] = provider_path.stem

        all_rows.extend(rows)

    logger.info(
        indent(
            color(
                f"📊 Total rows extracted: {bold(str(len(all_rows)))}",
                GREEN,
            )
        )
    )
    return all_rows
