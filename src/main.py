from pathlib import Path
from providers.readers import extract_from_path
from transform import transform
from load import load

from utils.logutils import (
    get_logger, color, bold, indent,
    CYAN, GREEN, YELLOW, MAGENTA, RED, ICONS
)

logger = get_logger("main")


BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DATA_DIR = BASE_DIR / "data/raw"
PROCESSED_DATA_PATH = BASE_DIR / "data/processed/movies_canonical.json"


def extract_all_providers(input_data: Path) -> list[dict]:
    logger.info(color(f"{ICONS['scan']} Scanning providers in: {input_data}", CYAN))

    all_rows: list[dict] = []

    for provider_path in input_data.iterdir():
        if not provider_path.is_file():
            continue

        if provider_path.suffix.lower() not in {".csv", ".json"}:
            logger.info(indent(f"⚪ Skipping non-data file: {provider_path.name}"))
            continue

        logger.info(indent(f"🔹 Found provider file: {bold(provider_path.name)}"))

        try:
            logger.info(indent("📥 Extracting rows...", 2))
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
            row["_provider"] = provider_path.stem

        all_rows.extend(rows)

    logger.info(
        color(
            f"📊 Total rows extracted: {bold(str(len(all_rows)))}",
            CYAN,
        )
    )
    return all_rows


def run_etl():
    logger.info(color(bold("🎬 Starting ETL pipeline"), MAGENTA))

    logger.info(color(" [1/4] Extract", YELLOW))
    raw = extract_all_providers(RAW_DATA_DIR)

    logger.info(color(" [2/4] Transform", YELLOW))
    transformed = transform(raw)

    logger.info(color(" [3/4] Merge", YELLOW))
    # keep your existing merge logic, even if commented out
    # merged = merge_movies(transformed)

    logger.info(color(" [4/4] Load", YELLOW))
    load(transformed, PROCESSED_DATA_PATH)
    #load(merged, PROCESSED_DATA_PATH)


    logger.info(color(f"💾 Output: {PROCESSED_DATA_PATH.name}", GREEN))


if __name__ == "__main__":
    run_etl()
