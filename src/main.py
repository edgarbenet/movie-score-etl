import sys
from pathlib import Path
from src.readers import extract_from_path
from src.transform import transform
from src.merge import merge_from_canonical
from src.load import write_canonical, load

from src.utils.logutils import (
    get_logger, color, bold, indent,
    CYAN, GREEN, YELLOW, MAGENTA, RED, ICONS
)

logger = get_logger("main")


BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DATA_DIR = BASE_DIR / "data/raw"
CANONICAL_DATA_PATH = BASE_DIR / "data/processed/movies_canonical.json"
MERGED_DATA_PATH = BASE_DIR / "data/processed/movies_merged.json"


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
        indent(color(
            f"📊 Total rows extracted: {bold(str(len(all_rows)))}",
            GREEN,
        ))
    )
    return all_rows


def run_etl():

    logger.info(color(" [1/4] Extract", YELLOW))
    all_raw_rows = extract_all_providers(RAW_DATA_DIR)

    logger.info(color(" [2/4] Transform", YELLOW))
    canonical_records = transform(all_raw_rows)
    write_canonical(canonical_records, CANONICAL_DATA_PATH)

    logger.info(color(" [3/4] Merge", YELLOW))
    merged_records = merge_from_canonical(CANONICAL_DATA_PATH)


    logger.info(color(" [4/4] Load", YELLOW))
    load(merged_records, MERGED_DATA_PATH)

    logger.info(color(f"\n{ICONS['result']}  OUTPUT: {MERGED_DATA_PATH.name}", MAGENTA))
    

if __name__ == "__main__":

    logger.info(color(bold(f"{ICONS['movie']} Starting ETL pipeline..."), MAGENTA))   

    try:
        run_etl()
        
    except KeyError as e:
        logger.error(color(f"{ICONS['err']} ETL failed: {e}", RED))
        sys.exit(1)
    except Exception:
        # Log full traceback to logs, but still crash
        logger.exception(color(f"{ICONS['err']} Unexpected ETL error", RED))
        raise