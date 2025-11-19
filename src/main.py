from pathlib import Path
import logging
import csv, json

# Import your pipeline steps
from providers.readers import extract_from_path
from transform import transform
from load import load
# Project root = one level above src/main.py
BASE_DIR = Path(__file__).resolve().parent.parent

RAW_DATA_DIR = BASE_DIR / "data/raw"
PROCESSED_DATA_PATH = BASE_DIR / "data/processed/movies_canonical.json"
MERGED_DATA_PATH = BASE_DIR / "data/processed/movies_canonical_merged.json"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
)
logger = logging.getLogger("main")

def extract_all_providers(input_data: Path) -> list[dict]:
    all_rows: list[dict] = []

    for provider_path in input_data.iterdir():
        if not provider_path.is_file():
            continue
        if provider_path.suffix.lower() not in {".csv", ".json"}:
            continue

        provider_name = provider_path.stem  # "provider1", "provider2", ...
        logger.info(f"Loading provider: {provider_path.name}")

        try:
            rows = extract_from_path(provider_path)
            logger.info(f"-> Extracted {len(rows)} rows from {provider_path.name}")
        except Exception as exc:
            logger.error(f"Failed to load provider {provider_name} from {provider_path:NAME}: {exc}")
            # optionally continue to the next provider
            continue

        for row in rows:
            row["_provider"] = provider_name

        all_rows.extend(rows)

    logger.info(f"Total rows from all providers: {len(all_rows)}")
    return all_rows


# ---------- ORCHESTRATION ----------

def run_etl() -> None:

    raw = extract_all_providers(RAW_DATA_DIR)

    # 1) Middle step: transformed (pre-merge)
    transformed = transform(raw)
    load(transformed, PROCESSED_DATA_PATH)
    logger.info(f"Wrote canonical movies to {PROCESSED_DATA_PATH}")

    # 2) Final step: merged per movie_id
    #merged = merge_movies(transformed)
    #load(merged, MERGED_DATA_PATH)
    #logger.info(f"Wrote merged movies to {MERGED_DATA_PATH}")


if __name__ == "__main__":
    # Hardcoded paths for iteration 1
    # Later: argparse / config file / env vars.
    run_etl()  # type: ignore
