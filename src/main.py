import sys, os
from pathlib import Path
from src.extract import extract_all_providers
from src.transform import transform
from src.merge import merge_from_canonical
from src.load import write_canonical, load

from src.utils.logutils import (
    get_logger, color, bold, indent,
    CYAN, GREEN, YELLOW, MAGENTA, RED, ICONS
)

logger = get_logger("main")


BASE_DIR = Path(__file__).resolve().parent.parent

RAW_FOLDER = os.getenv("RAW_FOLDER", "raw")   # default = raw
RAW_DATA_PATH = BASE_DIR / f"data/{RAW_FOLDER}"
CANONICAL_DATA_PATH = BASE_DIR / "data/processed/movies_canonical.json"
MERGED_DATA_PATH = BASE_DIR / "data/processed/movies_merged.json"


def run_etl():

    logger.info(color(" [1/4] Extract", YELLOW))
    all_raw_rows = extract_all_providers(RAW_DATA_PATH)

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