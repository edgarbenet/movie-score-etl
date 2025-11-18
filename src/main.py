from pathlib import Path
import logging

# Import your pipeline steps
from extract import extract
from transform import transform
from load import load

# Project root = one level above src/main.py
BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DATA_PATH = BASE_DIR / "data/raw/provider1.csv"
PROCESSED_DATA_PATH = BASE_DIR / "data/processed/movies_canonical.json"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
)
logger = logging.getLogger("main")


# ---------- ORCHESTRATION ----------

def run_etl(input_path: str, output_path: str) -> None:
    input_data = Path(input_path)
    output_data = Path(output_path)

    raw = extract(input_data)
    transformed = transform(raw)
    load(transformed, output_data)


if __name__ == "__main__":
    # Hardcoded paths for iteration 1
    # Later: argparse / config file / env vars.
    run_etl(RAW_DATA_PATH, PROCESSED_DATA_PATH)  # type: ignore
