from pathlib import Path
import csv
import json
import logging
import uuid

# Project root = one level above src/main.py
BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DATA_PATH = BASE_DIR / "data/raw/provider1.csv"
PROCESSED_DATA_PATH = BASE_DIR / "data/processed/movies_canonical.json"

FIELD_MAP = {
    "movie_title": ["movie_title", "title", "name"],
    "release_year": ["release_year", "year"],
    "critic_score": ["critic_score_percentage", "critic_score"],
    "top_critic_score": ["top_critic_score"],
    "total_critic_ratings": ["total_critic_reviews_counted"],
}

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
)
logger = logging.getLogger("main")
# ---------- EXTRACT ----------

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


# ---------- TRANSFORM ----------
#apply this in the transform step
def generate_movie_id(title: str, release_year: int | None) -> str:
    if release_year is None:
        raw = f"{title.lower().strip()}"
    else:
        raw = f"{title.lower().strip()}_{release_year}"
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, raw))[:8]

def get_first(record: dict, keys: list[str], default=None):
    """Return the first non-empty value among the given keys."""
    for key in keys:
        value = record.get(key)
        if value not in ("", None):
            return value
    return default

def transform(data_raw: list[dict]) -> list[dict]:
    """
    Transform raw provider rows into your canonical schema.
    Keep it stupid-simple for iteration 1.
    """
    transformed: list[dict] = []

    for row in data_raw:
        # title via FIELD_MAP
        title = get_first(row, FIELD_MAP["movie_title"], default="") or ""
        title = title.strip()

        # release_year via FIELD_MAP
        release_year_raw = get_first(row, FIELD_MAP["release_year"])
        release_year = int(release_year_raw) if release_year_raw not in (None, "") else None

        # critic_score: from percentage like "87" → 0.87
        critic_score_ratio_raw = get_first(row, FIELD_MAP["critic_score"])
        if critic_score_ratio_raw not in (None, ""):
            critic_score = float(critic_score_ratio_raw) / 100
        else:
            critic_score = None

        # top_critic_score: e.g. "8.1"
        top_critic_score_raw = get_first(row, FIELD_MAP["top_critic_score"])
        top_critic_score = (
            float(top_critic_score_raw)
            if top_critic_score_raw not in (None, "")
            else None
        )

        # total_critic_ratings
        total_critic_ratings_raw = get_first(row, FIELD_MAP["total_critic_ratings"])
        total_critic_ratings = (
            int(total_critic_ratings_raw)
            if total_critic_ratings_raw not in (None, "")
            else None
        )
        
        movie_id = generate_movie_id(title, release_year)

        movie = {
            "movie_id": movie_id,
            "movie_title": title,
            "release_year": release_year,
            "critic_score": critic_score,
            "top_critic_score": top_critic_score,
            "total_critic_ratings": total_critic_ratings,
            # other fields from your Pydantic model stay out / None for now
        }
        transformed.append(movie)

    return transformed

# ---------- LOAD ----------

def load(records: list[dict], output_path: Path) -> None:
    """
    First iteration: dump to a JSON file.
    Later: DB, Parquet, API, etc.
    """
    with output_path.open("w", encoding="utf-8") as f:
        json.dump(records, f, indent=2, ensure_ascii=False)
    logger.info(f"Wrote {len(records)} records to {output_path}")


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