from pathlib import Path
import csv
import json


# ---------- EXTRACT ----------

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
    return rows


# ---------- TRANSFORM ----------

def transform(raw_records: list[dict]) -> list[dict]:
    """
    Transform raw provider rows into your canonical schema.
    Keep it stupid-simple for iteration 1.
    """
    transformed: list[dict] = []

    for r in raw_records:
        # Example: normalize a "movie" record
        movie = {
            "movie_id": r.get("movie_id") or r.get("id"),
            "title": (r.get("title") or "").strip(),
            "release_year": r.get("release_year"),
            # add more fields later
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


# ---------- ORCHESTRATION ----------

def run_etl(input_path: str, output_path: str) -> None:
    input_p = Path(input_path)
    output_p = Path(output_path)

    raw = extract(input_p)
    transformed = transform(raw)
    load(transformed, output_p)


if __name__ == "__main__":
    # Hardcoded paths for iteration 1
    # Later: argparse / config file / env vars.
    run_etl("data/raw/movies_provider_a.csv",
            "data/processed/movies_canonical.json")
