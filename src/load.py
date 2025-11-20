from pathlib import Path
from datetime import datetime
import json

from src.utils.logutils import (
    get_logger, color, bold, indent, CYAN, GREEN, ICONS
)

logger = get_logger(__name__)

def to_presentation_model(record: dict) -> dict:
    """
    Convert a flat canonical movie record into a grouped, more readable structure.
    This does NOT change internal canonical fields — only the final output format.
    """

    return {
        "movie_id": record.get("movie_id"),
        "movie_title": record.get("movie_title"),
        "release_year": record.get("release_year"),
        "ratings": {
            "critic": {
                "score": record.get("critic_score"),
                "top_score": record.get("top_critic_score"),
                "total_ratings": record.get("total_critic_ratings"),
            },
            "audience": {
                "score": record.get("audience_avg_score"),
                "total_ratings": record.get("total_audience_ratings"),
            },
        },
        "financials": {
            "domestic_box_office_usd": record.get("domestic_box_office_gross"),
            "worldwide_box_office_usd": record.get("box_office_gross_usd"),
            "production_budget_usd": record.get("production_budget_usd"),
            "marketing_spend_usd": record.get("marketing_spend_usd"),
        },
        "providers": record.get("providers", []),
    }

def build_output_filename(output_dir: Path, prefix: str = "movies_merged") -> Path:
    """
    Generate a filename like:
      movies_merged_2025-11-20.json
    If it already exists, create:
      movies_merged_2025-11-20-2.json
      movies_merged_2025-11-20-3.json
    etc.
    """
    date_str = datetime.now().strftime("%Y-%m-%d")
    base_name = f"{prefix}_{date_str}.json"
    candidate = output_dir / base_name

    # If no collision → return it
    if not candidate.exists():
        return candidate

    # Else add -2, -3, ...
    counter = 2
    while True:
        name = f"{prefix}_{date_str}-{counter}.json"
        candidate = output_dir / name
        if not candidate.exists():
            return candidate
        counter += 1

def write_canonical(records: list[dict], output_path: Path) -> None:
    """
    Write flat canonical records (no grouping, used as input for merge).
    """
    with output_path.open("w", encoding="utf-8") as f:
        json.dump(records, f, indent=2, ensure_ascii=False)

def load(records: list[dict], output_path: Path) -> None:
    """
    Write processed records to the final output.
    """
    final_path = build_output_filename(output_path.parent)
    
    logger.info(indent(color(
        f"{ICONS['load']} Writing {bold(str(len(records)))} records → {final_path.name}",
        CYAN,
    )))
    
    shaped = [to_presentation_model(r) for r in records]

    with final_path.open("w", encoding="utf-8") as f:
        json.dump(shaped, f, indent=2, ensure_ascii=False)

    logger.info(indent(color(f"{ICONS['ok']} File written", GREEN)))
