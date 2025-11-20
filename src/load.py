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
    Only reshapes data — does not change meaning.
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

def build_output_filename(output_dir: Path, prefix: str = "movies_merged",) -> Path:
    """
    Always generate a filename with the current date:
        movies_merged_2025-11-20.json

    If it exists → overwrite it.
    """
    date_str = datetime.now().strftime("%Y-%m-%d")
    filename = f"{prefix}_{date_str}.json"
    return output_dir / filename

def write_canonical(records: list[dict], output_path: Path) -> None:
    """
    Write canonical records for the day.
    Always overwrites if run again on the same date.
    """
    final_path = build_output_filename(
        output_dir=output_path.parent,
        prefix="movies_canonical",
    )

    wrapper = {
        "generated_at": datetime.now().isoformat(),
        "records": records,
    }

    with final_path.open("w", encoding="utf-8") as f:
        json.dump(wrapper, f, indent=2, ensure_ascii=False)

    logger.info(indent(color(
        f"{ICONS['ok']} Wrote canonical data → {final_path.name}",
        GREEN
    )))

def load(records: list[dict], output_path: Path) -> None:
    """
    Write merged records for the day.
    Always overwrites if run again on the same date.
    """
    final_path = build_output_filename(
        output_dir=output_path.parent,
        prefix="movies_merged",
    )

    logger.info(indent(color(
        f"{ICONS['load']} Writing {bold(str(len(records)))} records → {final_path.name}",
        CYAN,
    )))

    shaped = [to_presentation_model(r) for r in records]

    wrapper = {
        "generated_at": datetime.now().isoformat(),
        "records": shaped,
    }

    with final_path.open("w", encoding="utf-8") as f:
        json.dump(wrapper, f, indent=2, ensure_ascii=False)

    logger.info(indent(color(f"{ICONS['ok']} File written", GREEN)))
