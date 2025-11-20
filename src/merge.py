from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List

from src.utils.logutils import get_logger, ICONS, color, indent, CYAN, GREEN

logger = get_logger(__name__)


def _get_latest_canonical_file(base_path: Path) -> Path:
    """
    If base_path is a file and exists -> return it.
    Otherwise, look for the newest movies_canonical_*.json
    in base_path's directory.
    """
    # Case 1: caller passed an existing file -> use it
    if base_path.is_file():
        return base_path

    # Case 2: caller passed a non-existing file path
    # e.g. data/processed/movies_canonical.json
    search_dir = base_path.parent if base_path.suffix else base_path

    candidates = sorted(
        search_dir.glob("movies_canonical_*.json"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )

    if not candidates:
        raise FileNotFoundError(f"No movies_canonical_*.json found in {search_dir}")

    return candidates[0]


def _merge_group(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Very simple merge strategy for a list of records with the same movie_id.

    Rules:
    - Start from an empty dict with just movie_id.
    - For each record:
        - Track provider in `providers`.
        - For each (key, value):
            - Ignore "movie_id" and "provider".
            - Ignore None values.
            - If key not set yet → take the value.
            - If key is set and both values are numeric → keep the max.
            - Otherwise → keep the existing value (first non-None wins).
    """
    if not records:
        return {}

    movie_id = records[0].get("movie_id")
    merged: Dict[str, Any] = {"movie_id": movie_id}
    providers: set[str] = set()

    for record in records:
        if record.get("provider"):
            providers.add(record["provider"])

        for key, value in record.items():
            if key in ("movie_id", "provider"):
                continue
            if value is None:
                continue

            if key not in merged or merged[key] is None:
                merged[key] = value
                continue

            existing = merged[key]

            # If both are numeric, keep the max
            if isinstance(existing, (int, float)) and isinstance(value, (int, float)):
                if value > existing:
                    merged[key] = value
            # Otherwise keep existing (first non-None wins)

    merged["providers"] = sorted(providers)
    return merged


def merge_from_canonical(movies_canonical_path: Path) -> List[Dict[str, Any]]:
    """
    Load canonical data and merge records by movie_id.

    Supports:
    - A direct file path (old style): data/processed/movies_canonical.json
    - Or, if that file does NOT exist, automatically uses the latest:
        data/processed/movies_canonical_*.json

    Also supports two JSON shapes:
    1) Old:
       [
         { ...movie record... },
         { ...movie record... }
       ]

    2) New (wrapped):
       {
         "generated_at": "2025-11-19T20:10:00",
         "records": [
           { ...movie record... },
           { ...movie record... }
         ]
       }
    """
    canonical_path = _get_latest_canonical_file(movies_canonical_path)

    logger.info(
        indent(f"{ICONS.get('merge', '🔀')} Merging canonical records from {canonical_path.name}")
    )

    with canonical_path.open("r", encoding="utf-8") as f:
        raw = json.load(f)

    # Handle wrapper {"generated_at": ..., "records": [...]}
    if isinstance(raw, dict):
        generated_at = raw.get("generated_at")
        records = raw.get("records", [])
        if generated_at:
            logger.info(indent(color(f"Canonical data generated at {generated_at}", CYAN)))
    else:
        records = raw

    groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for record in records:
        movie_id = record.get("movie_id")
        if not movie_id:
            logger.warning(f"{ICONS.get('err', '❌')} Skipping record without movie_id: {record}")
            continue
        groups[movie_id].append(record)

    logger.info(indent(color(f"Grouped into {len(groups)} movies (by movie_id)", CYAN)))

    merged_records: List[Dict[str, Any]] = []
    for movie_id, group in groups.items():
        merged = _merge_group(group)
        merged_records.append(merged)

    logger.info(
        indent(
            color(
                f"{ICONS.get('ok', '✅')} Merge completed: produced {len(merged_records)} merged movies",
                GREEN,
            )
        )
    )

    return merged_records
