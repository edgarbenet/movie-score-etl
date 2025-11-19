from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Tuple

from src.utils.logutils import (
    get_logger,
    ICONS,
    CYAN,
    YELLOW,
    GREEN,
    color,
    indent,
    bold,
)

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# 1) Generic merge policies (fallback)
#    - "first": take the first non-None value
#    - "max":   take the largest numeric value (fallback to first non-None)
# ---------------------------------------------------------------------------
MERGE_POLICIES: Dict[str, str] = {
    "movie_title": "first",
    "release_year": "first",

    # Scores
    "critic_score": "first",            # provider1 wins via FIELD_PROVIDER_PRIORITY
    "top_critic_score": "max",
    "audience_avg_score": "first",      # provider2 wins via FIELD_PROVIDER_PRIORITY

    # Counts
    "total_critic_ratings": "max",
    "total_audience_ratings": "max",

    # Financials
    "domestic_box_office_gross": "max",   # provider3_domestic > provider2 > max fallback
    "box_office_gross_usd": "first",      # provider3_international only; otherwise first
    "production_budget_usd": "first",     # provider3_financials only; otherwise first
    "marketing_spend_usd": "first",       # provider3_financials only; otherwise first
}


# ---------------------------------------------------------------------------
# 2) Field → provider priority rules
# ---------------------------------------------------------------------------
FIELD_PROVIDER_PRIORITY: Dict[str, List[str]] = {
    # domestic_box_office_gross:
    #   prefer provider3_domestic.domestic_box_office_gross
    #   fallback to provider2.domestic_box_office_gross
    "domestic_box_office_gross": [
        "provider3_domestic",
        "provider2",
    ],

    # box_office_gross_usd:
    #   take from provider3_international.box_office_gross_usd only
    "box_office_gross_usd": [
        "provider3_international",
    ],

    # audience_avg_score: from provider2
    "audience_avg_score": [
        "provider2",
    ],

    # critic_score: from provider1
    "critic_score": [
        "provider1",
    ],

    # production_budget_usd, marketing_spend_usd: from provider3_financials
    "production_budget_usd": [
        "provider3_financials",
    ],
    "marketing_spend_usd": [
        "provider3_financials",
    ],
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _merge_values_generic(values: List[Any], policy: str) -> Any:
    """Apply a simple generic merge policy to a list of values."""
    non_null = [v for v in values if v is not None]
    if not non_null:
        return None

    if policy == "first":
        return non_null[0]

    if policy == "max":
        numeric = [v for v in non_null if isinstance(v, (int, float))]
        if numeric:
            return max(numeric)
        return non_null[0]

    # Default fallback
    return non_null[0]


def _log_decision(
    movie_id: str,
    key: str,
    chosen_value: Any,
    chosen_provider: str | None,
    reason: str,
    candidates: List[Tuple[str | None, Any]],
) -> None:
    """
    Log one compact debug line per field.

    - Always logs which provider/value won and which rule.
    - Only lists candidates if there is a real choice (>= 2 distinct values).
    """
    if chosen_value is None:
        return

    distinct_values = {v for _, v in candidates if v is not None}
    base_msg = (
        f"{ICONS.get('merge', '🔀')} "
        f"[{movie_id}] {bold(key)} ← {bold(str(chosen_value))} "
        f"from {bold(chosen_provider or 'unknown')} "
        f"({reason})"
    )

    if len(distinct_values) > 1:
        cand_str = ", ".join(
            f"{p or 'unknown'}={v}" for p, v in candidates if v is not None
        )
        msg = f"{base_msg}; candidates: {cand_str}"
    else:
        msg = base_msg

    logger.debug(indent(color(msg, YELLOW), 2))


def _merge_value_for_field(
    movie_id: str,
    key: str,
    records: List[Dict[str, Any]],
) -> Any:
    """
    Merge a single field across all records for a movie.

    1) If FIELD_PROVIDER_PRIORITY has this field:
       - Try preferred providers in order, return first non-None.
    2) Otherwise, or if none of the preferred providers has a value:
       - Use MERGE_POLICIES (generic "first"/"max" policy).
    """
    # All non-null candidates for this field
    candidates: List[Tuple[str | None, Any]] = [
        (r.get("provider"), r.get(key))
        for r in records
        if r.get(key) is not None
    ]

    if not candidates:
        # Nothing at all for this field
        return None

    chosen_value: Any
    chosen_provider: str | None = None
    reason: str

    # 1) Provider-specific precedence
    preferred_providers = FIELD_PROVIDER_PRIORITY.get(key)
    if preferred_providers:
        for provider_name in preferred_providers:
            for prov, value in candidates:
                if prov == provider_name and value is not None:
                    chosen_value = value
                    chosen_provider = provider_name
                    reason = f"provider priority {preferred_providers}"
                    _log_decision(
                        movie_id,
                        key,
                        chosen_value,
                        chosen_provider,
                        reason,
                        candidates,
                    )
                    return chosen_value
        # Fall through to generic policy if none matched

    # 2) Generic merge policy
    policy = MERGE_POLICIES.get(key, "first")
    chosen_value = _merge_values_generic([v for _, v in candidates], policy)
    if chosen_value is None:
        return None

    # Find which provider supplied the chosen_value (first match)
    for prov, value in candidates:
        if value == chosen_value:
            chosen_provider = prov
            break

    reason = f"policy={policy}"
    _log_decision(
        movie_id,
        key,
        chosen_value,
        chosen_provider,
        reason,
        candidates,
    )
    return chosen_value


def _merge_group(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Merge all provider records for a single movie_id into one dict."""
    if not records:
        return {}

    merged: Dict[str, Any] = {}

    movie_id = records[0].get("movie_id")
    movie_title = records[0].get("movie_title") or "Unknown title"
    merged["movie_id"] = movie_id

    logger.info(
        indent(
            color(
                f"{ICONS.get('merge', '🔀')} Merging {bold(str(len(records)))} records "
                f"for movie_id={movie_id} ('{movie_title}')",
                CYAN,
            ),
            1,
        )
    )

    # Collect all keys across records
    all_keys = set().union(*(r.keys() for r in records))
    # These are handled separately
    all_keys.discard("movie_id")
    all_keys.discard("provider")

    for key in sorted(all_keys):
        merged[key] = _merge_value_for_field(movie_id, key, records) # type: ignore

    # Provenance: which providers contributed any record for this movie_id
    providers = sorted({r["provider"] for r in records if r.get("provider")})
    merged["providers"] = providers

    logger.info(
        indent(
            color(
                f"{ICONS.get('ok', '✅')} Merged movie "
                f"{bold(str(merged.get('movie_title')))} "
                f"({movie_id}) from: {', '.join(providers)}",
                GREEN,
            ),
            2,
        )
    )

    return merged


def merge_from_canonical(movies_canonical_path: Path) -> List[Dict[str, Any]]:
    """
    Load movies_canonical.json and merge records by movie_id.

    - Input: JSON file with a list[dict], each record including:
        - movie_id
        - provider
        - canonical fields (scores, counts, financials...)
    - Output: list[dict] with one record per movie_id and:
        - merged fields according to FIELD_PROVIDER_PRIORITY + MERGE_POLICIES
        - providers: sorted list of provider names that contributed
    """
    logger.info(
        indent(
            color(
                f"{ICONS.get('merge', '🔀')} Merging canonical records from: "
                f"{movies_canonical_path}",
                CYAN,
            ),
            1,
        )
    )

    with movies_canonical_path.open("r", encoding="utf-8") as f:
        records: List[Dict[str, Any]] = json.load(f)

    logger.info(
        indent(
            f"Found {bold(str(len(records)))} canonical records before grouping",
            1,
        )
    )

    groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for record in records:
        movie_id = record.get("movie_id")
        if not movie_id:
            logger.warning(
                indent(
                    color(
                        f"{ICONS.get('err', '❌')} Skipping record without movie_id: {record}",
                        YELLOW,
                    ),
                    1,
                )
            )
            continue
        groups[movie_id].append(record)

    logger.info(
        indent(
            f"Grouped into {bold(str(len(groups)))} movies (by movie_id)",
            1,
        )
    )

    merged_records: List[Dict[str, Any]] = []
    for movie_id, group in groups.items():
        merged_records.append(_merge_group(group))

    logger.info(
        indent(
            color(
                f"{ICONS.get('ok', '✅')} Merge completed: produced "
                f"{bold(str(len(merged_records)))} merged movies",
                GREEN,
            ),
            1,
        )
    )

    return merged_records
