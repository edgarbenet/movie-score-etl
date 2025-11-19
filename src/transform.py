import uuid

from src.utils.logutils import (
    get_logger, color, bold, indent,
    CYAN, GREEN, YELLOW, RED, ICONS
)

logger = get_logger(__name__)


FIELD_MAP = {
    # --- Identity ---
    "movie_title": ["movie_title", "title", "name", "film_name"],
    "release_year": ["release_year", "year", "year_of_release"],

    # --- Provider1 (critics) ---
    "critic_score": ["critic_score_percentage", "critic_score"],
    "top_critic_score": ["top_critic_score"],
    "total_critic_ratings": ["total_critic_reviews_counted"],

    # --- Provider2 (audience + domestic) ---
    "audience_avg_score": ["audience_average_score"],
    "total_audience_ratings": ["total_audience_ratings"],
    "domestic_box_office_gross": ["domestic_box_office_gross"],

    # --- Generic box office (e.g. provider3_international) ---
    "box_office_gross_usd": ["box_office_gross_usd"],

    # --- Provider3 (financials) ---
    "production_budget_usd": ["production_budget_usd", "production_budget"],
    "marketing_spend_usd": ["marketing_spend_usd", "marketing_spend"],
}


def generate_movie_id(movie_title: str, release_year: int | None) -> str:
    if release_year is None:
        raw = f"{movie_title.lower().strip()}"
    else:
        raw = f"{movie_title.lower().strip()}_{release_year}"
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, raw))[:8]


def get_first(record: dict, keys: list[str], default=None):
    """Return the first non-empty value among the given keys."""
    for key in keys:
        value = record.get(key)
        if value not in ("", None):
            return value
    return default


def transform(data_raw: list[dict]) -> list[dict]:
    logger.info(indent(color(
        f"{ICONS['transform']} Transforming {bold(str(len(data_raw)))} records...",
        CYAN
    )))

    transformed: list[dict] = []

    # Show a couple of sample inputs
    for i, row in enumerate(data_raw[:2], 1):
        logger.debug(indent(f"🧪 Sample raw #{i}: {row}", 2))

    for row in data_raw:
        provider_name = (row.get("provider") or "").lower()

        # -----------------------
        # FIELD TRANSFORMATIONS
        # -----------------------

        movie_title_raw = get_first(row, FIELD_MAP["movie_title"], default="") or ""
        movie_title = movie_title_raw.strip()
        if not movie_title:
            raise KeyError(
                f"{ICONS['err']} Missing required field 'movie_title' "
                f"for provider={provider_name} row={row}"
            )

        release_year_raw = get_first(row, FIELD_MAP["release_year"])
        release_year = int(release_year_raw) if release_year_raw not in (None, "") else None
        if release_year is None:
            raise KeyError(
                f"{ICONS['err']} Missing required field 'release_year' "
                f"for provider={provider_name} row={row}"
            )

        # critic_score -> float in [0,1] from percentage
        critic_score_ratio_raw = get_first(row, FIELD_MAP.get("critic_score", []))
        if critic_score_ratio_raw not in (None, ""):
            critic_score = float(critic_score_ratio_raw) / 100
        else:
            critic_score = None

        # top_critic_score
        top_critic_score_raw = get_first(row, FIELD_MAP.get("top_critic_score", []))
        top_critic_score = (
            float(top_critic_score_raw)
            if top_critic_score_raw not in (None, "")
            else None
        )

        # total_critic_ratings
        total_critic_ratings_raw = get_first(row, FIELD_MAP.get("total_critic_ratings", []))
        total_critic_ratings = (
            int(total_critic_ratings_raw)
            if total_critic_ratings_raw not in (None, "")
            else None
        )

        # Provider2 audience score
        audience_avg_score_raw = get_first(row, FIELD_MAP.get("audience_avg_score", []))
        audience_avg_score = (
            float(audience_avg_score_raw)
            if audience_avg_score_raw not in (None, "")
            else None
        )

        total_audience_ratings_raw = get_first(row, FIELD_MAP.get("total_audience_ratings", []))
        total_audience_ratings = (
            int(total_audience_ratings_raw)
            if total_audience_ratings_raw not in (None, "")
            else None
        )

        # --- Box office fields ---

        # 1) domestic_box_office_gross
        domestic_box_office_gross = None

        # from provider2
        domestic_box_office_gross_raw = get_first(
            row,
            FIELD_MAP.get("domestic_box_office_gross", []),
        )
        if domestic_box_office_gross_raw not in (None, ""):
            domestic_box_office_gross = int(domestic_box_office_gross_raw)

        # from provider3_domestic: box_office_gross_usd column
        if domestic_box_office_gross is None and "provider3_domestic" in provider_name:
            dom_raw = row.get("box_office_gross_usd")
            if dom_raw not in (None, ""):
                domestic_box_office_gross = int(dom_raw)

        # 2) box_office_gross_usd (we’ll treat provider3_international as this)
        box_office_gross_usd = None

        if "provider3_international" in provider_name:
            intl_raw = row.get("box_office_gross_usd")
            if intl_raw not in (None, ""):
                box_office_gross_usd = int(intl_raw)
        else:
            # generic mapping if other providers ever use this name
            gross_raw = get_first(row, FIELD_MAP.get("box_office_gross_usd", []))
            if gross_raw not in (None, ""):
                box_office_gross_usd = int(gross_raw)

        # --- Provider3_financials: budget + marketing ---
        production_budget_usd_raw = get_first(row, FIELD_MAP.get("production_budget_usd", []))
        production_budget_usd = (
            int(production_budget_usd_raw)
            if production_budget_usd_raw not in (None, "")
            else None
        )

        marketing_spend_usd_raw = get_first(row, FIELD_MAP.get("marketing_spend_usd", []))
        marketing_spend_usd = (
            int(marketing_spend_usd_raw)
            if marketing_spend_usd_raw not in (None, "")
            else None
        )

        # ID
        movie_id = generate_movie_id(movie_title, release_year)

        movie = {
            "movie_id": movie_id,
            "movie_title": movie_title,
            "release_year": release_year,

            # Scores
            "critic_score": critic_score,
            "top_critic_score": top_critic_score,
            "audience_avg_score": audience_avg_score,

            # Counts
            "total_critic_ratings": total_critic_ratings,
            "total_audience_ratings": total_audience_ratings,

            # Financials
            "domestic_box_office_gross": domestic_box_office_gross,
            "box_office_gross_usd": box_office_gross_usd,
            "production_budget_usd": production_budget_usd,
            "marketing_spend_usd": marketing_spend_usd,

            # for debugging / merge step
            "provider": row.get("provider"),
        }

        transformed.append(movie)

    logger.info(indent(color(
        f"{ICONS['ok']} Produced {bold(str(len(transformed)))} transformed movies",
        GREEN
    )))

    return transformed
