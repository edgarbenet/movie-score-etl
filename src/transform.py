import uuid
from typing import List

FIELD_MAP = {
    "movie_title": ["movie_title", "title", "name"],
    "release_year": ["release_year", "year"],

    # provider1 stuff
    "critic_score": ["critic_score_percentage", "critic_score"],
    "top_critic_score": ["top_critic_score"],
    "total_critic_ratings": ["total_critic_reviews_counted"],
    
    # provider2 stuff
    "audience_score": ["audience_average_score"],
    "audience_rating_count": ["total_audience_ratings"],
    "domestic_box_office": ["domestic_box_office_gross"],
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
    transformed: list[dict] = []

    for row in data_raw:
        movie_title_raw = get_first(row, FIELD_MAP["movie_title"], default="") or ""
        movie_title = movie_title_raw.strip()

        release_year_raw = get_first(row, FIELD_MAP["release_year"])
        release_year = int(release_year_raw) if release_year_raw not in (None, "") else None

        # critic_score
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

        # --- provider2 fields ---

        audience_score_raw = get_first(row, FIELD_MAP.get("audience_score", []))
        audience_score = (
            float(audience_score_raw)
            if audience_score_raw not in (None, "")
            else None
        )

        audience_rating_count_raw = get_first(row, FIELD_MAP.get("audience_rating_count", []))
        audience_rating_count = (
            int(audience_rating_count_raw)
            if audience_rating_count_raw not in (None, "")
            else None
        )

        domestic_box_office_raw = get_first(row, FIELD_MAP.get("domestic_box_office", []))
        domestic_box_office = (
            int(domestic_box_office_raw)
            if domestic_box_office_raw not in (None, "")
            else None
        )

        movie_id = generate_movie_id(movie_title, release_year)

        movie = {
            "movie_id": movie_id,
            "movie_title": movie_title,
            "release_year": release_year,
            "critic_score": critic_score,
            "top_critic_score": top_critic_score,
            "total_critic_ratings": total_critic_ratings,
            "audience_score": audience_score,
            "audience_rating_count": audience_rating_count,
            "domestic_box_office": domestic_box_office,
        }
        transformed.append(movie)

    return transformed
