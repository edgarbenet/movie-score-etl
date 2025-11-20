# tests/test_transform.py

from src.transform import transform


def test_transform_basic_fields():
    """
    Basic provider1-style row:
    - critic_score_percentage is given on a 0–100 scale
    - We now expect the canonical critic_score on a 0–10 scale (e.g. 87 -> 8.7)
    """
    raw = [
        {
            "movie_title": "Inception",
            "release_year": "2010",
            "critic_score_percentage": "87",
            "provider": "provider1",
        }
    ]

    result = transform(raw)
    assert len(result) == 1
    m = result[0]

    assert m["movie_title"] == "Inception"
    assert m["release_year"] == 2010

    # Flat canonical field
    assert m["critic_score"] == 8.7

    # Provider should be propagated
    assert m["provider"] == "provider1"

    # movie_id should exist and be non-empty
    assert isinstance(m["movie_id"], str)
    assert m["movie_id"] != ""


def test_transform_prefers_first_matching_field_in_field_map():
    """
    When multiple raw keys map to the same canonical field (e.g. critic_score),
    the first key in FIELD_MAP should win (critic_score_percentage vs critic_score).

    Now critic_score is on a 0–10 scale:
    - 91% -> 9.1
    """
    raw = [
        {
            "movie_title": "Interstellar",
            "release_year": "2014",
            "critic_score_percentage": "91",  # should be used
            "critic_score": 5.0,              # should be ignored
            "provider": "provider1",
        }
    ]

    result = transform(raw)
    assert len(result) == 1
    m = result[0]

    assert m["movie_title"] == "Interstellar"
    assert m["release_year"] == 2014

    # 91% -> 9.1 (0–10 scale) via first key in FIELD_MAP["critic_score"]
    assert abs(m["critic_score"] - 9.1) < 1e-6


def test_transform_provider2_style_fields():
    """
    Use provider2-like keys: title, year, audience_average_score,
    total_audience_ratings, domestic_box_office_gross, etc.,
    and ensure they land in the flat canonical fields.
    """
    raw = [
        {
            "title": "Tenet",
            "year": 2020,
            "audience_average_score": 7.8,      # already on 0–10 scale
            "total_audience_ratings": 550000,
            "domestic_box_office_gross": 58374665,
            "provider": "provider2",
        }
    ]

    result = transform(raw)
    assert len(result) == 1
    m = result[0]

    assert m["movie_title"] == "Tenet"
    assert m["release_year"] == 2020

    # Audience fields mapped flat
    assert m["audience_avg_score"] == 7.8
    assert m["total_audience_ratings"] == 550000

    # Financial mapping
    assert m["domestic_box_office_gross"] == 58374665

    assert m["provider"] == "provider2"


def test_transform_handles_unknown_fields_gracefully():
    """
    Add fields that are *not* in FIELD_MAP and ensure transform still works
    and returns a valid canonical record.

    We keep a valid movie_title/release_year, but add unmapped fields
    that should be ignored by the canonical output.
    """
    raw = [
        {
            "movie_title": "Parasite",
            "release_year": "2019",
            "critic_score_percentage": "99",  # 99% -> 9.9 on a 0–10 scale
            "weird_unmapped_field": "should_not_break_anything",
            "another_random_metric": 123.45,
            "provider": "provider1",
        }
    ]

    result = transform(raw)
    assert len(result) == 1
    m = result[0]

    assert m["movie_title"] == "Parasite"
    assert m["release_year"] == 2019

    assert abs(m["critic_score"] - 9.9) < 1e-6
    assert m["provider"] == "provider1"

    # Extra fields should not appear in the canonical output
    assert "weird_unmapped_field" not in m
    assert "another_random_metric" not in m
