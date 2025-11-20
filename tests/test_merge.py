# tests/test_merge.py

from pathlib import Path
import json
import tempfile

from src.merge import merge_from_canonical


def test_merge_simple_case():
    """
    Two canonical records for the same movie_id coming from different providers.

    Current merge rules (see src.merge._merge_group):
      - Track `provider` from each record into merged["providers"].
      - For each key:
          - Ignore movie_id/provider
          - Ignore None
          - If key not set -> take value
          - If key set and both values numeric -> keep the MAX
          - Else keep existing (first non-None wins)

    critic_score is on a 0–10 scale.
    """
    data = [
        {
            "movie_id": "abc12345",
            "movie_title": "Inception",
            "release_year": 2010,
            "critic_score": 8.0,
            "provider": "provider1",
        },
        {
            "movie_id": "abc12345",
            "movie_title": "Inception",
            "release_year": 2010,
            "critic_score": 9.0,
            "provider": "provider2",
        },
    ]

    with tempfile.TemporaryDirectory() as tmp:
        input_path = Path(tmp) / "canonical.json"
        with input_path.open("w", encoding="utf-8") as f:
            # merge_from_canonical supports both plain list and wrapped {"records": [...]}
            json.dump(data, f)

        merged = merge_from_canonical(input_path)

    assert len(merged) == 1
    m = merged[0]

    assert m["movie_id"] == "abc12345"
    assert m["movie_title"] == "Inception"
    assert m["release_year"] == 2010

    # According to _merge_group, max numeric wins: 9.0
    assert m["critic_score"] == 9.0

    # Providers list should contain both providers, without duplicates.
    assert "providers" in m
    assert set(m["providers"]) == {"provider1", "provider2"}
