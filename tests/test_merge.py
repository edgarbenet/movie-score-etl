from src.merge import merge_from_canonical
from pathlib import Path
import json
import tempfile


def test_merge_simple_case():
    data = [
        {
            "movie_id": "abc12345",
            "movie_title": "Inception",
            "release_year": 2010,
            "critic_score": 0.80,
            "provider": "provider1"
        },
        {
            "movie_id": "abc12345",
            "movie_title": "Inception",
            "release_year": 2010,
            "critic_score": 0.75,
            "provider": "provider2"
        },
    ]

    # prepare temp input file
    with tempfile.TemporaryDirectory() as tmp:
        input_path = Path(tmp) / "canonical.json"
        with input_path.open("w", encoding="utf-8") as f:
            json.dump(data, f)

        merged = merge_from_canonical(input_path)

    assert len(merged) == 1
    m = merged[0]

    # provider1 wins for critic_score (because of FIELD_PROVIDER_PRIORITY)
    assert m["critic_score"] == 0.80
    assert "provider1" in m["providers"]
    assert "provider2" in m["providers"]
