from src.transform import transform

def test_transform_basic_fields():
    raw = [
        {
            "movie_title": "Inception",
            "release_year": "2010",
            "critic_score_percentage": "87",
            "provider": "provider1"
        }
    ]

    result = transform(raw)
    m = result[0]

    assert m["movie_title"] == "Inception"
    assert m["release_year"] == 2010
    assert m["critic_score"] == 0.87
    assert m["provider"] == "provider1"
    assert len(result) == 1
