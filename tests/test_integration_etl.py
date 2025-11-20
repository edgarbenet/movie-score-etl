from pathlib import Path
import json
import tempfile

from src.extract import extract_all_providers
from src.transform import transform
from src.merge import merge_from_canonical
from src.load import write_canonical, load


def _run_full_pipeline(raw_dir: Path, processed_dir: Path):
    """
    Helper to run the full ETL chain on a given temp structure.
    Returns the parsed final merged JSON wrapper (with "generated_at" + "records").
    """
    # 1) Extract raw rows from providers
    all_raw_rows = extract_all_providers(raw_dir)

    # 2) Transform to canonical flat records
    canonical_records = transform(all_raw_rows)

    # 3) Write canonical to movies_canonical_<date>.json (in processed_dir)
    processed_dir.mkdir(parents=True, exist_ok=True)
    canonical_stub = processed_dir / "movies_canonical.json"
    write_canonical(canonical_records, canonical_stub)

    # 4) Merge from latest canonical file
    merged_records = merge_from_canonical(canonical_stub)

    # 5) Load to final presentation model → movies_merged_<date>.json
    merged_stub = processed_dir / "movies_merged.json"
    load(merged_records, merged_stub)

    # 6) Find the actual merged file with date suffix and load it
    merged_files = list(processed_dir.glob("movies_merged_*.json"))
    assert len(merged_files) == 1, "Expected exactly one merged output file"
    final_path = merged_files[0]

    with final_path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    return data


def test_full_etl_simple_two_providers():
    """
    Integration test:
    - provider1 (critics) as CSV
    - provider2 (audience + domestic box office) as JSON

    Check that:
      * Only one movie is produced
      * critic_score is 8.7 (87% / 10)
      * audience score and domestic box office are correctly mapped
      * providers contains both provider1 and provider2
      * final structure is the nested presentation model (ratings, financials, providers)
    """
    with tempfile.TemporaryDirectory() as tmp:
        base = Path(tmp)
        raw_dir = base / "raw_new"
        processed_dir = base / "processed"
        raw_dir.mkdir()

        # provider1.csv (critics)
        provider1_csv = raw_dir / "provider1.csv"
        provider1_csv.write_text(
            "movie_title,release_year,critic_score_percentage\n"
            "Inception,2010,87\n",
            encoding="utf-8",
        )

        # provider2.json (audience + domestic box office)
        provider2_json = raw_dir / "provider2.json"
        provider2_json.write_text(
            json.dumps(
                [
                    {
                        "title": "Inception",
                        "year": 2010,
                        "audience_average_score": 7.8,
                        "total_audience_ratings": 550000,
                        "domestic_box_office_gross": 58374665,
                    }
                ]
            ),
            encoding="utf-8",
        )

        data = _run_full_pipeline(raw_dir, processed_dir)

    # data = {"generated_at": "...", "records": [ ... ] }
    records = data["records"]
    assert len(records) == 1

    movie = records[0]
    assert movie["movie_title"] == "Inception"
    assert movie["release_year"] == 2010

    # Ratings nested
    critic = movie["ratings"]["critic"]
    audience = movie["ratings"]["audience"]

    assert critic["score"] == 8.7
    assert audience["score"] == 7.8
    assert audience["total_ratings"] == 550000

    # Financials nested
    financials = movie["financials"]
    assert financials["domestic_box_office_usd"] == 58374665

    # Providers list from merge step
    providers = set(movie["providers"])
    assert providers == {"provider1", "provider2"}


def test_full_etl_combines_multiple_providers_into_rich_record():
    """
    Integration test:
    Combine multiple providers for a single movie:

      - provider1.csv              → critics
      - provider2.json             → audience + domestic box office
      - provider3_financials.json  → production + marketing budget
      - provider3_international.json → worldwide box office

    Validate that the final presentation model matches the expected
    rich structure for that movie.
    """
    with tempfile.TemporaryDirectory() as tmp:
        base = Path(tmp)
        raw_dir = base / "raw_new"
        processed_dir = base / "processed"
        raw_dir.mkdir()

        # provider1.csv (critics)
        (raw_dir / "provider1.csv").write_text(
            "movie_title,release_year,critic_score_percentage,top_critic_score,total_critic_reviews_counted\n"
            "Inception,2010,87,8.1,450\n",
            encoding="utf-8",
        )

        # provider2.json (audience + domestic box office)
        (raw_dir / "provider2.json").write_text(
            json.dumps(
                [
                    {
                        "title": "Inception",
                        "year": 2010,
                        "audience_average_score": 9.1,
                        "total_audience_ratings": 1500000,
                        "domestic_box_office_gross": 292576195,
                    }
                ]
            ),
            encoding="utf-8",
        )

        # provider3_financials.json (budget + marketing)
        (raw_dir / "provider3_financials.json").write_text(
            json.dumps(
                [
                    {
                        "movie_title": "Inception",
                        "release_year": 2010,
                        "production_budget_usd": 160000000,
                        "marketing_spend_usd": 100000000,
                    }
                ]
            ),
            encoding="utf-8",
        )

        # provider3_international.json (worldwide box office)
        (raw_dir / "provider3_international.json").write_text(
            json.dumps(
                [
                    {
                        "movie_title": "Inception",
                        "release_year": 2010,
                        "box_office_gross_usd": 535700000,
                    }
                ]
            ),
            encoding="utf-8",
        )

        data = _run_full_pipeline(raw_dir, processed_dir)

    records = data["records"]
    assert len(records) == 1
    movie = records[0]

    assert movie["movie_title"] == "Inception"
    assert movie["release_year"] == 2010

    # Ratings
    critic = movie["ratings"]["critic"]
    audience = movie["ratings"]["audience"]

    # 87% / 10 => 8.7 on a 0–10 scale
    assert abs(critic["score"] - 8.7) < 1e-6
    assert abs(critic["top_score"] - 8.1) < 1e-6
    assert critic["total_ratings"] == 450

    assert abs(audience["score"] - 9.1) < 1e-6
    assert audience["total_ratings"] == 1500000

    # Financials
    financials = movie["financials"]
    assert financials["domestic_box_office_usd"] == 292576195
    assert financials["worldwide_box_office_usd"] == 535700000
    assert financials["production_budget_usd"] == 160000000
    assert financials["marketing_spend_usd"] == 100000000

    # Providers from all files
    providers = set(movie["providers"])
    assert providers == {
        "provider1",
        "provider2",
        "provider3_financials",
        "provider3_international",
    }
