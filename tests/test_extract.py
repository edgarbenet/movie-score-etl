# tests/test_extract.py

from pathlib import Path
import json
import tempfile

from src.extract import (
    read_raw_files,
    extract,
    extract_csv,
    extract_all_providers,
)


def test_read_raw_files_returns_csv_and_json_recursively():
    """
    read_raw_files should yield all CSV/JSON files under a directory,
    including nested ones, and ignore other extensions.
    """
    with tempfile.TemporaryDirectory() as tmp:
        base = Path(tmp)

        # Top-level files
        csv_path = base / "provider1.csv"
        csv_path.write_text("col1,col2\n1,foo\n", encoding="utf-8")

        json_path = base / "provider2.json"
        json_path.write_text(json.dumps([{"col1": "2", "col2": "bar"}]), encoding="utf-8")

        # Non-data file
        (base / "notes.txt").write_text("ignore me", encoding="utf-8")

        # Nested directory with another CSV
        subdir = base / "sub"
        subdir.mkdir()
        nested_csv = subdir / "nested.csv"
        nested_csv.write_text("col1\n3\n", encoding="utf-8")

        found_files = list(read_raw_files(base))

    # We expect only csv/json files, including nested csv
    names = {p.name for p in found_files}
    assert "provider1.csv" in names
    assert "provider2.json" in names
    assert "nested.csv" in names
    assert "notes.txt" not in names

    suffixes = {p.suffix.lower() for p in found_files}
    assert suffixes <= {".csv", ".json"}


def test_extract_csv_reads_rows_correctly():
    """
    extract_csv should read a CSV into a list[dict] without extra magic.
    """
    with tempfile.TemporaryDirectory() as tmp:
        p = Path(tmp) / "sample.csv"
        p.write_text("movie_title,year\nInception,2010\nTenet,2020\n", encoding="utf-8")

        rows = extract_csv(p)

    assert len(rows) == 2
    assert rows[0]["movie_title"] == "Inception"
    assert rows[0]["year"] == "2010"
    assert rows[1]["movie_title"] == "Tenet"
    assert rows[1]["year"] == "2020"


def test_extract_delegates_to_extract_csv():
    """
    extract() is a high-level wrapper that currently assumes CSV
    and delegates to extract_csv.
    """
    with tempfile.TemporaryDirectory() as tmp:
        p = Path(tmp) / "sample.csv"
        p.write_text("col1,col2\n1,foo\n", encoding="utf-8")

        via_csv = extract_csv(p)
        via_extract = extract(p)

    assert via_extract == via_csv
    assert len(via_extract) == 1
    assert via_extract[0]["col1"] == "1"
    assert via_extract[0]["col2"] == "foo"


def test_extract_all_providers_adds_provider_and_skips_non_data():
    """
    extract_all_providers should:
      - scan a directory for CSV/JSON files
      - use extract_from_path internally
      - attach a 'provider' field equal to the file stem (e.g. provider1)
      - skip non-data files such as .txt
    """
    with tempfile.TemporaryDirectory() as tmp:
        base = Path(tmp)

        # provider1 CSV
        csv_path = base / "provider1.csv"
        csv_path.write_text("movie_title\nDunkirk\n", encoding="utf-8")

        # provider2 JSON
        json_path = base / "provider2.json"
        json_path.write_text(
            json.dumps([{"movie_title": "Oppenheimer"}]),
            encoding="utf-8",
        )

        # Non-data file that should be skipped
        (base / "readme.txt").write_text("ignore", encoding="utf-8")

        rows = extract_all_providers(base)

    # We expect one row from each provider, with provider injected
    assert len(rows) == 2

    titles = {r["movie_title"] for r in rows}
    assert titles == {"Dunkirk", "Oppenheimer"}

    providers = {r["provider"] for r in rows}
    # stems: "provider1", "provider2"
    assert providers == {"provider1", "provider2"}
