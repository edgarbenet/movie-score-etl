# tests/test_readers.py

from pathlib import Path
import json
import tempfile

import pytest

from src.readers import extract_csv, extract_json, extract_from_path


def test_extract_csv_basic():
    """
    Basic CSV extraction: the function should return a list of dicts
    with the parsed rows.
    """
    with tempfile.TemporaryDirectory() as tmp:
        p = Path(tmp) / "sample.csv"
        p.write_text("col1,col2\n1,foo\n2,bar\n", encoding="utf-8")

        rows = extract_csv(p)

    assert len(rows) == 2
    assert rows[0]["col1"] == "1"
    assert rows[0]["col2"] == "foo"
    assert rows[1]["col1"] == "2"
    assert rows[1]["col2"] == "bar"


def test_extract_json_list_root():
    """
    When JSON root is a list, extract_json should just return that list.
    """
    sample = [{"a": 1}, {"a": 2}]
    with tempfile.TemporaryDirectory() as tmp:
        p = Path(tmp) / "sample.json"
        p.write_text(json.dumps(sample), encoding="utf-8")

        rows = extract_json(p)

    assert rows == sample
    assert len(rows) == 2


def test_extract_json_wrapped_list():
    """
    When JSON is an object with a list somewhere inside, extract_json
    should pick the first list value it finds.
    """
    data = {
        "generated_at": "2025-11-19T10:00:00",
        "records": [
            {"movie_title": "Inception"},
            {"movie_title": "Tenet"},
        ],
        "meta": {"something": "else"},
    }
    with tempfile.TemporaryDirectory() as tmp:
        p = Path(tmp) / "wrapped.json"
        p.write_text(json.dumps(data), encoding="utf-8")

        rows = extract_json(p)

    assert len(rows) == 2
    titles = {r["movie_title"] for r in rows}
    assert titles == {"Inception", "Tenet"}


def test_extract_from_path_unsupported_extension_raises_value_error():
    """
    A non-JSON/CSV file should cause extract_from_path to raise ValueError,
    as implemented in readers.py.
    """
    with tempfile.TemporaryDirectory() as tmp:
        p = Path(tmp) / "sample.txt"
        p.write_text("this should be ignored", encoding="utf-8")

        with pytest.raises(ValueError):
            extract_from_path(p)
