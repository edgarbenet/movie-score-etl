from src.readers import extract_from_path
from pathlib import Path
import json
import tempfile


def test_extract_json():
    sample = [{"a": 1}, {"a": 2}]

    with tempfile.TemporaryDirectory() as tmp:
        p = Path(tmp) / "sample.json"
        p.write_text(json.dumps(sample), encoding="utf-8")

        rows = extract_from_path(p)

    assert rows == sample
    assert len(rows) == 2


def test_extract_csv():
    with tempfile.TemporaryDirectory() as tmp:
        p = Path(tmp) / "sample.csv"
        p.write_text("col\n123\n456\n", encoding="utf-8")

        rows = extract_from_path(p)

    assert len(rows) == 2
    assert rows[0]["col"] == "123"
