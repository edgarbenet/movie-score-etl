from pathlib import Path
import json


def load(records: list[dict], output_path: Path) -> None:
    """
    First iteration: dump to a JSON file.
    Later: DB, Parquet, API, etc.
    """
    with output_path.open("w", encoding="utf-8") as f:
        json.dump(records, f, indent=2, ensure_ascii=False)
