from pathlib import Path
import csv
import logging
from providers.readers import extract_csv

logger = logging.getLogger("main")



def extract(input_path: Path) -> list[dict]:
    """
    High-level extract step.
    For now, assumes a single CSV file.
    Later you can dispatch on suffix / provider here.
    """
    return extract_csv(input_path)


