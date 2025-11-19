from pathlib import Path
import json

from utils.logutils import (
    get_logger, color, bold, indent, CYAN, GREEN, ICONS
)

logger = get_logger(__name__)


def load(records: list[dict], output_path: Path) -> None:
    logger.info(indent(color(
        f"{ICONS['load']} Writing {bold(str(len(records)))} records → {output_path}",
        CYAN,
    )))

    with output_path.open("w", encoding="utf-8") as f:
        json.dump(records, f, indent=2, ensure_ascii=False)

    logger.info(indent(color(f"{ICONS['ok']} File written", GREEN)))
