# src/logutils.py
import logging
import sys

# ---------- COLORS ----------
RESET = "\033[0m"
BOLD = "\033[1m"

BLACK = "\033[30m"
RED = "\033[91m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
BLUE = "\033[94m"
MAGENTA = "\033[95m"
CYAN = "\033[96m"
WHITE = "\033[97m"

# ---------- ICONS (optional helpers) ----------
ICONS = {
    "extract": "📥",
    "transform": "🔧",
    "load": "💾",
    "ok": "✅",
    "skip": "⚪",
    "err": "❌",
    "info": "ℹ️",
    "scan": "📂",
    "dispatch": "🔎",
}

# ---------- LOGGER FACTORY ----------
def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)

    # prevent double handlers in notebooks / reloads
    if logger.handlers:
        return logger

    handler = logging.StreamHandler(sys.stdout)

    formatter = logging.Formatter("%(message)s")
    handler.setFormatter(formatter)

    logger.setLevel(logging.INFO)
    logger.addHandler(handler)

    return logger


# ---------- CONVENIENCE HELPERS ----------
def color(text: str, c: str) -> str:
    return f"{c}{text}{RESET}"


def bold(text: str) -> str:
    return f"{BOLD}{text}{RESET}"


def indent(text: str, level: int = 1) -> str:
    """Indent with spaces for hierarchical logs."""
    return "   " * level + text
