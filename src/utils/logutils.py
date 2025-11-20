# src/logutils.py
import logging
import sys

# ----------------- GLOBAL LOG LEVEL -----------------
LOG_LEVEL = "INFO"
# LOG_LEVEL = "DEBUG"

# ----------------- COLORS -----------------
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

# ----------------- ICONS -----------------
ICONS = {
    "movie": "🎬",
    "extract": "📥",
    "transform": "🔧",
    "load": "💾",
    "ok": "✅",
    "skip": "⚪",
    "err": "❌",
    "info": "ℹ️",
    "scan": "📂",
    "dispatch": "🔎",
    "merge": "🔀",
    "result": "➡️",
}


# ----------------- LOGGER FACTORY -----------------
def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)

    # Avoid duplicated handlers
    if logger.handlers:
        return logger

    handler = logging.StreamHandler(sys.stdout)

    # Formatter depends on LOG_LEVEL:
    if LOG_LEVEL.upper() == "INFO":
        # CLEAN OUTPUT (no module, no level)
        formatter = logging.Formatter("%(message)s")
    else:
        # DEBUG → SHOW module name + level
        formatter = logging.Formatter("%(levelname)s [%(name)s]: \t %(message)s")

    handler.setFormatter(formatter)

    # Set logger level
    level = logging.DEBUG if LOG_LEVEL.upper() == "DEBUG" else logging.INFO
    logger.setLevel(level)
    logger.addHandler(handler)

    return logger


# ----------------- HELPERS -----------------
def color(text: str, c: str) -> str:
    return f"{c}{text}{RESET}"


def bold(text: str) -> str:
    return f"{BOLD}{text}{RESET}"


def indent(text: str, level: int = 1) -> str:
    return "   " * level + text
