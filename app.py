from __future__ import annotations

from pathlib import Path
import json
import re
from typing import Tuple, List, Dict, Any

import plotly.express as px
from dash import Dash, dcc, html

# ---------------------------------
# 1) Locate & load the merged file
# ---------------------------------
BASE_DIR = Path(__file__).resolve().parent
PROCESSED_DIR = BASE_DIR / "data" / "processed"
MERGED_PREFIX = "movies_merged_"


def find_latest_merged_file() -> Path:
    """Pick latest movies_merged_YYYY-MM-DD.json by date in filename."""
    candidates = list(PROCESSED_DIR.glob(f"{MERGED_PREFIX}*.json"))
    if not candidates:
        raise FileNotFoundError(f"No {MERGED_PREFIX}*.json files in {PROCESSED_DIR}")

    def extract_date(p: Path) -> Tuple[int, int, int]:
        m = re.search(r"(\d{4})-(\d{2})-(\d{2})", p.name)
        if not m:
            return (0, 0, 0)
        year = int(m.group(1))
        month = int(m.group(2))
        day = int(m.group(3))
        return (year, month, day)

    return max(candidates, key=extract_date)


def load_movies() -> Tuple[List[Dict[str, Any]], Path]:
    """Load movies_merged JSON and return list of nested movie dicts + path."""
    path = find_latest_merged_file()
    with path.open("r", encoding="utf-8") as f:
        raw = json.load(f)

    # Your schema: {"generated_at": "...", "records": [ ... ]}
    if isinstance(raw, dict) and "records" in raw:
        movies = raw["records"]
    else:
        movies = raw  # just in case it is already a list

    return movies, path # type: ignore


movies, source_path = load_movies()

# ---------------------------------------------------
# 2) Build a single Plotly figure from NESTED movies
# ---------------------------------------------------
# Filter movies that have the fields we need
movies_for_chart = []
for m in movies:
    year = m.get("release_year")
    audience_score = (
        m.get("ratings", {})
        .get("audience", {})
        .get("score")
    )
    if year is not None and audience_score is not None:
        movies_for_chart.append(m)

x_years = [m["release_year"] for m in movies_for_chart]
y_scores = [m["ratings"]["audience"]["score"] for m in movies_for_chart]
hover_titles = [m["movie_title"] for m in movies_for_chart]

fig = px.scatter(
    x=x_years,
    y=y_scores,
    hover_name=hover_titles,
    title="Audience score over time",
    labels={"x": "Release year", "y": "Audience score"},
)
fig.update_layout(
    margin=dict(l=60, r=40, t=80, b=60),
    font=dict(size=16),
)

# ------------------------------
# 3) Create Dash app + layout
# ------------------------------
app = Dash(__name__)
app.title = "Movie Score ETL – Mini Dashboard (Nested JSON)"

app.layout = html.Div(
    [
        html.H1("🎬 Movie Score ETL – Mini Dashboard"),
        html.P(
            f"Source file: {source_path.relative_to(BASE_DIR)}",
            style={"fontSize": "14px", "color": "#555"},
        ),
        dcc.Graph(figure=fig),
    ],
    style={
        "fontFamily": "system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif",
        "padding": "24px 32px",
        "backgroundColor": "#f8f9fc",
    },
)

# ------------------------------
# 4) Run server
# ------------------------------
if __name__ == "__main__":
    app.run(debug=True)
