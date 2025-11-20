# app.py
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Tuple

import pandas as pd
import plotly.express as px
from dash import Dash, dcc, html, Input, Output

# ---------- CONFIG ----------
BASE_DIR = Path(__file__).resolve().parent
PROCESSED_DIR = BASE_DIR / "data" / "processed"
MERGED_PREFIX = "movies_merged_"


def _find_latest_merged_file() -> Path:
    """
    Find the latest movies_merged_YYYY-MM-DD.json file by date
    in the filename. Raise a clear error if none found.
    """
    candidates = list(PROCESSED_DIR.glob(f"{MERGED_PREFIX}*.json"))
    if not candidates:
        raise FileNotFoundError(
            f"No {MERGED_PREFIX}*.json files found in {PROCESSED_DIR}. "
            "Run the ETL pipeline first."
        )

    def extract_date(p: Path):
        m = re.search(r"(\d{4})-(\d{2})-(\d{2})", p.name)
        if not m:
            return (0, 0, 0)
        return tuple(int(x) for x in m.groups())

    latest = max(candidates, key=extract_date)
    return latest


def load_data() -> Tuple[pd.DataFrame, Path]:
    """
    Load the latest merged JSON file into a DataFrame.

    Expected structure of the JSON:
        {
          "generated_at": "...",
          "records": [ { movie fields... }, ... ]
        }
    or simply a list of records: [ {...}, {...} ]
    """
    latest_file = _find_latest_merged_file()

    with latest_file.open("r", encoding="utf-8") as f:
        raw = json.load(f)

    if isinstance(raw, dict) and "records" in raw:
        records = raw["records"]
    elif isinstance(raw, list):
        records = raw
    else:
        raise ValueError(
            f"Unexpected JSON structure in {latest_file}. "
            "Expected a dict with 'records' or a list of records."
        )

    df = pd.DataFrame(records)

    # Ensure expected columns exist so the rest of the code doesn't crash
    expected_columns = [
        "movie_id",
        "movie_title",
        "release_year",
        "critic_score",
        "top_critic_score",
        "audience_avg_score",
        "total_critic_ratings",
        "total_audience_ratings",
        "domestic_box_office_gross",
        "box_office_gross_usd",
        "production_budget_usd",
        "marketing_spend_usd",
    ]
    for col in expected_columns:
        if col not in df.columns:
            df[col] = pd.NA

    # Coerce year + numeric columns
    df["release_year"] = pd.to_numeric(df["release_year"], errors="coerce")

    numeric_cols = [
        "critic_score",
        "top_critic_score",
        "audience_avg_score",
        "total_critic_ratings",
        "total_audience_ratings",
        "domestic_box_office_gross",
        "box_office_gross_usd",
        "production_budget_usd",
        "marketing_spend_usd",
    ]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    return df, latest_file


# ---------- LOAD DATA ----------
df, latest_file = load_data()

# Drop rows without a title – not useful to visualize
if "movie_title" in df.columns:
    df = df[df["movie_title"].notna()]

if df["release_year"].notna().any():
    min_year = int(df["release_year"].min())
    max_year = int(df["release_year"].max())
else:
    # Fallback if no years present
    min_year, max_year = 1980, 2025

# ---------- DASH APP ----------
app = Dash(__name__)
app.title = "🎬 Movie Score ETL – Dashboard"

app.layout = html.Div(
    children=[
        html.H1("🎬 Movie Score ETL – Dashboard"),
        html.Div(
            [
                html.Span("Source file: "),
                html.Code(str(latest_file.relative_to(BASE_DIR))),
                html.Span("  |  "),
                html.Span(f"{len(df)} movies"),
            ],
            style={"marginBottom": "20px"},
        ),
        html.Div(
            [
                html.Div(
                    [
                        html.Label("Release year range"),
                        dcc.RangeSlider(
                            id="year-range",
                            min=min_year,
                            max=max_year,
                            step=1,
                            value=[min_year, max_year],
                            marks={
                                int(y): str(int(y))
                                for y in range(
                                    min_year,
                                    max_year + 1,
                                    max(1, (max_year - min_year) // 8 or 1),
                                )
                            },
                            tooltip={"placement": "bottom", "always_visible": False},
                        ),
                    ],
                    style={"marginBottom": "30px"},
                )
            ]
        ),
        dcc.Tabs(
            id="tabs",
            value="tab-overview",
            children=[
                dcc.Tab(label="Overview", value="tab-overview"),
                dcc.Tab(label="Box Office vs Budget", value="tab-boxoffice"),
                dcc.Tab(label="Ratings", value="tab-ratings"),
            ],
        ),
        html.Div(id="tab-content", style={"marginTop": "20px"}),
    ],
    style={
        "fontFamily": "system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif",
        "padding": "30px",
    },
)


def filter_by_year(dframe: pd.DataFrame, year_range):
    low, high = year_range
    if "release_year" not in dframe.columns:
        return dframe.iloc[0:0]
    return dframe[
        (dframe["release_year"].notna())
        & (dframe["release_year"] >= low)
        & (dframe["release_year"] <= high)
    ]


# ---------- TAB CONTENT CALLBACK ----------
@app.callback(
    Output("tab-content", "children"),
    Input("tabs", "value"),
    Input("year-range", "value"),
)
def render_tab_content(tab, year_range):
    dff = filter_by_year(df, year_range)

    if dff.empty:
        return html.Div(
            [
                html.H2("No data"),
                html.P("No movies match the current filters / year range."),
            ]
        )

    if tab == "tab-overview":
        return overview_tab_content(dff)
    elif tab == "tab-boxoffice":
        return boxoffice_tab_content(dff)
    elif tab == "tab-ratings":
        return ratings_tab_content(dff)

    return html.Div("Unknown tab")


# ---------- TAB BUILDERS ----------
def overview_tab_content(dff: pd.DataFrame):
    # Histogram of movies per year
    fig_hist = px.histogram(
        dff,
        x="release_year",
        nbins=min(40, max(10, max_year - min_year)),
        title="Movies per year",
    )

    # Scatter of domestic vs worldwide
    fig_scatter = px.scatter(
        dff,
        x="domestic_box_office_gross",
        y="box_office_gross_usd",
        color="release_year",
        hover_name="movie_title",
        labels={
            "domestic_box_office_gross": "Domestic box office (USD)",
            "box_office_gross_usd": "Worldwide box office (USD)",
            "release_year": "Release year",
        },
        title="Domestic vs worldwide box office",
    )


    # Top 15 by worldwide box office
    dff_top = (
        dff.dropna(subset=["box_office_gross_usd"])
        .sort_values("box_office_gross_usd", ascending=False)
        .head(15)
    )
    fig_top = px.bar(
        dff_top,
        x="movie_title",
        y="box_office_gross_usd",
        text="box_office_gross_usd",
        title="Top 15 movies by worldwide box office",
    )
    fig_top.update_layout(xaxis_tickangle=-45)

    return html.Div(
        [
            html.H2("Overview"),
            dcc.Graph(figure=fig_hist),
            dcc.Graph(figure=fig_scatter),
            dcc.Graph(figure=fig_top),
        ]
    )


def boxoffice_tab_content(dff: pd.DataFrame):
    # ROI = worldwide / budget
    dff_roi = dff.copy()
    mask = (dff_roi["production_budget_usd"].notna()) & (
        dff_roi["production_budget_usd"] > 0
    )
    dff_roi["roi"] = pd.NA
    dff_roi.loc[mask, "roi"] = (
        dff_roi.loc[mask, "box_office_gross_usd"]
        / dff_roi.loc[mask, "production_budget_usd"]
    )

    dff_roi_sorted = (
        dff_roi.dropna(subset=["roi"]).sort_values("roi", ascending=False).head(15)
    )

    fig_roi = px.bar(
        dff_roi_sorted,
        x="movie_title",
        y="roi",
        hover_data=["box_office_gross_usd", "production_budget_usd"],
        title="Top 15 ROI (Box Office / Budget)",
    )
    fig_roi.update_layout(xaxis_tickangle=-45)

    # Budget vs worldwide
    fig_budget_scatter = px.scatter(
        dff_roi,
        x="production_budget_usd",
        y="box_office_gross_usd",
        size="total_audience_ratings",
        color="release_year",
        hover_name="movie_title",
        labels={
            "production_budget_usd": "Production budget (USD)",
            "box_office_gross_usd": "Worldwide box office (USD)",
        },
        title="Budget vs worldwide box office",
    )

    return html.Div(
        [
            html.H2("Box Office vs Budget"),
            dcc.Graph(figure=fig_roi),
            dcc.Graph(figure=fig_budget_scatter),
        ]
    )


def ratings_tab_content(dff: pd.DataFrame):
    fig_ratings = px.scatter(
        dff,
        x="critic_score",
        y="audience_avg_score",
        size="total_critic_ratings",
        color="release_year",
        hover_name="movie_title",
        labels={
            "critic_score": "Critic score",
            "audience_avg_score": "Audience score",
            "total_critic_ratings": "Total critic ratings",
        },
        title="Critic vs audience scores",
    )

    dff_top_audience = (
        dff.dropna(subset=["audience_avg_score"])
        .sort_values("audience_avg_score", ascending=False)
        .head(15)
    )
    fig_top_audience = px.bar(
        dff_top_audience,
        x="movie_title",
        y="audience_avg_score",
        title="Top 15 movies by audience score",
    )
    fig_top_audience.update_layout(xaxis_tickangle=-45)

    return html.Div(
        [
            html.H2("Ratings"),
            dcc.Graph(figure=fig_ratings),
            dcc.Graph(figure=fig_top_audience),
        ]
    )


if __name__ == "__main__":
    # For Docker/K8s: app.run(debug=False, host="0.0.0.0", port=8050)
    app.run(debug=True)
