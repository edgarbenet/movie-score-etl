# app.py
from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from typing import Tuple

import pandas as pd
import plotly.express as px
import plotly.io as pio
from dash import Dash, dcc, html, Input, Output

# ------------------------------------------------------------------------------
# GLOBAL PLOTLY STYLE
# ------------------------------------------------------------------------------
pio.templates.default = "plotly_white"  # clean light theme
px.defaults.height = 420                # uniform chart height

# ------------------------------------------------------------------------------
# LOGGING
# ------------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s] %(asctime)s - %(name)s - %(message)s",
)
logger = logging.getLogger("movie_dashboard")

# ------------------------------------------------------------------------------
# CONFIG
# ------------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
PROCESSED_DIR = BASE_DIR / "data" / "processed"
MERGED_PREFIX = "movies_merged_"


def _find_latest_merged_file() -> Path:
    """
    Find the latest movies_merged_YYYY-MM-DD.json file by date in the filename.
    """
    if not PROCESSED_DIR.exists():
        msg = f"Processed directory does not exist: {PROCESSED_DIR}"
        logger.error(msg)
        raise FileNotFoundError(msg)

    candidates = list(PROCESSED_DIR.glob(f"{MERGED_PREFIX}*.json"))
    if not candidates:
        msg = (
            f"No {MERGED_PREFIX}*.json files found in {PROCESSED_DIR}. "
            "Run the ETL pipeline first."
        )
        logger.error(msg)
        raise FileNotFoundError(msg)

    def extract_date(p: Path):
        m = re.search(r"(\d{4})-(\d{2})-(\d{2})", p.name)
        if not m:
            return (0, 0, 0)
        return tuple(int(x) for x in m.groups())

    latest = max(candidates, key=extract_date)
    logger.info(f"Using merged file: {latest}")
    return latest


def _style_fig(fig, title: str | None = None):
    """
    Apply consistent layout styling to a Plotly figure.
    """
    fig.update_layout(
        title=title or fig.layout.title.text,
        title_x=0.03,
        margin=dict(l=70, r=50, t=80, b=70),
        hovermode="closest",
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
            font=dict(size=14),
        ),
        font=dict(
            family="system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif",
            size=16,  # bigger base font
        ),
    )
    # Make axis tick labels a bit larger too
    fig.update_xaxes(tickfont=dict(size=14), title_font=dict(size=16))
    fig.update_yaxes(tickfont=dict(size=14), title_font=dict(size=16))
    return fig


def _format_money_axis(fig, axis: str = "y"):
    axis_obj = f"{axis}axis"
    fig.update_layout(
        **{
            axis_obj: dict(
                tickprefix="$",
                separatethousands=True,
            )
        }
    )
    return fig


def load_data() -> Tuple[pd.DataFrame, Path]:
    """
    Load the latest merged JSON into a DataFrame, flattening nested structures.
    """
    latest_file = _find_latest_merged_file()

    logger.info(f"Loading data from {latest_file}")
    with latest_file.open("r", encoding="utf-8") as f:
        raw = json.load(f)

    if isinstance(raw, dict) and "records" in raw:
        records = raw["records"]
        logger.info("Detected wrapper object with 'records' field.")
    elif isinstance(raw, list):
        records = raw
        logger.info("Detected top-level list of records.")
    else:
        msg = (
            f"Unexpected JSON structure in {latest_file}. "
            "Expected a dict with 'records' or a list of records."
        )
        logger.error(msg)
        raise ValueError(msg)

    # Flatten nested dictionaries: ratings.*, financials.*, etc.
    df = pd.json_normalize(records)
    logger.info(f"Loaded {len(df)} rows and {len(df.columns)} columns after normalize.")
    logger.info(f"Raw columns: {list(df.columns)}")

    # Map nested columns to flat names used in the plots
    rename_map = {
        "critic_score": "ratings.critic.score",
        "top_critic_score": "ratings.critic.top_score",
        "total_critic_ratings": "ratings.critic.total_ratings",
        "audience_avg_score": "ratings.audience.score",
        "total_audience_ratings": "ratings.audience.total_ratings",
        "domestic_box_office_gross": "financials.domestic_box_office_usd",
        "box_office_gross_usd": "financials.worldwide_box_office_usd",
        "production_budget_usd": "financials.production_budget_usd",
        "marketing_spend_usd": "financials.marketing_spend_usd",
    }

    for new_col, original_col in rename_map.items():
        if original_col in df.columns:
            df[new_col] = df[original_col]
        else:
            logger.warning(
                f"Expected column '{original_col}' not found; "
                f"filling '{new_col}' with NA."
            )
            df[new_col] = pd.NA

    # Ensure core columns exist
    for col in ["movie_title", "release_year"]:
        if col not in df.columns:
            logger.warning(f"Missing column {col}, filling with NA.")
            df[col] = pd.NA

    # Type coercion
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

    # Drop rows without title
    before = len(df)
    df = df[df["movie_title"].notna()]
    after = len(df)
    if before != after:
        logger.info(f"Dropped {before - after} rows with empty movie_title.")

    logger.info(f"Final dataframe shape: {df.shape}")
    logger.info(
        "Sample rows:\n"
        + str(
            df[
                [
                    "movie_title",
                    "release_year",
                    "critic_score",
                    "audience_avg_score",
                    "domestic_box_office_gross",
                    "box_office_gross_usd",
                    "production_budget_usd",
                ]
            ].head()
        )
    )

    return df, latest_file


# ------------------------------------------------------------------------------
# LOAD DATA
# ------------------------------------------------------------------------------
try:
    df, latest_file = load_data()
except Exception:
    logger.exception("Failed to load data for dashboard.")
    df = pd.DataFrame(
        columns=[
            "movie_title",
            "release_year",
            "critic_score",
            "audience_avg_score",
            "domestic_box_office_gross",
            "box_office_gross_usd",
            "production_budget_usd",
            "marketing_spend_usd",
        ]
    )
    latest_file = Path("NO_DATA")

# Year range for slider
if df["release_year"].notna().any():
    min_year = int(df["release_year"].min())
    max_year = int(df["release_year"].max())
else:
    min_year, max_year = 1980, 2025
    logger.warning("No valid release_year values; using 1980–2025 fallback.")

# ------------------------------------------------------------------------------
# DASH APP
# ------------------------------------------------------------------------------
app = Dash(__name__)
app.title = "🎬 Movie Score ETL – Dashboard"

app.layout = html.Div(
    children=[
        html.H1("🎬 Movie Score ETL – Dashboard"),
        html.Div(
            [
                html.Span("Source file: "),
                html.Code(
                    str(latest_file.relative_to(BASE_DIR))
                    if latest_file.exists()
                    else "NO_DATA"
                ),
                html.Span("  |  "),
                html.Span(f"{len(df)} movies"),
            ],
            style={"marginBottom": "20px", "fontSize": "15px"},
        ),
        html.Div(
            [
                html.Label("Release year range", style={"fontSize": "15px"}),
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
        ),
        dcc.Tabs(
            id="tabs",
            value="tab-overview",
            children=[
                dcc.Tab(label="📊 Overview", value="tab-overview"),
                dcc.Tab(label="💰 Box Office vs Budget", value="tab-boxoffice"),
                dcc.Tab(label="⭐ Ratings", value="tab-ratings"),
            ],
        ),
        html.Div(id="tab-content", style={"marginTop": "20px"}),
    ],
    style={
        "fontFamily": "system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif",
        "padding": "24px 32px",
        "backgroundColor": "#f8f9fc",
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


# ------------------------------------------------------------------------------
# CALLBACK
# ------------------------------------------------------------------------------
@app.callback(
    Output("tab-content", "children"),
    Input("tabs", "value"),
    Input("year-range", "value"),
)
def render_tab_content(tab, year_range):
    try:
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
    except Exception as e:
        logger.exception("Error in render_tab_content callback.")
        return html.Div(
            [
                html.H2("Error rendering dashboard"),
                html.P("Check server logs for details."),
                html.Pre(str(e)),
            ],
            style={"color": "red"},
        )


# ------------------------------------------------------------------------------
# TAB BUILDERS
# ------------------------------------------------------------------------------
def overview_tab_content(dff: pd.DataFrame):
    # Movies per year
    fig_hist = px.histogram(
        dff,
        x="release_year",
        nbins=min(40, max(5, max_year - min_year)),
        title="Movies per year",
    )
    fig_hist = _style_fig(fig_hist)

    # Bubble chart:
    #   x = release year
    #   y = worldwide box office
    #   bubble size = domestic box office
    dff_scatter = dff.dropna(
        subset=["release_year", "box_office_gross_usd", "domestic_box_office_gross"]
    )

    if dff_scatter.empty:
        fig_scatter = px.scatter(title="No box office data available")
    else:
        fig_scatter = px.scatter(
            dff_scatter,
            x="release_year",
            y="box_office_gross_usd",
            size="domestic_box_office_gross",
            size_max=40,
            hover_name="movie_title",
            labels={
                "release_year": "Release year",
                "box_office_gross_usd": "Worldwide box office (USD)",
                "domestic_box_office_gross": "Domestic box office (USD)",
            },
            title="Worldwide box office vs release year\n(bubble size = domestic box office)",
        )
        fig_scatter = _format_money_axis(fig_scatter, "y")
    fig_scatter = _style_fig(fig_scatter)

    # Top by worldwide box office
    dff_top = (
        dff.dropna(subset=["box_office_gross_usd"])
        .sort_values("box_office_gross_usd", ascending=False)
        .head(15)
    )
    if dff_top.empty:
        fig_top = px.bar(title="No worldwide box office data available")
    else:
        fig_top = px.bar(
            dff_top,
            x="movie_title",
            y="box_office_gross_usd",
            text="box_office_gross_usd",
            title="Top movies by worldwide box office",
        )
        fig_top.update_layout(xaxis_tickangle=-45)
        fig_top = _format_money_axis(fig_top, "y")
    fig_top = _style_fig(fig_top)

    return html.Div(
        [
            html.H2("Overview"),
            dcc.Graph(figure=fig_hist),
            dcc.Graph(figure=fig_scatter),
            dcc.Graph(figure=fig_top),
        ]
    )


def boxoffice_tab_content(dff: pd.DataFrame):
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

    if dff_roi_sorted.empty:
        fig_roi = px.bar(title="No movies with both budget and worldwide box office")
    else:
        fig_roi = px.bar(
            dff_roi_sorted,
            x="movie_title",
            y="roi",
            hover_data=["box_office_gross_usd", "production_budget_usd"],
            title="Top ROI (Box Office / Budget)",
        )
        fig_roi.update_layout(xaxis_tickangle=-45)
    fig_roi = _style_fig(fig_roi)

    fig_budget_scatter = px.scatter(
        dff_roi,
        x="production_budget_usd",
        y="box_office_gross_usd",
        hover_name="movie_title",
        labels={
            "production_budget_usd": "Production budget (USD)",
            "box_office_gross_usd": "Worldwide box office (USD)",
        },
        title="Budget vs worldwide box office",
    )
    fig_budget_scatter = _format_money_axis(fig_budget_scatter, "x")
    fig_budget_scatter = _format_money_axis(fig_budget_scatter, "y")
    fig_budget_scatter = _style_fig(fig_budget_scatter)

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
        color="release_year",
        hover_name="movie_title",
        labels={
            "critic_score": "Critic score",
            "audience_avg_score": "Audience score",
        },
        title="Critic vs audience scores",
    )
    fig_ratings = _style_fig(fig_ratings)

    dff_top_audience = (
        dff.dropna(subset=["audience_avg_score"])
        .sort_values("audience_avg_score", ascending=False)
        .head(15)
    )
    if dff_top_audience.empty:
        fig_top_audience = px.bar(title="No audience score data available")
    else:
        # Color bars by audience score
        fig_top_audience = px.bar(
            dff_top_audience,
            x="movie_title",
            y="audience_avg_score",
            color="audience_avg_score",
            color_continuous_scale="Blues",
            title="Top movies by audience score",
        )
        fig_top_audience.update_layout(xaxis_tickangle=-45)
    fig_top_audience = _style_fig(fig_top_audience)

    return html.Div(
        [
            html.H2("Ratings"),
            dcc.Graph(figure=fig_ratings),
            dcc.Graph(figure=fig_top_audience),
        ]
    )


# ------------------------------------------------------------------------------
# MAIN
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    logger.info("Starting Dash server on http://127.0.0.1:8050/")
    app.run(debug=True)
