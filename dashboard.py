"""
Weather Pipeline Dashboard
==========================
A live Streamlit dashboard that reads directly from the PostgreSQL database
populated by the hourly ETL pipeline.

Run with:
    streamlit run dashboard.py

Sections:
    1. Live City Cards     - current conditions for each city
    2. Temperature Chart   - side-by-side city comparison
    3. Trends Over Time    - temperature history line chart
    4. Pipeline Health     - run success rate and data volume
"""

import sys
import os
import warnings
warnings.filterwarnings("ignore")

sys.path.insert(0, os.path.dirname(__file__))

import streamlit as st
import streamlit.components.v1 as components
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from datetime import datetime, timedelta

from src.database.operations import WeatherDatabaseManager

# ── Page config ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Weather Pipeline Dashboard",
    page_icon="🌤️",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── Styling ───────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    .metric-card {
        background: #1e1e2e;
        border-radius: 12px;
        padding: 16px;
        border: 1px solid #313244;
    }
    .section-header {
        font-size: 1.1rem;
        font-weight: 600;
        color: #cdd6f4;
        margin-bottom: 0.5rem;
    }
</style>
""", unsafe_allow_html=True)

# ── Animated pipeline diagram ────────────────────────────────────────────────
def render_pipeline_flow():
    html = """
    <style>
      .pipeline-wrap {
        display: flex;
        align-items: center;
        justify-content: center;
        padding: 28px 16px;
        background: #11111b;
        border-radius: 14px;
        border: 1px solid #313244;
        overflow: hidden;
        gap: 0;
      }

      /* ── Stage boxes ── */
      .stage {
        display: flex;
        flex-direction: column;
        align-items: center;
        justify-content: center;
        width: 110px;
        padding: 12px 8px;
        border-radius: 10px;
        border: 1.5px solid;
        text-align: center;
        flex-shrink: 0;
        z-index: 2;
      }
      .stage .icon  { font-size: 1.5rem; margin-bottom: 4px; }
      .stage .label { font-size: 0.72rem; font-weight: 700; letter-spacing: .04em; color: #cdd6f4; }
      .stage .sub   { font-size: 0.6rem;  color: #a6adc8; margin-top: 3px; }

      .s1 { background:#1a1a2e; border-color:#89b4fa; }
      .s2 { background:#1a1a2e; border-color:#a6e3a1; }
      .s3 { background:#1a1a2e; border-color:#f9e2af; }
      .s4 { background:#1a1a2e; border-color:#cba6f7; }
      .s5 { background:#1a1a2e; border-color:#89dceb; }

      /* ── Connector track ── */
      .connector {
        position: relative;
        flex: 1;
        height: 4px;
        background: #313244;
        border-radius: 2px;
        overflow: visible;
        min-width: 30px;
      }

      /* ── Animated dot ── */
      .dot {
        position: absolute;
        top: 50%;
        left: -8px;
        transform: translateY(-50%);
        width: 12px;
        height: 12px;
        border-radius: 50%;
        animation: flow 2s linear infinite;
      }
      .dot.d1 { background: #89b4fa; animation-delay: 0s;   }
      .dot.d2 { background: #a6e3a1; animation-delay: 0.5s; }
      .dot.d3 { background: #f9e2af; animation-delay: 1s;   }
      .dot.d4 { background: #cba6f7; animation-delay: 1.5s; }

      @keyframes flow {
        0%   { left: -8px;   opacity: 0;   }
        10%  { opacity: 1; }
        90%  { opacity: 1; }
        100% { left: calc(100% + 8px); opacity: 0; }
      }

      /* ── Data label on hover ── */
      .connector:hover .data-label {
        opacity: 1;
      }
      .data-label {
        opacity: 0;
        transition: opacity 0.2s;
        position: absolute;
        top: -26px;
        left: 50%;
        transform: translateX(-50%);
        background: #1e1e2e;
        border: 1px solid #45475a;
        border-radius: 5px;
        padding: 2px 7px;
        font-size: 0.58rem;
        color: #a6adc8;
        white-space: nowrap;
        pointer-events: none;
        z-index: 10;
      }
    </style>

    <div class="pipeline-wrap">

      <!-- Stage 1: API -->
      <div class="stage s1">
        <div class="icon">🌐</div>
        <div class="label">OpenWeatherMap</div>
        <div class="sub">28 locations</div>
      </div>

      <div class="connector">
        <div class="dot d1"></div>
        <div class="data-label">Raw JSON</div>
      </div>

      <!-- Stage 2: Extract -->
      <div class="stage s2">
        <div class="icon">📥</div>
        <div class="label">Extract</div>
        <div class="sub">lat/lon lookup</div>
      </div>

      <div class="connector">
        <div class="dot d2"></div>
        <div class="data-label">List[Dict]</div>
      </div>

      <!-- Stage 3: Transform -->
      <div class="stage s3">
        <div class="icon">⚙️</div>
        <div class="label">Transform</div>
        <div class="sub">clean + enrich</div>
      </div>

      <div class="connector">
        <div class="dot d3"></div>
        <div class="data-label">DataFrame</div>
      </div>

      <!-- Stage 4: Load -->
      <div class="stage s4">
        <div class="icon">💾</div>
        <div class="label">Load</div>
        <div class="sub">insert rows</div>
      </div>

      <div class="connector">
        <div class="dot d4"></div>
        <div class="data-label">SQL INSERT</div>
      </div>

      <!-- Stage 5: PostgreSQL -->
      <div class="stage s5">
        <div class="icon">🗄️</div>
        <div class="label">PostgreSQL</div>
        <div class="sub">weather_records</div>
      </div>

    </div>
    <p style="text-align:center; font-size:0.65rem; color:#585b70; margin-top:8px;">
      Hover over each arrow to see what data is passed between stages
    </p>
    """
    components.html(html, height=160)

# ── Data loading ──────────────────────────────────────────────────────────────
@st.cache_data(ttl=300)   # cache for 5 minutes, then re-query
def load_weather_data(hours: int = 168) -> pd.DataFrame:
    db = WeatherDatabaseManager()
    return db.get_recent_weather_data(hours=hours)

@st.cache_data(ttl=300)
def load_pipeline_runs() -> pd.DataFrame:
    from src.database.models import create_database_session, PipelineRun
    SessionLocal = create_database_session()
    session = SessionLocal()
    try:
        runs = session.query(PipelineRun).order_by(PipelineRun.start_time.desc()).limit(50).all()
        if not runs:
            return pd.DataFrame()
        return pd.DataFrame([{
            "run_id":            r.run_id,
            "start_time":        r.start_time,
            "status":            r.status,
            "cities_processed":  r.cities_processed,
            "records_processed": r.records_processed,
            "error_message":     r.error_message,
        } for r in runs])
    finally:
        session.close()

# ── Header ────────────────────────────────────────────────────────────────────
st.title("🌤️ Weather Pipeline Dashboard")
st.caption(
    "Live data from the hourly ETL pipeline · "
    f"Page loaded at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
)
render_pipeline_flow()
st.divider()

# ── Load data ─────────────────────────────────────────────────────────────────
df = load_weather_data(hours=168)
runs_df = load_pipeline_runs()

if df.empty:
    st.error(
        "No weather data found in the database. "
        "Run the pipeline first: `python -m src.main`"
    )
    st.stop()

# Ensure timestamp is datetime
df["timestamp"] = pd.to_datetime(df["timestamp"])

# ── SECTION 1 - Live City Cards ───────────────────────────────────────────────
st.markdown("### 📍 Current Conditions")
st.caption("Most recent reading per city")

latest = (
    df.sort_values("timestamp", ascending=False)
    .groupby("city")
    .first()
    .reset_index()
)

cols = st.columns(len(latest))

WEATHER_ICONS = {
    "Clear":        "☀️",
    "Clouds":       "☁️",
    "Rain":         "🌧️",
    "Drizzle":      "🌦️",
    "Thunderstorm": "⛈️",
    "Snow":         "❄️",
    "Mist":         "🌫️",
    "Fog":          "🌫️",
    "Haze":         "🌫️",
}

for col, (_, row) in zip(cols, latest.iterrows()):
    icon = WEATHER_ICONS.get(row.get("weather_main", ""), "🌡️")
    with col:
        st.metric(
            label=f"{icon} {row['city']}, {row.get('country', '')}",
            value=f"{row['temperature']:.1f}°C",
            delta=f"Feels like {row['feels_like']:.1f}°C" if pd.notna(row.get('feels_like')) else None,
        )
        st.caption(
            f"💧 {row['humidity']:.0f}%  "
            f"💨 {row['wind_speed']:.1f} m/s  "
            f"🏷️ {row.get('temperature_category', '')}  "
            f"🍂 {row.get('season', '')}"
        )

st.divider()

# ── SECTION 2 - Temperature Comparison ───────────────────────────────────────
st.markdown("### 🌡️ Temperature Comparison Across Cities")

col_left, col_right = st.columns(2)

with col_left:
    # Bar chart: avg temp per city
    avg_temp = (
        df.groupby("city")["temperature"]
        .agg(["mean", "min", "max"])
        .reset_index()
        .rename(columns={"mean": "avg", "min": "min_temp", "max": "max_temp"})
        .sort_values("avg", ascending=True)
    )

    fig_bar = px.bar(
        avg_temp,
        x="avg",
        y="city",
        orientation="h",
        title="Average Temperature (all collected data)",
        labels={"avg": "Avg Temp (°C)", "city": ""},
        color="avg",
        color_continuous_scale="RdYlBu_r",
        error_x=avg_temp["max_temp"] - avg_temp["avg"],
    )
    fig_bar.update_layout(
        height=300,
        coloraxis_showscale=False,
        margin=dict(l=0, r=0, t=40, b=0),
    )
    st.plotly_chart(fig_bar, width='stretch')

with col_right:
    # Humidity vs Temperature scatter
    fig_scatter = px.scatter(
        latest,
        x="temperature",
        y="humidity",
        text="city",
        title="Humidity vs Temperature (latest reading)",
        labels={"temperature": "Temperature (°C)", "humidity": "Humidity (%)"},
        color="temperature",
        color_continuous_scale="RdYlBu_r",
        size_max=20,
    )
    fig_scatter.update_traces(textposition="top center", marker_size=14)
    fig_scatter.update_layout(
        height=300,
        coloraxis_showscale=False,
        margin=dict(l=0, r=0, t=40, b=0),
    )
    st.plotly_chart(fig_scatter, width='stretch')

st.divider()

# ── SECTION 3 - Temperature Trends ───────────────────────────────────────────
st.markdown("### 📈 Temperature Over Time")

cities_available = sorted(df["city"].unique().tolist())
selected_cities = st.multiselect(
    "Select cities to display",
    options=cities_available,
    default=cities_available,
)

if selected_cities:
    filtered = df[df["city"].isin(selected_cities)].sort_values("timestamp")

    fig_line = px.line(
        filtered,
        x="timestamp",
        y="temperature",
        color="city",
        title="Temperature History",
        labels={"timestamp": "Time", "temperature": "Temperature (°C)", "city": "City"},
        markers=True,
    )
    fig_line.update_layout(
        height=380,
        hovermode="x unified",
        margin=dict(l=0, r=0, t=40, b=0),
    )
    st.plotly_chart(fig_line, width='stretch')

    # Wind speed chart
    fig_wind = px.line(
        filtered,
        x="timestamp",
        y="wind_speed",
        color="city",
        title="Wind Speed History",
        labels={"timestamp": "Time", "wind_speed": "Wind Speed (m/s)", "city": "City"},
    )
    fig_wind.update_layout(
        height=280,
        hovermode="x unified",
        margin=dict(l=0, r=0, t=40, b=0),
    )
    st.plotly_chart(fig_wind, width='stretch')

st.divider()

# ── SECTION 4 - Pipeline Health ───────────────────────────────────────────────
st.markdown("### ⚙️ Pipeline Health")

if runs_df.empty:
    st.info("No pipeline run history found.")
else:
    runs_df["start_time"] = pd.to_datetime(runs_df["start_time"])

    h1, h2, h3, h4 = st.columns(4)

    total_runs      = len(runs_df)
    successful_runs = (runs_df["status"] == "completed").sum()
    failed_runs     = (runs_df["status"] == "failed").sum()
    success_rate    = successful_runs / total_runs * 100 if total_runs else 0
    total_records   = runs_df[runs_df["status"] == "completed"]["records_processed"].sum()

    h1.metric("Total Runs",       total_runs)
    h2.metric("Successful",       successful_runs)
    h3.metric("Failed",           failed_runs)
    h4.metric("Success Rate",     f"{success_rate:.1f}%")

    st.caption(f"Total records collected across all successful runs: **{total_records:,}**")

    # Run history table
    display_runs = runs_df[["start_time", "status", "cities_processed",
                             "records_processed", "error_message"]].copy()
    display_runs["start_time"] = display_runs["start_time"].dt.strftime("%Y-%m-%d %H:%M")
    display_runs.columns = ["Time", "Status", "Cities", "Records", "Error"]

    def colour_status(val):
        if val == "completed":
            return "color: #a6e3a1"
        elif val == "failed":
            return "color: #f38ba8"
        return ""

    st.dataframe(
        display_runs.style.applymap(colour_status, subset=["Status"]),
        width='stretch',
        height=280,
    )

st.divider()

# ── Footer ────────────────────────────────────────────────────────────────────
st.caption(
    "Weather Pipeline · Built by Drona Gangarapu · "
    "Data source: OpenWeatherMap API · "
    "Part of the AI Data Center Carbon & Water Footprint Predictor capstone project"
)
