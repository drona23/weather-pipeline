"""
Weather ETL DAG

Runs hourly to ingest, transform, and load weather data for configured cities.
Uses TaskFlow API (Airflow 2.x) to wire up the existing pipeline components.

Pipeline stages:
    1. check_api_connection  - validate OpenWeatherMap API is reachable
    2. ingest_weather_data   - fetch raw weather JSON for all cities
    3. transform_weather_data - clean, validate, add derived columns
    4. load_to_database      - persist to PostgreSQL + log data quality
    5. log_pipeline_summary  - record final run metadata
"""

import sys
import os
import uuid
import json
import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

# Ensure the project root is on the path so src.* imports resolve inside Airflow
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

log = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "drona",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

@dag(
    dag_id="weather_etl_hourly",
    description="Hourly ETL: OpenWeatherMap API (28 capstone data center locations) → PostgreSQL",
    schedule_interval="@hourly",
    start_date=days_ago(1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["weather", "etl", "postgres", "capstone"],
)
def weather_etl_pipeline():

    @task()
    def check_api_connection() -> bool:
        """Validate the OpenWeatherMap API is reachable before doing any work."""
        from src.ingestion.weather_api import WeatherAPIClient

        client = WeatherAPIClient()
        connected = client.test_connection()
        if not connected:
            raise ConnectionError("OpenWeatherMap API connection check failed.")
        log.info("API connection check passed.")
        return connected

    @task()
    def load_locations() -> list:
        """
        Load the 28 capstone data center locations from config/capstone_cities.csv.
        Returns a list of dicts (city, state, latitude, longitude, category).
        """
        from src.utils.config import config

        locations = config.CAPSTONE_LOCATIONS
        if not locations:
            raise ValueError("No locations found in config/capstone_cities.csv.")

        log.info("Loaded %d capstone locations.", len(locations))
        return locations

    @task()
    def ingest_weather_data(locations: list) -> list:
        """
        Fetch current weather for each location using GPS coordinates.
        Coordinate-based lookup works for counties and townships that
        OpenWeatherMap cannot find by name.
        Returns raw weather records as a list of dicts (XCom-serializable).
        """
        from src.ingestion.weather_api import WeatherAPIClient

        client = WeatherAPIClient()
        records = client.get_weather_for_locations(locations)

        if not records:
            raise ValueError("No weather data returned for capstone locations.")

        log.info("Ingested %d records for %d locations.", len(records), len(locations))
        return records

    @task()
    def transform_weather_data(raw_records: list) -> dict:
        """
        Clean and enrich raw records.
        Returns a dict with 'records' (list of dicts) and 'quality_report' (dict).
        DataFrames are not XCom-safe, so we serialize to records here.
        """
        from src.transformation.weather_transformer import WeatherDataTransformer

        transformer = WeatherDataTransformer()
        df = transformer.transform_weather_data(raw_records)

        if df.empty:
            raise ValueError("Transformation produced an empty DataFrame.")

        quality_report = transformer.get_data_quality_report(df)
        log.info("Transformation complete. Records: %d", len(df))

        # XCom serializes via JSON - convert Timestamps to ISO strings so they survive
        raw_records_out = df.to_dict(orient="records")
        for row in raw_records_out:
            for k, v in row.items():
                if hasattr(v, "isoformat"):
                    row[k] = v.isoformat()

        return {
            "records": raw_records_out,
            "quality_report": quality_report,
        }

    @task()
    def load_to_database(transformed: dict, pipeline_run_id: str) -> int:
        """
        Load transformed records into PostgreSQL and log data quality metrics.
        Returns the number of rows loaded.
        Note: parameter is 'pipeline_run_id' not 'run_id' - Airflow reserves 'run_id'
        as a context keyword and will reject it if used as a task argument.
        """
        import pandas as pd
        from src.database.operations import WeatherDatabaseManager

        db = WeatherDatabaseManager()
        df = pd.DataFrame(transformed["records"])

        success = db.load_weather_data(df, pipeline_run_id)
        if not success:
            raise RuntimeError("Database load failed.")

        db.log_data_quality(pipeline_run_id, transformed["quality_report"])
        log.info("Loaded %d records (pipeline_run_id=%s).", len(df), pipeline_run_id)
        return len(df)

    @task()
    def log_pipeline_summary(locations: list, records_loaded: int, pipeline_run_id: str) -> None:
        """Record final pipeline run metadata to the pipeline_runs table."""
        from src.database.operations import WeatherDatabaseManager

        db = WeatherDatabaseManager()
        db.log_pipeline_run(
            run_id=pipeline_run_id,
            cities_processed=len(locations),
            records_processed=records_loaded,
            status="completed",
        )
        log.info(
            "Pipeline run logged. run_id=%s, locations=%d, records=%d",
            pipeline_run_id, len(locations), records_loaded,
        )

    # ── Wire up the DAG ──────────────────────────────────────────────────────
    pipeline_run_id = str(uuid.uuid4())

    api_ok         = check_api_connection()
    locs           = load_locations()
    raw            = ingest_weather_data.override(task_id="ingest")(locs)
    transformed    = transform_weather_data.override(task_id="transform")(raw)
    records_loaded = load_to_database.override(task_id="load")(transformed, pipeline_run_id)
    log_pipeline_summary.override(task_id="log_summary")(locs, records_loaded, pipeline_run_id)

    # Don't ingest if API check fails; load locations before ingesting
    api_ok >> locs >> raw


weather_etl_dag = weather_etl_pipeline()
