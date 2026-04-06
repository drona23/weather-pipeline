"""
Weather Maintenance DAG

Runs weekly to keep the database healthy:
    1. purge_old_records    - delete weather_records older than 30 days
    2. generate_weekly_report - compute and log aggregated stats per city
    3. export_weekly_csv    - dump the past week's data to a dated CSV backup

Separating maintenance from ETL is a best practice:
- ETL failures don't block cleanup, and vice versa.
- Retention policy is explicit and auditable.
"""

import sys
import os
import logging
from datetime import timedelta
from pathlib import Path

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

log = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "drona",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": False,
}

RETENTION_DAYS = int(os.getenv("RETENTION_DAYS", "30"))
BACKUP_DIR = os.getenv("BACKUP_DIR", "data/weekly_backups")


@dag(
    dag_id="weather_maintenance_weekly",
    description="Weekly: purge old records, generate stats report, export CSV backup",
    schedule_interval="@weekly",
    start_date=days_ago(1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["weather", "maintenance"],
)
def weather_maintenance_pipeline():

    @task()
    def purge_old_records(retention_days: int) -> int:
        """
        Delete records older than retention_days from weather_records.
        Returns the number of rows deleted.
        """
        from src.database.operations import WeatherDatabaseManager

        db = WeatherDatabaseManager()
        deleted = db.cleanup_old_data(days=retention_days)
        log.info("Purged %d records older than %d days.", deleted, retention_days)
        return deleted

    @task()
    def generate_weekly_report() -> dict:
        """
        Compute aggregated statistics for the past 7 days across all cities.
        Returns a stats dict that gets stored in XCom for audit purposes.
        """
        from src.database.operations import WeatherDatabaseManager

        db = WeatherDatabaseManager()
        stats = db.get_weather_statistics(days=7)

        if not stats:
            log.warning("No statistics returned for the past 7 days.")
            return {}

        log.info("Weekly stats - cities: %s", stats.get("cities", []))
        log.info(
            "Avg temperature: %.1f°C | Avg humidity: %.1f%%",
            stats.get("temperature", {}).get("avg", 0),
            stats.get("humidity", {}).get("avg", 0),
        )
        return stats

    @task()
    def export_weekly_csv() -> str:
        """
        Dump the past 7 days of weather data to a dated CSV backup file.
        Returns the output file path.
        """
        import pandas as pd
        from datetime import datetime
        from src.database.operations import WeatherDatabaseManager

        db = WeatherDatabaseManager()
        df = db.get_recent_weather_data(hours=7 * 24)

        if df.empty:
            log.warning("No data to export for the past week.")
            return ""

        Path(BACKUP_DIR).mkdir(parents=True, exist_ok=True)
        filename = f"weekly_backup_{datetime.now().strftime('%Y%m%d')}.csv"
        filepath = os.path.join(BACKUP_DIR, filename)
        df.to_csv(filepath, index=False)

        log.info("Exported %d rows to %s", len(df), filepath)
        return filepath

    # ── Wire up the DAG ──────────────────────────────────────────────────────
    # Run purge first, then generate report and export in parallel
    purged = purge_old_records(RETENTION_DAYS)
    report = generate_weekly_report()
    csv_path = export_weekly_csv()

    purged >> [report, csv_path]


weather_maintenance_dag = weather_maintenance_pipeline()
