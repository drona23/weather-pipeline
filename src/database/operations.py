"""
Database operations for the weather data pipeline.

This module handles all database operations including data loading,
querying, and database management.
"""

import pandas as pd
from typing import List, Dict, Optional
from datetime import datetime, timedelta
import uuid
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from src.database.models import (
    WeatherRecord, DataQualityLog, PipelineRun,
    create_database_engine, create_database_session, create_tables
)
from src.utils.logger import get_logger

logger = get_logger(__name__)


class WeatherDatabaseManager:
    """Manage database operations for weather data."""
    
    def __init__(self):
        """Initialize the database manager."""
        self.engine = create_database_engine()
        self.SessionLocal = create_database_session()
        self._ensure_tables_exist()
    
    def _ensure_tables_exist(self):
        """Ensure all required tables exist in the database."""
        try:
            create_tables(self.engine)
        except Exception as e:
            logger.error(f"Error ensuring tables exist: {str(e)}")
            raise
    
    def load_weather_data(self, df: pd.DataFrame, run_id: str) -> bool:
        """
        Load weather data from DataFrame into database.
        
        Args:
            df: Weather data DataFrame
            run_id: Unique identifier for this pipeline run
            
        Returns:
            bool: True if successful, False otherwise
        """
        if df.empty:
            logger.warning("No data to load into database")
            return False
        
        session = self.SessionLocal()
        try:
            records_created = 0
            
            for _, row in df.iterrows():
                # Convert pandas row to dictionary
                weather_data = row.to_dict()
                
                # Create WeatherRecord object
                weather_record = WeatherRecord(
                    city=weather_data.get('city'),
                    country=weather_data.get('country'),
                    latitude=weather_data.get('latitude'),
                    longitude=weather_data.get('longitude'),
                    timestamp=weather_data.get('timestamp'),
                    sunrise=weather_data.get('sunrise'),
                    sunset=weather_data.get('sunset'),
                    temperature=weather_data.get('temperature'),
                    feels_like=weather_data.get('feels_like'),
                    humidity=weather_data.get('humidity'),
                    pressure=weather_data.get('pressure'),
                    wind_speed=weather_data.get('wind_speed'),
                    wind_direction=weather_data.get('wind_direction'),
                    description=weather_data.get('description'),
                    weather_main=weather_data.get('weather_main'),
                    visibility=weather_data.get('visibility'),
                    clouds=weather_data.get('clouds'),
                    date=weather_data.get('date'),
                    hour=weather_data.get('hour'),
                    day_of_week=weather_data.get('day_of_week'),
                    temperature_category=weather_data.get('temperature_category'),
                    humidity_category=weather_data.get('humidity_category'),
                    wind_category=weather_data.get('wind_category'),
                    season=weather_data.get('season')
                )
                
                session.add(weather_record)
                records_created += 1
            
            session.commit()
            logger.info(f"Successfully loaded {records_created} weather records into database")
            return True
            
        except Exception as e:
            session.rollback()
            logger.error(f"Error loading weather data into database: {str(e)}")
            return False
        finally:
            session.close()
    
    def log_data_quality(self, run_id: str, quality_report: Dict) -> bool:
        """
        Log data quality metrics to database.
        
        Args:
            run_id: Unique identifier for this pipeline run
            quality_report: Data quality report dictionary
            
        Returns:
            bool: True if successful, False otherwise
        """
        session = self.SessionLocal()
        try:
            total_records = quality_report.get('total_records', 0)
            valid_records = total_records  # Simplified for now
            invalid_records = 0
            quality_score = 100.0 if total_records > 0 else 0.0
            
            quality_log = DataQualityLog(
                run_id=run_id,
                total_records=total_records,
                valid_records=valid_records,
                invalid_records=invalid_records,
                quality_score=quality_score,
                success=True
            )
            
            session.add(quality_log)
            session.commit()
            logger.info(f"Data quality logged for run {run_id}")
            return True
            
        except Exception as e:
            session.rollback()
            logger.error(f"Error logging data quality: {str(e)}")
            return False
        finally:
            session.close()
    
    def log_pipeline_run(self, run_id: str, cities_processed: int, 
                        records_processed: int, status: str = 'completed',
                        error_message: Optional[str] = None) -> bool:
        """
        Log pipeline execution details to database.
        
        Args:
            run_id: Unique identifier for this pipeline run
            cities_processed: Number of cities processed
            records_processed: Number of records processed
            status: Pipeline status ('running', 'completed', 'failed')
            error_message: Error message if pipeline failed
            
        Returns:
            bool: True if successful, False otherwise
        """
        session = self.SessionLocal()
        try:
            pipeline_run = PipelineRun(
                run_id=run_id,
                start_time=datetime.utcnow(),
                end_time=datetime.utcnow() if status != 'running' else None,
                status=status,
                cities_processed=cities_processed,
                records_processed=records_processed,
                error_message=error_message
            )
            
            session.add(pipeline_run)
            session.commit()
            logger.info(f"Pipeline run logged: {run_id} - {status}")
            return True
            
        except Exception as e:
            session.rollback()
            logger.error(f"Error logging pipeline run: {str(e)}")
            return False
        finally:
            session.close()
    
    def get_recent_weather_data(self, city: Optional[str] = None, 
                               hours: int = 24) -> pd.DataFrame:
        """
        Get recent weather data from database.
        
        Args:
            city: Filter by specific city (optional)
            hours: Number of hours to look back
            
        Returns:
            pd.DataFrame: Recent weather data
        """
        session = self.SessionLocal()
        try:
            # Calculate time threshold
            time_threshold = datetime.utcnow() - timedelta(hours=hours)
            
            # Build query
            query = session.query(WeatherRecord).filter(
                WeatherRecord.timestamp >= time_threshold
            )
            
            if city:
                query = query.filter(WeatherRecord.city == city)
            
            # Execute query
            records = query.all()
            
            # Convert to DataFrame
            data = []
            for record in records:
                data.append({
                    'city': record.city,
                    'timestamp': record.timestamp,
                    'temperature': record.temperature,
                    'humidity': record.humidity,
                    'pressure': record.pressure,
                    'wind_speed': record.wind_speed,
                    'description': record.description,
                    'temperature_category': record.temperature_category,
                    'season': record.season
                })
            
            df = pd.DataFrame(data)
            logger.info(f"Retrieved {len(df)} recent weather records")
            return df
            
        except Exception as e:
            logger.error(f"Error retrieving weather data: {str(e)}")
            return pd.DataFrame()
        finally:
            session.close()
    
    def get_weather_statistics(self, city: Optional[str] = None, 
                              days: int = 7) -> Dict:
        """
        Get weather statistics from database.
        
        Args:
            city: Filter by specific city (optional)
            days: Number of days to analyze
            
        Returns:
            Dict: Weather statistics
        """
        session = self.SessionLocal()
        try:
            # Calculate time threshold
            time_threshold = datetime.utcnow() - timedelta(days=days)
            
            # Build query
            query = session.query(WeatherRecord).filter(
                WeatherRecord.timestamp >= time_threshold
            )
            
            if city:
                query = query.filter(WeatherRecord.city == city)
            
            # Execute query
            records = query.all()
            
            if not records:
                return {}
            
            # Calculate statistics
            temperatures = [r.temperature for r in records if r.temperature is not None]
            humidities = [r.humidity for r in records if r.humidity is not None]
            pressures = [r.pressure for r in records if r.pressure is not None]
            
            stats = {
                'total_records': len(records),
                'cities': list(set([r.city for r in records])),
                'temperature': {
                    'min': min(temperatures) if temperatures else None,
                    'max': max(temperatures) if temperatures else None,
                    'avg': sum(temperatures) / len(temperatures) if temperatures else None
                },
                'humidity': {
                    'min': min(humidities) if humidities else None,
                    'max': max(humidities) if humidities else None,
                    'avg': sum(humidities) / len(humidities) if humidities else None
                },
                'pressure': {
                    'min': min(pressures) if pressures else None,
                    'max': max(pressures) if pressures else None,
                    'avg': sum(pressures) / len(pressures) if pressures else None
                }
            }
            
            logger.info(f"Calculated weather statistics for {len(records)} records")
            return stats
            
        except Exception as e:
            logger.error(f"Error calculating weather statistics: {str(e)}")
            return {}
        finally:
            session.close()
    
    def cleanup_old_data(self, days: int = 30) -> int:
        """
        Clean up old weather data from database.
        
        Args:
            days: Number of days to keep
            
        Returns:
            int: Number of records deleted
        """
        session = self.SessionLocal()
        try:
            # Calculate cutoff date
            cutoff_date = datetime.utcnow() - timedelta(days=days)
            
            # Delete old records
            deleted_count = session.query(WeatherRecord).filter(
                WeatherRecord.timestamp < cutoff_date
            ).delete()
            
            session.commit()
            logger.info(f"Cleaned up {deleted_count} old weather records")
            return deleted_count
            
        except Exception as e:
            session.rollback()
            logger.error(f"Error cleaning up old data: {str(e)}")
            return 0
        finally:
            session.close() 