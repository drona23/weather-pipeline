"""
Main weather data pipeline orchestrator.

This module coordinates the entire ETL pipeline:
1. Data ingestion from OpenWeatherMap API
2. Data transformation and cleaning
3. Database loading and logging
"""

import uuid
from datetime import datetime
from typing import List, Dict, Optional
import sys
import os
import pandas as pd

# Add current directory to path for imports
sys.path.append(os.path.dirname(__file__))

from src.ingestion.weather_api import WeatherAPIClient
from src.transformation.weather_transformer import WeatherDataTransformer
from src.database.operations import WeatherDatabaseManager
from src.utils.config import config
from src.utils.logger import get_logger

logger = get_logger(__name__)


class WeatherDataPipeline:
    """Main orchestrator for the weather data pipeline."""
    
    def __init__(self):
        """Initialize the weather data pipeline."""
        self.api_client = WeatherAPIClient()
        self.transformer = WeatherDataTransformer()
        self.db_manager = WeatherDatabaseManager()
        self.run_id = str(uuid.uuid4())
    
    def run_pipeline(self, cities: Optional[List[str]] = None) -> bool:
        """
        Run the complete weather data pipeline.
        
        Args:
            cities: List of cities to fetch weather for (uses config if None)
            
        Returns:
            bool: True if pipeline completed successfully, False otherwise
        """
        start_time = datetime.utcnow()
        logger.info(f"🚀 Starting weather data pipeline - Run ID: {self.run_id}")
        
        try:
            # Use configured cities if none provided
            if cities is None:
                cities = config.CITIES
            
            logger.info(f"Processing weather data for cities: {', '.join(cities)}")
            
            # Step 1: Data Ingestion
            logger.info("📥 Step 1: Data Ingestion")
            weather_data = self._ingest_data(cities)
            
            if not weather_data:
                logger.error("❌ No weather data retrieved. Pipeline failed.")
                self._log_pipeline_run(len(cities), 0, 'failed', "No data retrieved")
                return False
            
            # Step 2: Data Transformation
            logger.info("🔄 Step 2: Data Transformation")
            transformed_data = self._transform_data(weather_data)
            
            if transformed_data.empty:
                logger.error("❌ No valid data after transformation. Pipeline failed.")
                self._log_pipeline_run(len(cities), 0, 'failed', "No valid data after transformation")
                return False
            
            # Step 3: Data Loading
            logger.info("💾 Step 3: Data Loading")
            load_success = self._load_data(transformed_data)
            
            if not load_success:
                logger.error("❌ Failed to load data into database. Pipeline failed.")
                self._log_pipeline_run(len(cities), len(transformed_data), 'failed', "Database load failed")
                return False
            
            # Step 4: Logging and Cleanup
            logger.info("📊 Step 4: Logging and Cleanup")
            self._log_pipeline_run(len(cities), len(transformed_data), 'completed')
            
            # Calculate execution time
            end_time = datetime.utcnow()
            execution_time = (end_time - start_time).total_seconds()
            
            logger.info(f"✅ Pipeline completed successfully!")
            logger.info(f"📈 Summary:")
            logger.info(f"   - Cities processed: {len(cities)}")
            logger.info(f"   - Records processed: {len(transformed_data)}")
            logger.info(f"   - Execution time: {execution_time:.2f} seconds")
            logger.info(f"   - Run ID: {self.run_id}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Pipeline failed with error: {str(e)}")
            self._log_pipeline_run(len(cities) if cities else 0, 0, 'failed', str(e))
            return False
    
    def _ingest_data(self, cities: List[str]) -> List[Dict]:
        """
        Ingest weather data from API.
        
        Args:
            cities: List of cities to fetch weather for
            
        Returns:
            List[Dict]: Raw weather data
        """
        try:
            # Test API connection first
            if not self.api_client.test_connection():
                logger.error("❌ API connection test failed")
                return []
            
            # Fetch weather data for all cities
            weather_data = self.api_client.get_weather_for_cities(cities)
            
            if weather_data:
                logger.info(f"✅ Successfully ingested weather data for {len(weather_data)} cities")
            else:
                logger.warning("⚠️ No weather data retrieved from API")
            
            return weather_data
            
        except Exception as e:
            logger.error(f"❌ Error during data ingestion: {str(e)}")
            return []
    
    def _transform_data(self, weather_data: List[Dict]) -> 'pd.DataFrame':
        """
        Transform and clean weather data.
        
        Args:
            weather_data: Raw weather data from API
            
        Returns:
            pd.DataFrame: Transformed and cleaned data
        """
        try:
            # Transform the data
            transformed_data = self.transformer.transform_weather_data(weather_data)
            
            if not transformed_data.empty:
                # Generate data quality report
                quality_report = self.transformer.get_data_quality_report(transformed_data)
                logger.info(f"📊 Data Quality Report:")
                logger.info(f"   - Total records: {quality_report['total_records']}")
                
                # Log data quality metrics
                self.db_manager.log_data_quality(self.run_id, quality_report)
                
                # Save to CSV for backup
                csv_path = f"data/weather_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                os.makedirs("data", exist_ok=True)
                self.transformer.save_to_csv(transformed_data, csv_path)
                
                logger.info(f"✅ Data transformation completed successfully")
            else:
                logger.warning("⚠️ No data after transformation")
            
            return transformed_data
            
        except Exception as e:
            logger.error(f"❌ Error during data transformation: {str(e)}")
            return pd.DataFrame()
    
    def _load_data(self, transformed_data: 'pd.DataFrame') -> bool:
        """
        Load transformed data into database.
        
        Args:
            transformed_data: Transformed weather data DataFrame
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Load data into database
            success = self.db_manager.load_weather_data(transformed_data, self.run_id)
            
            if success:
                logger.info(f"✅ Successfully loaded {len(transformed_data)} records into database")
            else:
                logger.error("❌ Failed to load data into database")
            
            return success
            
        except Exception as e:
            logger.error(f"❌ Error during data loading: {str(e)}")
            return False
    
    def _log_pipeline_run(self, cities_processed: int, records_processed: int,
                         status: str, error_message: str = None):
        """
        Log pipeline execution details.
        
        Args:
            cities_processed: Number of cities processed
            records_processed: Number of records processed
            status: Pipeline status
            error_message: Error message if failed
        """
        try:
            self.db_manager.log_pipeline_run(
                self.run_id, cities_processed, records_processed, status, error_message
            )
        except Exception as e:
            logger.error(f"Error logging pipeline run: {str(e)}")
    
    def get_recent_data(self, city: Optional[str] = None, hours: int = 24) -> pd.DataFrame:
        """
        Get recent weather data from database.
        
        Args:
            city: Filter by specific city (optional)
            hours: Number of hours to look back
            
        Returns:
            pd.DataFrame: Recent weather data
        """
        return self.db_manager.get_recent_weather_data(city, hours)
    
    def get_statistics(self, city: Optional[str] = None, days: int = 7) -> Dict:
        """
        Get weather statistics from database.
        
        Args:
            city: Filter by specific city (optional)
            days: Number of days to analyze
            
        Returns:
            Dict: Weather statistics
        """
        return self.db_manager.get_weather_statistics(city, days)


def main():
    """Main entry point for the weather data pipeline."""
    try:
        # Validate configuration
        if not config.validate():
            logger.error("❌ Configuration validation failed. Please check your .env file.")
            return False
        
        # Create and run pipeline
        pipeline = WeatherDataPipeline()
        success = pipeline.run_pipeline()
        
        if success:
            # Show some statistics
            stats = pipeline.get_statistics(days=1)
            if stats:
                logger.info("📊 Recent Weather Statistics:")
                logger.info(f"   - Total records: {stats.get('total_records', 0)}")
                logger.info(f"   - Cities: {', '.join(stats.get('cities', []))}")
                if stats.get('temperature', {}).get('avg'):
                    logger.info(f"   - Avg temperature: {stats['temperature']['avg']:.1f}°C")
        
        return success
        
    except Exception as e:
        logger.error(f"❌ Unexpected error in main: {str(e)}")
        return False


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1) 