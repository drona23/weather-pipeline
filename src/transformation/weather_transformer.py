"""
Data transformation module for weather data pipeline.

This module handles data cleaning, validation, and transformation
of weather data before loading into the database.
"""

import pandas as pd
import numpy as np
from typing import List, Dict, Optional
from datetime import datetime
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from src.utils.logger import get_logger

logger = get_logger(__name__)


class WeatherDataTransformer:
    """Transform and clean weather data for database storage."""
    
    def __init__(self):
        """Initialize the weather data transformer."""
        self.required_fields = ['city', 'timestamp', 'temperature']
        self.numeric_fields = [
            'temperature', 'feels_like', 'humidity', 'pressure',
            'wind_speed', 'wind_direction', 'visibility', 'clouds'
        ]
    
    def transform_weather_data(self, weather_data: List[Dict]) -> pd.DataFrame:
        """
        Transform list of weather data dictionaries to a pandas DataFrame.
        
        Args:
            weather_data: List of weather data dictionaries
            
        Returns:
            pd.DataFrame: Transformed and cleaned weather data
        """
        if not weather_data:
            logger.warning("No weather data provided for transformation")
            return pd.DataFrame()
        
        try:
            # Convert to DataFrame
            df = pd.DataFrame(weather_data)
            logger.info(f"Transforming {len(df)} weather records")
            
            # Clean and validate data
            df = self._clean_data(df)
            df = self._validate_data(df)
            df = self._add_derived_columns(df)
            
            logger.info(f"Successfully transformed {len(df)} weather records")
            return df
            
        except Exception as e:
            logger.error(f"Error transforming weather data: {str(e)}")
            return pd.DataFrame()
    
    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean the weather data by handling missing values and data types.
        
        Args:
            df: Raw weather data DataFrame
            
        Returns:
            pd.DataFrame: Cleaned DataFrame
        """
        # Create a copy to avoid modifying original data
        df_clean = df.copy()
        
        # Handle missing values
        for field in self.numeric_fields:
            if field in df_clean.columns:
                # Fill missing numeric values with NaN
                df_clean[field] = pd.to_numeric(df_clean[field], errors='coerce')
        
        # Convert timestamp to datetime
        if 'timestamp' in df_clean.columns:
            df_clean['timestamp'] = pd.to_datetime(df_clean['timestamp'], errors='coerce')
        
        # Handle sunrise/sunset timestamps (Unix timestamps)
        for field in ['sunrise', 'sunset']:
            if field in df_clean.columns:
                df_clean[field] = pd.to_datetime(df_clean[field], unit='s', errors='coerce')
        
        # Clean string fields
        string_fields = ['city', 'description', 'weather_main', 'country']
        for field in string_fields:
            if field in df_clean.columns:
                df_clean[field] = df_clean[field].astype(str).str.strip()
                # Replace empty strings with None
                df_clean[field] = df_clean[field].replace('', None)
        
        logger.info("Data cleaning completed")
        return df_clean
    
    def _validate_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Validate weather data and remove invalid records.
        
        Args:
            df: Cleaned weather data DataFrame
            
        Returns:
            pd.DataFrame: Validated DataFrame
        """
        initial_count = len(df)
        
        # Remove records with missing required fields
        for field in self.required_fields:
            if field in df.columns:
                df = df.dropna(subset=[field])
        
        # Validate temperature range (reasonable weather temperatures)
        if 'temperature' in df.columns:
            df = df[
                (df['temperature'] >= -50) & 
                (df['temperature'] <= 60)
            ]
        
        # Validate humidity range (0-100%)
        if 'humidity' in df.columns:
            df = df[
                (df['humidity'] >= 0) & 
                (df['humidity'] <= 100)
            ]
        
        # Validate pressure range (reasonable atmospheric pressure)
        if 'pressure' in df.columns:
            df = df[
                (df['pressure'] >= 800) & 
                (df['pressure'] <= 1200)
            ]
        
        # Validate wind speed (reasonable wind speeds)
        if 'wind_speed' in df.columns:
            df = df[df['wind_speed'] >= 0]
        
        # Validate wind direction (0-360 degrees)
        if 'wind_direction' in df.columns:
            df = df[
                (df['wind_direction'] >= 0) & 
                (df['wind_direction'] <= 360)
            ]
        
        removed_count = initial_count - len(df)
        if removed_count > 0:
            logger.warning(f"Removed {removed_count} invalid records during validation")
        
        logger.info(f"Data validation completed. {len(df)} valid records remaining")
        return df
    
    def _add_derived_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add derived columns for analysis.
        
        Args:
            df: Validated weather data DataFrame
            
        Returns:
            pd.DataFrame: DataFrame with derived columns
        """
        # Add date columns for easier analysis
        if 'timestamp' in df.columns:
            df['date'] = df['timestamp'].dt.date
            df['hour'] = df['timestamp'].dt.hour
            df['day_of_week'] = df['timestamp'].dt.day_name()
        
        # Add temperature categories
        if 'temperature' in df.columns:
            df['temperature_category'] = pd.cut(
                df['temperature'],
                bins=[-np.inf, 0, 10, 20, 30, np.inf],
                labels=['Freezing', 'Cold', 'Cool', 'Warm', 'Hot']
            )
        
        # Add humidity categories
        if 'humidity' in df.columns:
            df['humidity_category'] = pd.cut(
                df['humidity'],
                bins=[0, 30, 60, 100],
                labels=['Low', 'Medium', 'High']
            )
        
        # Add wind speed categories
        if 'wind_speed' in df.columns:
            df['wind_category'] = pd.cut(
                df['wind_speed'],
                bins=[0, 5, 10, 20, np.inf],
                labels=['Calm', 'Light', 'Moderate', 'Strong']
            )
        
        # Add season (simplified)
        if 'timestamp' in df.columns:
            df['season'] = df['timestamp'].dt.month.map({
                12: 'Winter', 1: 'Winter', 2: 'Winter',
                3: 'Spring', 4: 'Spring', 5: 'Spring',
                6: 'Summer', 7: 'Summer', 8: 'Summer',
                9: 'Fall', 10: 'Fall', 11: 'Fall'
            })
        
        logger.info("Derived columns added successfully")
        return df
    
    def get_data_quality_report(self, df: pd.DataFrame) -> Dict:
        """
        Generate a data quality report for the weather data.
        
        Args:
            df: Weather data DataFrame
            
        Returns:
            Dict: Data quality report
        """
        report = {
            'total_records': len(df),
            'missing_values': {},
            'data_types': {},
            'value_ranges': {}
        }
        
        # Check missing values
        for column in df.columns:
            missing_count = df[column].isnull().sum()
            missing_percentage = (missing_count / len(df)) * 100
            report['missing_values'][column] = {
                'count': missing_count,
                'percentage': round(missing_percentage, 2)
            }
        
        # Check data types
        for column in df.columns:
            report['data_types'][column] = str(df[column].dtype)
        
        # Check value ranges for numeric columns
        for column in self.numeric_fields:
            if column in df.columns:
                report['value_ranges'][column] = {
                    'min': float(df[column].min()) if not df[column].isnull().all() else None,
                    'max': float(df[column].max()) if not df[column].isnull().all() else None,
                    'mean': float(df[column].mean()) if not df[column].isnull().all() else None
                }
        
        return report
    
    def save_to_csv(self, df: pd.DataFrame, filepath: str) -> bool:
        """
        Save transformed data to CSV file.
        
        Args:
            df: Transformed weather data DataFrame
            filepath: Path to save the CSV file
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            df.to_csv(filepath, index=False)
            logger.info(f"Data saved to CSV: {filepath}")
            return True
        except Exception as e:
            logger.error(f"Error saving data to CSV: {str(e)}")
            return False 