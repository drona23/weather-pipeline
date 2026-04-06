"""
Configuration management for the weather data pipeline.

This module handles loading environment variables and provides
centralized configuration for the entire application.
"""

import os
from typing import List
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


class Config:
    """Centralized configuration class for the weather pipeline."""
    
    # Database Configuration
    DB_HOST = os.getenv('DB_HOST', 'localhost')
    DB_PORT = int(os.getenv('DB_PORT', 5432))
    DB_NAME = os.getenv('DB_NAME', 'weather_data')
    DB_USER = os.getenv('DB_USER', 'weather_user')
    DB_PASSWORD = os.getenv('DB_PASSWORD', 'weather_password')
    
    # Database URL for SQLAlchemy
    @property
    def DATABASE_URL(self) -> str:
        """Generate database URL for SQLAlchemy connection."""
        return f"postgresql://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
    
    # API Configuration
    OPENWEATHER_API_KEY = os.getenv('OPENWEATHER_API_KEY')
    OPENWEATHER_BASE_URL = os.getenv('OPENWEATHER_BASE_URL', 'https://api.openweathermap.org/data/2.5')
    
    # Application Configuration
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    DATA_DIR = os.getenv('DATA_DIR', './data')
    
    # Cities to fetch weather data for
    @property
    def CITIES(self) -> List[str]:
        """Get list of cities from environment variable."""
        cities_str = os.getenv('CITIES', 'London,New York,Tokyo,Paris,Berlin')
        return [city.strip() for city in cities_str.split(',')]
    
    def validate(self) -> bool:
        """
        Validate that all required configuration is present.
        
        Returns:
            bool: True if configuration is valid, False otherwise
        """
        required_fields = [
            'OPENWEATHER_API_KEY',
            'DB_PASSWORD'
        ]
        
        missing_fields = []
        for field in required_fields:
            if not getattr(self, field):
                missing_fields.append(field)
        
        if missing_fields:
            print(f"❌ Missing required configuration: {', '.join(missing_fields)}")
            print("Please check your .env file or environment variables.")
            return False
        
        return True


# Global configuration instance
config = Config() 