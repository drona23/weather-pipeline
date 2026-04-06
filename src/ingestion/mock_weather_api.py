"""
Mock Weather API client for testing purposes.

This module provides sample weather data when the real API is not available.
"""

import random
from typing import Dict, List, Optional
from datetime import datetime
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from src.utils.logger import get_logger

logger = get_logger(__name__)


class MockWeatherAPIClient:
    """Mock client for generating sample weather data."""
    
    def __init__(self):
        """Initialize the mock weather API client."""
        self.cities_data = {
            'London': {'lat': 51.5074, 'lon': -0.1278, 'country': 'GB'},
            'New York': {'lat': 40.7128, 'lon': -74.0060, 'country': 'US'},
            'Tokyo': {'lat': 35.6762, 'lon': 139.6503, 'country': 'JP'},
            'Paris': {'lat': 48.8566, 'lon': 2.3522, 'country': 'FR'},
            'Berlin': {'lat': 52.5200, 'lon': 13.4050, 'country': 'DE'},
            'Sydney': {'lat': -33.8688, 'lon': 151.2093, 'country': 'AU'},
            'Mumbai': {'lat': 19.0760, 'lon': 72.8777, 'country': 'IN'},
            'Cairo': {'lat': 30.0444, 'lon': 31.2357, 'country': 'EG'},
            'Rio de Janeiro': {'lat': -22.9068, 'lon': -43.1729, 'country': 'BR'},
            'Toronto': {'lat': 43.6532, 'lon': -79.3832, 'country': 'CA'}
        }
    
    def get_current_weather(self, city: str) -> Optional[Dict]:
        """
        Generate mock weather data for a specific city.
        
        Args:
            city: City name to generate weather for
            
        Returns:
            Dict containing mock weather data
        """
        try:
            logger.info(f"Generating mock weather data for {city}")
            
            # Get city coordinates
            city_info = self.cities_data.get(city, {'lat': 0, 'lon': 0, 'country': 'Unknown'})
            
            # Generate realistic weather data
            temperature = random.uniform(-10, 35)  # -10 to 35°C
            humidity = random.uniform(30, 90)  # 30-90%
            pressure = random.uniform(980, 1030)  # 980-1030 hPa
            wind_speed = random.uniform(0, 25)  # 0-25 m/s
            wind_direction = random.uniform(0, 360)  # 0-360 degrees
            visibility = random.uniform(5000, 10000)  # 5-10 km
            clouds = random.uniform(0, 100)  # 0-100%
            
            # Weather descriptions based on temperature
            if temperature < 0:
                description = random.choice(['snow', 'freezing rain', 'clear sky'])
                weather_main = 'Snow'
            elif temperature < 10:
                description = random.choice(['cold', 'chilly', 'overcast clouds'])
                weather_main = 'Clouds'
            elif temperature < 20:
                description = random.choice(['mild', 'partly cloudy', 'scattered clouds'])
                weather_main = 'Clouds'
            else:
                description = random.choice(['warm', 'clear sky', 'few clouds'])
                weather_main = 'Clear'
            
            # Generate sunrise/sunset times (simplified)
            now = datetime.now()
            sunrise = now.replace(hour=6, minute=30, second=0, microsecond=0)
            sunset = now.replace(hour=18, minute=30, second=0, microsecond=0)
            
            weather_data = {
                'city': city,
                'timestamp': now.isoformat(),
                'temperature': round(temperature, 1),
                'feels_like': round(temperature + random.uniform(-2, 2), 1),
                'humidity': round(humidity, 1),
                'pressure': round(pressure, 1),
                'wind_speed': round(wind_speed, 1),
                'wind_direction': round(wind_direction, 1),
                'description': description,
                'weather_main': weather_main,
                'visibility': round(visibility, 1),
                'clouds': round(clouds, 1),
                'sunrise': int(sunrise.timestamp()),
                'sunset': int(sunset.timestamp()),
                'country': city_info['country'],
                'latitude': city_info['lat'],
                'longitude': city_info['lon']
            }
            
            logger.info(f"Successfully generated mock weather data for {city}")
            return weather_data
            
        except Exception as e:
            logger.error(f"Error generating mock weather data for {city}: {str(e)}")
            return None
    
    def get_weather_for_cities(self, cities: List[str]) -> List[Dict]:
        """
        Generate mock weather data for multiple cities.
        
        Args:
            cities: List of city names
            
        Returns:
            List of mock weather data dictionaries
        """
        weather_data = []
        
        for city in cities:
            data = self.get_current_weather(city)
            if data:
                weather_data.append(data)
        
        logger.info(f"Generated mock weather data for {len(weather_data)} out of {len(cities)} cities")
        return weather_data
    
    def test_connection(self) -> bool:
        """
        Test the mock API connection.
        
        Returns:
            bool: Always True for mock client
        """
        logger.info("✅ Mock API connection test successful")
        return True 