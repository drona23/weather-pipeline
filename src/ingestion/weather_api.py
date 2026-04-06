"""
Weather API client for fetching weather data from OpenWeatherMap.

This module handles API calls to OpenWeatherMap, including
error handling, rate limiting, and data validation.
"""

import requests
import time
from typing import Dict, List, Optional
from datetime import datetime
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from src.utils.config import config
from src.utils.logger import get_logger

logger = get_logger(__name__)


class WeatherAPIClient:
    """Client for fetching weather data from OpenWeatherMap API."""
    
    def __init__(self, api_key: Optional[str] = None, base_url: Optional[str] = None):
        """
        Initialize the Weather API client.
        
        Args:
            api_key: OpenWeatherMap API key
            base_url: Base URL for the API
        """
        self.api_key = api_key or config.OPENWEATHER_API_KEY
        self.base_url = base_url or config.OPENWEATHER_BASE_URL
        
        if not self.api_key:
            raise ValueError("API key is required. Please set OPENWEATHER_API_KEY in your environment.")
        
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'WeatherPipeline/1.0'
        })
    
    def get_current_weather(self, city: str) -> Optional[Dict]:
        """
        Fetch current weather data for a specific city.
        
        Args:
            city: City name to fetch weather for
            
        Returns:
            Dict containing weather data or None if failed
        """
        try:
            url = f"{self.base_url}/weather"
            params = {
                'q': city,
                'appid': self.api_key,
                'units': 'metric'  # Use metric units (Celsius, m/s)
            }
            
            logger.info(f"Fetching weather data for {city}")
            response = self.session.get(url, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                logger.info(f"Successfully fetched weather data for {city}")
                return self._process_weather_data(data, city)
            else:
                logger.error(f"Failed to fetch weather data for {city}. Status: {response.status_code}")
                return None
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error while fetching weather for {city}: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error while fetching weather for {city}: {str(e)}")
            return None
    
    def get_weather_for_cities(self, cities: List[str]) -> List[Dict]:
        """
        Fetch weather data for multiple cities.
        
        Args:
            cities: List of city names
            
        Returns:
            List of weather data dictionaries
        """
        weather_data = []
        
        for city in cities:
            data = self.get_current_weather(city)
            if data:
                weather_data.append(data)
            
            # Rate limiting: wait 1 second between requests
            time.sleep(1)
        
        logger.info(f"Fetched weather data for {len(weather_data)} out of {len(cities)} cities")
        return weather_data
    
    def _process_weather_data(self, raw_data: Dict, city: str) -> Dict:
        """
        Process and clean raw weather data from API.
        
        Args:
            raw_data: Raw weather data from API
            city: City name
            
        Returns:
            Processed weather data dictionary
        """
        try:
            # Extract relevant weather information
            weather_info = {
                'city': city,
                'timestamp': datetime.now().isoformat(),
                'temperature': raw_data.get('main', {}).get('temp'),
                'feels_like': raw_data.get('main', {}).get('feels_like'),
                'humidity': raw_data.get('main', {}).get('humidity'),
                'pressure': raw_data.get('main', {}).get('pressure'),
                'wind_speed': raw_data.get('wind', {}).get('speed'),
                'wind_direction': raw_data.get('wind', {}).get('deg'),
                'description': raw_data.get('weather', [{}])[0].get('description'),
                'weather_main': raw_data.get('weather', [{}])[0].get('main'),
                'visibility': raw_data.get('visibility'),
                'clouds': raw_data.get('clouds', {}).get('all'),
                'sunrise': raw_data.get('sys', {}).get('sunrise'),
                'sunset': raw_data.get('sys', {}).get('sunset'),
                'country': raw_data.get('sys', {}).get('country'),
                'latitude': raw_data.get('coord', {}).get('lat'),
                'longitude': raw_data.get('coord', {}).get('lon')
            }
            
            # Remove None values
            weather_info = {k: v for k, v in weather_info.items() if v is not None}
            
            return weather_info
            
        except Exception as e:
            logger.error(f"Error processing weather data for {city}: {str(e)}")
            return {
                'city': city,
                'timestamp': datetime.now().isoformat(),
                'error': str(e)
            }
    
    def test_connection(self) -> bool:
        """
        Test the API connection with a simple request.
        
        Returns:
            bool: True if connection is successful, False otherwise
        """
        try:
            # Test with a simple city
            test_data = self.get_current_weather("London")
            if test_data and 'error' not in test_data:
                logger.info("✅ API connection test successful")
                return True
            else:
                logger.error("❌ API connection test failed")
                return False
        except Exception as e:
            logger.error(f"❌ API connection test failed: {str(e)}")
            return False 