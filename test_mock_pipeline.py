#!/usr/bin/env python3
"""
Test script for the weather data pipeline using mock data.

This script tests the pipeline components using mock weather data
instead of the real API, so you can test the full functionality
without needing a valid API key.
"""

import sys
import os
from datetime import datetime

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.utils.config import config
from src.utils.logger import get_logger
from src.ingestion.mock_weather_api import MockWeatherAPIClient
from src.transformation.weather_transformer import WeatherDataTransformer
from src.database.operations import WeatherDatabaseManager

logger = get_logger(__name__)


def test_configuration():
    """Test configuration loading and validation."""
    print("🔧 Testing Configuration...")
    
    try:
        # Test config validation
        if config.validate():
            print("✅ Configuration is valid")
            print(f"   - Database URL: {config.DATABASE_URL}")
            print(f"   - Cities: {config.CITIES}")
            print(f"   - Log Level: {config.LOG_LEVEL}")
            return True
        else:
            print("❌ Configuration validation failed")
            return False
    except Exception as e:
        print(f"❌ Configuration test failed: {str(e)}")
        return False


def test_mock_api_connection():
    """Test mock API connection and data generation."""
    print("\n🌤️ Testing Mock API Connection...")
    
    try:
        api_client = MockWeatherAPIClient()
        
        # Test connection
        if api_client.test_connection():
            print("✅ Mock API connection successful")
            
            # Test data generation for one city
            test_city = "London"
            weather_data = api_client.get_current_weather(test_city)
            
            if weather_data:
                print(f"✅ Successfully generated mock weather data for {test_city}")
                print(f"   - Temperature: {weather_data.get('temperature')}°C")
                print(f"   - Humidity: {weather_data.get('humidity')}%")
                print(f"   - Description: {weather_data.get('description')}")
                return True
            else:
                print(f"❌ Failed to generate mock weather data for {test_city}")
                return False
        else:
            print("❌ Mock API connection failed")
            return False
    except Exception as e:
        print(f"❌ Mock API test failed: {str(e)}")
        return False


def test_data_transformation():
    """Test data transformation functionality."""
    print("\n🔄 Testing Data Transformation...")
    
    try:
        transformer = WeatherDataTransformer()
        
        # Create sample data using mock API
        mock_api = MockWeatherAPIClient()
        sample_data = mock_api.get_weather_for_cities(["London", "Paris"])
        
        # Transform data
        df = transformer.transform_weather_data(sample_data)
        
        if not df.empty:
            print("✅ Data transformation successful")
            print(f"   - Records processed: {len(df)}")
            print(f"   - Columns: {list(df.columns)}")
            
            # Test data quality report
            quality_report = transformer.get_data_quality_report(df)
            print(f"   - Data quality score: {quality_report.get('total_records', 0)} records")
            
            return True
        else:
            print("❌ Data transformation failed")
            return False
    except Exception as e:
        print(f"❌ Data transformation test failed: {str(e)}")
        return False


def test_database_connection():
    """Test database connection and operations."""
    print("\n💾 Testing Database Connection...")
    
    try:
        db_manager = WeatherDatabaseManager()
        print("✅ Database connection successful")
        
        # Test basic operations
        stats = db_manager.get_weather_statistics(days=1)
        print(f"   - Current records in database: {stats.get('total_records', 0)}")
        
        return True
    except Exception as e:
        print(f"❌ Database test failed: {str(e)}")
        print("   Make sure PostgreSQL is running and accessible")
        return False


def test_full_mock_pipeline():
    """Test the complete pipeline with mock data."""
    print("\n🚀 Testing Full Mock Pipeline...")
    
    try:
        from src.main import WeatherDataPipeline
        
        # Create a custom pipeline that uses mock API
        class MockWeatherDataPipeline(WeatherDataPipeline):
            def __init__(self):
                super().__init__()
                # Replace the real API client with mock client
                from src.ingestion.mock_weather_api import MockWeatherAPIClient
                self.api_client = MockWeatherAPIClient()
        
        pipeline = MockWeatherDataPipeline()
        
        # Test with multiple cities
        test_cities = ["London", "New York", "Tokyo"]
        success = pipeline.run_pipeline(test_cities)
        
        if success:
            print("✅ Full mock pipeline test successful!")
            
            # Show some results
            stats = pipeline.get_statistics(days=1)
            if stats:
                print(f"   - Records in database: {stats.get('total_records', 0)}")
                print(f"   - Cities: {', '.join(stats.get('cities', []))}")
            
            return True
        else:
            print("❌ Full mock pipeline test failed")
            return False
    except Exception as e:
        print(f"❌ Full mock pipeline test failed: {str(e)}")
        return False


def main():
    """Run all tests with mock data."""
    print("🧪 Weather Data Pipeline Test Suite (Mock Data)")
    print("=" * 60)
    
    tests = [
        ("Configuration", test_configuration),
        ("Mock API Connection", test_mock_api_connection),
        ("Data Transformation", test_data_transformation),
        ("Database Connection", test_database_connection),
        ("Full Mock Pipeline", test_full_mock_pipeline)
    ]
    
    results = []
    
    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"❌ {test_name} test crashed: {str(e)}")
            results.append((test_name, False))
    
    # Summary
    print("\n" + "=" * 60)
    print("📊 Test Results Summary:")
    
    passed = 0
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"   {test_name}: {status}")
        if result:
            passed += 1
    
    print(f"\nOverall: {passed}/{len(results)} tests passed")
    
    if passed == len(results):
        print("🎉 All tests passed! Your pipeline is working with mock data.")
        print("\nNext steps:")
        print("1. Get a valid OpenWeatherMap API key")
        print("2. Update your .env file with the real API key")
        print("3. Run: python test_pipeline.py (with real API)")
        print("4. Check the data/ directory for CSV backups")
        print("5. Query the database for insights")
    else:
        print("⚠️ Some tests failed. Please check the errors above.")
        print("\nCommon issues:")
        print("1. PostgreSQL not running")
        print("2. Missing dependencies (run: pip install -r requirements.txt)")
        print("3. Database connection issues")
    
    return passed == len(results)


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1) 