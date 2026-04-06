#!/usr/bin/env python3
"""
Test script for the weather data pipeline.

This script tests the pipeline components and validates the setup.
Run this after setting up the environment to ensure everything works.
"""

import sys
import os
from datetime import datetime

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.utils.config import config
from src.utils.logger import get_logger
from src.ingestion.weather_api import WeatherAPIClient
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


def test_api_connection():
    """Test API connection and data fetching."""
    print("\n🌤️ Testing API Connection...")
    
    try:
        api_client = WeatherAPIClient()
        
        # Test connection
        if api_client.test_connection():
            print("✅ API connection successful")
            
            # Test data fetching for one city
            test_city = "London"
            weather_data = api_client.get_current_weather(test_city)
            
            if weather_data and 'error' not in weather_data:
                print(f"✅ Successfully fetched weather data for {test_city}")
                print(f"   - Temperature: {weather_data.get('temperature')}°C")
                print(f"   - Humidity: {weather_data.get('humidity')}%")
                print(f"   - Description: {weather_data.get('description')}")
                return True
            else:
                print(f"❌ Failed to fetch weather data for {test_city}")
                return False
        else:
            print("❌ API connection failed")
            return False
    except Exception as e:
        print(f"❌ API test failed: {str(e)}")
        return False


def test_data_transformation():
    """Test data transformation functionality."""
    print("\n🔄 Testing Data Transformation...")
    
    try:
        transformer = WeatherDataTransformer()
        
        # Create sample data
        sample_data = [
            {
                'city': 'London',
                'timestamp': datetime.now().isoformat(),
                'temperature': 15.5,
                'humidity': 65,
                'pressure': 1013,
                'wind_speed': 5.2,
                'description': 'scattered clouds',
                'weather_main': 'Clouds'
            },
            {
                'city': 'Paris',
                'timestamp': datetime.now().isoformat(),
                'temperature': 18.2,
                'humidity': 70,
                'pressure': 1015,
                'wind_speed': 3.1,
                'description': 'clear sky',
                'weather_main': 'Clear'
            }
        ]
        
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


def test_full_pipeline():
    """Test the complete pipeline with a single city."""
    print("\n🚀 Testing Full Pipeline...")
    
    try:
        from src.main import WeatherDataPipeline
        
        pipeline = WeatherDataPipeline()
        
        # Test with a single city
        test_cities = ["London"]
        success = pipeline.run_pipeline(test_cities)
        
        if success:
            print("✅ Full pipeline test successful!")
            
            # Show some results
            stats = pipeline.get_statistics(days=1)
            if stats:
                print(f"   - Records in database: {stats.get('total_records', 0)}")
                print(f"   - Cities: {', '.join(stats.get('cities', []))}")
            
            return True
        else:
            print("❌ Full pipeline test failed")
            return False
    except Exception as e:
        print(f"❌ Full pipeline test failed: {str(e)}")
        return False


def main():
    """Run all tests."""
    print("🧪 Weather Data Pipeline Test Suite")
    print("=" * 50)
    
    tests = [
        ("Configuration", test_configuration),
        ("API Connection", test_api_connection),
        ("Data Transformation", test_data_transformation),
        ("Database Connection", test_database_connection),
        ("Full Pipeline", test_full_pipeline)
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
    print("\n" + "=" * 50)
    print("📊 Test Results Summary:")
    
    passed = 0
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"   {test_name}: {status}")
        if result:
            passed += 1
    
    print(f"\nOverall: {passed}/{len(results)} tests passed")
    
    if passed == len(results):
        print("🎉 All tests passed! Your pipeline is ready to use.")
        print("\nNext steps:")
        print("1. Set up your .env file with your API key")
        print("2. Run: python src/main.py")
        print("3. Check the data/ directory for CSV backups")
        print("4. Query the database for insights")
    else:
        print("⚠️ Some tests failed. Please check the errors above.")
        print("\nCommon issues:")
        print("1. Missing API key in .env file")
        print("2. PostgreSQL not running")
        print("3. Missing dependencies (run: pip install -r requirements.txt)")
    
    return passed == len(results)


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1) 