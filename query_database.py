#!/usr/bin/env python3
"""
Simple script to query the weather database and show the data.
"""

from src.database.operations import WeatherDatabaseManager
# from src.config import get_config  # Not needed

def safe_fmt(val, fmt=".1f"):
    """Safely format a value as a float, or return as string if not possible."""
    try:
        return format(float(val), fmt)
    except (ValueError, TypeError):
        return str(val)

def main():
    """Query and display weather data from the database."""
    print("🔍 Querying Weather Database")
    print("=" * 40)
    
    # Initialize database manager
    db = WeatherDatabaseManager()
    
    try:
        # Get recent weather data
        recent_data = db.get_recent_weather_data(hours=24)
        
        print(f"📊 Total records in database: {len(recent_data)}")
        
        if not recent_data.empty:
            print("\n🌤️ Recent Weather Records:")
            print("-" * 60)
            
            for _, row in recent_data.iterrows():
                print(f"  {row['city']}: {row['temperature']:.1f}°C, {row['description']}")
                print(f"    Humidity: {row['humidity']}%, Wind: {row['wind_speed']} m/s")
                print(f"    Time: {row['timestamp']}")
                print()
        
        # Get weather statistics
        stats = db.get_weather_statistics(days=7)
        
        print("📈 Weather Statistics (Last 7 days):")
        print("-" * 40)
        print(f"  Average temperature: {safe_fmt(stats.get('avg_temperature'))}°C")
        print(f"  Temperature range: {safe_fmt(stats.get('min_temperature'))}°C to {safe_fmt(stats.get('max_temperature'))}°C")
        print(f"  Average humidity: {safe_fmt(stats.get('avg_humidity'))}%")
        print(f"  Most common weather: {stats.get('most_common_weather', 'N/A')}")
        print(f"  Cities with data: {stats.get('cities_count', 'N/A')}")
        
    except Exception as e:
        print(f"❌ Error querying database: {str(e)}")

if __name__ == "__main__":
    main() 