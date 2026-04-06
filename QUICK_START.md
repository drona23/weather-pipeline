# Weather Data Pipeline - Quick Start Guide

## 🚀 Get Started in 5 Minutes

This guide will get your weather data pipeline running quickly!

### Prerequisites
- Python 3.8+ installed
- Docker installed (for PostgreSQL)
- Git installed

### Step 1: Clone and Setup

```bash
# Navigate to the project directory
cd weather_pipeline

# Run the automated setup script
python setup.py
```

The setup script will:
- ✅ Create a virtual environment
- ✅ Install all dependencies
- ✅ Create necessary directories
- ✅ Start PostgreSQL with Docker
- ✅ Create configuration files

### Step 2: Get Your API Key

1. Go to [OpenWeatherMap](https://openweathermap.org/api)
2. Sign up for a free account
3. Get your API key
4. Edit the `.env` file and add your API key:
   ```
   OPENWEATHER_API_KEY=your_api_key_here
   ```

### Step 3: Test the Pipeline

```bash
# Activate virtual environment
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Run the test suite
python test_pipeline.py
```

You should see:
```
🧪 Weather Data Pipeline Test Suite
==================================================
🔧 Testing Configuration...
✅ Configuration is valid
🌤️ Testing API Connection...
✅ API connection successful
🔄 Testing Data Transformation...
✅ Data transformation successful
💾 Testing Database Connection...
✅ Database connection successful
🚀 Testing Full Pipeline...
✅ Full pipeline test successful!

📊 Test Results Summary:
   Configuration: ✅ PASS
   API Connection: ✅ PASS
   Data Transformation: ✅ PASS
   Database Connection: ✅ PASS
   Full Pipeline: ✅ PASS

Overall: 5/5 tests passed
🎉 All tests passed! Your pipeline is ready to use.
```

### Step 4: Run the Pipeline

```bash
# Run the complete pipeline
python src/main.py
```

You should see output like:
```
🚀 Starting weather data pipeline - Run ID: abc123-def456
Processing weather data for cities: London, New York, Tokyo, Paris, Berlin
📥 Step 1: Data Ingestion
✅ Successfully ingested weather data for 5 cities
🔄 Step 2: Data Transformation
✅ Data transformation completed successfully
💾 Step 3: Data Loading
✅ Successfully loaded 5 records into database
📊 Step 4: Logging and Cleanup
✅ Pipeline completed successfully!
📈 Summary:
   - Cities processed: 5
   - Records processed: 5
   - Execution time: 12.34 seconds
   - Run ID: abc123-def456
```

### Step 5: Explore Your Data

#### Check the Database
```bash
# Connect to PostgreSQL
docker exec -it weather_postgres psql -U weather_user -d weather_data

# View recent weather data
SELECT city, temperature, humidity, timestamp 
FROM weather_records 
ORDER BY timestamp DESC 
LIMIT 10;
```

#### Check CSV Backups
```bash
# View processed data files
ls -la data/
```

#### View Pipeline Logs
```bash
# Check database logs
docker-compose logs postgres

# Check pipeline statistics
docker exec -it weather_postgres psql -U weather_user -d weather_data -c "
SELECT run_id, status, cities_processed, records_processed, start_time 
FROM pipeline_runs 
ORDER BY start_time DESC 
LIMIT 5;
"
```

## 🔧 Troubleshooting

### Common Issues

#### 1. "API connection failed"
- Check your API key in `.env` file
- Verify internet connection
- Try the API manually: `curl "https://api.openweathermap.org/data/2.5/weather?q=London&appid=YOUR_API_KEY"`

#### 2. "Database connection failed"
- Ensure Docker is running: `docker --version`
- Start PostgreSQL: `docker-compose up -d postgres`
- Check container status: `docker-compose ps`

#### 3. "Module not found" errors
- Activate virtual environment: `source venv/bin/activate`
- Reinstall dependencies: `pip install -r requirements.txt`

#### 4. Permission errors
- On macOS/Linux: `chmod +x setup.py test_pipeline.py`
- On Windows: Run as administrator

### Getting Help

1. **Check the logs**: Look for error messages in the output
2. **Verify configuration**: Ensure `.env` file has correct values
3. **Test components**: Run individual tests in `test_pipeline.py`
4. **Check Docker**: Ensure containers are running properly

## 📊 What You've Built

Your pipeline now:
- ✅ Fetches weather data from OpenWeatherMap API
- ✅ Cleans and validates the data
- ✅ Stores it in PostgreSQL database
- ✅ Generates CSV backups
- ✅ Logs pipeline execution
- ✅ Handles errors gracefully

## 🎯 Next Steps

### Immediate Next Steps
1. **Customize cities**: Edit `CITIES` in `.env` file
2. **Schedule the pipeline**: Set up cron job or use Airflow
3. **Add data analysis**: Write SQL queries for insights
4. **Monitor performance**: Track execution times

### Advanced Features (Phase 2)
1. **Apache Airflow**: Schedule and monitor pipelines
   ```bash
   docker-compose --profile airflow up -d
   ```
2. **Data Quality**: Add comprehensive validation
3. **Monitoring**: Set up alerts and dashboards
4. **Cloud Integration**: Move to AWS/GCP

### Learning Path
1. **Read the code**: Understand each component
2. **Modify the pipeline**: Add new data sources
3. **Optimize performance**: Improve database queries
4. **Scale up**: Handle more data and cities

## 📚 Resources

- **Learning Guide**: `LEARNING_GUIDE.md` - Detailed explanations
- **Code Documentation**: Well-commented source code
- **Test Suite**: `test_pipeline.py` - Component testing
- **Docker Setup**: `docker-compose.yml` - Infrastructure

## 🎉 Congratulations!

You've successfully built a production-ready data pipeline! This foundation will help you learn:

- **Data Engineering**: ETL pipeline design
- **Python Development**: Modular, testable code
- **Database Design**: Schema design and optimization
- **DevOps**: Docker, environment management
- **API Integration**: REST APIs and error handling

**Ready for the next challenge?** Check out the `LEARNING_GUIDE.md` for advanced exercises and concepts!

---

**Happy Data Engineering! 🚀** 