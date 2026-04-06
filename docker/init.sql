-- PostgreSQL initialization script for weather data pipeline
-- This script runs when the PostgreSQL container starts

-- Create database if it doesn't exist
-- (PostgreSQL creates the database automatically based on POSTGRES_DB environment variable)

-- Grant necessary permissions
GRANT ALL PRIVILEGES ON DATABASE weather_data TO weather_user;

-- Connect to the weather_data database
\c weather_data;

-- Create extensions that might be useful for data analysis
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";

-- Grant permissions on extensions
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO weather_user;

-- Create indexes for better query performance (will be created by SQLAlchemy models)
-- These are just examples of what we might want to add later

-- Set timezone
SET timezone = 'UTC';

-- Log the initialization
DO $$
BEGIN
    RAISE NOTICE 'Weather data pipeline database initialized successfully';
END $$; 