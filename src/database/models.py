"""
SQLAlchemy models for the weather data pipeline.

This module defines the database schema and models for storing
weather data in PostgreSQL.
"""

from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Text, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from src.utils.config import config
from src.utils.logger import get_logger

logger = get_logger(__name__)

# Create declarative base
Base = declarative_base()


class WeatherRecord(Base):
    """SQLAlchemy model for weather data records."""
    
    __tablename__ = 'weather_records'
    
    # Primary key
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Location information
    city = Column(String(100), nullable=False, index=True)
    country = Column(String(50), nullable=True)
    latitude = Column(Float, nullable=True)
    longitude = Column(Float, nullable=True)
    
    # Timestamp information
    timestamp = Column(DateTime, nullable=False, index=True)
    sunrise = Column(DateTime, nullable=True)
    sunset = Column(DateTime, nullable=True)
    
    # Weather measurements
    temperature = Column(Float, nullable=False)
    feels_like = Column(Float, nullable=True)
    humidity = Column(Float, nullable=True)
    pressure = Column(Float, nullable=True)
    
    # Wind information
    wind_speed = Column(Float, nullable=True)
    wind_direction = Column(Float, nullable=True)
    
    # Additional weather data
    description = Column(Text, nullable=True)
    weather_main = Column(String(50), nullable=True)
    visibility = Column(Float, nullable=True)
    clouds = Column(Float, nullable=True)
    
    # Derived columns for analysis
    date = Column(String(10), nullable=True, index=True)
    hour = Column(Integer, nullable=True)
    day_of_week = Column(String(10), nullable=True)
    temperature_category = Column(String(20), nullable=True)
    humidity_category = Column(String(20), nullable=True)
    wind_category = Column(String(20), nullable=True)
    season = Column(String(10), nullable=True)
    
    # Metadata
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    def __repr__(self):
        return f"<WeatherRecord(city='{self.city}', temperature={self.temperature}, timestamp='{self.timestamp}')>"


class DataQualityLog(Base):
    """SQLAlchemy model for data quality logging."""
    
    __tablename__ = 'data_quality_logs'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(String(50), nullable=False, index=True)
    timestamp = Column(DateTime, default=datetime.utcnow)
    total_records = Column(Integer, nullable=False)
    valid_records = Column(Integer, nullable=False)
    invalid_records = Column(Integer, nullable=False)
    quality_score = Column(Float, nullable=True)
    error_message = Column(Text, nullable=True)
    success = Column(Boolean, default=True)
    
    def __repr__(self):
        return f"<DataQualityLog(run_id='{self.run_id}', success={self.success})>"


class PipelineRun(Base):
    """SQLAlchemy model for pipeline execution logging."""
    
    __tablename__ = 'pipeline_runs'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(String(50), nullable=False, unique=True, index=True)
    start_time = Column(DateTime, nullable=False)
    end_time = Column(DateTime, nullable=True)
    status = Column(String(20), nullable=False)  # 'running', 'completed', 'failed'
    cities_processed = Column(Integer, nullable=False)
    records_processed = Column(Integer, nullable=False)
    error_message = Column(Text, nullable=True)
    
    def __repr__(self):
        return f"<PipelineRun(run_id='{self.run_id}', status='{self.status}')>"


# Database connection setup
def create_database_engine():
    """Create SQLAlchemy engine for database connection."""
    try:
        engine = create_engine(
            config.DATABASE_URL,
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,
            echo=False  # Set to True for SQL query logging
        )
        logger.info("Database engine created successfully")
        return engine
    except Exception as e:
        logger.error(f"Error creating database engine: {str(e)}")
        raise


def create_database_session():
    """Create database session factory."""
    engine = create_database_engine()
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    return SessionLocal


def create_tables(engine):
    """Create all database tables."""
    try:
        Base.metadata.create_all(bind=engine)
        logger.info("Database tables created successfully")
    except Exception as e:
        logger.error(f"Error creating database tables: {str(e)}")
        raise


def drop_tables(engine):
    """Drop all database tables (use with caution!)."""
    try:
        Base.metadata.drop_all(bind=engine)
        logger.warning("Database tables dropped successfully")
    except Exception as e:
        logger.error(f"Error dropping database tables: {str(e)}")
        raise 