"""
Logging configuration for the weather data pipeline.

This module provides structured logging with different levels
and consistent formatting across the application.
"""

import logging
import sys
from typing import Optional
from .config import config


def setup_logger(name: str = "weather_pipeline", level: Optional[str] = None) -> logging.Logger:
    """
    Setup and configure logger for the weather pipeline.
    
    Args:
        name: Logger name
        level: Logging level (INFO, DEBUG, WARNING, ERROR)
        
    Returns:
        logging.Logger: Configured logger instance
    """
    # Get log level from config if not provided
    if level is None:
        level = config.LOG_LEVEL
    
    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper()))
    
    # Avoid adding handlers multiple times
    if logger.handlers:
        return logger
    
    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, level.upper()))
    
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console_handler.setFormatter(formatter)
    
    # Add handler to logger
    logger.addHandler(console_handler)
    
    return logger


def get_logger(name: str = "weather_pipeline") -> logging.Logger:
    """
    Get a logger instance for the specified name.
    
    Args:
        name: Logger name
        
    Returns:
        logging.Logger: Logger instance
    """
    return setup_logger(name)


# Default logger instance
logger = get_logger() 