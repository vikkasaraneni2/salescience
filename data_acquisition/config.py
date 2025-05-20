"""
Configuration Module for Data Acquisition Pipeline
-------------------------------------------------

This module centralizes all configuration settings for the data acquisition
pipeline, providing a single source of truth for configuration parameters.
It includes settings for:

1. Data Sources (SEC, Yahoo)
2. Redis Connection and Message Bus
3. Worker Configuration
4. Logging and Monitoring
5. API Keys and Authentication
6. Environment-specific settings

The configuration is loaded from environment variables with sensible defaults,
supporting different deployment environments (development, testing, production).

Usage:
    from data_acquisition.config import settings
    
    # Access settings
    redis_url = settings.REDIS_URL
    worker_concurrency = settings.WORKER_CONCURRENCY
"""

import os
import logging
from typing import Dict, Any, Optional, List

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("config")

# Try to import dotenv for local development
try:
    from dotenv import load_dotenv
    load_dotenv()
    logger.info("Loaded environment variables from .env file")
except ImportError:
    logger.warning("dotenv not installed, relying on existing environment variables")


class Settings:
    """
    Settings class with all configuration parameters for the data acquisition pipeline.
    This class centralizes configuration loading from environment variables with defaults.
    """
    
    def __init__(self):
        # Redis configuration
        self.REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
        self.REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", None)
        self.REDIS_SSL = os.getenv("REDIS_SSL", "false").lower() == "true"
        
        # SEC API configuration
        self.SEC_API_KEY = os.getenv("SEC_API_KEY")
        self.SEC_API_BASE_URL = os.getenv("SEC_API_BASE_URL", "https://api.sec-api.io")
        self.SEC_EDGAR_BASE_URL = os.getenv("SEC_EDGAR_BASE_URL", "https://www.sec.gov/Archives/edgar/data")
        self.SEC_USER_AGENT = os.getenv("SEC_USER_AGENT", "salescience/1.0")
        
        # SEC compliance settings (required by SEC guidelines)
        # See: https://www.sec.gov/os/accessing-edgar-data
        self.SEC_RATE_LIMIT_REQUESTS = int(os.getenv("SEC_RATE_LIMIT_REQUESTS", "10"))
        self.SEC_RATE_LIMIT_PERIOD_SEC = int(os.getenv("SEC_RATE_LIMIT_PERIOD_SEC", "1"))
        self.SEC_REQUEST_TIMEOUT_SEC = float(os.getenv("SEC_REQUEST_TIMEOUT_SEC", "15.0"))
        
        # Yahoo Finance API configuration
        self.YAHOO_API_KEY = os.getenv("YAHOO_API_KEY")
        self.YAHOO_API_BASE_URL = os.getenv("YAHOO_API_BASE_URL", "https://query1.finance.yahoo.com/v10/finance")
        self.YAHOO_REQUEST_TIMEOUT_SEC = float(os.getenv("YAHOO_REQUEST_TIMEOUT_SEC", "10.0"))
        
        # Worker configuration
        self.WORKER_CONCURRENCY = int(os.getenv("WORKER_CONCURRENCY", "10"))
        self.WORKER_MAX_RETRIES = int(os.getenv("WORKER_MAX_RETRIES", "3"))
        self.WORKER_RETRY_DELAY_SEC = int(os.getenv("WORKER_RETRY_DELAY_SEC", "5"))
        self.WORKER_POLL_INTERVAL_SEC = int(os.getenv("WORKER_POLL_INTERVAL_SEC", "5"))
        self.WORKER_TIMEOUT_SEC = int(os.getenv("WORKER_TIMEOUT_SEC", "300"))  # 5 minutes
        
        # Message bus configuration
        self.MESSAGE_BUS_ENABLED = os.getenv("MESSAGE_BUS_ENABLED", "true").lower() == "true"
        self.SEC_TOPIC = os.getenv("SEC_TOPIC", "data.sec")
        self.YAHOO_TOPIC = os.getenv("YAHOO_TOPIC", "data.yahoo")
        self.XBRL_TOPIC_PREFIX = os.getenv("XBRL_TOPIC_PREFIX", "data.xbrl")
        self.MESSAGE_RETENTION_MS = int(os.getenv("MESSAGE_RETENTION_MS", "86400000"))  # 24 hours
        
        # Queue configuration
        self.QUEUE_NAME = os.getenv("QUEUE_NAME", "data_jobs")
        self.JOB_EXPIRY_SEC = int(os.getenv("JOB_EXPIRY_SEC", "604800"))  # 1 week
        
        # Prometheus metrics configuration
        self.PROMETHEUS_ENABLED = os.getenv("PROMETHEUS_ENABLED", "true").lower() == "true"
        self.METRICS_PORT = int(os.getenv("METRICS_PORT", "8000"))
        
        # Logging configuration
        self.LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
        self.JSON_LOGGING_ENABLED = os.getenv("JSON_LOGGING_ENABLED", "true").lower() == "true"
        
        # Document processing configuration
        self.MAX_DOCUMENT_SIZE_MB = int(os.getenv("MAX_DOCUMENT_SIZE_MB", "50"))
        self.TEXT_TRUNCATION_CHARS = int(os.getenv("TEXT_TRUNCATION_CHARS", "500"))
        
        # Default data acquisition parameters
        self.DEFAULT_FORM_TYPE = os.getenv("DEFAULT_FORM_TYPE", "10-K")
        self.DEFAULT_N_YEARS = int(os.getenv("DEFAULT_N_YEARS", "5"))
        self.DEFAULT_XBRL_CONCEPTS = self._parse_list_env("DEFAULT_XBRL_CONCEPTS", [
            "us-gaap:Revenues", "us-gaap:NetIncomeLoss", "us-gaap:Assets",
            "us-gaap:Liabilities", "us-gaap:StockholdersEquity"
        ])
        
        # Feature flags
        self.FEATURE_XBRL_ENABLED = os.getenv("FEATURE_XBRL_ENABLED", "true").lower() == "true"
        self.FEATURE_SEC_INSIDER_ENABLED = os.getenv("FEATURE_SEC_INSIDER_ENABLED", "true").lower() == "true"
        self.FEATURE_BATCH_JOBS_ENABLED = os.getenv("FEATURE_BATCH_JOBS_ENABLED", "true").lower() == "true"
        
        # Validate critical settings
        self._validate_settings()
        
    def _validate_settings(self):
        """Validate critical settings to warn about potential issues."""
        if not self.SEC_API_KEY:
            logger.warning("SEC_API_KEY is not set - SEC data source will not function")
            
        if not self.YAHOO_API_KEY:
            logger.warning("YAHOO_API_KEY is not set - Yahoo data source may have limitations")
            
        if self.WORKER_CONCURRENCY > 20:
            logger.warning(f"WORKER_CONCURRENCY is set to {self.WORKER_CONCURRENCY}, which is high and may overload external APIs")
            
        # Set numeric log level
        self._set_log_level()
    
    def _set_log_level(self):
        """Set numeric log level from string."""
        log_level_name = self.LOG_LEVEL.upper()
        numeric_level = getattr(logging, log_level_name, None)
        if not isinstance(numeric_level, int):
            logger.warning(f"Invalid LOG_LEVEL: {self.LOG_LEVEL}, defaulting to INFO")
            numeric_level = logging.INFO
        
        logging.getLogger().setLevel(numeric_level)
        logger.setLevel(numeric_level)
        
    def _parse_list_env(self, env_name: str, default: List[str]) -> List[str]:
        """Parse a comma-separated environment variable into a list."""
        env_value = os.getenv(env_name)
        if not env_value:
            return default
        return [item.strip() for item in env_value.split(",")]
        
    def as_dict(self) -> Dict[str, Any]:
        """Convert settings to dictionary, masking sensitive values."""
        settings_dict = {key: value for key, value in self.__dict__.items() 
                        if not key.startswith('_')}
        
        # Mask sensitive values
        for key in settings_dict:
            if any(sensitive in key.lower() for sensitive in ['key', 'password', 'secret', 'token']):
                if settings_dict[key]:
                    settings_dict[key] = '****'
        
        return settings_dict
    
    def log_settings(self):
        """Log all settings at startup for visibility."""
        logger.info("Data Acquisition Pipeline Configuration:")
        for key, value in self.as_dict().items():
            logger.info(f"  {key}: {value}")


# Create singleton instance
settings = Settings()

# For convenient import
__all__ = ['settings']


# If this module is executed directly, log all settings
if __name__ == "__main__":
    settings.log_settings()