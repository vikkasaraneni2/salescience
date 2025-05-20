"""
Configuration Module for Data Acquisition Pipeline
-------------------------------------------------

This module centralizes all configuration settings for the data acquisition
pipeline using Pydantic for validation and type safety.

Key features:
- Pydantic BaseSettings for type validation and .env loading
- Hierarchical settings organization by functional area
- Extensive validation rules to prevent misconfiguration
- LRU caching for efficient settings access
- Secure handling of sensitive configuration values
"""

import os
import re
from functools import lru_cache
from typing import List, Optional, Dict, Any, Union
from pydantic import BaseSettings, Field, validator, root_validator


class RedisSettings(BaseSettings):
    """Redis connection settings"""
    URL: str = Field("redis://localhost:6379/0", env="REDIS_URL")
    PASSWORD: Optional[str] = Field(None, env="REDIS_PASSWORD")
    SSL: bool = Field(False, env="REDIS_SSL")
    POOL_SIZE: int = Field(10, env="REDIS_POOL_SIZE")
    SOCKET_TIMEOUT: int = Field(5, env="REDIS_SOCKET_TIMEOUT")
    SOCKET_CONNECT_TIMEOUT: int = Field(5, env="REDIS_SOCKET_CONNECT_TIMEOUT")
    RETRY_ON_TIMEOUT: bool = Field(True, env="REDIS_RETRY_ON_TIMEOUT")

    @validator('URL')
    def validate_redis_url(cls, v):
        """
        Validate Redis URL format.
        
        This validator ensures that the Redis URL starts with the proper 
        protocol prefix ('redis://' for standard connections or 'rediss://' for SSL).
        An incorrect URL would cause connection failures at runtime.
        """
        if not v.startswith(('redis://', 'rediss://')):
            raise ValueError("Redis URL must start with 'redis://' or 'rediss://'")
        return v
    
    @validator('POOL_SIZE', 'SOCKET_TIMEOUT', 'SOCKET_CONNECT_TIMEOUT')
    def validate_positive_int(cls, v, values, field):
        """
        Ensure numeric Redis parameters are positive integers.
        
        These settings control connection pool behavior and timeouts.
        Negative or zero values would create invalid configurations that
        could cause connection issues or resource exhaustion.
        """
        if v <= 0:
            raise ValueError(f"{field.name} must be a positive integer")
        return v

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = True


class ApiKeySettings(BaseSettings):
    """API key settings for external services"""
    SEC_API_KEY: Optional[str] = Field(None, env="SEC_API_KEY")
    YAHOO_API_KEY: Optional[str] = Field(None, env="YAHOO_API_KEY")
    OPENAI_API_KEY: Optional[str] = Field(None, env="OPENAI_API_KEY")

    @validator('SEC_API_KEY')
    def validate_sec_api_key(cls, v):
        """
        Validate SEC API key when provided.
        
        While the SEC API key is optional (can be None), if a value is provided,
        it should not be an empty string. Empty strings could cause silent API 
        failures that are difficult to debug, as they would be treated as missing
        authentication rather than triggering a clear configuration error.
        """
        if v is not None and not v.strip():
            raise ValueError("SEC_API_KEY cannot be an empty string")
        return v

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = True


class ServiceUrlSettings(BaseSettings):
    """Service URL settings for external APIs"""
    SEC_API_BASE_URL: str = Field("https://api.sec-api.io", env="SEC_API_BASE_URL")
    SEC_EDGAR_BASE_URL: str = Field("https://www.sec.gov/Archives/edgar/data", env="SEC_EDGAR_BASE_URL")
    YAHOO_API_BASE_URL: str = Field("https://query1.finance.yahoo.com/v10/finance", env="YAHOO_API_BASE_URL")
    
    @validator('*')
    def validate_url_format(cls, v, values, field):
        """
        Validate all service URLs have proper HTTP/HTTPS prefixes.
        
        This validator applies to all URL fields to ensure they begin with
        an appropriate protocol (http:// or https://). Malformed URLs would
        cause request failures when trying to access these services.
        The wildcard (*) ensures this validation runs on all fields in this class.
        """
        if not v.startswith(('http://', 'https://')):
            raise ValueError(f"{field.name} must be a valid URL starting with 'http://' or 'https://'")
        return v
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = True


class WorkerSettings(BaseSettings):
    """Worker configuration settings"""
    CONCURRENCY: int = Field(10, env="WORKER_CONCURRENCY")
    MAX_RETRIES: int = Field(3, env="WORKER_MAX_RETRIES")
    RETRY_DELAY_SEC: int = Field(5, env="WORKER_RETRY_DELAY_SEC")
    POLL_INTERVAL_SEC: int = Field(5, env="WORKER_POLL_INTERVAL_SEC")
    TIMEOUT_SEC: int = Field(300, env="WORKER_TIMEOUT_SEC")
    
    @validator('CONCURRENCY')
    def check_concurrency(cls, v):
        """
        Validate worker concurrency setting.
        
        This validator ensures the concurrency level is a positive integer,
        and notes when it exceeds the recommended maximum of 20.
        High concurrency values could overload external APIs or consume
        excessive system resources.
        """
        if v <= 0:
            raise ValueError("CONCURRENCY must be a positive integer")
        if v > 20:
            # Still allow values over 20, but a warning will be logged elsewhere
            pass
        return v
    
    @validator('MAX_RETRIES', 'RETRY_DELAY_SEC', 'POLL_INTERVAL_SEC', 'TIMEOUT_SEC')
    def validate_positive_int(cls, v, values, field):
        """
        Ensure all worker timing and retry settings are positive integers.
        
        These settings control job processing behavior and must be positive
        to ensure proper operation. Zero or negative values would cause
        logical errors or infinite loops in the worker process.
        """
        if v <= 0:
            raise ValueError(f"{field.name} must be a positive integer")
        return v
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = True


class MessageBusSettings(BaseSettings):
    """Message bus configuration settings"""
    ENABLED: bool = Field(True, env="MESSAGE_BUS_ENABLED")
    SEC_TOPIC: str = Field("data.sec", env="SEC_TOPIC")
    YAHOO_TOPIC: str = Field("data.yahoo", env="YAHOO_TOPIC")
    XBRL_TOPIC_PREFIX: str = Field("data.xbrl", env="XBRL_TOPIC_PREFIX")
    RETENTION_MS: int = Field(86400000, env="MESSAGE_RETENTION_MS")  # 24 hours
    
    @validator('RETENTION_MS')
    def validate_retention_ms(cls, v):
        """
        Validate message retention period.
        
        Ensures the message retention time is a positive integer.
        A non-positive value would cause invalid Redis stream configuration
        and potentially immediate message deletion.
        """
        if v <= 0:
            raise ValueError("RETENTION_MS must be a positive integer")
        return v
    
    @validator('SEC_TOPIC', 'YAHOO_TOPIC', 'XBRL_TOPIC_PREFIX')
    def validate_topic_format(cls, v, values, field):
        """
        Validate message topic name format.
        
        Ensures that topic names only contain alphanumeric characters,
        dots, and underscores, which are safe characters for Redis stream keys.
        Invalid characters could cause errors when publishing messages.
        """
        if not re.match(r'^[a-zA-Z0-9_.]+$', v):
            raise ValueError(f"{field.name} must contain only alphanumeric characters, dots, and underscores")
        return v
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = True


class Settings(BaseSettings):
    """
    Main settings class that combines all sub-settings.
    
    This class provides a hierarchical configuration structure with
    comprehensive validation rules to ensure a consistent and valid
    configuration across all components of the data acquisition pipeline.
    """
    # Sub-configuration sections
    redis: RedisSettings = Field(default_factory=RedisSettings)
    api_keys: ApiKeySettings = Field(default_factory=ApiKeySettings)
    service_urls: ServiceUrlSettings = Field(default_factory=ServiceUrlSettings)
    worker: WorkerSettings = Field(default_factory=WorkerSettings)
    message_bus: MessageBusSettings = Field(default_factory=MessageBusSettings)
    
    # Additional settings
    ENVIRONMENT: str = Field("development", env="ENVIRONMENT")
    LOG_LEVEL: str = Field("INFO", env="LOG_LEVEL")
    JSON_LOGGING_ENABLED: bool = Field(True, env="JSON_LOGGING_ENABLED")
    
    # API Rate Limits and Timeouts
    SEC_RATE_LIMIT_REQUESTS: int = Field(10, env="SEC_RATE_LIMIT_REQUESTS")
    SEC_RATE_LIMIT_PERIOD_SEC: int = Field(1, env="SEC_RATE_LIMIT_PERIOD_SEC")
    SEC_REQUEST_TIMEOUT_SEC: float = Field(15.0, env="SEC_REQUEST_TIMEOUT_SEC")
    YAHOO_REQUEST_TIMEOUT_SEC: float = Field(10.0, env="YAHOO_REQUEST_TIMEOUT_SEC")
    
    # Queue configuration
    QUEUE_NAME: str = Field("data_jobs", env="QUEUE_NAME")
    JOB_EXPIRY_SEC: int = Field(604800, env="JOB_EXPIRY_SEC")  # 1 week
    
    # Prometheus metrics configuration
    PROMETHEUS_ENABLED: bool = Field(True, env="PROMETHEUS_ENABLED")
    METRICS_PORT: int = Field(8000, env="METRICS_PORT")
    
    # Document processing configuration
    MAX_DOCUMENT_SIZE_MB: int = Field(50, env="MAX_DOCUMENT_SIZE_MB")
    TEXT_TRUNCATION_CHARS: int = Field(500, env="TEXT_TRUNCATION_CHARS")
    
    # Default data acquisition parameters
    DEFAULT_FORM_TYPE: str = Field("10-K", env="DEFAULT_FORM_TYPE")
    DEFAULT_N_YEARS: int = Field(5, env="DEFAULT_N_YEARS")
    DEFAULT_XBRL_CONCEPTS: List[str] = Field(
        ["us-gaap:Revenues", "us-gaap:NetIncomeLoss", "us-gaap:Assets", 
         "us-gaap:Liabilities", "us-gaap:StockholdersEquity"],
        env="DEFAULT_XBRL_CONCEPTS"
    )
    
    # Feature flags
    FEATURE_XBRL_ENABLED: bool = Field(True, env="FEATURE_XBRL_ENABLED")
    FEATURE_SEC_INSIDER_ENABLED: bool = Field(True, env="FEATURE_SEC_INSIDER_ENABLED")
    FEATURE_BATCH_JOBS_ENABLED: bool = Field(True, env="FEATURE_BATCH_JOBS_ENABLED")
    
    @validator('DEFAULT_XBRL_CONCEPTS', pre=True)
    def parse_xbrl_concepts(cls, v):
        """
        Parse XBRL concepts from string to list.
        
        Handles the case where XBRL concepts are provided as a comma-separated
        string via environment variable instead of a list. The pre=True flag
        ensures this runs before type validation.
        """
        if isinstance(v, str):
            return [concept.strip() for concept in v.split(",")]
        return v
    
    @validator('LOG_LEVEL')
    def validate_log_level(cls, v):
        """
        Validate and normalize logging level.
        
        Ensures the log level is one of the standard Python logging levels.
        Invalid levels are automatically corrected to 'INFO' to prevent
        logging configuration errors.
        """
        valid_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
        if v.upper() not in valid_levels:
            return 'INFO'
        return v.upper()
    
    @validator('ENVIRONMENT')
    def validate_environment(cls, v):
        """
        Validate deployment environment setting.
        
        Ensures the environment is one of the supported values, which may 
        affect behavior like requiring API keys in production but not
        in development environments.
        """
        valid_environments = ['development', 'testing', 'production']
        if v.lower() not in valid_environments:
            raise ValueError(f"ENVIRONMENT must be one of: {', '.join(valid_environments)}")
        return v.lower()
    
    @validator('SEC_RATE_LIMIT_REQUESTS', 'SEC_RATE_LIMIT_PERIOD_SEC', 'DEFAULT_N_YEARS',
               'METRICS_PORT', 'MAX_DOCUMENT_SIZE_MB', 'TEXT_TRUNCATION_CHARS', 'JOB_EXPIRY_SEC')
    def validate_positive_int(cls, v, values, field):
        """
        Validate that numeric configuration values are positive integers.
        
        This validator applies to multiple integer settings that must be
        positive to be valid. Zero or negative values would cause logical errors
        in various parts of the application.
        """
        if v <= 0:
            raise ValueError(f"{field.name} must be a positive integer")
        return v
    
    @validator('SEC_REQUEST_TIMEOUT_SEC', 'YAHOO_REQUEST_TIMEOUT_SEC')
    def validate_positive_float(cls, v, values, field):
        """
        Validate that timeout values are positive.
        
        Timeout settings must be positive numbers to be valid. Zero or negative
        timeouts would cause immediate request failures or other unexpected behavior.
        """
        if v <= 0:
            raise ValueError(f"{field.name} must be a positive number")
        return v
    
    @root_validator
    def validate_sec_settings(cls, values):
        """
        Cross-field validation for SEC API settings.
        
        This validator enforces rules that depend on multiple settings:
        1. SEC API key must be set in production environments
        2. Rate limit requests cannot exceed a reasonable maximum
        
        Using a root_validator allows checking relationships between different
        fields that can't be validated independently.
        """
        # Require SEC API key in production
        if values.get('api_keys').SEC_API_KEY is None and values.get('ENVIRONMENT') == 'production':
            raise ValueError("SEC_API_KEY must be set in production environment")
        
        # Rate limits should be reasonable
        if values.get('SEC_RATE_LIMIT_REQUESTS') > 100:
            raise ValueError("SEC_RATE_LIMIT_REQUESTS cannot exceed 100 requests per period")
        
        return values
    
    def as_dict(self) -> Dict[str, Any]:
        """
        Convert settings to dictionary, masking sensitive values.
        
        Returns a dictionary representation of all settings with sensitive
        values (like API keys and passwords) masked for safer logging and display.
        """
        # Convert to dict, excluding private attributes
        settings_dict = self.dict(exclude_none=True)
        
        # Recursively process the dict to mask sensitive values
        return self._mask_sensitive_values(settings_dict)
    
    def _mask_sensitive_values(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Recursively mask sensitive values in a dictionary.
        
        Implements security best practices by ensuring sensitive data like
        API keys and passwords are not exposed in logs or debug output.
        Traverses nested dictionaries to handle hierarchical configuration.
        """
        sensitive_keywords = ['key', 'password', 'secret', 'token']
        result = {}
        
        for key, value in data.items():
            # Check if this key might contain sensitive information
            if isinstance(value, dict):
                # Recursively process nested dictionaries
                result[key] = self._mask_sensitive_values(value)
            elif any(sensitive in key.lower() for sensitive in sensitive_keywords) and value:
                # Mask sensitive value
                result[key] = "****"
            else:
                # Keep original value
                result[key] = value
                
        return result
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = True


@lru_cache()
def get_settings() -> Settings:
    """
    Create and cache a Settings instance.
    
    Uses functools.lru_cache to cache the settings object, improving
    performance when settings are accessed frequently. This prevents
    repeated parsing of environment variables and validation checks
    when settings are accessed multiple times.
    
    Returns:
        Settings: Cached settings instance
    """
    return Settings()


# Expose the settings object for convenient imports
settings = get_settings()


# If this module is executed directly, print all settings
if __name__ == "__main__":
    import json
    print(json.dumps(settings.as_dict(), indent=4))