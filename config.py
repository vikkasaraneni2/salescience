"""
Salescience Configuration Module
-------------------------------

This module provides centralized configuration management using Pydantic for
validation, typing, and environment variable loading. It organizes settings
by category and implements proper validation rules.

Key features:
- Organized by functional category (Redis, API Keys, etc.)
- Strong type validation with Pydantic
- Reasonable defaults for development
- Environment variable loading with .env support
- Configuration caching to prevent reloading
- Custom validators for critical settings
- LRU caching for efficient settings access
- Secure handling of sensitive configuration values

Usage:
    from config import settings
    
    # Access configuration values with proper typing
    redis_url = settings.redis.url
    sec_api_key = settings.api_keys.sec
"""

import os
import re
import logging
from functools import lru_cache
from typing import List, Optional, Dict, Any, Union
from pydantic import (
    BaseSettings, 
    Field, 
    validator, 
    AnyHttpUrl,
    PositiveInt,
    root_validator
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("config")


class RedisSettings(BaseSettings):
    """Redis connection settings"""
    url: str = Field("redis://localhost:6379/0", env="REDIS_URL")
    password: Optional[str] = Field(None, env="REDIS_PASSWORD")
    ssl: bool = Field(False, env="REDIS_SSL")
    pool_size: int = Field(10, env="REDIS_POOL_SIZE")
    socket_timeout: int = Field(5, env="REDIS_SOCKET_TIMEOUT")
    socket_connect_timeout: int = Field(5, env="REDIS_SOCKET_CONNECT_TIMEOUT")
    retry_on_timeout: bool = Field(True, env="REDIS_RETRY_ON_TIMEOUT")

    @validator('url')
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
    
    @validator('pool_size', 'socket_timeout', 'socket_connect_timeout')
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
        case_sensitive = False


class ApiKeySettings(BaseSettings):
    """API key settings for external services"""
    sec: Optional[str] = Field(None, env="SEC_API_KEY")
    yahoo: Optional[str] = Field(None, env="YAHOO_API_KEY")
    openai: Optional[str] = Field(None, env="OPENAI_API_KEY")

    @validator('sec')
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
        case_sensitive = False


class ServiceUrlSettings(BaseSettings):
    """Service URL settings for external APIs"""
    sec_api_base: str = Field("https://api.sec-api.io", env="SEC_API_BASE_URL")
    sec_edgar_base: str = Field("https://www.sec.gov/Archives/edgar/data", env="SEC_EDGAR_BASE_URL")
    yahoo_api_base: str = Field("https://query1.finance.yahoo.com/v10/finance", env="YAHOO_API_BASE_URL")
    
    @validator('sec_api_base', 'sec_edgar_base', 'yahoo_api_base')
    def validate_url_format(cls, v, values, field):
        """
        Validate all service URLs have proper HTTP/HTTPS prefixes.
        
        This validator applies to all URL fields to ensure they begin with
        an appropriate protocol (http:// or https://). Malformed URLs would
        cause request failures when trying to access these services.
        """
        if not v.startswith(('http://', 'https://')):
            raise ValueError(f"{field.name} must be a valid URL starting with 'http://' or 'https://'")
        return v
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False


class WorkerSettings(BaseSettings):
    """Worker configuration settings"""
    concurrency: int = Field(10, env="WORKER_CONCURRENCY")
    max_retries: int = Field(3, env="WORKER_MAX_RETRIES")
    retry_delay_sec: int = Field(5, env="WORKER_RETRY_DELAY_SEC")
    poll_interval_sec: int = Field(5, env="WORKER_POLL_INTERVAL_SEC")
    timeout_sec: int = Field(300, env="WORKER_TIMEOUT_SEC")
    
    @validator('concurrency')
    def check_concurrency(cls, v):
        """
        Validate worker concurrency setting.
        
        This validator ensures the concurrency level is a positive integer,
        and notes when it exceeds the recommended maximum of 20.
        High concurrency values could overload external APIs or consume
        excessive system resources.
        """
        if v <= 0:
            raise ValueError("concurrency must be a positive integer")
        if v > 20:
            # Still allow values over 20, but a warning will be logged elsewhere
            pass
        return v
    
    @validator('max_retries', 'retry_delay_sec', 'poll_interval_sec', 'timeout_sec')
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
        case_sensitive = False


class MessageBusSettings(BaseSettings):
    """Message bus configuration settings"""
    enabled: bool = Field(True, env="MESSAGE_BUS_ENABLED")
    sec_topic: str = Field("data.sec", env="SEC_TOPIC")
    yahoo_topic: str = Field("data.yahoo", env="YAHOO_TOPIC")
    xbrl_topic_prefix: str = Field("data.xbrl", env="XBRL_TOPIC_PREFIX")
    retention_ms: int = Field(86400000, env="MESSAGE_RETENTION_MS")  # 24 hours
    
    @validator('retention_ms')
    def validate_retention_ms(cls, v):
        """
        Validate message retention period.
        
        Ensures the message retention time is a positive integer.
        A non-positive value would cause invalid Redis stream configuration
        and potentially immediate message deletion.
        """
        if v <= 0:
            raise ValueError("retention_ms must be a positive integer")
        return v
    
    @validator('sec_topic', 'yahoo_topic', 'xbrl_topic_prefix')
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
        case_sensitive = False


class EmbeddingSettings(BaseSettings):
    """Embedding and vector store configuration settings."""
    
    model: str = Field(
        default="text-embedding-ada-002",
        env="EMBED_MODEL",
        description="Embedding model to use"
    )
    
    vector_dim: PositiveInt = Field(
        default=1536,
        env="VECTOR_DIM",
        description="Dimensionality of vector embeddings"
    )
    
    @validator('model')
    def validate_model_name(cls, v):
        """Validate that the model name is supported."""
        supported_models = [
            "text-embedding-ada-002",
            "text-embedding-3-small",
            "text-embedding-3-large"
        ]
        if v not in supported_models:
            logger.warning(f"Using unsupported embedding model: {v}")
            logger.warning(f"Supported models are: {', '.join(supported_models)}")
        return v


class Settings(BaseSettings):
    """
    Main settings class that combines all sub-settings.
    
    This class provides a hierarchical configuration structure with
    comprehensive validation rules to ensure a consistent and valid
    configuration across all components of the application.
    """
    # Sub-configuration sections
    redis: RedisSettings = Field(default_factory=RedisSettings)
    api_keys: ApiKeySettings = Field(default_factory=ApiKeySettings)
    service_urls: ServiceUrlSettings = Field(default_factory=ServiceUrlSettings)
    worker: WorkerSettings = Field(default_factory=WorkerSettings)
    message_bus: MessageBusSettings = Field(default_factory=MessageBusSettings)
    embedding: EmbeddingSettings = Field(default_factory=EmbeddingSettings)
    
    # Additional settings
    app_name: str = Field("salescience", env="APP_NAME", description="Application name")
    environment: str = Field("development", env="ENVIRONMENT")
    log_level: str = Field("INFO", env="LOG_LEVEL")
    json_logging_enabled: bool = Field(True, env="JSON_LOGGING_ENABLED")
    
    # API Rate Limits and Timeouts
    sec_rate_limit_requests: int = Field(10, env="SEC_RATE_LIMIT_REQUESTS")
    sec_rate_limit_period_sec: int = Field(1, env="SEC_RATE_LIMIT_PERIOD_SEC")
    sec_request_timeout_sec: float = Field(15.0, env="SEC_REQUEST_TIMEOUT_SEC")
    yahoo_request_timeout_sec: float = Field(10.0, env="YAHOO_REQUEST_TIMEOUT_SEC")
    
    # Queue configuration
    queue_name: str = Field("data_jobs", env="QUEUE_NAME")
    job_expiry_sec: int = Field(604800, env="JOB_EXPIRY_SEC")  # 1 week
    
    # Prometheus metrics configuration
    prometheus_enabled: bool = Field(True, env="PROMETHEUS_ENABLED")
    metrics_port: int = Field(8000, env="METRICS_PORT")
    
    # Document processing configuration
    max_document_size_mb: int = Field(50, env="MAX_DOCUMENT_SIZE_MB")
    text_truncation_chars: int = Field(500, env="TEXT_TRUNCATION_CHARS")
    
    # Default data acquisition parameters
    default_form_type: str = Field("10-K", env="DEFAULT_FORM_TYPE")
    default_n_years: int = Field(5, env="DEFAULT_N_YEARS")
    default_xbrl_concepts: List[str] = Field(
        ["us-gaap:Revenues", "us-gaap:NetIncomeLoss", "us-gaap:Assets", 
         "us-gaap:Liabilities", "us-gaap:StockholdersEquity"],
        env="DEFAULT_XBRL_CONCEPTS"
    )
    
    # Feature flags
    feature_xbrl_enabled: bool = Field(True, env="FEATURE_XBRL_ENABLED")
    feature_sec_insider_enabled: bool = Field(True, env="FEATURE_SEC_INSIDER_ENABLED")
    feature_batch_jobs_enabled: bool = Field(True, env="FEATURE_BATCH_JOBS_ENABLED")
    
    # SEC User Agent
    sec_user_agent: str = Field("salescience/1.0", env="SEC_USER_AGENT")
    
    @validator('default_xbrl_concepts', pre=True)
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
    
    @validator('log_level')
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
    
    @validator('environment')
    def validate_environment(cls, v):
        """
        Validate deployment environment setting.
        
        Ensures the environment is one of the supported values, which may 
        affect behavior like requiring API keys in production but not
        in development environments.
        """
        valid_environments = ['development', 'testing', 'production']
        if v.lower() not in valid_environments:
            raise ValueError(f"environment must be one of: {', '.join(valid_environments)}")
        return v.lower()
    
    @validator('sec_rate_limit_requests', 'sec_rate_limit_period_sec', 'default_n_years',
               'metrics_port', 'max_document_size_mb', 'text_truncation_chars', 'job_expiry_sec')
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
    
    @validator('sec_request_timeout_sec', 'yahoo_request_timeout_sec')
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
        if values.get('api_keys').sec is None and values.get('environment') == 'production':
            raise ValueError("SEC_API_KEY must be set in production environment")
        
        # Rate limits should be reasonable
        if values.get('sec_rate_limit_requests') > 100:
            raise ValueError("sec_rate_limit_requests cannot exceed 100 requests per period")
        
        # Validate embedding model and vector_dim combination
        model = values.get('embedding').model
        vector_dim = values.get('embedding').vector_dim
        
        if model == "text-embedding-ada-002" and vector_dim != 1536:
            logger.warning(f"Model {model} produces 1536-dim vectors, but VECTOR_DIM is set to {vector_dim}")
        
        if model == "text-embedding-3-small" and vector_dim != 1536:
            logger.warning(f"Model {model} produces 1536-dim vectors, but VECTOR_DIM is set to {vector_dim}")
            
        if model == "text-embedding-3-large" and vector_dim != 3072:
            logger.warning(f"Model {model} produces 3072-dim vectors, but VECTOR_DIM is set to {vector_dim}")
        
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
    
    def __str__(self) -> str:
        """Human-readable representation of configuration."""
        env = self.environment.upper()
        return (
            f"Salescience Configuration ({env}):\n"
            f"  App Name: {self.app_name}\n"
            f"  Redis URL: {self.redis.url}\n"
            f"  SEC API Key: {'[SET]' if self.api_keys.sec else '[NOT SET]'}\n"
            f"  OpenAI API Key: {'[SET]' if self.api_keys.openai else '[NOT SET]'}\n"
            f"  Worker Concurrency: {self.worker.concurrency}\n"
            f"  Embedding Model: {self.embedding.model} ({self.embedding.vector_dim} dimensions)\n"
            f"  Log Level: {self.log_level}\n"
        )
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False


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
    logger.debug("Loading application settings")
    return Settings()


# Expose the settings object for convenient imports
settings = get_settings()


# If this module is executed directly, print all settings
if __name__ == "__main__":
    print(settings)
    
    # Show how to access nested settings
    print(f"\nAccessing nested settings:")
    print(f"Redis URL: {settings.redis.url}")
    print(f"SEC API Base URL: {settings.service_urls.sec_api_base}")
    print(f"Embedding Model: {settings.embedding.model}")