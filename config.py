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
from typing import List, Optional, Dict, Any, Union, Type, TypeVar, Callable
from pydantic import (
    BaseSettings, 
    Field, 
    validator, 
    AnyHttpUrl,
    PositiveInt,
    root_validator,
    create_model
)

# Set up logger for config module
logger = logging.getLogger("config")

# Define environment variables for consistency
ENVIRONMENT_DEV = "development"
ENVIRONMENT_TEST = "testing"
ENVIRONMENT_PROD = "production"
ENVIRONMENTS = [ENVIRONMENT_DEV, ENVIRONMENT_TEST, ENVIRONMENT_PROD]

# Define log levels for consistency
LOG_LEVELS = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]

# Common validation functions
def validate_positive_int(v: int, field_name: str) -> int:
    """Validate that a value is a positive integer."""
    if v <= 0:
        raise ValueError(f"{field_name} must be a positive integer")
    return v

def validate_positive_float(v: float, field_name: str) -> float:
    """Validate that a value is a positive float."""
    if v <= 0:
        raise ValueError(f"{field_name} must be a positive number")
    return v

def validate_url(v: str) -> str:
    """Validate that a value is a proper URL."""
    if not v.startswith(('http://', 'https://')):
        raise ValueError(f"URL must start with 'http://' or 'https://'")
    return v

def validate_redis_url(v: str) -> str:
    """Validate Redis URL format."""
    if not v.startswith(('redis://', 'rediss://')):
        raise ValueError("Redis URL must start with 'redis://' or 'rediss://'")
    return v

# Base settings class with common configuration
class BaseAppSettings(BaseSettings):
    """Base settings class with common configuration."""
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False


class RedisSettings(BaseAppSettings):
    """Redis connection settings"""
    url: str = Field("redis://localhost:6379/0", env="REDIS_URL")
    password: Optional[str] = Field(None, env="REDIS_PASSWORD")
    ssl: bool = Field(False, env="REDIS_SSL")
    pool_size: int = Field(10, env="REDIS_POOL_SIZE")
    socket_timeout: int = Field(5, env="REDIS_SOCKET_TIMEOUT")
    socket_connect_timeout: int = Field(5, env="REDIS_SOCKET_CONNECT_TIMEOUT")
    retry_on_timeout: bool = Field(True, env="REDIS_RETRY_ON_TIMEOUT")

    # Validators
    _validate_url = validator("url", allow_reuse=True)(validate_redis_url)
    _validate_pool_size = validator("pool_size", allow_reuse=True)(
        lambda v, values, field: validate_positive_int(v, field.name)
    )
    _validate_timeouts = validator("socket_timeout", "socket_connect_timeout", allow_reuse=True)(
        lambda v, values, field: validate_positive_int(v, field.name)
    )


class ApiKeySettings(BaseAppSettings):
    """API key settings for external services"""
    sec: Optional[str] = Field(None, env="SEC_API_KEY")
    yahoo: Optional[str] = Field(None, env="YAHOO_API_KEY")
    openai: Optional[str] = Field(None, env="OPENAI_API_KEY")

    @validator("sec")
    def validate_sec_api_key(cls, v):
        """Validate SEC API key when provided."""
        if v is not None and not v.strip():
            raise ValueError("SEC_API_KEY cannot be an empty string")
        return v


class ServiceUrlSettings(BaseAppSettings):
    """Service URL settings for external APIs"""
    sec_api_base: str = Field("https://api.sec-api.io", env="SEC_API_BASE_URL")
    sec_edgar_base: str = Field("https://www.sec.gov/Archives/edgar/data", env="SEC_EDGAR_BASE_URL")
    yahoo_api_base: str = Field("https://query1.finance.yahoo.com/v10/finance", env="YAHOO_API_BASE_URL")
    
    # Validators
    _validate_urls = validator("sec_api_base", "sec_edgar_base", "yahoo_api_base", allow_reuse=True)(
        lambda v, values, field: validate_url(v)
    )


class WorkerSettings(BaseAppSettings):
    """Worker configuration settings"""
    concurrency: int = Field(10, env="WORKER_CONCURRENCY")
    max_retries: int = Field(3, env="WORKER_MAX_RETRIES")
    retry_delay_sec: int = Field(5, env="WORKER_RETRY_DELAY_SEC")
    poll_interval_sec: int = Field(5, env="WORKER_POLL_INTERVAL_SEC")
    timeout_sec: int = Field(300, env="WORKER_TIMEOUT_SEC")
    
    # Validators
    @validator("concurrency")
    def check_concurrency(cls, v):
        """Validate worker concurrency setting."""
        validate_positive_int(v, "concurrency")
        if v > 20:
            logger.warning(f"Worker concurrency ({v}) exceeds recommended maximum (20)")
        return v
    
    _validate_worker_settings = validator(
        "max_retries", "retry_delay_sec", "poll_interval_sec", "timeout_sec", 
        allow_reuse=True
    )(lambda v, values, field: validate_positive_int(v, field.name))


class MessageBusSettings(BaseAppSettings):
    """Message bus configuration settings"""
    enabled: bool = Field(True, env="MESSAGE_BUS_ENABLED")
    sec_topic: str = Field("data.sec", env="SEC_TOPIC")
    yahoo_topic: str = Field("data.yahoo", env="YAHOO_TOPIC")
    xbrl_topic_prefix: str = Field("data.xbrl", env="XBRL_TOPIC_PREFIX")
    retention_ms: int = Field(86400000, env="MESSAGE_RETENTION_MS")  # 24 hours
    
    # Validators
    _validate_retention = validator("retention_ms", allow_reuse=True)(
        lambda v, values, field: validate_positive_int(v, field.name)
    )
    
    @validator("sec_topic", "yahoo_topic", "xbrl_topic_prefix")
    def validate_topic_format(cls, v, values, field):
        """Validate message topic name format."""
        if not re.match(r'^[a-zA-Z0-9_.]+$', v):
            raise ValueError(f"{field.name} must contain only alphanumeric characters, dots, and underscores")
        return v


class EmbeddingSettings(BaseAppSettings):
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
    
    @validator("model")
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


class Settings(BaseAppSettings):
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
    
    # Application settings
    app_name: str = Field("salescience", env="APP_NAME", description="Application name")
    environment: str = Field(ENVIRONMENT_DEV, env="ENVIRONMENT")
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
    
    # Validators
    @validator("default_xbrl_concepts", pre=True)
    def parse_xbrl_concepts(cls, v):
        """Parse XBRL concepts from string to list."""
        if isinstance(v, str):
            return [concept.strip() for concept in v.split(",")]
        return v
    
    @validator("log_level")
    def validate_log_level(cls, v):
        """Validate and normalize logging level."""
        if v.upper() not in LOG_LEVELS:
            logger.warning(f"Invalid log level: {v}, defaulting to INFO")
            return "INFO"
        return v.upper()
    
    @validator("environment")
    def validate_environment(cls, v):
        """Validate deployment environment setting."""
        if v.lower() not in ENVIRONMENTS:
            raise ValueError(f"environment must be one of: {', '.join(ENVIRONMENTS)}")
        return v.lower()
    
    # Use common validators for numeric fields
    _validate_positive_ints = validator(
        "sec_rate_limit_requests", "sec_rate_limit_period_sec", "default_n_years",
        "metrics_port", "max_document_size_mb", "text_truncation_chars", "job_expiry_sec",
        allow_reuse=True
    )(lambda v, values, field: validate_positive_int(v, field.name))
    
    _validate_positive_floats = validator(
        "sec_request_timeout_sec", "yahoo_request_timeout_sec",
        allow_reuse=True
    )(lambda v, values, field: validate_positive_float(v, field.name))
    
    @root_validator
    def validate_sec_settings(cls, values):
        """Cross-field validation for SEC API settings."""
        # Require SEC API key in production
        if values.get('api_keys').sec is None and values.get('environment') == ENVIRONMENT_PROD:
            raise ValueError("SEC_API_KEY must be set in production environment")
        
        # Rate limits should be reasonable
        if values.get('sec_rate_limit_requests') > 100:
            raise ValueError("sec_rate_limit_requests cannot exceed 100 requests per period")
        
        # Validate embedding model and vector_dim combination
        model = values.get('embedding').model
        vector_dim = values.get('embedding').vector_dim
        
        model_dims = {
            "text-embedding-ada-002": 1536,
            "text-embedding-3-small": 1536,
            "text-embedding-3-large": 3072
        }
        
        if model in model_dims and vector_dim != model_dims[model]:
            logger.warning(
                f"Model {model} produces {model_dims[model]}-dim vectors, "
                f"but vector_dim is set to {vector_dim}"
            )
        
        return values
    
    def as_dict(self) -> Dict[str, Any]:
        """Convert settings to dictionary, masking sensitive values."""
        # Convert to dict, excluding private attributes
        settings_dict = self.dict(exclude_none=True)
        
        # Recursively process the dict to mask sensitive values
        return self._mask_sensitive_values(settings_dict)
    
    def _mask_sensitive_values(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Recursively mask sensitive values in a dictionary."""
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