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

Usage:
    from config import settings
    
    # Access configuration values with proper typing
    redis_url = settings.redis.url
    sec_api_key = settings.api_keys.sec
"""

import os
import logging
from functools import lru_cache
from typing import Optional, Dict, Any, List

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
    """Redis-related configuration settings."""
    
    url: str = Field(
        default="redis://localhost:6379/0",
        env="REDIS_URL",
        description="Redis connection URL for job queue and result storage"
    )
    
    @validator('url')
    def validate_redis_url(cls, v):
        """Validate the Redis URL format."""
        if not v.startswith(('redis://', 'rediss://')):
            raise ValueError("Redis URL must start with redis:// or rediss://")
        return v


class ApiKeySettings(BaseSettings):
    """API key configuration settings."""
    
    sec: Optional[str] = Field(
        default=None,
        env="SEC_API_KEY",
        description="API key for SEC data acquisition"
    )
    
    openai: Optional[str] = Field(
        default=None,
        env="OPENAI_API_KEY",
        description="API key for OpenAI services"
    )


class ServiceUrlSettings(BaseSettings):
    """External service URL configuration settings."""
    
    sec_api_base: str = Field(
        default="https://api.sec-api.io",
        env="SEC_API_BASE_URL",
        description="Base URL for SEC API"
    )
    
    sec_edgar_base: str = Field(
        default="https://www.sec.gov/Archives/edgar/data",
        env="SEC_EDGAR_BASE_URL",
        description="Base URL for SEC EDGAR archive"
    )


class WorkerSettings(BaseSettings):
    """Worker-related configuration settings."""
    
    concurrency: PositiveInt = Field(
        default=10,
        env="WORKER_CONCURRENCY",
        description="Maximum number of concurrent tasks per worker"
    )
    
    sec_topic: str = Field(
        default="data.sec",
        env="SEC_TOPIC",
        description="Redis Stream topic for SEC data"
    )
    
    yahoo_topic: str = Field(
        default="data.yahoo",
        env="YAHOO_TOPIC",
        description="Redis Stream topic for Yahoo data"
    )


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
    Main application settings class that includes all sub-settings.
    
    This class acts as a container for all other settings categories,
    organizing them in a logical structure while providing global
    validation and documentation.
    """
    
    # Sub-settings by category
    redis: RedisSettings = RedisSettings()
    api_keys: ApiKeySettings = ApiKeySettings()
    service_urls: ServiceUrlSettings = ServiceUrlSettings()
    worker: WorkerSettings = WorkerSettings()
    embedding: EmbeddingSettings = EmbeddingSettings()
    
    # Global application settings
    app_name: str = Field(
        default="salescience",
        env="APP_NAME",
        description="Application name"
    )
    
    environment: str = Field(
        default="development",
        env="ENVIRONMENT",
        description="Deployment environment (development, testing, production)"
    )
    
    log_level: str = Field(
        default="INFO",
        env="LOG_LEVEL",
        description="Application logging level"
    )
    
    metrics_port: int = Field(
        default=8000,
        env="METRICS_PORT",
        description="Port for Prometheus metrics server"
    )
    
    # Critical dependency validation
    @root_validator
    def validate_critical_dependencies(cls, values):
        """Validate critical settings that are required for operation."""
        # Validate SEC API key if in production
        if values.get('environment') == 'production' and not values.get('api_keys').sec:
            logger.error("SEC_API_KEY is required in production environment")
            raise ValueError("SEC_API_KEY is required in production environment")
        
        # Issue warning for missing SEC API key in development
        if values.get('environment') != 'production' and not values.get('api_keys').sec:
            logger.warning("SEC_API_KEY is not set. SEC data acquisition will not function.")
        
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
    
    class Config:
        """Pydantic configuration for environment variable loading."""
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False

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
    
    This function uses lru_cache to ensure that settings are loaded only once,
    improving performance and preventing unnecessary reloading of environment
    variables.
    
    Returns:
        Cached Settings instance
    """
    logger.debug("Loading application settings")
    return Settings()


# Create global instance for easy importing
settings = get_settings()


# Basic demonstration if run directly
if __name__ == "__main__":
    print(settings)
    
    # Show how to access nested settings
    print(f"\nAccessing nested settings:")
    print(f"Redis URL: {settings.redis.url}")
    print(f"SEC API Base URL: {settings.service_urls.sec_api_base}")
    print(f"Embedding Model: {settings.embedding.model}")