"""
Error Handling Framework for Salescience Data Acquisition Layer
--------------------------------------------------------------

This module provides a comprehensive error handling framework
for the Salescience data acquisition pipeline, including:

1. A hierarchy of specialized exception types for different error categories
2. Context enrichment for error tracking and debugging
3. Standardized error formatting and serialization
4. Utilities for error logging and reporting

The error hierarchy follows the system architecture to provide
specific error types for each component and error condition.
This enables precise error handling, proper error propagation,
and informative error messages throughout the system.
"""

import json
import logging
import datetime
import traceback
from typing import Dict, Any, Optional, Union, List

# Set up module logger
logger = logging.getLogger(__name__)


def timestamp_now() -> str:
    """Return current timestamp in ISO format with Z suffix."""
    return datetime.datetime.utcnow().isoformat() + "Z"


class ErrorContext:
    """
    Container for error context information.
    
    Stores metadata about the error context including request ID,
    organization ID, user ID, and source-specific details.
    """
    
    def __init__(
        self,
        request_id: Optional[str] = None,
        organization_id: Optional[str] = None,
        user_id: Optional[str] = None,
        company: Optional[Dict[str, Any]] = None,
        source_type: Optional[str] = None,
        **kwargs
    ):
        """
        Initialize error context with provided information.
        
        Args:
            request_id: Unique identifier for the request
            organization_id: Organization identifier for multi-tenant isolation
            user_id: User identifier who initiated the request
            company: Company information (ticker, CIK, etc.)
            source_type: Data source type (SEC, Yahoo, etc.)
            **kwargs: Additional context information
        """
        self.request_id = request_id
        self.organization_id = organization_id
        self.user_id = user_id
        self.company = company or {}
        self.source_type = source_type
        self.timestamp = timestamp_now()
        self.additional = kwargs
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert error context to dictionary representation."""
        context = {
            "timestamp": self.timestamp,
        }
        
        if self.request_id:
            context["request_id"] = self.request_id
            
        if self.organization_id:
            context["organization_id"] = self.organization_id
            
        if self.user_id:
            context["user_id"] = self.user_id
            
        if self.company:
            context["company"] = self.company
            
        if self.source_type:
            context["source_type"] = self.source_type
            
        # Include additional context
        context.update(self.additional)
        
        return context
    
    def __str__(self) -> str:
        """String representation of error context."""
        parts = []
        
        if self.request_id:
            parts.append(f"request_id={self.request_id}")
            
        if self.organization_id:
            parts.append(f"org={self.organization_id}")
            
        if self.source_type:
            parts.append(f"source={self.source_type}")
            
        if self.company and "ticker" in self.company:
            parts.append(f"ticker={self.company['ticker']}")
        elif self.company and "cik" in self.company:
            parts.append(f"cik={self.company['cik']}")
            
        return " ".join(parts)


class SalescienceError(Exception):
    """
    Base exception class for all Salescience errors.
    
    Provides context enrichment and standardized formatting.
    All specific error types should inherit from this base class.
    """
    
    # Default error code and status for derived classes to override
    error_code = "GENERAL_ERROR"
    http_status = 500
    
    def __init__(
        self,
        message: str,
        context: Optional[ErrorContext] = None,
        cause: Optional[Exception] = None
    ):
        """
        Initialize the error with message and optional context.
        
        Args:
            message: Human-readable error description
            context: Error context information
            cause: Original exception that caused this error
        """
        super().__init__(message)
        self.message = message
        self.context = context or ErrorContext()
        self.cause = cause
        self.timestamp = timestamp_now()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert error to dictionary representation for serialization."""
        error_dict = {
            "error_code": self.error_code,
            "message": self.message,
            "timestamp": self.timestamp,
            "context": self.context.to_dict()
        }
        
        # Include cause information if available
        if self.cause:
            error_dict["cause"] = {
                "type": type(self.cause).__name__,
                "message": str(self.cause)
            }
            
        return error_dict
    
    def to_json(self) -> str:
        """Convert error to JSON string."""
        try:
            return json.dumps(self.to_dict())
        except Exception as e:
            logger.error(f"Error serializing exception to JSON: {e}")
            return json.dumps({
                "error_code": "JSON_SERIALIZATION_ERROR",
                "message": f"Error serializing exception: {e}",
                "timestamp": timestamp_now()
            })
    
    def to_log_entry(self) -> Dict[str, Any]:
        """Convert error to log entry format."""
        log_entry = self.to_dict()
        
        # Add stack trace for logging
        log_entry["traceback"] = traceback.format_exc()
        
        return log_entry
    
    def with_context(self, **kwargs) -> 'SalescienceError':
        """Add additional context to the error."""
        for key, value in kwargs.items():
            setattr(self.context, key, value)
        return self
    
    def __str__(self) -> str:
        """String representation including context if available."""
        error_str = self.message
        context_str = str(self.context)
        
        if context_str:
            error_str = f"{error_str} [{context_str}]"
            
        if self.cause:
            error_str = f"{error_str} Caused by: {type(self.cause).__name__}: {self.cause}"
            
        return error_str


# Configuration Errors
class ConfigurationError(SalescienceError):
    """Error raised when there is a configuration issue."""
    error_code = "CONFIGURATION_ERROR"
    http_status = 500


class MissingConfigurationError(ConfigurationError):
    """Error raised when a required configuration value is missing."""
    error_code = "MISSING_CONFIGURATION"
    http_status = 500


class InvalidConfigurationError(ConfigurationError):
    """Error raised when a configuration value is invalid."""
    error_code = "INVALID_CONFIGURATION"
    http_status = 500


# Data Source Errors
class DataSourceError(SalescienceError):
    """Base class for all data source errors."""
    error_code = "DATA_SOURCE_ERROR"
    http_status = 502  # Bad Gateway


class DataSourceConnectionError(DataSourceError):
    """Error raised when connection to a data source fails."""
    error_code = "DATA_SOURCE_CONNECTION_ERROR"
    http_status = 502


class DataSourceTimeoutError(DataSourceError):
    """Error raised when a data source request times out."""
    error_code = "DATA_SOURCE_TIMEOUT"
    http_status = 504  # Gateway Timeout


class DataSourceAuthenticationError(DataSourceError):
    """Error raised when authentication with a data source fails."""
    error_code = "DATA_SOURCE_AUTHENTICATION_ERROR"
    http_status = 401  # Unauthorized


class DataSourceRateLimitError(DataSourceError):
    """Error raised when a data source rate limit is exceeded."""
    error_code = "DATA_SOURCE_RATE_LIMIT"
    http_status = 429  # Too Many Requests


class DataSourceNotFoundError(DataSourceError):
    """Error raised when requested data is not found in the data source."""
    error_code = "DATA_SOURCE_NOT_FOUND"
    http_status = 404  # Not Found


# SEC-specific errors
class SECError(DataSourceError):
    """Base class for SEC-specific errors."""
    error_code = "SEC_ERROR"
    http_status = 502


class SECAuthenticationError(SECError, DataSourceAuthenticationError):
    """Error raised when SEC API authentication fails."""
    error_code = "SEC_AUTHENTICATION_ERROR"
    http_status = 401


class SECRateLimitError(SECError, DataSourceRateLimitError):
    """Error raised when SEC API rate limit is exceeded."""
    error_code = "SEC_RATE_LIMIT_ERROR"
    http_status = 429


class SECNotFoundError(SECError, DataSourceNotFoundError):
    """Error raised when SEC data is not found."""
    error_code = "SEC_NOT_FOUND_ERROR"
    http_status = 404


class SECParsingError(SECError):
    """Error raised when parsing SEC data fails."""
    error_code = "SEC_PARSING_ERROR"
    http_status = 500


# Yahoo-specific errors
class YahooError(DataSourceError):
    """Base class for Yahoo Finance-specific errors."""
    error_code = "YAHOO_ERROR"
    http_status = 502


class YahooAuthenticationError(YahooError, DataSourceAuthenticationError):
    """Error raised when Yahoo Finance API authentication fails."""
    error_code = "YAHOO_AUTHENTICATION_ERROR"
    http_status = 401


class YahooRateLimitError(YahooError, DataSourceRateLimitError):
    """Error raised when Yahoo Finance API rate limit is exceeded."""
    error_code = "YAHOO_RATE_LIMIT_ERROR"
    http_status = 429


class YahooNotFoundError(YahooError, DataSourceNotFoundError):
    """Error raised when Yahoo Finance data is not found."""
    error_code = "YAHOO_NOT_FOUND_ERROR"
    http_status = 404


class YahooParsingError(YahooError):
    """Error raised when parsing Yahoo Finance data fails."""
    error_code = "YAHOO_PARSING_ERROR"
    http_status = 500


# Redis errors
class RedisError(SalescienceError):
    """Base class for Redis-related errors."""
    error_code = "REDIS_ERROR"
    http_status = 500


class RedisConnectionError(RedisError):
    """Error raised when Redis connection fails."""
    error_code = "REDIS_CONNECTION_ERROR"
    http_status = 500


class RedisOperationError(RedisError):
    """Error raised when a Redis operation fails."""
    error_code = "REDIS_OPERATION_ERROR"
    http_status = 500


# Worker errors
class WorkerError(SalescienceError):
    """Base class for worker-related errors."""
    error_code = "WORKER_ERROR"
    http_status = 500


class WorkerProcessingError(WorkerError):
    """Error raised when worker job processing fails."""
    error_code = "WORKER_PROCESSING_ERROR"
    http_status = 500


class WorkerTimeoutError(WorkerError):
    """Error raised when worker job times out."""
    error_code = "WORKER_TIMEOUT_ERROR"
    http_status = 504  # Gateway Timeout


class WorkerConcurrencyError(WorkerError):
    """Error raised when worker concurrency limit is exceeded."""
    error_code = "WORKER_CONCURRENCY_ERROR"
    http_status = 429  # Too Many Requests


# API errors
class APIError(SalescienceError):
    """Base class for API-related errors."""
    error_code = "API_ERROR"
    http_status = 500


class ValidationError(APIError):
    """Error raised when request validation fails."""
    error_code = "VALIDATION_ERROR"
    http_status = 400  # Bad Request


class AuthorizationError(APIError):
    """Error raised when request authorization fails."""
    error_code = "AUTHORIZATION_ERROR"
    http_status = 403  # Forbidden


class RateLimitError(APIError):
    """Error raised when API rate limit is exceeded."""
    error_code = "RATE_LIMIT_ERROR"
    http_status = 429  # Too Many Requests


# Utility functions
def format_error_response(
    error: Union[SalescienceError, Exception],
    status_code: Optional[int] = None
) -> Dict[str, Any]:
    """
    Format error for API response.
    
    Converts error to a standardized format for API responses.
    If the error is not a SalescienceError, wraps it in one.
    
    Args:
        error: Exception to format
        status_code: Optional HTTP status code to override default
        
    Returns:
        Dictionary with standardized error response
    """
    if not isinstance(error, SalescienceError):
        # Wrap generic exception in SalescienceError
        error = SalescienceError(str(error), cause=error)
    
    response = {
        "success": False,
        "error": {
            "code": error.error_code,
            "message": str(error)
        }
    }
    
    # Add context if available
    if hasattr(error, 'context') and error.context:
        response["error"]["context"] = error.context.to_dict()
    
    return response


def log_error(
    error: Union[SalescienceError, Exception],
    logger_instance: Optional[logging.Logger] = None
) -> None:
    """
    Log error with appropriate level and context.
    
    Args:
        error: Exception to log
        logger_instance: Logger to use (defaults to module logger)
    """
    log = logger_instance or logger
    
    if isinstance(error, SalescienceError):
        log_entry = error.to_log_entry()
        log.error(f"{error.error_code}: {error.message}", extra=log_entry)
    else:
        log.error(f"Unhandled exception: {error}", exc_info=True)


def map_http_status(error: Union[SalescienceError, Exception]) -> int:
    """
    Map error to HTTP status code.
    
    Args:
        error: Exception to map
        
    Returns:
        HTTP status code
    """
    if isinstance(error, SalescienceError):
        return error.http_status
    return 500  # Default to Internal Server Error


def map_http_error(status_code: int, error_message: str) -> SalescienceError:
    """
    Map HTTP error to appropriate SalescienceError.
    
    Args:
        status_code: HTTP status code
        error_message: Error message
        
    Returns:
        Appropriate SalescienceError instance
    """
    if status_code == 401:
        return DataSourceAuthenticationError(error_message)
    elif status_code == 403:
        return AuthorizationError(error_message)
    elif status_code == 404:
        return DataSourceNotFoundError(error_message)
    elif status_code == 429:
        return DataSourceRateLimitError(error_message)
    elif status_code >= 500:
        return DataSourceError(error_message)
    else:
        return SalescienceError(error_message)