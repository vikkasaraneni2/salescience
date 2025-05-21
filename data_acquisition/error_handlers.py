"""
Error Handlers for Orchestrator API
----------------------------------

This module provides standardized error handling for the Orchestrator API.
It integrates with FastAPI's exception handling system to provide consistent,
well-structured error responses for various error conditions.

The handlers ensure that:
1. All errors follow a consistent format
2. Appropriate HTTP status codes are returned
3. Error messages are clear and informative
4. Sensitive information is properly masked
5. Errors are properly logged with context
"""

import logging
from typing import Dict, Any, Optional, Union, Type
from fastapi import Request, FastAPI, HTTPException
from fastapi.responses import JSONResponse
from fastapi.exception_handlers import http_exception_handler
from pydantic import ValidationError

from data_acquisition.errors import (
    SalescienceError, ErrorContext,
    ValidationError as SalescienceValidationError,
    SECError, YahooError, RedisError, WorkerError, APIError,
    format_error_response, log_error, map_http_status
)

# Set up logger
logger = logging.getLogger("error_handlers")


async def salescience_exception_handler(request: Request, exc: SalescienceError) -> JSONResponse:
    """
    FastAPI exception handler for SalescienceError exceptions.
    
    This handler converts SalescienceError instances to standardized JSON responses
    with appropriate HTTP status codes based on the error type.
    
    Args:
        request: FastAPI request object
        exc: SalescienceError instance
        
    Returns:
        JSONResponse with error details and appropriate status code
    """
    # Log the error with context
    log_error(exc, logger)
    
    # Format error response
    response_body = format_error_response(exc)
    
    # Get appropriate status code
    status_code = map_http_status(exc)
    
    # Return formatted JSON response
    return JSONResponse(
        status_code=status_code,
        content=response_body
    )


async def validation_exception_handler(request: Request, exc: ValidationError) -> JSONResponse:
    """
    FastAPI exception handler for Pydantic ValidationError exceptions.
    
    This handler converts Pydantic ValidationError exceptions to standardized error
    responses with detailed information about validation failures.
    
    Args:
        request: FastAPI request object
        exc: Pydantic ValidationError instance
        
    Returns:
        JSONResponse with validation error details
    """
    # Create SalescienceValidationError from Pydantic ValidationError
    validation_error = SalescienceValidationError(
        message="Request validation failed",
        context=ErrorContext(
            request_id=request.headers.get("X-Request-ID"),
            path=request.url.path,
            method=request.method
        ),
        cause=exc
    )
    
    # Extract validation errors
    error_details = []
    for error in exc.errors():
        error_details.append({
            "field": ".".join(str(loc) for loc in error["loc"]),
            "type": error["type"],
            "message": error["msg"]
        })
    
    # Log the validation error
    log_error(validation_error, logger)
    
    # Format the response
    response_body = {
        "success": False,
        "error": {
            "code": validation_error.error_code,
            "message": "Request validation failed",
            "details": error_details
        }
    }
    
    # Return formatted JSON response
    return JSONResponse(
        status_code=400,
        content=response_body
    )


async def http_exception_with_logging(request: Request, exc: HTTPException) -> JSONResponse:
    """
    Enhanced HTTP exception handler with logging.
    
    This handler adds logging to FastAPI's default HTTP exception handler
    to provide better visibility into API errors.
    
    Args:
        request: FastAPI request object
        exc: HTTPException instance
        
    Returns:
        Default HTTP exception response
    """
    # Log the HTTP exception
    logger.error(
        f"HTTP Exception: {exc.status_code} {exc.detail}",
        extra={
            "request_id": request.headers.get("X-Request-ID"),
            "path": request.url.path,
            "method": request.method,
            "status_code": exc.status_code
        }
    )
    
    # Use FastAPI's default HTTP exception handler
    return await http_exception_handler(request, exc)


async def generic_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """
    Handler for unhandled exceptions.
    
    This is a catch-all handler for any exceptions that aren't specifically
    handled elsewhere. It provides a generic error response without exposing
    sensitive details.
    
    Args:
        request: FastAPI request object
        exc: Any exception
        
    Returns:
        JSONResponse with generic error message
    """
    # Wrap in SalescienceError for consistent handling
    error = SalescienceError(
        message="An unexpected error occurred",
        context=ErrorContext(
            request_id=request.headers.get("X-Request-ID"),
            path=request.url.path,
            method=request.method
        ),
        cause=exc
    )
    
    # Log with full details for debugging
    log_error(error, logger)
    
    # Return generic error to client (don't expose internal details)
    return JSONResponse(
        status_code=500,
        content={
            "success": False,
            "error": {
                "code": "INTERNAL_SERVER_ERROR",
                "message": "An unexpected error occurred"
            }
        }
    )


def register_exception_handlers(app: FastAPI) -> None:
    """
    Register all exception handlers with a FastAPI application.
    
    This function sets up the exception handlers for various error types
    to ensure consistent error handling throughout the API.
    
    Args:
        app: FastAPI application instance
    """
    # Register handler for SalescienceError and all its subclasses
    app.add_exception_handler(SalescienceError, salescience_exception_handler)
    
    # Register handler for Pydantic ValidationError
    app.add_exception_handler(ValidationError, validation_exception_handler)
    
    # Register enhanced handler for HTTPException
    app.add_exception_handler(HTTPException, http_exception_with_logging)
    
    # Register catch-all handler for unhandled exceptions
    app.add_exception_handler(Exception, generic_exception_handler)