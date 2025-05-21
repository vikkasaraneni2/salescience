"""
Health Check Module for Salescience Data Acquisition System
----------------------------------------------------------

This module provides comprehensive health monitoring for the data acquisition system,
designed to integrate with Kubernetes liveness and readiness probes while providing
detailed system status information for operations and debugging.

The module implements three distinct endpoint types:
1. Liveness Probe (/health): Simple check that the service is running
2. Readiness Probe (/readiness): Comprehensive dependency availability checks  
3. Status Endpoint (/status): Detailed system metrics and component health

Key Features:
- Redis connectivity verification with configurable timeouts
- External API availability checks with circuit breaker awareness
- Cached health check results to minimize external service impact
- Integration with centralized configuration and error handling
- Structured JSON logging via JsonLogger
- Request ID tracing for distributed debugging
- ISO-8601 timestamps with Z suffix for consistent time handling
- Service uptime tracking for operational monitoring

Architecture Integration:
- Uses centralized settings from config.py for all configuration
- Integrates with error framework for consistent error handling
- Provides FastAPI router for easy integration with main application
- Follows multi-tenant patterns established in the system
- Compatible with Kubernetes health probe requirements
"""

import time
import httpx
import asyncio
import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional, Tuple
from fastapi import APIRouter, status, Request
from fastapi.responses import JSONResponse

# Import centralized configuration and utilities
from config import settings
from data_acquisition.utils import configure_logging, JsonLogger
from data_acquisition.errors import (
    SalescienceError, ErrorContext,
    RedisConnectionError, DataSourceConnectionError,
    format_error_response, log_error
)

# Redis client setup
import redis

# Initialize logger
logger = configure_logging("health")
json_logger = JsonLogger("health")

# Service start time for uptime calculation
SERVICE_START_TIME = time.time()

# Cache for health check results to avoid hammering external services
class HealthCheckCache:
    """Simple in-memory cache for health check results."""
    
    def __init__(self, default_ttl: int = 30):
        self.cache: Dict[str, Tuple[Any, float]] = {}
        self.default_ttl = default_ttl
    
    def get(self, key: str) -> Optional[Any]:
        """Get cached value if not expired."""
        if key in self.cache:
            value, expires_at = self.cache[key]
            if time.time() < expires_at:
                return value
            else:
                del self.cache[key]
        return None
    
    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """Cache value with TTL."""
        if ttl is None:
            ttl = self.default_ttl
        expires_at = time.time() + ttl
        self.cache[key] = (value, expires_at)
    
    def clear(self) -> None:
        """Clear all cached values."""
        self.cache.clear()

# Global cache instance
health_cache = HealthCheckCache(default_ttl=30)

def get_iso_timestamp() -> str:
    """Return current timestamp in ISO format with Z suffix."""
    return datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')

def get_service_uptime() -> Dict[str, Any]:
    """Calculate and return service uptime information."""
    uptime_seconds = time.time() - SERVICE_START_TIME
    
    days = int(uptime_seconds // 86400)
    hours = int((uptime_seconds % 86400) // 3600)
    minutes = int((uptime_seconds % 3600) // 60)
    
    return {
        "uptime_seconds": round(uptime_seconds, 2),
        "uptime_human": f"{days}d {hours}h {minutes}m",
        "started_at": datetime.fromtimestamp(SERVICE_START_TIME, timezone.utc).isoformat().replace('+00:00', 'Z')
    }

async def check_redis_connectivity(request_id: str) -> Dict[str, Any]:
    """
    Check Redis connectivity with comprehensive testing.
    
    Args:
        request_id: Request ID for tracing
        
    Returns:
        Dictionary with Redis health status, timing, and details
    """
    context = ErrorContext(
        request_id=request_id,
        operation="check_redis_connectivity",
        source_type="Redis"
    )
    
    # Check cache first
    cache_key = "redis_health"
    cached_result = health_cache.get(cache_key)
    if cached_result:
        return cached_result
    
    start_time = time.time()
    
    try:
        # Create Redis client with timeout configuration
        redis_client = redis.Redis.from_url(
            settings.redis.url,
            decode_responses=True,
            password=settings.redis.password,
            ssl=settings.redis.ssl,
            socket_timeout=min(settings.redis.socket_timeout, 5),  # Cap at 5s for health checks
            socket_connect_timeout=min(settings.redis.socket_connect_timeout, 3),  # Cap at 3s
            retry_on_timeout=False  # Don't retry for health checks
        )
        
        # Test basic connectivity
        ping_start = time.time()
        ping_result = redis_client.ping()
        ping_duration = (time.time() - ping_start) * 1000  # Convert to ms
        
        if not ping_result:
            raise RedisConnectionError("Redis ping returned False", context=context)
        
        # Test write/read operations
        test_key = f"health_check:{request_id}"
        test_value = f"health_test_{int(time.time())}"
        
        write_start = time.time()
        redis_client.setex(test_key, 60, test_value)  # Expire in 60 seconds
        write_duration = (time.time() - write_start) * 1000
        
        read_start = time.time()
        retrieved_value = redis_client.get(test_key)
        read_duration = (time.time() - read_start) * 1000
        
        if retrieved_value != test_value:
            raise RedisConnectionError("Redis write/read test failed", context=context)
        
        # Clean up test key
        redis_client.delete(test_key)
        
        total_duration = (time.time() - start_time) * 1000
        
        result = {
            "status": "healthy",
            "ping_ms": round(ping_duration, 2),
            "write_ms": round(write_duration, 2),
            "read_ms": round(read_duration, 2),
            "total_ms": round(total_duration, 2),
            "url": settings.redis.url.split('@')[-1],  # Mask credentials
            "ssl_enabled": settings.redis.ssl,
            "connection_pool_size": settings.redis.pool_size
        }
        
        # Cache successful result
        health_cache.set(cache_key, result, ttl=20)  # Cache for 20 seconds
        
        logger.debug(f"[{request_id}] Redis health check passed", extra={
            "ping_ms": result["ping_ms"],
            "total_ms": result["total_ms"]
        })
        
        return result
        
    except Exception as e:
        error_duration = (time.time() - start_time) * 1000
        
        result = {
            "status": "unhealthy",
            "error": str(e),
            "error_type": type(e).__name__,
            "duration_ms": round(error_duration, 2),
            "url": settings.redis.url.split('@')[-1] if hasattr(settings.redis, 'url') else "unknown"
        }
        
        # Cache failed result for shorter duration
        health_cache.set(cache_key, result, ttl=5)  # Cache failures for 5 seconds
        
        logger.error(f"[{request_id}] Redis health check failed", extra={
            "error": str(e),
            "duration_ms": result["duration_ms"]
        })
        
        return result

async def check_external_api_availability(api_name: str, base_url: str, request_id: str) -> Dict[str, Any]:
    """
    Check external API availability with minimal impact.
    
    Args:
        api_name: Name of the API (e.g., "SEC", "Yahoo")
        base_url: Base URL of the API
        request_id: Request ID for tracing
        
    Returns:
        Dictionary with API health status and timing
    """
    context = ErrorContext(
        request_id=request_id,
        operation="check_external_api",
        source_type=api_name
    )
    
    # Check cache first
    cache_key = f"api_health_{api_name.lower()}"
    cached_result = health_cache.get(cache_key)
    if cached_result:
        return cached_result
    
    start_time = time.time()
    
    try:
        # Use HEAD request for minimal impact
        timeout = 5.0  # 5 second timeout for health checks
        
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.head(
                base_url,
                headers={
                    "User-Agent": settings.sec_user_agent,
                    "X-Request-ID": request_id
                },
                follow_redirects=True
            )
            
        duration_ms = (time.time() - start_time) * 1000
        
        # Consider 2xx, 3xx, and even 405 (Method Not Allowed) as healthy
        # since HEAD might not be supported but the service is reachable
        is_healthy = response.status_code < 500 or response.status_code == 405
        
        result = {
            "status": "healthy" if is_healthy else "unhealthy",
            "status_code": response.status_code,
            "response_time_ms": round(duration_ms, 2),
            "url": base_url
        }
        
        if not is_healthy:
            result["error"] = f"HTTP {response.status_code}"
        
        # Cache result
        ttl = 60 if is_healthy else 10  # Cache healthy results longer
        health_cache.set(cache_key, result, ttl=ttl)
        
        logger.debug(f"[{request_id}] {api_name} API health check completed", extra={
            "status_code": response.status_code,
            "response_time_ms": result["response_time_ms"]
        })
        
        return result
        
    except Exception as e:
        duration_ms = (time.time() - start_time) * 1000
        
        result = {
            "status": "unhealthy",
            "error": str(e),
            "error_type": type(e).__name__,
            "response_time_ms": round(duration_ms, 2),
            "url": base_url
        }
        
        # Cache failed result for short duration
        health_cache.set(cache_key, result, ttl=5)
        
        logger.warning(f"[{request_id}] {api_name} API health check failed", extra={
            "error": str(e),
            "response_time_ms": result["response_time_ms"]
        })
        
        return result

# Create FastAPI router
router = APIRouter(prefix="", tags=["Health"])

@router.get("/health")
async def health_check(request: Request) -> JSONResponse:
    """
    Liveness probe endpoint - simple check that the service is running.
    
    This endpoint is designed for Kubernetes liveness probes and should be
    as lightweight as possible. It only checks that the service can respond
    to requests without testing external dependencies.
    
    Returns:
        JSONResponse with basic health status
    """
    request_id = request.headers.get("X-Request-ID", f"health_{int(time.time() * 1000)}")
    
    try:
        uptime_info = get_service_uptime()
        
        response_data = {
            "status": "healthy",
            "timestamp": get_iso_timestamp(),
            "service": "salescience-data-acquisition",
            "version": "1.0.0",
            "uptime": uptime_info
        }
        
        json_logger.log_json(
            level="debug",
            action="health_check",
            message="Liveness check successful",
            status="healthy",
            extra={"request_id": request_id, "uptime_seconds": uptime_info["uptime_seconds"]}
        )
        
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content=response_data
        )
        
    except Exception as e:
        context = ErrorContext(
            request_id=request_id,
            operation="health_check"
        )
        
        error = SalescienceError(
            "Health check failed",
            context=context,
            cause=e
        )
        
        log_error(error, logger)
        
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={
                "status": "unhealthy",
                "timestamp": get_iso_timestamp(),
                "error": "Service health check failed"
            }
        )

@router.get("/readiness")
async def readiness_check(request: Request) -> JSONResponse:
    """
    Readiness probe endpoint - check that all dependencies are available.
    
    This endpoint is designed for Kubernetes readiness probes and verifies
    that the service can handle requests by checking all critical dependencies.
    If any dependency is unavailable, the service is considered not ready.
    
    Returns:
        JSONResponse with dependency health status
    """
    request_id = request.headers.get("X-Request-ID", f"readiness_{int(time.time() * 1000)}")
    
    try:
        logger.debug(f"[{request_id}] Starting readiness check")
        
        # Check Redis connectivity
        redis_health = await check_redis_connectivity(request_id)
        
        # Check external APIs
        sec_health = await check_external_api_availability(
            "SEC", 
            settings.service_urls.sec_api_base, 
            request_id
        )
        
        yahoo_health = await check_external_api_availability(
            "Yahoo", 
            settings.service_urls.yahoo_api_base, 
            request_id
        )
        
        # Determine overall readiness
        dependencies = {
            "redis": redis_health,
            "sec_api": sec_health,
            "yahoo_api": yahoo_health
        }
        
        # Service is ready if Redis is healthy (critical dependency)
        # External APIs can be temporarily unavailable without affecting readiness
        is_ready = redis_health["status"] == "healthy"
        overall_status = "ready" if is_ready else "not_ready"
        
        response_data = {
            "status": overall_status,
            "timestamp": get_iso_timestamp(),
            "dependencies": dependencies,
            "uptime": get_service_uptime()
        }
        
        http_status = status.HTTP_200_OK if is_ready else status.HTTP_503_SERVICE_UNAVAILABLE
        
        json_logger.log_json(
            level="info" if is_ready else "warning",
            action="readiness_check",
            message=f"Readiness check completed - {overall_status}",
            status=overall_status,
            extra={
                "request_id": request_id,
                "redis_healthy": redis_health["status"] == "healthy",
                "sec_api_healthy": sec_health["status"] == "healthy",
                "yahoo_api_healthy": yahoo_health["status"] == "healthy"
            }
        )
        
        return JSONResponse(
            status_code=http_status,
            content=response_data
        )
        
    except Exception as e:
        context = ErrorContext(
            request_id=request_id,
            operation="readiness_check"
        )
        
        error = SalescienceError(
            "Readiness check failed",
            context=context,
            cause=e
        )
        
        log_error(error, logger)
        
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={
                "status": "not_ready",
                "timestamp": get_iso_timestamp(),
                "error": "Readiness check failed due to internal error"
            }
        )

@router.get("/status")
async def detailed_status(request: Request) -> JSONResponse:
    """
    Detailed status endpoint - comprehensive system health and metrics.
    
    This endpoint provides detailed information about all system components,
    performance metrics, and configuration details for operations and debugging.
    It's more comprehensive than the health/readiness probes.
    
    Returns:
        JSONResponse with detailed system status information
    """
    request_id = request.headers.get("X-Request-ID", f"status_{int(time.time() * 1000)}")
    
    try:
        logger.debug(f"[{request_id}] Starting detailed status check")
        
        # Get comprehensive health information
        redis_health = await check_redis_connectivity(request_id)
        
        sec_health = await check_external_api_availability(
            "SEC", 
            settings.service_urls.sec_api_base, 
            request_id
        )
        
        yahoo_health = await check_external_api_availability(
            "Yahoo", 
            settings.service_urls.yahoo_api_base, 
            request_id
        )
        
        # System configuration (non-sensitive)
        system_config = {
            "environment": settings.environment,
            "log_level": settings.log_level,
            "worker_concurrency": settings.worker.concurrency,
            "redis_pool_size": settings.redis.pool_size,
            "prometheus_enabled": settings.prometheus_enabled,
            "json_logging_enabled": settings.json_logging_enabled,
            "queue_name": settings.queue_name
        }
        
        # Feature flags
        feature_flags = {
            "xbrl_enabled": settings.feature_xbrl_enabled,
            "sec_insider_enabled": settings.feature_sec_insider_enabled,
            "batch_jobs_enabled": settings.feature_batch_jobs_enabled,
            "message_bus_enabled": settings.message_bus.enabled
        }
        
        # Overall health determination
        redis_healthy = redis_health["status"] == "healthy"
        sec_healthy = sec_health["status"] == "healthy"
        yahoo_healthy = yahoo_health["status"] == "healthy"
        
        # Overall status logic
        if redis_healthy and (sec_healthy or yahoo_healthy):
            overall_status = "healthy"
        elif redis_healthy:
            overall_status = "degraded"  # Redis works but APIs have issues
        else:
            overall_status = "unhealthy"
        
        response_data = {
            "status": overall_status,
            "timestamp": get_iso_timestamp(),
            "service": {
                "name": "salescience-data-acquisition",
                "version": "1.0.0",
                "environment": settings.environment
            },
            "uptime": get_service_uptime(),
            "dependencies": {
                "redis": redis_health,
                "sec_api": sec_health,
                "yahoo_api": yahoo_health
            },
            "configuration": system_config,
            "features": feature_flags,
            "health_cache": {
                "entries": len(health_cache.cache),
                "default_ttl": health_cache.default_ttl
            }
        }
        
        json_logger.log_json(
            level="info",
            action="detailed_status_check",
            message=f"Detailed status check completed - {overall_status}",
            status=overall_status,
            extra={
                "request_id": request_id,
                "overall_status": overall_status,
                "dependencies_healthy": {
                    "redis": redis_healthy,
                    "sec_api": sec_healthy,
                    "yahoo_api": yahoo_healthy
                }
            }
        )
        
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content=response_data
        )
        
    except Exception as e:
        context = ErrorContext(
            request_id=request_id,
            operation="detailed_status_check"
        )
        
        error = SalescienceError(
            "Detailed status check failed",
            context=context,
            cause=e
        )
        
        log_error(error, logger)
        
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={
                "status": "error",
                "timestamp": get_iso_timestamp(),
                "error": "Status check failed due to internal error"
            }
        )

# Helper function to clear health cache (useful for testing)
def clear_health_cache() -> None:
    """Clear all cached health check results."""
    health_cache.clear()
    logger.info("Health check cache cleared")