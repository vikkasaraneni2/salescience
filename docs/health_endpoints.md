# Health Check Endpoints Documentation

## Overview

The Salescience data acquisition system provides three health check endpoints designed for different monitoring scenarios:

- `/health` - Liveness probe for Kubernetes
- `/readiness` - Readiness probe for Kubernetes  
- `/status` - Detailed system status for operations

## Endpoints

### 1. `/health` - Liveness Probe

**Purpose**: Simple check that the service is running
**Use Case**: Kubernetes liveness probe, load balancer health checks
**Response Time**: < 50ms typically

**Example Response**:
```json
{
  "status": "healthy",
  "timestamp": "2024-01-15T10:30:45.123Z",
  "service": "salescience-data-acquisition",
  "version": "1.0.0",
  "uptime": {
    "uptime_seconds": 3661.45,
    "uptime_human": "0d 1h 1m",
    "started_at": "2024-01-15T09:29:03.673Z"
  }
}
```

**HTTP Status Codes**:
- `200` - Service is healthy
- `503` - Service is unhealthy

### 2. `/readiness` - Readiness Probe

**Purpose**: Check that all critical dependencies are available
**Use Case**: Kubernetes readiness probe, determining if service can handle traffic
**Response Time**: < 2s typically

**Example Response**:
```json
{
  "status": "ready",
  "timestamp": "2024-01-15T10:30:45.456Z",
  "dependencies": {
    "redis": {
      "status": "healthy",
      "ping_ms": 1.23,
      "write_ms": 2.45,
      "read_ms": 1.87,
      "total_ms": 8.91,
      "url": "localhost:6379/0",
      "ssl_enabled": false,
      "connection_pool_size": 10
    },
    "sec_api": {
      "status": "healthy",
      "status_code": 200,
      "response_time_ms": 245.67,
      "url": "https://api.sec-api.io"
    },
    "yahoo_api": {
      "status": "healthy", 
      "status_code": 200,
      "response_time_ms": 189.23,
      "url": "https://query1.finance.yahoo.com/v10/finance"
    }
  },
  "uptime": {
    "uptime_seconds": 3661.45,
    "uptime_human": "0d 1h 1m",
    "started_at": "2024-01-15T09:29:03.673Z"
  }
}
```

**HTTP Status Codes**:
- `200` - Service is ready (Redis healthy)
- `503` - Service is not ready (Redis unhealthy)

### 3. `/status` - Detailed Status

**Purpose**: Comprehensive system health and configuration information
**Use Case**: Operations dashboard, debugging, monitoring systems
**Response Time**: < 3s typically

**Example Response**:
```json
{
  "status": "healthy",
  "timestamp": "2024-01-15T10:30:45.789Z",
  "service": {
    "name": "salescience-data-acquisition",
    "version": "1.0.0",
    "environment": "production"
  },
  "uptime": {
    "uptime_seconds": 3661.45,
    "uptime_human": "0d 1h 1m", 
    "started_at": "2024-01-15T09:29:03.673Z"
  },
  "dependencies": {
    "redis": {
      "status": "healthy",
      "ping_ms": 1.23,
      "write_ms": 2.45,
      "read_ms": 1.87,
      "total_ms": 8.91,
      "url": "localhost:6379/0",
      "ssl_enabled": false,
      "connection_pool_size": 10
    },
    "sec_api": {
      "status": "healthy",
      "status_code": 200,
      "response_time_ms": 245.67,
      "url": "https://api.sec-api.io"
    },
    "yahoo_api": {
      "status": "healthy",
      "status_code": 200,
      "response_time_ms": 189.23,
      "url": "https://query1.finance.yahoo.com/v10/finance"
    }
  },
  "configuration": {
    "environment": "production",
    "log_level": "INFO",
    "worker_concurrency": 10,
    "redis_pool_size": 10,
    "prometheus_enabled": true,
    "json_logging_enabled": true,
    "queue_name": "data_jobs"
  },
  "features": {
    "xbrl_enabled": true,
    "sec_insider_enabled": true,
    "batch_jobs_enabled": true,
    "message_bus_enabled": true
  },
  "health_cache": {
    "entries": 3,
    "default_ttl": 30
  }
}
```

**HTTP Status Codes**:
- `200` - Always returns 200 with detailed status
- `500` - Internal error during status collection

## Health Determination Logic

### Overall Status Calculation:
- **healthy**: Redis healthy AND (SEC API healthy OR Yahoo API healthy)
- **degraded**: Redis healthy BUT both external APIs unhealthy  
- **unhealthy**: Redis unhealthy (critical dependency)

### Readiness Logic:
- **ready**: Redis is healthy (external APIs can be temporarily down)
- **not_ready**: Redis is unhealthy

## Caching

Health checks are cached to prevent overwhelming external services:
- **Successful checks**: Cached for 20-60 seconds
- **Failed checks**: Cached for 5-10 seconds  
- **Cache keys**: `redis_health`, `api_health_sec`, `api_health_yahoo`

## Kubernetes Integration

### Deployment Configuration:
```yaml
livenessProbe:
  httpGet:
    path: /health
    port: 8100
  initialDelaySeconds: 10
  periodSeconds: 20
  timeoutSeconds: 5
  
readinessProbe:
  httpGet:
    path: /readiness
    port: 8100
  initialDelaySeconds: 5
  periodSeconds: 10
  timeoutSeconds: 5
```

## Testing

Run the test script to verify endpoints:
```bash
python test_health_endpoints.py
```

Or use curl:
```bash
# Test liveness
curl -w "\nStatus: %{http_code}\nTime: %{time_total}s\n" \
  http://localhost:8100/health

# Test readiness  
curl -w "\nStatus: %{http_code}\nTime: %{time_total}s\n" \
  http://localhost:8100/readiness

# Test detailed status
curl -w "\nStatus: %{http_code}\nTime: %{time_total}s\n" \
  http://localhost:8100/status
```

## Monitoring Integration

The endpoints provide structured JSON suitable for:
- Prometheus metrics scraping
- ELK stack log analysis  
- Custom dashboard integration
- Automated alerting systems

## Request Tracing

All endpoints support request ID tracing:
```bash
curl -H "X-Request-ID: my-trace-id" http://localhost:8100/health
```

Request IDs appear in logs for distributed tracing.