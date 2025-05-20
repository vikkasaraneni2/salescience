# JSON Logger Documentation

## Overview
The `json_logger.py` module provides a specialized structured logging implementation designed for production-grade observability in the data acquisition pipeline. It transforms Python's standard logging into a structured JSON format that's optimized for log aggregation systems and automated analysis.

## Key Features

### Structured JSON Logging
Rather than traditional text logs, this module produces machine-readable JSON output that enables:
- Automated log parsing and analysis (ELK, Splunk, CloudWatch)
- Advanced filtering and searching of log data
- Standardized field names and formats across the system

### Sensitive Data Protection
Built-in security features to prevent accidental exposure of:
- API keys and authentication credentials
- Database passwords and connection strings
- Any field containing sensitive keywords is automatically masked

### Standardized Context Fields
Each log entry includes consistent fields for comprehensive operational visibility:
- Timestamps in ISO-8601 format with UTC timezone
- Organization ID for multi-tenant isolation
- Job ID for cross-component correlation
- Action identifiers for operation tracking
- Status indicators for monitoring progress

### Multi-level Logging
Supports all standard Python logging levels with appropriate handling:
- INFO for normal operations
- ERROR for operational failures
- WARNING for potential issues
- DEBUG for detailed troubleshooting
- CRITICAL for system-level problems

## Components

### SENSITIVE_KEYS Set
A comprehensive list of key patterns that might contain sensitive information, used to identify fields for masking:
```python
SENSITIVE_KEYS = {
    # Authentication credentials
    "api_key", "password", "token", "secret", "auth", "credential", "passwd",
    # Specific API keys
    "OPENAI_API_KEY", "SEC_API_KEY", "POSTGRES_PASSWORD", "REDIS_PASSWORD",
    # General patterns
    "key", "secret", "private"
}
```

### mask_sensitive() Function
Security utility that examines dictionary keys to identify and mask sensitive information:
```python
def mask_sensitive(data: dict) -> dict:
    """
    Mask or omit sensitive fields in a dictionary for safe logging.
    """
    # Creates a safe copy with sensitive values masked
    # Recursively processes nested dictionaries
    # Returns data safe for logging
```

### JsonLogger Class
The primary class for structured logging throughout the system:

#### Constructor
```python
def __init__(self, name: str):
    """
    Initialize a new JsonLogger with the given name.
    
    Args:
        name: Component identifier (e.g., "orchestrator_api", "worker")
    """
```

#### log_json() Method
```python
def log_json(
    self, level: str, action: str, message: str, 
    organization_id: Optional[str] = None, 
    job_id: Optional[str] = None, 
    status: Optional[str] = None, 
    extra: Optional[Dict[str, Any]] = None
):
    """
    Create and output a structured JSON log entry.
    """
```

## Standard Log Structure
Each log entry follows a consistent format:
```json
{
    "timestamp": "2023-06-01T14:23:45.123Z",
    "level": "info",
    "action": "fetch_sec_data",
    "organization_id": "acme-corp",
    "job_id": "550e8400-e29b-41d4-a716...",
    "status": "success",
    "message": "SEC data retrieved",
    ... additional context fields ...
}
```

## Usage Examples

### Basic Logging
```python
from data_acquisition.json_logger import JsonLogger

logger = JsonLogger("orchestrator_api")
logger.log_json(
    level="info",
    action="submit_job",
    message="Job submitted successfully",
    organization_id="acme-corp",
    job_id="job-123",
    status="success"
)
```

### Logging with Additional Context
```python
logger.log_json(
    level="error",
    action="fetch_data",
    message="API request failed",
    organization_id="acme-corp",
    job_id="job-123",
    status="error",
    extra={
        "api_endpoint": "https://api.example.com/data",
        "http_status": 403,
        "error_code": "AUTH_FAILED",
        "retry_count": 3,
        "api_key": "secret-will-be-masked"  # Will be automatically masked
    }
)
```

### Integration with System Components
The JSON Logger is designed to be used across all components of the data acquisition system, replacing standard Python logging:

```python
# In orchestrator_api.py
json_logger = JsonLogger("orchestrator_api")

# In worker.py
json_logger = JsonLogger("worker")

# In sec_client.py
json_logger = JsonLogger("sec_client")
```

## Benefits for Production Systems

### Observability
Rich, structured logs enable deep visibility into system operations, performance bottlenecks, and error conditions.

### Compliance & Auditing
The standardized format with organization and job identifiers supports compliance requirements and audit trails.

### Security
Automatic masking of sensitive data prevents accidental credential exposure in logs.

### Troubleshooting
Consistent format and correlation IDs make it easier to trace issues across distributed system components.

### Monitoring & Alerting
Structured fields enable automated monitoring systems to trigger alerts based on specific patterns or conditions.