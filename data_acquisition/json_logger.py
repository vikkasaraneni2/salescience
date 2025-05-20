"""
Structured JSON Logging Module for Data Acquisition System
---------------------------------------------------------

This module provides a specialized JSON logging implementation designed for
production-grade observability in the data acquisition pipeline. It supports:

1. Structured JSON logs for machine-readable outputs
2. Sensitive data masking to prevent credential exposure
3. Standardized timestamp format for consistent time analysis
4. Multi-field context logging for rich operational insights

The structured JSON format is critical for modern cloud-native applications as it enables:
- Log aggregation systems (ELK, Splunk, CloudWatch) to process logs efficiently
- Advanced filtering and searching of log data
- Automated alerting based on specific log patterns
- Correlation of events across distributed system components

This module acts as a foundation for all system logging, ensuring consistency
across all components of the data acquisition system.
"""

import logging
import json
import sys
from datetime import datetime
from typing import Optional, Dict, Any

# Security-critical: comprehensive list of key patterns that might contain 
# sensitive information. These values will be masked in logs to prevent
# accidental credential or PII exposure.
SENSITIVE_KEYS = {
    # Authentication credentials
    "api_key", "password", "token", "secret", "auth", "credential", "passwd",
    # Specific API keys used in the system 
    "OPENAI_API_KEY", "SEC_API_KEY", "POSTGRES_PASSWORD", "REDIS_PASSWORD",
    # General patterns that might indicate sensitive content
    "key", "secret", "private"
}

def mask_sensitive(data: dict) -> dict:
    """
    Mask or omit sensitive fields in a dictionary for safe logging.
    
    This security-focused function examines dictionary keys to identify potentially
    sensitive information that should not be logged in plain text. It creates a
    safe copy of the data with sensitive values replaced by a masked placeholder.
    
    The approach balances several needs:
    1. Security: Prevent accidental exposure of credentials and secrets
    2. Debugging: Preserve the structure and keys for troubleshooting
    3. Compliance: Support regulatory requirements (GDPR, CCPA, etc.)
    4. Usability: Allow log analysis while protecting sensitive data
    
    Args:
        data: Dictionary that may contain sensitive information
        
    Returns:
        A new dictionary with the same structure but with sensitive values masked
        
    Security notes:
    - This is applied to all logs before they're output
    - Uses a case-insensitive check against the SENSITIVE_KEYS set
    - Only handles top-level keys; nested dictionaries would need recursive processing
    - Does not attempt to identify patterns in the values themselves
    """
    masked = {}
    for k, v in data.items():
        # Check if any sensitive key pattern appears in the key (case-insensitive)
        if any(sensitive_key in k.lower() for sensitive_key in SENSITIVE_KEYS):
            masked[k] = "***MASKED***"
        else:
            # For nested dictionaries, recursively apply masking
            if isinstance(v, dict):
                masked[k] = mask_sensitive(v)
            else:
                masked[k] = v
    return masked

class JsonLogger:
    """
    Structured JSON logger for the data acquisition system.
    
    This class implements a standardized approach to structured logging across
    the entire data acquisition system. It ensures that all log entries:
    
    1. Have a consistent field structure for machine processing
    2. Include critical contextual fields like organization_id and job_id
    3. Follow security best practices by masking sensitive information
    4. Use ISO-8601 timestamps with UTC timezone for reliable time analysis
    5. Support various log levels for appropriate filtering
    
    The JSON format allows log aggregation tools to efficiently process logs
    without needing custom parsing rules, enabling:
    
    - Advanced filtering by organization, job ID, status, or action
    - Automated alerting based on specific error patterns
    - Performance metrics extraction for operational dashboards
    - Audit trails for compliance and security reviews
    
    This standardized logging is critical for a multi-tenant SaaS architecture,
    as it enables proper isolation and attribution of system events.
    """
    
    def __init__(self, name: str):
        """
        Initialize a new JsonLogger with the given name.
        
        Args:
            name: Identifier for the logger, typically the component name
                 (e.g., "orchestrator_api", "worker", "sec_client")
        """
        # Create a new logger with the specified name
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.INFO)
        
        # Configure stdout handler with minimal formatting
        # The actual formatting happens in the log_json method
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter('%(message)s'))
        
        # Only add a handler if none exists yet (prevents duplicate logs)
        if not self.logger.hasHandlers():
            self.logger.addHandler(handler)

    def log_json(self, level: str, action: str, message: str, 
                organization_id: Optional[str] = None, 
                job_id: Optional[str] = None, 
                status: Optional[str] = None, 
                extra: Optional[Dict[str, Any]] = None):
        """
        Create and output a structured JSON log entry.
        
        This method handles the creation of a standardized log structure
        with common fields and security processing. It provides a consistent
        interface for all logging throughout the system.
        
        Args:
            level: Log severity level ("debug", "info", "warning", "error", "critical")
            action: The operation or event being logged (e.g., "submit_job", "fetch_data")
            message: Human-readable description of the event
            organization_id: Multi-tenant organization identifier (for isolation and attribution)
            job_id: Unique job identifier (for correlation across system components)
            status: Current status of the operation (e.g., "success", "error", "running")
            extra: Additional contextual data to include in the log entry
                  (any sensitive fields will be automatically masked)
                  
        Standard log entry structure:
        {
            "timestamp": "2023-06-01T14:23:45.123Z",  # ISO-8601 format with UTC timezone
            "level": "info",                          # Log severity
            "action": "fetch_sec_data",               # Operation identifier
            "organization_id": "acme-corp",           # Multi-tenant isolation
            "job_id": "550e8400-e29b-41d4-a716...",   # Correlation identifier
            "status": "success",                      # Operation status
            "message": "SEC data retrieved",          # Human-readable description
            ... additional fields from 'extra' ...    # Extended context
        }
        
        Security features:
        - All fields from 'extra' are processed by mask_sensitive()
        - Null values are omitted from the final JSON
        - The log is serialized as a single JSON line for easier processing
        """
        # Create the base log entry with standard fields
        log_entry = {
            "timestamp": datetime.utcnow().isoformat() + "Z",  # UTC time with Z suffix
            "level": level,
            "action": action,
            "organization_id": organization_id,
            "job_id": job_id,
            "status": status,
            "message": message,
        }
        
        # Add any extra contextual data, with sensitive information masked
        if extra:
            log_entry.update(mask_sensitive(extra))
            
        # Remove None values and serialize to JSON
        # This creates a more compact log entry without null fields
        log_line = json.dumps({k: v for k, v in log_entry.items() if v is not None})
        
        # Output the log at the appropriate level
        if level == "error":
            self.logger.error(log_line)
        elif level == "warning":
            self.logger.warning(log_line)
        elif level == "info":
            self.logger.info(log_line)
        elif level == "critical":
            self.logger.critical(log_line)
        else:
            # Default to debug for any other level
            self.logger.debug(log_line)

# Usage example:
# logger = JsonLogger("orchestrator_api")
# logger.log_json("info", action="submit_job", message="Job submitted", organization_id="acme", job_id="1234", status="queued", extra={"user_id": "u1"}) 