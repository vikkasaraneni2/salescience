"""
Utility Functions for Data Acquisition Pipeline
----------------------------------------------

This module provides a set of utility functions and classes that support the
data acquisition process. These utilities handle common tasks like:

1. Redis Key Management: Consistent key generation for multi-tenant isolation
2. Logging: Structured logging with standardized formats
3. Data Normalization: Ensuring consistent data formats across diverse sources
4. Error Handling: Standardized error reporting and tracking

The utilities in this file are designed to be used by all components of the data
acquisition layer, ensuring consistency, reducing code duplication, and supporting
separation of concerns in the architecture.

Key architectural benefits:
- Multi-tenancy support through prefix-based isolation
- Consistent error handling and reporting
- Standardized envelope formats for communication between components
- Structured logging for observability and troubleshooting
- Helper functions to improve code readability and maintainability
"""

import logging
import datetime
from typing import Dict, Any, Optional

# Configure logging with a standard format
# This ensures all log entries have consistent timestamps and structure
# for easier filtering, searching, and analysis
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("utils")

def get_job_redis_key(organization_id: Optional[str], job_id: str, key_type: str) -> str:
    """
    Creates a Redis key with organization prefix for proper namespace isolation.
    
    This function implements a critical part of the multi-tenant architecture by ensuring
    that each organization's data is properly isolated through key prefixing. This approach
    provides strong logical separation without requiring separate Redis instances.
    
    The key structure follows a consistent pattern:
    - org:{organization_id}:job:{job_id}:{key_type}  
    - job:{job_id}:{key_type} (when no org ID is provided)
    
    This pattern enables:
    1. Efficient querying of all keys for a specific organization
    2. Simple key-based permission checks for API access controls
    3. Clear attribution of data for auditing and troubleshooting
    4. Consistent access patterns across the codebase
    
    Args:
        organization_id: The organization ID for multi-tenant namespacing
                         Required for proper data isolation in SaaS model
        job_id: The unique job identifier (typically a UUID)
               Used to group all data related to a specific acquisition job
        key_type: Type of key (status, result, overall_status, etc.)
                  Differentiates between different aspects of job data
    
    Returns:
        Properly formatted Redis key with organization prefix
        
    Example usage:
        # For job status
        redis.set(get_job_redis_key("acme-corp", "job-123", "status"), "processing")
        
        # For job results
        redis.hset(get_job_redis_key("acme-corp", "job-123", "result"), "AAPL:SEC", data)
    """
    if not organization_id:
        # Log warning for missing organization_id as this likely indicates a system issue
        # Falling back to non-org key to prevent complete failure, but this may cause 
        # data leakage between organizations if not handled carefully
        logger.warning(f"No organization_id provided for Redis key (job={job_id}, type={key_type})")
        return f"job:{job_id}:{key_type}"
    return f"org:{organization_id}:job:{job_id}:{key_type}"

def log_job_status(job_id: str, status: str, organization_id: Optional[str] = None, details: Dict[str, Any] = None) -> None:
    """
    Logs job status with appropriate details.
    
    Creates standardized log entries for job status changes throughout the pipeline,
    enabling consistent tracking and monitoring of job progression. This function
    centralizes logging logic to ensure uniform format across components.
    
    Args:
        job_id: The job identifier for correlation across system components
        status: Current job status (queued, processing, success, error, etc.)
        organization_id: Optional organization ID for multi-tenant context
        details: Additional details to include in the log (company info, source, etc.)
    
    The function automatically adjusts the log level based on status:
    - 'error' status logs use ERROR level
    - All other statuses use INFO level
    
    Example usage:
        # Log job start
        log_job_status('job-123', 'processing', 'acme-corp', 
                      {'company': 'AAPL', 'source': 'SEC'})
        
        # Log job error
        log_job_status('job-123', 'error', 'acme-corp',
                      {'company': 'AAPL', 'error': 'API timeout'})
    """
    log_data = {
        'job_id': job_id,
        'status': status,
    }
    
    if organization_id:
        log_data['organization_id'] = organization_id
        
    if details:
        log_data.update(details)
        
    if status == 'error':
        logger.error(f"Job {job_id} error", extra=log_data)
    else:
        logger.info(f"Job {job_id} {status}", extra=log_data)

def normalize_envelope(envelope: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalizes a data envelope to ensure it has all required fields and consistent structure.
    
    This function plays a crucial role in maintaining the data contract between system components.
    It ensures that all data envelopes, regardless of their source or the specific implementation
    that created them, conform to the expected structure. This standardization enables reliable
    processing, storage, and retrieval throughout the pipeline.
    
    The envelope pattern provides several architectural benefits:
    1. Metadata Separation: The actual content is separated from its metadata, allowing
       processing components to make decisions without examining the full content
    2. Source Traceability: Each envelope carries information about its origin for
       debugging and auditing
    3. Error Handling: Errors are encoded in a consistent way that lets consuming
       components handle them uniformly
    4. Content Type Declaration: Content type is explicitly marked, supporting
       proper parsing and rendering
    
    Standard envelope structure:
    {
        'content': <the actual data payload, can be any type>,
        'content_type': <string indicating the format: 'json', 'html', 'text', etc.>,
        'source': <string identifying the data source: 'sec', 'yahoo', etc.>,
        'status': <string status code: 'success', 'error', 'not_found', etc.>,
        'metadata': <dictionary of additional context information>,
        'error': <optional error message or details, present only when status is 'error'>
    }
    
    Args:
        envelope: The data envelope to normalize
        
    Returns:
        Normalized envelope with all required fields, ensuring standard structure
        
    Example:
        # Normalizing a minimal result
        raw_envelope = {'content': html_content, 'source': 'sec'}
        normalized = normalize_envelope(raw_envelope)
        # normalized now has all required fields with default values
    """
    # Ensure required fields with appropriate defaults
    if 'content' not in envelope:
        envelope['content'] = None
        
    if 'status' not in envelope:
        # Derive status from error field if present, otherwise assume success
        envelope['status'] = 'error' if 'error' in envelope else 'success'
        
    if 'source' not in envelope:
        # Mark unknown source for debugging and auditability
        envelope['source'] = 'unknown'
        
    if 'metadata' not in envelope:
        # Ensure metadata field exists, even if empty
        envelope['metadata'] = {}
        
    if 'content_type' not in envelope:
        # Default to text content type when not specified
        envelope['content_type'] = 'text'
        
    # Ensure error handling consistency - if an error field exists,
    # the status should always be 'error' to prevent inconsistent states
    if 'error' in envelope and envelope['status'] != 'error':
        envelope['status'] = 'error'
        
    return envelope

def timestamp_now():
    """
    Returns the current timestamp in ISO format with Z suffix.
    
    This utility function provides a standardized timestamp format used
    throughout the data acquisition pipeline. Using ISO 8601 format with
    UTC timezone ('Z' suffix) ensures:
    
    1. Consistent timestamp formatting across all logs and data records
    2. Timezone clarity for distributed systems operating across regions
    3. Sortable string representation for chronological ordering
    4. Compatibility with most data storage and analysis systems
    
    Returns:
        String timestamp in ISO 8601 format with 'Z' suffix indicating UTC
        Example: "2023-04-15T13:45:30.123456Z"
        
    Example usage:
        created_at = timestamp_now()
        event_log = {'id': '12345', 'timestamp': timestamp_now(), 'action': 'fetch'}
    """
    return datetime.datetime.utcnow().isoformat() + 'Z'

def truncate_content(content, max_chars=500):
    """
    Truncates content for logging purposes.
    
    When logging data payloads, especially large documents or API responses,
    this function prevents excessive log size while preserving enough context
    for debugging. It adds a truncation indicator and total length information
    when truncation occurs.
    
    Args:
        content: The content string to truncate
        max_chars: Maximum number of characters to include (default: 500)
        
    Returns:
        Truncated content with indicator if truncation occurred,
        or the original content if it's shorter than max_chars
        
    Example:
        # For logging API responses
        log_data = {
            'endpoint': '/api/data',
            'response': truncate_content(large_response_body)
        }
        logger.debug("API response received", extra=log_data)
    """
    if not content:
        return None
        
    if isinstance(content, str) and len(content) > max_chars:
        return content[:max_chars] + f"... [truncated, total length: {len(content)}]"
    
    return content

def get_source_key(company, source, idx=None):
    """
    Generates a consistent source key for a company and data source.
    
    This function creates standardized keys for identifying data from specific
    companies and sources within the Redis storage system. These keys are used
    in hash maps for storing results and tracking status across the pipeline.
    
    The consistent key format enables:
    1. Predictable lookup patterns for retrieving company data
    2. Logical grouping of related data points
    3. Clear organization of multi-year or multi-document datasets
    
    Key format examples:
    - "AAPL:SEC:0" (first year of SEC data for Apple)
    - "MSFT:Yahoo" (Yahoo Finance data for Microsoft)
    
    Args:
        company: Company dict with ticker/name/cik fields, or a ticker string
        source: Source identifier (e.g., 'SEC', 'Yahoo', 'XBRL')
        idx: Optional index for multi-document sources (e.g., year index for SEC filings)
        
    Returns:
        Consistent source key string for use in Redis operations
        
    Example usage:
        # For a company dictionary
        company = {'ticker': 'AAPL', 'name': 'Apple Inc.'}
        key = get_source_key(company, 'SEC', 0)  # "AAPL:SEC:0"
        
        # For a simple ticker string
        key = get_source_key('MSFT', 'Yahoo')  # "MSFT:Yahoo"
    """
    if isinstance(company, dict):
        ticker = company.get('ticker') or company.get('cik') or company.get('name')
    else:
        ticker = company
        
    if idx is not None:
        return f"{ticker}:{source}:{idx}"
    else:
        return f"{ticker}:{source}"

def mask_sensitive(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Masks sensitive values in dictionaries to prevent credential exposure in logs.
    
    This security function identifies and masks potentially sensitive information 
    in data dictionaries before they are logged or stored. It helps prevent 
    accidental exposure of credentials, API keys, and other sensitive information
    in logs and monitoring systems.
    
    Sensitive fields are identified by keywords in their keys, including:
    - 'password', 'passwd', 'pwd'
    - 'key', 'secret', 'token'
    - 'auth', 'credentials'
    - 'api_key', 'access_key', 'secret_key'
    
    Args:
        data: Dictionary potentially containing sensitive information
        
    Returns:
        Copy of the dictionary with sensitive values masked
        
    Example:
        # Before logging API request details
        safe_data = mask_sensitive({
            'endpoint': 'https://api.example.com/data',
            'api_key': 'abcd1234secret',
            'params': {'query': 'sales data'}
        })
        logger.info("API request", extra=safe_data)
        # The 'api_key' value will be masked as '****'
    """
    if not isinstance(data, dict):
        return data
        
    result = data.copy()
    sensitive_keywords = [
        'password', 'passwd', 'pwd',
        'key', 'secret', 'token',
        'auth', 'credentials', 
        'api_key', 'access_key', 'secret_key'
    ]
    
    for key, value in result.items():
        # Check if this key contains any sensitive keywords
        if any(keyword in key.lower() for keyword in sensitive_keywords):
            # Mask the value
            if isinstance(value, str):
                if len(value) > 0:
                    masked_length = min(len(value), 4)
                    result[key] = '*' * masked_length
            elif value is not None:
                # For non-string values, just use a generic mask
                result[key] = '****'
                
        # Recursively mask nested dictionaries
        elif isinstance(value, dict):
            result[key] = mask_sensitive(value)
            
        # Handle lists of dictionaries
        elif isinstance(value, list):
            result[key] = [
                mask_sensitive(item) if isinstance(item, dict) else item
                for item in value
            ]
            
    return result

class JsonLogger:
    """
    Structured JSON logger for data acquisition events.
    
    This class provides a consistent approach to structured logging throughout
    the data acquisition system. By outputting logs in a JSON format, we enable:
    
    1. Automated log parsing and analysis by tools like ELK stack, Splunk, etc.
    2. Standardized log fields for easier filtering and alerting
    3. Rich context data for each log event without custom parsing
    4. Consistent log format across all components of the system
    
    Why this matters:
    - Observability: In a distributed system with many components, structured logs
      are essential for troubleshooting and monitoring
    - Auditability: The structured logs capture operation details and context for
      security and compliance needs
    - Performance Analysis: Consistent timestamp and operation ID fields allow for
      tracking system performance and bottlenecks
    - Error Correlation: Related errors across system components can be linked through
      common fields like job_id or operation_id
    
    This class also implements security features like masking sensitive data to prevent
    accidental exposure of credentials or personal information in logs.
    """
    
    def __init__(self, logger_name="json_logger"):
        """
        Initialize a JsonLogger with the specified name.
        
        Args:
            logger_name: A unique identifier for this logger instance
                         Used to distinguish logs from different components
        """
        self.logger = logging.getLogger(logger_name)
        
    def log_json(self, level="info", action: str = None, message: str = None, 
                 organization_id: Optional[str] = None, job_id: Optional[str] = None, 
                 status: Optional[str] = None, **kwargs):
        """
        Log a structured JSON message with the given level and key-value pairs.
        
        This method creates a comprehensive log entry with standardized fields
        and additional contextual data. The resulting JSON structure enables
        powerful querying and analysis in logging systems.
        
        Args:
            level: Log level (debug, info, warning, error, critical)
            action: The specific operation being performed (e.g., 'fetch_data', 'process_job')
            message: Human-readable description of the event
            organization_id: Multi-tenant organization identifier for isolation
            job_id: Unique job identifier for correlation
            status: Status of the operation (success, error, etc.)
            **kwargs: Additional key-value pairs to include in the log
                      These are merged with the standard fields for extended context
        
        Standard fields automatically included:
            - timestamp: ISO-8601 timestamp with UTC timezone (Z suffix)
            - level: Log severity level
            - logger: Source logger name (component identifier)
            
        Security features:
            - Sensitive fields (keys containing "password", "key", "secret", etc.)
              are automatically masked to prevent credential exposure
            
        Example usage:
            logger = JsonLogger("worker")
            logger.log_json(
                level="info",
                action="fetch_sec_data",
                message="Successfully fetched SEC filing",
                organization_id="acme-corp",
                job_id="job-123",
                status="success",
                ticker="AAPL", 
                filing_type="10-K",
                elapsed_time_ms=1500
            )
        """
        # Create the log entry with standard fields
        log_entry = {
            "timestamp": timestamp_now(),
            "level": level,
            "logger": self.logger.name
        }
        
        # Add the optional standard fields if provided
        if action:
            log_entry["action"] = action
            
        if message:
            log_entry["message"] = message
            
        if organization_id:
            log_entry["organization_id"] = organization_id
            
        if job_id:
            log_entry["job_id"] = job_id
            
        if status:
            log_entry["status"] = status
            
        # Merge any additional keyword arguments, masking sensitive values
        if kwargs:
            # Use helper function to mask sensitive data like API keys
            log_entry.update(mask_sensitive(kwargs))
            
        # Call appropriate log method based on the specified level
        if level == "debug":
            self.logger.debug(log_entry)
        elif level == "info":
            self.logger.info(log_entry)
        elif level == "warning":
            self.logger.warning(log_entry)
        elif level == "error":
            self.logger.error(log_entry)
        elif level == "critical":
            self.logger.critical(log_entry)
        else:
            # Default to info level for unknown level strings
            self.logger.info(log_entry)