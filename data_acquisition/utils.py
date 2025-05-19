import logging
import datetime
from typing import Dict, Any, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("utils")

def get_job_redis_key(organization_id: Optional[str], job_id: str, key_type: str) -> str:
    """
    Creates a Redis key with organization prefix for proper namespace isolation.
    
    Args:
        organization_id: The organization ID for namespacing
        job_id: The unique job identifier
        key_type: Type of key (status, result, overall_status, etc.)
    
    Returns:
        Properly formatted Redis key with organization prefix
    """
    if not organization_id:
        logger.warning(f"No organization_id provided for Redis key (job={job_id}, type={key_type})")
        return f"job:{job_id}:{key_type}"
    return f"org:{organization_id}:job:{job_id}:{key_type}"

def log_job_status(job_id: str, status: str, organization_id: Optional[str] = None, details: Dict[str, Any] = None) -> None:
    """
    Logs job status with appropriate details.
    
    Args:
        job_id: The job identifier
        status: Current status (queued, processing, success, error, etc.)
        organization_id: Optional organization ID
        details: Additional details to include in the log
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
    Normalizes a data envelope to ensure it has all required fields.
    
    Args:
        envelope: The data envelope to normalize
        
    Returns:
        Normalized envelope with all required fields
    """
    # Ensure required fields
    if 'content' not in envelope:
        envelope['content'] = None
        
    if 'status' not in envelope:
        envelope['status'] = 'error' if 'error' in envelope else 'success'
        
    if 'source' not in envelope:
        envelope['source'] = 'unknown'
        
    if 'metadata' not in envelope:
        envelope['metadata'] = {}
        
    if 'content_type' not in envelope:
        envelope['content_type'] = 'text'
        
    # Ensure error handling
    if 'error' in envelope and envelope['status'] != 'error':
        envelope['status'] = 'error'
        
    return envelope

def timestamp_now():
    """Returns the current timestamp in ISO format with Z suffix."""
    return datetime.datetime.utcnow().isoformat() + 'Z'

def truncate_content(content, max_chars=500):
    """
    Truncates content for logging purposes.
    
    Args:
        content: The content to truncate
        max_chars: Maximum number of characters to include
        
    Returns:
        Truncated content with indicator if truncated
    """
    if not content:
        return None
        
    if isinstance(content, str) and len(content) > max_chars:
        return content[:max_chars] + f"... [truncated, total length: {len(content)}]"
    
    return content

def get_source_key(company, source, idx=None):
    """
    Generates a consistent source key for a company and data source.
    
    Args:
        company: Company dict or ticker/name string
        source: Source identifier (e.g., 'SEC', 'Yahoo')
        idx: Optional index for multi-document sources (e.g., year index)
        
    Returns:
        Consistent source key string
    """
    if isinstance(company, dict):
        ticker = company.get('ticker') or company.get('cik') or company.get('name')
    else:
        ticker = company
        
    if idx is not None:
        return f"{ticker}:{source}:{idx}"
    else:
        return f"{ticker}:{source}"

class JsonLogger:
    """
    Structured JSON logger for data acquisition events.
    Helps with tracking job flows and debugging issues.
    """
    
    def __init__(self, logger_name="json_logger"):
        self.logger = logging.getLogger(logger_name)
        
    def log_json(self, level="info", **kwargs):
        """
        Log a structured JSON message with the given level and key-value pairs.
        
        Args:
            level: Log level (debug, info, warning, error, critical)
            **kwargs: Key-value pairs to include in the log
        """
        # Add timestamp if not present
        if 'timestamp' not in kwargs:
            kwargs['timestamp'] = timestamp_now()
            
        # Call appropriate log method
        if level == "debug":
            self.logger.debug(kwargs)
        elif level == "info":
            self.logger.info(kwargs)
        elif level == "warning":
            self.logger.warning(kwargs)
        elif level == "error":
            self.logger.error(kwargs)
        elif level == "critical":
            self.logger.critical(kwargs)
        else:
            self.logger.info(kwargs)