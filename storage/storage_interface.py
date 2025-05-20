"""
Storage Interface Module for Salescience Platform
------------------------------------------------

This module provides a simplified interface to the storage layer components
of the Salescience platform. It serves as a bridge between the application 
and the underlying storage systems, abstracting away the implementation details
and providing a consistent API for job status and result retrieval.

System Architecture Context:
---------------------------
The storage layer is a critical component that handles:
1. Persistent storage of job status information
2. Storage and retrieval of job results
3. Caching for improved performance

┌──────────────┐     ┌───────────────┐     ┌───────────────┐
│ API Layer    │     │ Storage       │     │ Redis/DB      │
│ (Endpoints)  │────▶│ Interface     │────▶│ Backends      │
└──────────────┘     └───────────────┘     └───────────────┘
                           │                       ▲
                           │                       │
                           └───────────────────────┘

This interface abstracts Redis key patterns and query logic, ensuring:
- Consistent access patterns across the application
- Simplified refactoring if storage backends need to change
- Centralized error handling and retry logic
- Logical separation between application and persistence layers

Current implementation uses Redis as the primary storage backend,
with plans to expand to PostgreSQL for structured data and vector stores
for semantic search capabilities in future iterations.
"""

import redis
import os
from dotenv import load_dotenv
from typing import Dict, Any, Optional

# Load environment variables
load_dotenv()

# Redis connection configuration
# The Redis URL is read from environment variables with a sensible default
# for local development. This allows for configuration across different
# environments (dev, staging, production) without code changes.
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

# Initialize Redis client with automatic response decoding
# The decode_responses=True setting ensures that Redis binary responses
# are automatically converted to Python strings, simplifying data handling
redis_client = redis.Redis.from_url(REDIS_URL, decode_responses=True)

def get_status(job_id: str, organization_id: Optional[str] = None) -> str:
    """
    Retrieve the current status of a data acquisition job.
    
    This function encapsulates the Redis key pattern and query logic for
    status retrieval, providing a simple interface for application code.
    The status values follow a standardized set of strings:
    - 'pending': Job is in queue but processing hasn't started
    - 'processing': Job is currently being processed
    - 'complete': Job has completed successfully
    - 'error': Job encountered an error during processing
    - 'not_found': Job status couldn't be determined
    - 'unknown': Default fallback if no status is explicitly set
    
    Args:
        job_id: Unique identifier for the job
        organization_id: Optional organization identifier for multi-tenant isolation
            
    Returns:
        Current status of the job as a string
        
    Example:
        status = get_status("550e8400-e29b-41d4-a716-446655440000")
        if status == "complete":
            results = get_results(job_id)
    """
    key = f"org:{organization_id}:job:{job_id}:overall_status" if organization_id else f"job:{job_id}:overall_status"
    overall_status = redis_client.get(key) or "unknown"
    return overall_status

def get_results(job_id: str, organization_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Retrieve the results of a completed data acquisition job.
    
    This function encapsulates the Redis key pattern and query logic for
    result retrieval. Results are stored in a Redis hash where each field
    represents a different source/company combination with the value being
    the serialized result data.
    
    The returned dictionary maps source/company identifiers to their corresponding
    result data (typically JSON strings that need to be deserialized by the caller).
    
    Args:
        job_id: Unique identifier for the job
        organization_id: Optional organization identifier for multi-tenant isolation
            
    Returns:
        Dictionary mapping source/company identifiers to result data
        
    Example:
        results = get_results("550e8400-e29b-41d4-a716-446655440000")
        apple_sec_data = json.loads(results.get("AAPL:SEC:0"))
    
    Note:
        This method does not deserialize the results, as different result types
        may require different deserialization approaches. The calling code is
        responsible for deserializing the string values as needed.
    """
    key = f"org:{organization_id}:job:{job_id}:result" if organization_id else f"job:{job_id}:result"
    results = redis_client.hgetall(key)
    return results
