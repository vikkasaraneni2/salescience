"""
Enhanced Orchestrator API for Data Acquisition System
---------------------------------------------------

This is an enhanced version of the orchestrator API with robust error handling
using the Salescience error framework. It provides a RESTful API with consistent
error responses, proper request validation, and detailed error logging.

Key improvements over the original orchestrator_api.py:
1. Integration with the Salescience error framework
2. Standardized error responses with proper HTTP status codes
3. Request ID propagation for end-to-end tracing
4. Enhanced request validation with detailed error messages
5. Improved error handling with proper error hierarchy
6. Consistent logging with context information
"""

import json
import uuid
import redis
import logging
import datetime
from typing import Dict, Any, List, Optional

from fastapi import FastAPI, Depends, HTTPException, Header, Query, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, validator, root_validator

# Import configuration and utilities
from config import settings

# Import error handling framework
from data_acquisition.errors import (
    SalescienceError, ErrorContext, format_error_response,
    ValidationError, AuthorizationError, RedisError, RedisOperationError,
    APIError, DataSourceNotFoundError
)

# Import error handlers
from data_acquisition.error_handlers import register_exception_handlers

# Import utility functions
from data_acquisition.utils import (
    get_job_redis_key, timestamp_now, normalize_envelope, JsonLogger
)

# Create FastAPI app
app = FastAPI(
    title="Salescience Data Acquisition API",
    description="API for orchestrating data acquisition from SEC, Yahoo Finance, and other sources",
    version="1.0.0"
)

# Register exception handlers
register_exception_handlers(app)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Adjust for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize Redis client
redis_client = redis.Redis.from_url(
    settings.redis.url,
    decode_responses=True,
    password=settings.redis.password,
    ssl=settings.redis.ssl,
    socket_timeout=settings.redis.socket_timeout,
    socket_connect_timeout=settings.redis.socket_connect_timeout,
    retry_on_timeout=settings.redis.retry_on_timeout
)

# Initialize structured logger
json_logger = JsonLogger("orchestrator_api")

# Pydantic models for request/response validation
class CompanyRequest(BaseModel):
    """
    Data model for a company request in a job submission.
    
    This model defines the structure for company data that clients send 
    when requesting data acquisition. It supports multiple identification
    methods (name, ticker, CIK) and allows selective enabling of data sources.
    """
    name: Optional[str] = Field(None, description="Company name")
    ticker: Optional[str] = Field(None, description="Stock ticker symbol")
    cik: Optional[str] = Field(None, description="SEC CIK identifier")
    sec: Optional[bool] = Field(False, description="Whether to fetch SEC data for this company")
    yahoo: Optional[bool] = Field(False, description="Whether to fetch Yahoo data for this company")
    xbrl: Optional[bool] = Field(False, description="Whether to fetch XBRL data for this company")
    
    @root_validator
    def validate_identifiers(cls, values):
        """Validate that at least one company identifier is provided."""
        if not any((values.get('name'), values.get('ticker'), values.get('cik'))):
            raise ValidationError(
                "At least one company identifier (name, ticker, or cik) must be provided",
                context=ErrorContext(company=values)
            )
        return values
    
    @validator('ticker')
    def validate_ticker_format(cls, v):
        """Validate ticker format if provided."""
        if v is not None and (not v.isalnum() or len(v) > 5):
            raise ValidationError(
                "Ticker must be alphanumeric and 5 characters or less",
                context=ErrorContext(ticker=v)
            )
        return v
    
    @validator('cik')
    def validate_cik_format(cls, v):
        """Validate CIK format if provided."""
        if v is not None:
            # CIK should be numeric or 10-digit string with leading zeros
            if not (v.isdigit() or (v.startswith('0') and len(v) == 10 and v.replace('0', '').isdigit())):
                raise ValidationError(
                    "CIK must be numeric or a 10-digit string with leading zeros",
                    context=ErrorContext(cik=v)
                )
        return v
    
    @root_validator
    def validate_yahoo_requires_ticker(cls, values):
        """Validate that Yahoo data requests include a ticker."""
        if values.get('yahoo') and not values.get('ticker'):
            raise ValidationError(
                "Yahoo data acquisition requires a ticker symbol",
                context=ErrorContext(company=values)
            )
        return values

class ConceptRequest(BaseModel):
    """XBRL concept request."""
    concept: str = Field(..., description="XBRL concept/tag name (e.g., 'us-gaap:Revenues')")
    
    @validator('concept')
    def validate_concept_format(cls, v):
        """Validate XBRL concept format."""
        if not ':' in v:
            raise ValidationError(
                "XBRL concept must include namespace (e.g., 'us-gaap:Revenues')",
                context=ErrorContext(concept=v)
            )
        return v

class SubmitJobsRequest(BaseModel):
    """
    Data model for the job submission request.
    
    This model defines the structure for the main API input when clients 
    submit data acquisition jobs. It includes the list of companies to fetch data for,
    acquisition parameters, and multi-tenancy identifiers.
    """
    companies: List[CompanyRequest] = Field(..., description="List of companies to fetch data for")
    n_years: Optional[int] = Field(1, ge=1, le=10, description="Number of years of filings to fetch (SEC only)")
    form_type: Optional[str] = Field("10-K", description="SEC form type to fetch")
    organization_id: str = Field(..., description="Organization identifier for multi-tenant isolation")
    user_id: Optional[str] = Field(None, description="User identifier for audit trails")
    concepts: Optional[List[ConceptRequest]] = Field(None, description="XBRL concepts to fetch (if xbrl enabled for any company)")
    
    @validator('n_years')
    def validate_n_years(cls, v):
        """Validate years range."""
        if v < 1 or v > 10:
            raise ValidationError(
                "n_years must be between 1 and 10",
                context=ErrorContext(n_years=v)
            )
        return v
    
    @validator('form_type')
    def validate_form_type(cls, v):
        """Validate form type."""
        valid_form_types = [
            '10-K', '10-Q', '8-K', '20-F', '40-F',
            'DEF 14A', 'DEFA14A', 'DEFM14A',
            'S-1', 'S-3', 'S-4', 'F-1',
            '4', '13F', '13G', '13D',
            'SD', 'CORRESP', 'FWP',
            '10-K/A', '10-Q/A', '8-K/A'
        ]
        if v not in valid_form_types:
            raise ValidationError(
                f"Invalid form_type. Must be one of: {', '.join(valid_form_types)}",
                context=ErrorContext(form_type=v)
            )
        return v
    
    @validator('companies')
    def validate_companies_not_empty(cls, v):
        """Validate companies list is not empty."""
        if not v:
            raise ValidationError(
                "At least one company must be specified",
                context=ErrorContext()
            )
        return v
    
    @root_validator
    def validate_xbrl_concepts(cls, values):
        """Validate that XBRL concepts are provided if XBRL is enabled for any company."""
        companies = values.get('companies', [])
        concepts = values.get('concepts')
        
        has_xbrl_enabled = any(company.xbrl for company in companies)
        
        if has_xbrl_enabled and not concepts:
            raise ValidationError(
                "XBRL concepts must be provided when XBRL is enabled for any company",
                context=ErrorContext()
            )
        
        return values

class SubmitJobsResponse(BaseModel):
    """
    Response model for job submission API.
    
    This model defines the structure for the API response when clients submit
    data acquisition jobs. It includes the job IDs for the created jobs, allowing
    clients to track their status and retrieve results later.
    """
    success: bool = Field(True, description="Whether the request was successful")
    job_ids: List[str] = Field(..., description="List of created job IDs")
    message: str = Field("Jobs submitted successfully", description="Human-readable message")
    submitted_at: str = Field(..., description="Timestamp when jobs were submitted")

class JobStatusResponse(BaseModel):
    """
    Response model for job status API.
    
    This model defines the structure for the API response when clients check
    the status of their data acquisition jobs. It includes overall job status
    and individual status for each requested company and data source.
    """
    success: bool = Field(True, description="Whether the request was successful")
    job_id: str = Field(..., description="Job ID")
    organization_id: str = Field(..., description="Organization ID")
    overall_status: str = Field(..., description="Overall job status")
    details: Dict[str, str] = Field(..., description="Status details for each company/source combination")
    submitted_at: Optional[str] = Field(None, description="Timestamp when job was submitted")
    updated_at: str = Field(..., description="Timestamp when status was last updated")

class JobResultsResponse(BaseModel):
    """
    Response model for job results API.
    
    This model defines the structure for the API response when clients retrieve
    the results of their data acquisition jobs. It includes the actual data payloads
    from various sources, along with metadata about the acquisition process.
    """
    success: bool = Field(True, description="Whether the request was successful")
    job_id: str = Field(..., description="Job ID")
    organization_id: str = Field(..., description="Organization ID")
    status: str = Field(..., description="Job status")
    results: Dict[str, Any] = Field(..., description="Results for each company/source combination")
    retrieved_at: str = Field(..., description="Timestamp when results were retrieved")

# Dependency for validating organization access
async def validate_organization(
    organization_id: str = Query(..., description="Organization identifier for multi-tenant isolation"),
    x_request_id: Optional[str] = Header(None, description="Request ID for tracing")
) -> str:
    """
    Validate organization ID and ensure proper access.
    
    This dependency function validates that the organization_id is provided and
    properly formatted. In a production system, this would also check authentication
    and authorization, but for now it just ensures that the organization_id is valid.
    
    Args:
        organization_id: Organization identifier from query parameter
        x_request_id: Request ID from headers for tracing
        
    Returns:
        Validated organization_id
        
    Raises:
        ValidationError: If organization_id is invalid
        AuthorizationError: If access is not authorized
    """
    # Create context for error handling
    context = ErrorContext(
        request_id=x_request_id,
        organization_id=organization_id
    )
    
    # Validate organization ID format
    if not organization_id or len(organization_id) < 3:
        raise ValidationError("Invalid organization_id. Must be at least 3 characters.", context=context)
    
    # In a real system, this would check authentication and authorization
    # For now, we just validate the format
    
    return organization_id

# Endpoint for submitting jobs
@app.post("/submit", response_model=SubmitJobsResponse)
async def submit_jobs(
    request: Request,
    jobs_request: SubmitJobsRequest,
    x_request_id: Optional[str] = Header(None, description="Request ID for tracing")
) -> SubmitJobsResponse:
    """
    Submit a batch of data acquisition jobs.
    
    This endpoint allows clients to request data for multiple companies from various
    sources in a single batch. The jobs are processed asynchronously, and clients
    can check their status and retrieve results using the returned job IDs.
    
    Args:
        request: FastAPI request object
        jobs_request: Job submission request with companies and parameters
        x_request_id: Request ID from headers for tracing
        
    Returns:
        SubmitJobsResponse with job IDs and submission status
        
    Raises:
        ValidationError: If request parameters are invalid
        RedisError: If there's an issue with Redis operations
        APIError: For other API-related errors
    """
    # Generate request_id if not provided
    request_id = x_request_id or str(uuid.uuid4())
    
    # Extract request parameters
    organization_id = jobs_request.organization_id
    user_id = jobs_request.user_id
    companies = jobs_request.companies
    n_years = jobs_request.n_years
    form_type = jobs_request.form_type
    concepts = jobs_request.concepts
    
    # Create context for error handling
    context = ErrorContext(
        request_id=request_id,
        organization_id=organization_id,
        user_id=user_id
    )
    
    # Log job submission
    json_logger.log_json(
        level="info",
        action="submit_jobs",
        request_id=request_id,
        organization_id=organization_id,
        user_id=user_id,
        company_count=len(companies),
        n_years=n_years,
        form_type=form_type
    )
    
    # Generate job ID
    job_id = f"{organization_id}-{uuid.uuid4()}"
    
    try:
        # Prepare job data
        job_data = {
            "job_id": job_id,
            "companies": [company.dict() for company in companies],
            "n_years": n_years,
            "form_type": form_type,
            "organization_id": organization_id,
            "user_id": user_id,
            "submitted_at": timestamp_now()
        }
        
        # Add XBRL concepts if provided
        if concepts:
            job_data["concepts"] = [concept.dict() for concept in concepts]
        
        # Initialize job status in Redis
        redis_client.set(
            get_job_redis_key(organization_id, job_id, "overall_status"),
            "queued"
        )
        redis_client.expire(
            get_job_redis_key(organization_id, job_id, "overall_status"),
            settings.job_expiry_sec
        )
        
        # Enqueue job for processing
        redis_client.rpush(settings.queue_name, json.dumps(job_data))
        
        # Set Redis key expiry
        redis_client.expire(
            get_job_redis_key(organization_id, job_id, "overall_status"),
            settings.job_expiry_sec
        )
        
        # Return response with job ID
        return SubmitJobsResponse(
            success=True,
            job_ids=[job_id],
            message="Job submitted successfully",
            submitted_at=timestamp_now()
        )
        
    except redis.RedisError as e:
        # Handle Redis errors
        error = RedisOperationError(
            message=f"Failed to enqueue job: {str(e)}",
            context=context,
            cause=e
        )
        json_logger.log_json(
            level="error",
            action="submit_jobs_error",
            request_id=request_id,
            organization_id=organization_id,
            error=str(error),
            error_code=error.error_code
        )
        raise error
        
    except Exception as e:
        # Handle unexpected errors
        error = APIError(
            message=f"Failed to submit job: {str(e)}",
            context=context,
            cause=e
        )
        json_logger.log_json(
            level="error",
            action="submit_jobs_error",
            request_id=request_id,
            organization_id=organization_id,
            error=str(error),
            error_code=error.error_code
        )
        raise error

# Endpoint for checking job status
@app.get("/status/{job_id}", response_model=JobStatusResponse)
async def get_job_status(
    job_id: str,
    request: Request,
    organization_id: str = Depends(validate_organization),
    x_request_id: Optional[str] = Header(None, description="Request ID for tracing")
) -> JobStatusResponse:
    """
    Get the status of a data acquisition job.
    
    This endpoint allows clients to check the status of a previously submitted
    job. It returns the overall status (queued, processing, complete, error)
    and individual status for each company/source combination.
    
    Args:
        job_id: Job identifier
        request: FastAPI request object
        organization_id: Organization identifier (from dependency)
        x_request_id: Request ID from headers for tracing
        
    Returns:
        JobStatusResponse with current job status
        
    Raises:
        DataSourceNotFoundError: If job is not found
        RedisError: If there's an issue with Redis operations
        APIError: For other API-related errors
    """
    # Generate request_id if not provided
    request_id = x_request_id or str(uuid.uuid4())
    
    # Create context for error handling
    context = ErrorContext(
        request_id=request_id,
        organization_id=organization_id,
        job_id=job_id
    )
    
    try:
        # Check if job exists
        overall_status = redis_client.get(get_job_redis_key(organization_id, job_id, "overall_status"))
        
        if not overall_status:
            error = DataSourceNotFoundError(
                message=f"Job not found: {job_id}",
                context=context
            )
            json_logger.log_json(
                level="warning",
                action="job_not_found",
                request_id=request_id,
                organization_id=organization_id,
                job_id=job_id
            )
            raise error
        
        # Get detailed status for each company/source
        status_details = redis_client.hgetall(get_job_redis_key(organization_id, job_id, "status"))
        
        # Get submission timestamp
        submitted_at = redis_client.get(get_job_redis_key(organization_id, job_id, "submitted_at"))
        
        # Return response with status information
        return JobStatusResponse(
            success=True,
            job_id=job_id,
            organization_id=organization_id,
            overall_status=overall_status,
            details=status_details or {},
            submitted_at=submitted_at,
            updated_at=timestamp_now()
        )
        
    except RedisError as e:
        # Handle Redis errors
        error = RedisOperationError(
            message=f"Failed to retrieve job status: {str(e)}",
            context=context,
            cause=e
        )
        json_logger.log_json(
            level="error",
            action="get_status_error",
            request_id=request_id,
            organization_id=organization_id,
            job_id=job_id,
            error=str(error),
            error_code=error.error_code
        )
        raise error
        
    except SalescienceError:
        # Re-raise SalescienceErrors
        raise
        
    except Exception as e:
        # Handle unexpected errors
        error = APIError(
            message=f"Failed to retrieve job status: {str(e)}",
            context=context,
            cause=e
        )
        json_logger.log_json(
            level="error",
            action="get_status_error",
            request_id=request_id,
            organization_id=organization_id,
            job_id=job_id,
            error=str(error),
            error_code=error.error_code
        )
        raise error

# Endpoint for retrieving job results
@app.get("/results/{job_id}", response_model=JobResultsResponse)
async def get_job_results(
    job_id: str,
    request: Request,
    organization_id: str = Depends(validate_organization),
    x_request_id: Optional[str] = Header(None, description="Request ID for tracing")
) -> JobResultsResponse:
    """
    Get the results of a completed data acquisition job.
    
    This endpoint allows clients to retrieve the results of a previously submitted
    job once it's complete. It returns the actual data payloads from various sources,
    along with metadata about the acquisition process.
    
    Args:
        job_id: Job identifier
        request: FastAPI request object
        organization_id: Organization identifier (from dependency)
        x_request_id: Request ID from headers for tracing
        
    Returns:
        JobResultsResponse with job results
        
    Raises:
        DataSourceNotFoundError: If job is not found
        ValidationError: If job is not complete
        RedisError: If there's an issue with Redis operations
        APIError: For other API-related errors
    """
    # Generate request_id if not provided
    request_id = x_request_id or str(uuid.uuid4())
    
    # Create context for error handling
    context = ErrorContext(
        request_id=request_id,
        organization_id=organization_id,
        job_id=job_id
    )
    
    try:
        # Check if job exists and get status
        overall_status = redis_client.get(get_job_redis_key(organization_id, job_id, "overall_status"))
        
        if not overall_status:
            error = DataSourceNotFoundError(
                message=f"Job not found: {job_id}",
                context=context
            )
            json_logger.log_json(
                level="warning",
                action="job_not_found",
                request_id=request_id,
                organization_id=organization_id,
                job_id=job_id
            )
            raise error
        
        # Get job results from Redis
        results_data = redis_client.hgetall(get_job_redis_key(organization_id, job_id, "result"))
        
        # Parse JSON results
        parsed_results = {}
        for key, value in results_data.items():
            try:
                parsed_results[key] = json.loads(value)
            except json.JSONDecodeError:
                # Handle invalid JSON
                parsed_results[key] = {
                    "status": "error",
                    "error": "Invalid JSON data",
                    "content": None
                }
        
        # Return response with results
        return JobResultsResponse(
            success=True,
            job_id=job_id,
            organization_id=organization_id,
            status=overall_status,
            results=parsed_results,
            retrieved_at=timestamp_now()
        )
        
    except RedisError as e:
        # Handle Redis errors
        error = RedisOperationError(
            message=f"Failed to retrieve job results: {str(e)}",
            context=context,
            cause=e
        )
        json_logger.log_json(
            level="error",
            action="get_results_error",
            request_id=request_id,
            organization_id=organization_id,
            job_id=job_id,
            error=str(error),
            error_code=error.error_code
        )
        raise error
        
    except SalescienceError:
        # Re-raise SalescienceErrors
        raise
        
    except Exception as e:
        # Handle unexpected errors
        error = APIError(
            message=f"Failed to retrieve job results: {str(e)}",
            context=context,
            cause=e
        )
        json_logger.log_json(
            level="error",
            action="get_results_error",
            request_id=request_id,
            organization_id=organization_id,
            job_id=job_id,
            error=str(error),
            error_code=error.error_code
        )
        raise error

# Health check endpoint
@app.get("/health")
async def health_check():
    """Health check endpoint."""
    try:
        # Check Redis connection
        redis_client.ping()
        
        return {
            "status": "healthy",
            "timestamp": timestamp_now(),
            "services": {
                "redis": "connected"
            }
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "timestamp": timestamp_now(),
            "error": str(e),
            "services": {
                "redis": "disconnected"
            }
        }

# Middleware for request ID tracking
@app.middleware("http")
async def request_id_middleware(request: Request, call_next):
    """
    Middleware for request ID tracking.
    
    This middleware ensures that every request has a unique request ID for tracing.
    If the client provides an X-Request-ID header, it's used; otherwise, a new UUID is generated.
    """
    # Get or generate request ID
    request_id = request.headers.get("X-Request-ID") or str(uuid.uuid4())
    
    # Process the request
    response = await call_next(request)
    
    # Add request ID to response headers
    response.headers["X-Request-ID"] = request_id
    
    return response

# Main entrypoint for running the API
if __name__ == "__main__":
    import uvicorn
    
    # Start the API server
    uvicorn.run(
        "orchestrator_api:app",
        host="0.0.0.0",
        port=8100,
        reload=True if settings.environment != "production" else False
    )