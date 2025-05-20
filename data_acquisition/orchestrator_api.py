"""
Orchestrator API for Data Acquisition System
--------------------------------------------

This module implements the API gateway and orchestration layer for the data acquisition system.
It provides a RESTful API that enables clients to request data from multiple sources (SEC, Yahoo, etc.)
in a unified, asynchronous manner.

System Architecture Context:
---------------------------
The Orchestrator API plays a central role in the data acquisition pipeline:

┌───────────────┐      ┌───────────────┐      ┌───────────────┐      ┌───────────────┐
│  Client Apps  │      │  Orchestrator │      │ Redis Queue   │      │ Worker Nodes  │
│  & Services   │─────▶│  API Gateway  │─────▶│ & State Store │─────▶│ (Processors)  │
└───────────────┘      └───────────────┘      └───────────────┘      └───────────────┘
                              │                      ▲                      │
                              │                      │                      │
                              └──────────────────────┴──────────────────────┘
                                    Status & Result Monitoring

Key responsibilities:
1. Client Request Management: Accept and validate data acquisition requests
2. Job Creation: Generate unique job IDs and initialize tracking structures
3. Work Distribution: Enqueue tasks for async processing by worker nodes
4. Status Tracking: Provide endpoints to monitor job progress
5. Result Retrieval: Enable access to completed job results
6. Multi-tenancy: Enforce organization-based isolation of data and requests

Design Benefits:
--------------
1. Decoupled Processing: Separating the API from the actual data acquisition
   allows for independent scaling of request handling and processing components.

2. Asynchronous Workflow: Clients can submit jobs and retrieve results later,
   enabling efficient handling of long-running operations.

3. Stateless API: The API itself maintains no state; all job state is stored
   in Redis, enabling horizontal scaling and fault tolerance.

4. Multi-tenant Architecture: Organization-prefixed Redis keys ensure proper
   data isolation in a multi-tenant environment.

5. Standardized Result Format: All results follow the same envelope pattern
   regardless of data source, simplifying client integration.

API Endpoints:
------------
- POST /submit: Submit new data acquisition jobs
- GET /status/{job_id}: Check the status of a job
- GET /results/{job_id}: Retrieve results of a completed job

Data Flow:
---------
1. Client submits a job via POST /submit
2. API generates job ID and initializes tracking in Redis
3. API enqueues job details to 'data_jobs' Redis list
4. Worker nodes pick up jobs from the queue and process them
5. Workers update job status and store results in Redis
6. Client polls status endpoint until job is complete
7. Client retrieves results via results endpoint

Security and Multi-tenancy:
-------------------------
- All endpoints require an organization_id parameter
- Redis keys are prefixed with organization ID for proper isolation
- Each organization can only access its own jobs and results
- No authentication is currently implemented (internal/trusted use only)

Usage Examples:
-------------
# Example usage (curl):

Submit a job:
$ curl -X POST http://localhost:8100/submit \\
  -H "Content-Type: application/json" \\
  -d '{"companies": [{"ticker": "AAPL"}, {"ticker": "MSFT"}], "n_years": 2, "form_type": "10-K", "organization_id": "acme-corp"}'

Check job status:
$ curl "http://localhost:8100/status/<job_id>?organization_id=acme-corp"

Get job results:
$ curl "http://localhost:8100/results/<job_id>?organization_id=acme-corp"

# Example usage (Python):

import requests

# Submit job
resp = requests.post("http://localhost:8100/submit", json={
    "companies": [{"ticker": "AAPL"}, {"ticker": "MSFT"}],
    "n_years": 2,
    "form_type": "10-K",
    "organization_id": "acme-corp"
})
job_id = resp.json()["job_ids"][0]

# Check status
status = requests.get(
    f"http://localhost:8100/status/{job_id}",
    params={"organization_id": "acme-corp"}
).json()

# Get results when complete
if status["status"] == "complete":
    results = requests.get(
        f"http://localhost:8100/results/{job_id}",
        params={"organization_id": "acme-corp"}
    ).json()

Deployment Instructions:
----------------------
1. Ensure Redis is running and accessible (see README for Kubernetes setup)
2. Install dependencies:
    pip install fastapi uvicorn redis pydantic python-dotenv
3. Start the API:
    uvicorn data_acquisition.orchestrator_api:app --host 0.0.0.0 --port 8100
4. Start the worker (see worker.py)
5. Use the endpoints to submit and track jobs.

Environment Variables:
--------------------
- REDIS_URL: Redis connection string (default: redis://localhost:6379/0)
- LOG_LEVEL: Logging level (default: INFO)
"""
import os
import uuid
import logging
import json
from typing import List, Optional, Dict, Any
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field
import redis
from dotenv import load_dotenv
from data_acquisition.json_logger import JsonLogger

# Load environment variables from .env if present
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("orchestrator_api")

# Redis connection
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
redis_client = redis.Redis.from_url(REDIS_URL, decode_responses=True)

# Replace logger with structured JSON logger
json_logger = JsonLogger("orchestrator_api")

# Pydantic models for request/response validation
class CompanyRequest(BaseModel):
    """
    Data model for a company request in a job submission.
    
    This model defines the structure for company data that clients send 
    when requesting data acquisition. It supports multiple identification
    methods (name, ticker, CIK) and allows selective enabling of data sources.
    
    This flexible structure enables clients to:
    1. Request data for companies identified by different means
    2. Selectively enable only the data sources they need
    3. Optimize performance by limiting unnecessary data acquisition
    
    Fields:
        name: Human-readable company name (optional)
        ticker: Stock ticker symbol (optional, but required for Yahoo data)
        cik: SEC Central Index Key identifier (optional, but required for SEC data)
        sec: Flag to enable SEC data acquisition (default: False)
        yahoo: Flag to enable Yahoo data acquisition (default: False)
    """
    name: Optional[str] = Field(None, description="Company name")
    ticker: Optional[str] = Field(None, description="Stock ticker symbol")
    cik: Optional[str] = Field(None, description="SEC CIK identifier")
    sec: Optional[bool] = Field(False, description="Whether to fetch SEC data for this company")
    yahoo: Optional[bool] = Field(False, description="Whether to fetch Yahoo data for this company")

class SubmitJobsRequest(BaseModel):
    """
    Data model for the job submission request.
    
    This model defines the structure for the main API input when clients 
    submit data acquisition jobs. It includes the list of companies to fetch data for,
    acquisition parameters, and multi-tenancy identifiers.
    
    The structure supports batch processing of multiple companies in a single request,
    which is more efficient than making separate requests for each company.
    
    Fields:
        companies: List of companies to fetch data for
        n_years: Number of years of filings to fetch (SEC only)
        form_type: SEC form type to fetch (e.g., '10-K')
        organization_id: Required for multi-tenant isolation
        user_id: Optional, for audit trails
    """
    companies: List[CompanyRequest]
    n_years: Optional[int] = Field(1, description="Number of years of filings to fetch (SEC only)")
    form_type: Optional[str] = Field("10-K", description="SEC form type to fetch (e.g., '10-K')")
    organization_id: str = Field(..., description="Organization ID for multi-tenancy")  # Required for tenant isolation
    user_id: Optional[str] = Field(None, description="User ID (optional, for audit)")    # Optional, for audit trails

class SubmitJobsResponse(BaseModel):
    """
    Data model for the job submission response.
    
    This simple model defines the structure for the API response when 
    clients submit a job. It contains the unique job IDs that clients 
    can use to track status and retrieve results.
    
    Fields:
        job_ids: List of unique job identifiers
    """
    job_ids: List[str]

class JobStatusResponse(BaseModel):
    """
    Data model for the job status response.
    
    This model defines the structure returned when clients check job status.
    It provides both an overall status and detailed status for each source/company 
    combination in the job.
    
    This granular status reporting allows clients to:
    1. Track overall job progress
    2. Identify which specific parts of a job may have failed
    3. Implement partial result handling for jobs with mixed success
    
    Fields:
        job_id: The unique job identifier
        status: Overall job status (e.g., "pending", "processing", "complete", "error")
        sources: Detailed status for each source/company combination
    """
    job_id: str
    status: str
    sources: Dict[str, str]  # e.g., {"AAPL:SEC": "queued", ...}

class JobResultsResponse(BaseModel):
    """
    Data model for the job results response.
    
    This model defines the structure returned when clients retrieve job results.
    It maps source/company identifiers to their corresponding result data.
    
    The results are returned as a dictionary to allow clients to:
    1. Access specific results by company and source
    2. Process only the results they're interested in
    3. Handle partial results in case some acquisitions failed
    
    Fields:
        job_id: The unique job identifier
        results: Dictionary mapping source/company identifiers to result data
    """
    job_id: str
    results: Dict[str, Any]  # e.g., {"AAPL:SEC": {...}, ...}

# Simulated data source list (replace with real source classes)
DATA_SOURCES = ["SEC", "Yahoo"]

# FastAPI app
app = FastAPI(
    title="Data Acquisition Orchestrator API",
    description="API for orchestrating data acquisition from multiple sources in an asynchronous manner",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Utility: Generate a new job ID
def generate_job_id() -> str:
    """
    Generate a unique job identifier.
    
    This function creates a UUID (version 4) to uniquely identify each job
    in the system. UUIDs are used to:
    1. Avoid collisions even in high-volume distributed environments
    2. Prevent predictable IDs that could enable enumeration attacks
    3. Provide globally unique identifiers without coordination
    
    Returns:
        str: A unique job identifier as a string
    """
    return str(uuid.uuid4())

def get_job_redis_key(organization_id: str, job_id: str, suffix: str) -> str:
    """
    Helper to build org-prefixed Redis keys for job metadata, status, and results.
    
    This function implements the multi-tenant key isolation pattern by prefixing
    all job-related Redis keys with the organization ID. This ensures that:
    1. Organizations cannot access each other's data
    2. Keys are consistently formatted throughout the codebase
    3. Redis data can be easily partitioned or migrated by organization
    
    Args:
        organization_id: Multi-tenant organization identifier
        job_id: Unique job identifier
        suffix: Key type (e.g., "meta", "status", "result")
        
    Returns:
        str: Properly formatted Redis key with organization prefix
    """
    return f"org:{organization_id}:job:{job_id}:{suffix}"

# Utility: Initialize job in Redis
def init_job_in_redis(job_id: str, companies: List[CompanyRequest], n_years: int, form_type: str, organization_id: str, user_id: Optional[str]):
    """
    Initialize job tracking structures in Redis.
    
    This function sets up all the necessary Redis data structures to track a job:
    1. Metadata hash storing organizational context
    2. Status hash for each company/source combination
    3. Result hash (initialized empty) for storing outcomes
    4. Overall status key for high-level job status
    
    The Redis structures follow a consistent pattern that supports:
    - Efficient status querying and updates
    - Granular tracking of individual sub-tasks
    - Proper multi-tenant isolation
    - Support for both batch status checks and detailed monitoring
    
    Args:
        job_id: Unique job identifier
        companies: List of companies to process
        n_years: Number of years of filings to fetch (for SEC)
        form_type: Type of SEC form to fetch
        organization_id: Multi-tenant organization identifier
        user_id: Optional user identifier for audit trails
    """
    # Store org/user context for the job in a dedicated Redis hash for metadata
    redis_client.hset(get_job_redis_key(organization_id, job_id, "meta"), mapping={
        "organization_id": organization_id,  # Used for access control and audit
        "user_id": user_id or ""            # Optional, for audit trails
    })
    # For each company and data source, initialize job status and clear any previous results
    for company in companies:
        for source in DATA_SOURCES:
            if source == "SEC":
                # For SEC, create a status/result entry for each year requested
                for year_offset in range(n_years):
                    key = f"{company.ticker or company.cik or company.name}:{source}:{year_offset}"
                    redis_client.hset(get_job_redis_key(organization_id, job_id, "status"), key, "queued")
                    redis_client.hdel(get_job_redis_key(organization_id, job_id, "result"), key)
            else:
                # For other sources, just one entry per company
                key = f"{company.ticker or company.cik or company.name}:{source}"
                redis_client.hset(get_job_redis_key(organization_id, job_id, "status"), key, "queued")
                redis_client.hdel(get_job_redis_key(organization_id, job_id, "result"), key)
    # Set the overall job status to pending
    redis_client.set(get_job_redis_key(organization_id, job_id, "overall_status"), "pending")

# Utility: Push job to Redis list for worker to process
def enqueue_job(job_id: str, companies: List[CompanyRequest], n_years: int, form_type: str, organization_id: str, user_id: Optional[str]):
    """
    Enqueue a job for asynchronous processing by worker nodes.
    
    This function prepares the job data and pushes it to the Redis list
    that worker nodes monitor for new tasks. It handles:
    1. Processing company data to enable appropriate sources
    2. Creating a comprehensive job payload with all required information
    3. Pushing the job to the Redis queue in a serialized format
    4. Logging the enqueue operation for audit and monitoring
    
    The job payload includes all information workers need to process the job
    independently, following a self-contained task pattern.
    
    Args:
        job_id: Unique job identifier
        companies: List of companies to process
        n_years: Number of years of filings to fetch (for SEC)
        form_type: Type of SEC form to fetch
        organization_id: Multi-tenant organization identifier
        user_id: Optional user identifier for audit trails
    """
    # Ensure at least one data source is enabled for each company
    processed_companies = []
    for company in companies:
        # Create a copy of the company dict that we can modify
        company_dict = company.dict()
        
        # If a ticker is provided but no source flags, default to enabling both sources
        if (company.ticker or company.cik) and not (company.sec or company.yahoo):
            company_dict['sec'] = True
            company_dict['yahoo'] = company.ticker is not None  # Only enable Yahoo if ticker is provided
            logger.info(f"Auto-enabling sources for company with ticker/cik: {company_dict}")
        
        # If only a name is provided and no source flags, default to SEC
        elif company.name and not (company.sec or company.yahoo):
            company_dict['sec'] = True
            logger.info(f"Auto-enabling SEC source for company with name only: {company_dict}")
            
        processed_companies.append(company_dict)
    
    job_payload = {
        "job_id": job_id,
        "companies": processed_companies,
        "n_years": n_years,
        "form_type": form_type,
        "organization_id": organization_id,
        "user_id": user_id
    }
    
    # Debug logging
    logger.info(f"Enqueueing job to Redis: {job_payload}")
    result = redis_client.rpush("data_jobs", json.dumps(job_payload))
    logger.info(f"Redis rpush result: {result}")
    
    json_logger.log_json(
        level="info",
        action="enqueue_job",
        message="Enqueued job to data_jobs queue",
        organization_id=organization_id,
        job_id=job_id,
        status="queued",
        extra={"user_id": user_id, "job_payload": str(job_payload)}
    )

# Endpoint: Submit jobs (single or batch)
@app.post("/submit", response_model=SubmitJobsResponse, tags=["Jobs"])
def submit_jobs(request: SubmitJobsRequest):
    """
    Submit one or more companies for data acquisition from all sources.
    
    This endpoint is the main entry point for clients to request data acquisition.
    It handles:
    1. Validating the incoming request
    2. Generating a unique job ID
    3. Initializing job tracking in Redis
    4. Enqueueing the job for asynchronous processing
    5. Returning the job ID for clients to track progress
    
    The asynchronous design allows for long-running operations without
    blocking the client request thread.
    
    Args:
        request: The job submission request containing companies and parameters
        
    Returns:
        SubmitJobsResponse: Contains the job ID(s) for tracking
        
    Raises:
        HTTPException: If the request is invalid or processing fails
    """
    print("DEBUG: CompanyRequest fields:", CompanyRequest.__fields__)
    print("DEBUG: Received companies:", request.companies)
    print("DEBUG: Company dicts:", [c.dict() for c in request.companies])
    if not request.companies:
        json_logger.log_json(
            level="warning",
            action="submit_job",
            message="No companies provided in job submission",
            organization_id=request.organization_id,
            status="rejected",
            extra={"user_id": request.user_id}
        )
        raise HTTPException(status_code=400, detail="No companies provided.")
    if not request.organization_id:
        json_logger.log_json(
            level="warning",
            action="submit_job",
            message="organization_id is required in job submission",
            status="rejected",
            extra={"user_id": request.user_id}
        )
        raise HTTPException(status_code=400, detail="organization_id is required.")
    job_id = generate_job_id()
    json_logger.log_json(
        level="info",
        action="submit_job",
        message="Job submitted",
        organization_id=request.organization_id,
        job_id=job_id,
        status="submitted",
        extra={"user_id": request.user_id, "num_companies": len(request.companies)}
    )
    n_years = request.n_years or 1
    form_type = request.form_type or "10-K"
    init_job_in_redis(job_id, request.companies, n_years, form_type, request.organization_id, request.user_id)
    enqueue_job(job_id, request.companies, n_years, form_type, request.organization_id, request.user_id)
    return SubmitJobsResponse(job_ids=[job_id])

# Endpoint: Check job status
@app.get("/status/{job_id}", response_model=JobStatusResponse, tags=["Jobs"])
def get_job_status(job_id: str, organization_id: str = Query(..., description="Organization ID for multi-tenancy")):
    """
    Get the status of a job (per source, per company).
    
    This endpoint allows clients to check the progress of their jobs.
    It provides both overall job status and detailed status for each
    company/source combination.
    
    The endpoint enforces multi-tenancy by verifying the organization ID
    matches the one associated with the job.
    
    Args:
        job_id: The unique job identifier
        organization_id: Multi-tenant organization identifier (required for access control)
        
    Returns:
        JobStatusResponse: Contains overall and detailed job status
        
    Raises:
        HTTPException: If the job doesn't exist or belongs to a different organization
    """
    meta = redis_client.hgetall(get_job_redis_key(organization_id, job_id, "meta"))
    if not meta or meta.get("organization_id") != organization_id:
        json_logger.log_json(
            level="warning",
            action="get_job_status",
            message="Forbidden status check: organization_id does not match",
            organization_id=organization_id,
            job_id=job_id,
            status="forbidden"
        )
        raise HTTPException(status_code=403, detail="Forbidden: organization_id does not match.")
    status = redis_client.hgetall(get_job_redis_key(organization_id, job_id, "status"))
    if not status:
        json_logger.log_json(
            level="warning",
            action="get_job_status",
            message="Job not found",
            organization_id=organization_id,
            job_id=job_id,
            status="not_found"
        )
        raise HTTPException(status_code=404, detail="Job not found.")
    overall_status = redis_client.get(get_job_redis_key(organization_id, job_id, "overall_status")) or "unknown"
    json_logger.log_json(
        level="info",
        action="get_job_status",
        message="Status check successful",
        organization_id=organization_id,
        job_id=job_id,
        status=overall_status
    )
    return JobStatusResponse(job_id=job_id, status=overall_status, sources=status)

# Endpoint: Get job results
@app.get("/results/{job_id}", response_model=JobResultsResponse, tags=["Jobs"])
def get_job_results(job_id: str, organization_id: str = Query(..., description="Organization ID for multi-tenancy")):
    """
    Get the results of a job (per source, per company).
    
    This endpoint allows clients to retrieve the results of completed jobs.
    It returns a dictionary mapping company/source identifiers to their
    corresponding result data.
    
    The endpoint enforces multi-tenancy by verifying the organization ID
    matches the one associated with the job.
    
    Args:
        job_id: The unique job identifier
        organization_id: Multi-tenant organization identifier (required for access control)
        
    Returns:
        JobResultsResponse: Contains all the job results
        
    Raises:
        HTTPException: If the job doesn't exist, belongs to a different organization,
                      or has no results yet
    """
    meta = redis_client.hgetall(get_job_redis_key(organization_id, job_id, "meta"))
    if not meta or meta.get("organization_id") != organization_id:
        json_logger.log_json(
            level="warning",
            action="get_job_results",
            message="Forbidden results access: organization_id does not match",
            organization_id=organization_id,
            job_id=job_id,
            status="forbidden"
        )
        raise HTTPException(status_code=403, detail="Forbidden: organization_id does not match.")
    results = redis_client.hgetall(get_job_redis_key(organization_id, job_id, "result"))
    if not results:
        json_logger.log_json(
            level="warning",
            action="get_job_results",
            message="No results found for this job",
            organization_id=organization_id,
            job_id=job_id,
            status="not_found"
        )
        raise HTTPException(status_code=404, detail="No results found for this job.")
    json_logger.log_json(
        level="info",
        action="get_job_results",
        message="Results access successful",
        organization_id=organization_id,
        job_id=job_id,
        status="success"
    )
    return JobResultsResponse(job_id=job_id, results=results)