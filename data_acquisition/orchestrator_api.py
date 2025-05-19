"""
Orchestrator API for Data Acquisition (Redis List Queue)
-------------------------------------------------------
This FastAPI app is the main entry point for users to look up company info from multiple sources (SEC, Yahoo, etc.).
It accepts job requests, tracks status/results, and enqueues jobs to a Redis list ('data_jobs') for processing by the async worker.

No authentication or security layer is enforced (public API for internal/trusted use).

---

# Example usage (curl):

Submit a job:
$ curl -X POST http://localhost:8100/submit \
  -H "Content-Type: application/json" \
  -d '{"companies": [{"ticker": "AAPL"}, {"ticker": "MSFT"}], "n_years": 2, "form_type": "10-K"}'

Check job status:
$ curl http://localhost:8100/status/<job_id>

Get job results:
$ curl http://localhost:8100/results/<job_id>

---

# Example usage (Python):

import requests
resp = requests.post("http://localhost:8100/submit", json={
    "companies": [{"ticker": "AAPL"}, {"ticker": "MSFT"}],
    "n_years": 2,
    "form_type": "10-K"
})
job_id = resp.json()["job_ids"][0]
status = requests.get(f"http://localhost:8100/status/{job_id}").json()
results = requests.get(f"http://localhost:8100/results/{job_id}").json()

---

How to run:
1. Ensure Redis is running and accessible (see README for Kubernetes setup)
2. Install dependencies:
    pip install fastapi uvicorn redis pydantic python-dotenv
3. Start the API:
    uvicorn data_acquisition.orchestrator_api:app --host 0.0.0.0 --port 8100
4. Start the worker (see worker.py)
5. Use the endpoints to submit and track jobs.

Environment variables:
- REDIS_URL (e.g., redis://redis:6379/0)

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
    name: Optional[str] = Field(None, description="Company name")
    ticker: Optional[str] = Field(None, description="Stock ticker symbol")
    cik: Optional[str] = Field(None, description="SEC CIK identifier")
    sec: Optional[bool] = Field(False, description="Whether to fetch SEC data for this company")
    yahoo: Optional[bool] = Field(False, description="Whether to fetch Yahoo data for this company")

class SubmitJobsRequest(BaseModel):
    companies: List[CompanyRequest]
    n_years: Optional[int] = Field(1, description="Number of years of filings to fetch (SEC only)")
    form_type: Optional[str] = Field("10-K", description="SEC form type to fetch (e.g., '10-K')")
    organization_id: str = Field(..., description="Organization ID for multi-tenancy")  # Required for tenant isolation
    user_id: Optional[str] = Field(None, description="User ID (optional, for audit)")    # Optional, for audit trails

class SubmitJobsResponse(BaseModel):
    job_ids: List[str]

class JobStatusResponse(BaseModel):
    job_id: str
    status: str
    sources: Dict[str, str]  # e.g., {"AAPL:SEC": "queued", ...}

class JobResultsResponse(BaseModel):
    job_id: str
    results: Dict[str, Any]  # e.g., {"AAPL:SEC": {...}, ...}

# Simulated data source list (replace with real source classes)
DATA_SOURCES = ["SEC", "Yahoo"]

# FastAPI app
app = FastAPI(title="Data Acquisition Orchestrator API")

# Utility: Generate a new job ID
def generate_job_id() -> str:
    return str(uuid.uuid4())

def get_job_redis_key(organization_id: str, job_id: str, suffix: str) -> str:
    """
    Helper to build org-prefixed Redis keys for job metadata, status, and results.
    """
    return f"org:{organization_id}:job:{job_id}:{suffix}"

# Utility: Initialize job in Redis
def init_job_in_redis(job_id: str, companies: List[CompanyRequest], n_years: int, form_type: str, organization_id: str, user_id: Optional[str]):
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
@app.post("/submit", response_model=SubmitJobsResponse)
def submit_jobs(request: SubmitJobsRequest):
    """
    Submit one or more companies for data acquisition from all sources.
    Returns a list of job IDs (one per request).
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
@app.get("/status/{job_id}", response_model=JobStatusResponse)
def get_job_status(job_id: str, organization_id: str = Query(..., description="Organization ID for multi-tenancy")):
    """
    Get the status of a job (per source, per company).
    Requires organization_id for access control.
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
@app.get("/results/{job_id}", response_model=JobResultsResponse)
def get_job_results(job_id: str, organization_id: str = Query(..., description="Organization ID for multi-tenancy")):
    """
    Get the results of a job (per source, per company).
    Requires organization_id for access control.
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