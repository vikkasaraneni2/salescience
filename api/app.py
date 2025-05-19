# Always activate your venv before running this app to ensure correct Python and package versions.
# api/app.py
# -----------------------------
# Main API Layer for Data Processing Service
# -----------------------------
# This file defines the FastAPI application that serves as the entry point for all data processing requests.
# The API is intentionally minimal and stateless, acting as a thin orchestration layer.
# All business logic (processing, analysis, storage) is delegated to separate modules.
# This design allows for easy scaling, maintainability, and future expansion (e.g., microservices, new endpoints).
#
# Endpoints provided:
#   - /health: Service health check
#   - /process: Single data processing
#   - /batch_process: Batch data processing
#   - /status/{job_id}: Check job status
#   - /results/{job_id}: Retrieve job results
#   - /interpret_query: Interpret a high-level query
#
# Each endpoint is fully decoupled from business logic, which should be implemented in the respective modules.

from fastapi import FastAPI, Request  # FastAPI is the web framework; Request is used to access incoming request data
# Import the QueryInterpreter from the dedicated module
from api.query_interpreter import QueryInterpreter
from typing import Optional, List, Dict, Any
from pydantic import BaseModel

# Create the FastAPI app instance. This is the main application object used by the server (e.g., Uvicorn, Gunicorn).
app = FastAPI()

# -----------------------------
# Health Check Endpoint
# -----------------------------
# Simple GET endpoint to verify that the API is running and healthy.
# Useful for monitoring, orchestration, and load balancer health checks.
@app.get("/health")
def health():
    # Returns a simple status message. No authentication or business logic.
    return {"status": "ok"}

# -----------------------------
# Single Data Processing Endpoint
# -----------------------------
# Receives a single data item via POST, processes it, and stores the result.
# - Expects: JSON payload in the request body (single data item)
# - Calls: process_data (business logic, to be implemented in processing/)
# - Calls: save_results (database logic, to be implemented in storage/)
# - Returns: Success status (can be extended to return job ID or result summary)
@app.post("/process")
async def process_endpoint(request: Request):
    # Parse the incoming JSON data from the request body
    data = await request.json()
    # Call the processing/analysis logic (to be implemented in processing/)
    results = process_data(data)
    # Store the processed results in the database (to be implemented in storage/)
    save_results(results)
    # Return a success response (can be extended to return more info)
    return {"status": "success"}

# -----------------------------
# Batch Data Processing Endpoint
# -----------------------------
# Receives a list of jobs/data via POST, processes them in batch, and returns a list of job IDs.
# - Expects: JSON payload (list of jobs/data)
# - Calls: batch_process_data (to be implemented in processing/)
# - Returns: List of job IDs (each job can be tracked individually)
# This endpoint is designed for scalability and high-throughput scenarios.
@app.post("/batch_process")
async def batch_process_endpoint(request: Request):
    # Parse the incoming JSON data (should be a list of jobs/data)
    batch_data = await request.json()
    # Call the batch processing logic (to be implemented in processing/)
    # This function should handle job creation, queuing, and return job IDs for tracking
    job_ids = batch_process_data(batch_data)
    # Return the list of job IDs so clients can track progress/results
    return {"job_ids": job_ids}

# -----------------------------
# Job Status Endpoint
# -----------------------------
# Allows users/services to check the status of a job by its unique ID.
# - Expects: job ID as a path parameter
# - Calls: get_status (to be implemented in storage/)
# - Returns: Status of the job (e.g., pending, processing, complete, failed)
# This endpoint supports asynchronous and batch workflows.
@app.get("/status/{job_id}")
def status_endpoint(job_id: str):
    # Retrieve the status of the job from storage (to be implemented in storage/)
    status = get_status(job_id)
    # Return the job ID and its current status
    return {"job_id": job_id, "status": status}

# -----------------------------
# Job Results Endpoint
# -----------------------------
# Allows users/services to fetch the results of a job by its unique ID.
# - Expects: job ID as a path parameter
# - Calls: get_results (to be implemented in storage/)
# - Returns: Results of the job (format depends on your business logic)
# This endpoint supports asynchronous and batch workflows, and can be extended for pagination or streaming.
@app.get("/results/{job_id}")
def results_endpoint(job_id: str):
    # Retrieve the results of the job from storage (to be implemented in storage/)
    results = get_results(job_id)
    # Return the job ID and its results (can be extended for more metadata)
    return {"job_id": job_id, "results": results}

# --- FastAPI Endpoint for Query Interpretation ---

class InterpretQueryRequest(BaseModel):
    query: str
    company: str
    date_range: Optional[List[str]] = None
    organization_id: Optional[str] = None
    user_id: Optional[str] = None

class InterpretQueryResponse(BaseModel):
    job_spec: Dict[str, Any]

@app.post("/interpret_query", response_model=InterpretQueryResponse)
def interpret_query(request: InterpretQueryRequest):
    """
    Accepts a high-level, business-friendly query and returns a technical job spec for the orchestrator API.
    Uses the QueryInterpreter from api/query_interpreter.py for clean separation of concerns.
    """
    interpreter = QueryInterpreter()
    job_spec = interpreter.interpret(
        query=request.query,
        company=request.company,
        date_range=request.date_range,
        organization_id=request.organization_id,
        user_id=request.user_id
    )
    return InterpretQueryResponse(job_spec=job_spec)
