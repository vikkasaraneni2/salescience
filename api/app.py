"""
Main API Layer for Salescience Data Processing Platform
------------------------------------------------------

This module implements the FastAPI application that serves as the primary 
entry point for all data processing requests in the Salescience platform.
It provides a RESTful interface for interacting with the system's core
functionality, abstracting away the underlying complexity.

System Architecture Context:
---------------------------
The API layer serves as the gateway interface and orchestration layer:

┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│ Client Apps │     │ API Layer   │     │ Processing  │     │ Storage     │
│ & Services  │────▶│ (This File) │────▶│ & Analysis  │────▶│ Layer       │
└─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
                          │                                       ▲
                          │                                       │
                          └───────────────────────────────────────┘
                                Status Monitoring & Results

Key Design Principles:
--------------------
1. Thin Controller Pattern: The API is intentionally minimal and stateless,
   acting primarily as a coordination layer without complex business logic.

2. Separation of Concerns: All business logic (processing, analysis, storage)
   is delegated to separate modules, allowing for independent development,
   testing, and deployment.

3. Domain-Driven Design: Endpoint structures and request/response models 
   reflect the business domain concepts rather than technical implementations.

4. Stateless Architecture: No client-specific state is maintained between requests,
   enabling horizontal scaling and resilience.

5. Asynchronous Processing: Long-running operations are handled asynchronously
   with job tracking, allowing clients to submit requests and retrieve results later.

Endpoints Overview:
-----------------
- /health: Service health check for monitoring and load balancers
- /process: Single data processing request
- /batch_process: Batch data processing for multiple items
- /status/{job_id}: Check the status of an asynchronous job
- /results/{job_id}: Retrieve the results of a completed job
- /interpret_query: Translate natural language queries to technical job specifications

Each endpoint is fully decoupled from business logic implementation, which
is contained in dedicated modules for better code organization, testing,
and future expansion (e.g., microservices, new functionality).

Performance Considerations:
-------------------------
- All endpoints are designed to be non-blocking where appropriate
- Heavy computation is offloaded to background workers
- Connection pooling to downstream services prevents bottlenecks
- Caching is applied at strategic points for frequently accessed data

Security Notes:
-------------
- This API currently includes basic input validation through Pydantic
- Production deployments should implement authentication and authorization
- Rate limiting should be applied to prevent abuse
- CORS configuration should be properly set for web clients
"""

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
@app.get("/health", tags=["System"])
def health():
    """
    Service health check endpoint.
    
    This lightweight endpoint verifies that the API service is running and 
    operational. It's designed for use by:
    
    1. Load balancers to determine if the service instance should receive traffic
    2. Kubernetes readiness/liveness probes to manage container lifecycle
    3. Monitoring systems to track service availability
    4. Client applications to verify connectivity
    
    The endpoint performs minimal work, avoiding database or external service 
    calls to ensure it remains responsive even under high system load or when
    downstream dependencies are experiencing issues.
    
    Returns:
        Dict: Simple status message with "ok" value
        
    Response Example:
        {"status": "ok"}
        
    Status Codes:
        200: Service is healthy and operational
        (Service unavailable if no response)
    """
    # Returns a simple status message. No authentication or business logic.
    return {"status": "ok"}

# -----------------------------
# Single Data Processing Endpoint
# -----------------------------
@app.post("/process", tags=["Processing"])
async def process_endpoint(request: Request):
    """
    Process a single data item for financial analysis.
    
    This endpoint accepts a financial data payload (such as a company filing, 
    news article, or market data), processes it through the analysis pipeline,
    and returns a success status or job ID for asynchronous processing.
    
    The endpoint serves as a thin controller, delegating to specialized modules:
    1. Data validation and normalization
    2. Core processing and analysis algorithms
    3. Persistent storage of results
    
    For large or complex data items that require significant processing time,
    the operation is performed asynchronously, returning a job ID that can
    be used to check status and retrieve results later.
    
    Request Body:
        JSON object containing the data to process. Structure varies depending
        on data type, but typically includes:
        - data_type: Type of data being submitted (e.g., "sec_filing", "news")
        - content: The actual content to process
        - metadata: Additional contextual information (company, date, etc.)
        
    Returns:
        Dict containing:
        - status: "success" if processing completed immediately, "pending" if async
        - job_id: Unique identifier for the job (only for async processing)
        - summary: Brief summary of processing results (for immediate processing)
        
    Response Example:
        {"status": "pending", "job_id": "550e8400-e29b-41d4-a716-446655440000"}
        
    Status Codes:
        202: Request accepted, processing initiated
        400: Invalid request format
        500: Server error during processing
    """
    # Parse the incoming JSON data from the request body
    data = await request.json()
    
    # TODO: Implement data validation with Pydantic models
    
    # Call the processing/analysis logic (to be implemented in processing/)
    # This would typically:
    # 1. Normalize the input data
    # 2. Apply business logic and analysis algorithms
    # 3. Generate structured results
    results = process_data(data)  # Mock function, to be implemented
    
    # Store the processed results (to be implemented in storage/)
    # This would typically:
    # 1. Format results for storage
    # 2. Insert/update in appropriate data store
    # 3. Return storage confirmation
    save_results(results)  # Mock function, to be implemented
    
    # Return a success response (would include job ID for async processing)
    return {"status": "success"}

# -----------------------------
# Batch Data Processing Endpoint
# -----------------------------
@app.post("/batch_process", tags=["Processing"])
async def batch_process_endpoint(request: Request):
    """
    Process multiple data items in a batch operation.
    
    This endpoint is designed for high-throughput scenarios where multiple
    data items need to be processed in a single request. It accepts an array
    of data items, submits them for asynchronous processing, and returns
    a list of job IDs that can be used to track the status of each item.
    
    The batch processing approach offers several advantages:
    1. Reduced network overhead compared to multiple single-item requests
    2. Ability to process items in parallel for improved throughput
    3. Consolidated monitoring through batch job IDs
    4. Potential optimizations for items with shared context (e.g., same company)
    
    All processing is performed asynchronously, with results available through
    the /status/{job_id} and /results/{job_id} endpoints.
    
    Request Body:
        JSON array of objects, where each object follows the same structure
        as the single /process endpoint:
        [
            {
                "data_type": string,
                "content": object,
                "metadata": object
            },
            ...
        ]
    
    Returns:
        Dict containing:
        - batch_id: Unique identifier for the entire batch
        - job_ids: Array of job IDs corresponding to each item in the batch
        - count: Number of jobs submitted
        
    Response Example:
        {
            "batch_id": "batch-550e8400",
            "job_ids": ["job-a716446655", "job-41d4a71664"],
            "count": 2
        }
        
    Status Codes:
        202: Batch accepted for processing
        400: Invalid batch format
        413: Batch size exceeds limit
        500: Server error
    
    Performance Considerations:
    - Maximum batch size is limited (default: 100 items)
    - Very large batches may be split into sub-batches automatically
    - Items within a batch are processed in parallel subject to system capacity
    """
    # Parse the incoming JSON data (should be a list of jobs/data)
    batch_data = await request.json()
    
    # TODO: Validate batch format and size
    # TODO: Implement rate limiting and size restrictions
    
    # Call the batch processing logic (to be implemented in processing/)
    # This function would typically:
    # 1. Create individual jobs for each item in the batch
    # 2. Submit jobs to a processing queue
    # 3. Return job IDs for tracking
    job_ids = batch_process_data(batch_data)  # Mock function, to be implemented
    
    # Generate a batch ID for the entire operation
    batch_id = f"batch-{job_ids[0][:8]}" if job_ids else "batch-empty"
    
    # Return the batch details including all job IDs
    return {
        "batch_id": batch_id,
        "job_ids": job_ids,
        "count": len(job_ids)
    }

# -----------------------------
# Job Status Endpoint
# -----------------------------
@app.get("/status/{job_id}", tags=["Jobs"])
def status_endpoint(job_id: str, organization_id: Optional[str] = None):
    """
    Retrieve the current status of an asynchronous job.
    
    This endpoint allows clients to check the progress of previously submitted
    processing jobs. It retrieves the current status from the storage layer and
    returns it to the client, enabling polling for job completion.
    
    The status follows a standard lifecycle:
    - 'pending': Job is queued but not yet started
    - 'processing': Job is actively being processed
    - 'complete': Job has finished successfully
    - 'error': Job encountered an error during processing
    - 'not_found': Job ID doesn't exist or has expired
    
    This endpoint is essential for the asynchronous processing model, allowing
    clients to submit jobs and periodically check their status until completion.
    It's designed to be lightweight and responsive, suitable for frequent polling.
    
    Path Parameters:
        job_id: Unique identifier for the job to check
        
    Query Parameters:
        organization_id: Optional organization identifier for multi-tenant isolation
        
    Returns:
        Dict containing:
        - job_id: Echo of the requested job ID
        - status: Current status string
        - progress: Optional progress percentage (0-100) for 'processing' status
        - estimated_completion: Optional ISO timestamp for expected completion
        - detail: Optional additional context about the current status
        
    Response Example:
        {
            "job_id": "550e8400-e29b-41d4-a716-446655440000",
            "status": "processing",
            "progress": 45,
            "estimated_completion": "2023-06-01T14:30:00Z"
        }
        
    Status Codes:
        200: Status retrieved successfully
        404: Job not found
        403: Unauthorized access (for multi-tenant environments)
    
    Note:
        For multi-tenant environments, the organization_id parameter ensures
        that clients can only access jobs belonging to their organization.
    """
    # Import the storage utility (should be properly imported at module level)
    from storage.storage_interface import get_status
    
    # Retrieve the status from storage using the job ID and optional org ID
    # The storage layer handles the appropriate key format and retrieval logic
    status = get_status(job_id, organization_id)
    
    # Build the response with the job ID and status
    response = {
        "job_id": job_id,
        "status": status
    }
    
    # Add additional metadata for jobs in progress
    if status == "processing":
        # In a real implementation, these would be retrieved from storage
        # along with the status, rather than hard-coded
        response["progress"] = 50  # Example progress percentage
        response["estimated_completion"] = "2023-06-01T15:00:00Z"  # Example timestamp
    
    return response

# -----------------------------
# Job Results Endpoint
# -----------------------------
@app.get("/results/{job_id}", tags=["Jobs"])
def results_endpoint(job_id: str, organization_id: Optional[str] = None):
    """
    Retrieve the results of a completed job.
    
    This endpoint allows clients to fetch the results of previously submitted
    processing jobs once they have completed. It retrieves the full result data
    from the storage layer and returns it to the client.
    
    The endpoint should only be called after checking that the job status
    is 'complete' via the /status/{job_id} endpoint, as attempting to retrieve
    results for incomplete jobs will result in an error or partial data.
    
    For large result sets, the response may include pagination information
    or provide links to separate result files for download, depending on
    the size and nature of the processed data.
    
    Path Parameters:
        job_id: Unique identifier for the job to retrieve results for
        
    Query Parameters:
        organization_id: Optional organization identifier for multi-tenant isolation
        
    Returns:
        Dict containing:
        - job_id: Echo of the requested job ID
        - results: Object containing the job results
          Format depends on the type of job and data processed
        - metadata: Information about the result data (timestamps, sources, etc.)
        
    Response Example:
        {
            "job_id": "550e8400-e29b-41d4-a716-446655440000",
            "results": {
                "AAPL:SEC:0": {
                    "content": "...",
                    "source": "sec",
                    "metadata": { ... }
                },
                "AAPL:Yahoo": {
                    "content": { ... },
                    "source": "yahoo",
                    "metadata": { ... }
                }
            },
            "metadata": {
                "processed_at": "2023-06-01T12:34:56Z",
                "source_count": 2
            }
        }
        
    Status Codes:
        200: Results retrieved successfully
        202: Job still in progress (no results yet)
        404: Job not found
        403: Unauthorized access (for multi-tenant environments)
        
    Performance Considerations:
    - Results may be cached for improved response times on repeated requests
    - Very large results may be paginated or streamed
    - Results typically expire after a configurable retention period
    """
    # Import the storage utility (should be properly imported at module level)
    from storage.storage_interface import get_status, get_results
    
    # First check if the job is complete
    status = get_status(job_id, organization_id)
    
    # If job is still in progress, return an appropriate response
    if status == "processing" or status == "pending":
        return {
            "job_id": job_id,
            "status": status,
            "message": "Job is still in progress. Results not yet available."
        }
    
    # If job had an error, return the error information
    if status == "error":
        return {
            "job_id": job_id,
            "status": "error",
            "message": "Job encountered an error during processing."
        }
    
    # Job is complete, retrieve results from storage
    results = get_results(job_id, organization_id)
    
    # Return comprehensive response with results and metadata
    return {
        "job_id": job_id,
        "results": results,
        "metadata": {
            "processed_at": "2023-06-01T12:34:56Z",  # Example timestamp
            "source_count": len(results) if results else 0
        }
    }

# -----------------------------
# Query Interpretation Endpoint
# -----------------------------

class InterpretQueryRequest(BaseModel):
    """
    Data model for natural language query interpretation request.
    
    This model defines the structure of requests to the query interpretation
    endpoint, which converts natural language requests into structured job
    specifications for the data acquisition and processing pipeline.
    
    Fields:
        query: The natural language query string from the user
               (e.g., "Get me Apple's annual reports from last 3 years")
        company: The company identifier (name, ticker) to focus on
        date_range: Optional date range to limit results
        organization_id: Multi-tenant organization identifier
        user_id: Optional user identifier for tracking and personalization
    
    Example:
        {
            "query": "Find AAPL's earnings reports for 2020-2022",
            "company": "AAPL",
            "organization_id": "acme-corp"
        }
    """
    query: str
    company: str
    date_range: Optional[List[str]] = None
    organization_id: Optional[str] = None
    user_id: Optional[str] = None

class InterpretQueryResponse(BaseModel):
    """
    Data model for the query interpretation response.
    
    This model defines the structure of responses from the query interpretation
    endpoint, containing the structured job specification derived from the
    natural language query.
    
    Fields:
        job_spec: A structured job specification object that can be
                 submitted directly to the data acquisition orchestrator
    
    Example:
        {
            "job_spec": {
                "organization_id": "acme-corp",
                "company": "AAPL",
                "sec_form_types": ["10-K", "10-Q"],
                "date_range": ["2020-01-01", "2022-12-31"],
                "matched_concepts": ["earnings"]
            }
        }
    """
    job_spec: Dict[str, Any]

@app.post("/interpret_query", response_model=InterpretQueryResponse, tags=["Natural Language"])
def interpret_query(request: InterpretQueryRequest):
    """
    Convert a natural language query into a structured job specification.
    
    This endpoint serves as a bridge between human language and the technical API,
    allowing users to express their data needs in natural language. It uses NLP
    techniques to extract key entities, concepts, and parameters from the query
    and maps them to the appropriate technical specifications.
    
    The interpreter handles:
    1. Entity extraction (companies, dates, financial concepts)
    2. Intent recognition (what type of data is being requested)
    3. Parameter mapping (translating concepts to technical parameters)
    4. Ambiguity resolution (making reasonable assumptions when needed)
    
    The resulting job specification can be submitted directly to the data
    acquisition orchestrator API to fulfill the user's request.
    
    Request Body:
        InterpretQueryRequest object containing:
        - query: Natural language query string
        - company: Company identifier to focus on
        - date_range: Optional date range constraint
        - organization_id: Optional organization identifier
        - user_id: Optional user identifier
        
    Returns:
        InterpretQueryResponse containing:
        - job_spec: Structured job specification object
        
    Example Input:
        {
            "query": "Get me Apple's annual reports and earnings from 2020 to 2022",
            "company": "AAPL",
            "organization_id": "acme-corp"
        }
        
    Example Output:
        {
            "job_spec": {
                "organization_id": "acme-corp",
                "company": "AAPL",
                "sec_form_types": ["10-K", "10-Q", "8-K"],
                "yahoo_data_types": ["income_statement"],
                "date_range": ["2020-01-01", "2022-12-31"],
                "matched_concepts": ["financial reports", "earnings"]
            }
        }
        
    Status Codes:
        200: Query successfully interpreted
        400: Invalid query format
        422: Query could not be interpreted
    """
    # Initialize the query interpreter from the dedicated module
    # This maintains separation of concerns, with NLP logic in its own module
    interpreter = QueryInterpreter()
    
    # Process the query through the interpreter to extract structured parameters
    job_spec = interpreter.interpret(
        query=request.query,
        company=request.company,
        date_range=request.date_range,
        organization_id=request.organization_id,
        user_id=request.user_id
    )
    
    # Return the structured job specification
    return InterpretQueryResponse(job_spec=job_spec)
