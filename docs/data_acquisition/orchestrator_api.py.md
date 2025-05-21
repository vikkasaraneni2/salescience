# Orchestrator API Documentation

## Overview
The `orchestrator_api.py` module implements the API gateway and orchestration layer for the data acquisition system. It provides a RESTful API that enables clients to request data from multiple sources (SEC, Yahoo, etc.) in a unified, asynchronous manner.

## System Architecture Context

The Orchestrator API plays a central role in the data acquisition pipeline:

```
┌───────────────┐      ┌───────────────┐      ┌───────────────┐      ┌───────────────┐
│  Client Apps  │      │  Orchestrator │      │ Redis Queue   │      │ Worker Nodes  │
│  & Services   │─────▶│  API Gateway  │─────▶│ & State Store │─────▶│ (Processors)  │
└───────────────┘      └───────────────┘      └───────────────┘      └───────────────┘
                              │                      ▲                      │
                              │                      │                      │
                              └──────────────────────┴──────────────────────┘
                                    Status & Result Monitoring
```

## Key Responsibilities

1. **Client Request Management**: Accept and validate data acquisition requests
2. **Job Creation**: Generate unique job IDs and initialize tracking structures 
3. **Work Distribution**: Enqueue tasks for async processing by worker nodes
4. **Status Tracking**: Provide endpoints to monitor job progress
5. **Result Retrieval**: Enable access to completed job results
6. **Multi-tenancy**: Enforce organization-based isolation of data and requests

## Design Benefits

### Decoupled Processing
Separating the API from the actual data acquisition allows for independent scaling of request handling and processing components.

### Asynchronous Workflow
Clients can submit jobs and retrieve results later, enabling efficient handling of long-running operations.

### Stateless API
The API itself maintains no state; all job state is stored in Redis, enabling horizontal scaling and fault tolerance.

### Multi-tenant Architecture
Organization-prefixed Redis keys ensure proper data isolation in a multi-tenant environment.

### Standardized Result Format
All results follow the same envelope pattern regardless of data source, simplifying client integration.

## API Endpoints

### POST /submit
Submit new data acquisition jobs with specified companies and parameters.

**Request Body:**
```json
{
  "companies": [
    {
      "ticker": "AAPL",
      "sec": true,
      "yahoo": true
    }
  ],
  "n_years": 2,
  "form_type": "10-K",
  "organization_id": "acme-corp",
  "user_id": "user-123"
}
```

**Response:**
```json
{
  "job_ids": ["550e8400-e29b-41d4-a716-446655440000"]
}
```

### GET /status/{job_id}
Check the status of a job.

**Query Parameters:**
- `organization_id`: Required for multi-tenant isolation

**Response:**
```json
{
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "processing",
  "sources": {
    "AAPL:SEC:0": "success",
    "AAPL:SEC:1": "processing",
    "AAPL:Yahoo": "success"
  }
}
```

### GET /results/{job_id}
Retrieve results of a completed job.

**Query Parameters:**
- `organization_id`: Required for multi-tenant isolation

**Response:**
```json
{
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "results": {
    "AAPL:SEC:0": {
      "content": "...",
      "content_type": "html",
      "source": "sec",
      "metadata": { ... }
    },
    "AAPL:Yahoo": {
      "content": { ... },
      "content_type": "json",
      "source": "yahoo",
      "metadata": { ... }
    }
  }
}
```

## Data Flow

1. Client submits a job via `POST /submit`
2. API generates job ID and initializes tracking in Redis
3. API enqueues job details to 'data_jobs' Redis list
4. Worker nodes pick up jobs from the queue and process them
5. Workers update job status and store results in Redis
6. Client polls status endpoint until job is complete
7. Client retrieves results via results endpoint

## Security and Multi-tenancy

- All endpoints require an `organization_id` parameter
- Redis keys are prefixed with organization ID for proper isolation
- Each organization can only access its own jobs and results
- API enforces access control based on organization ID

## Implementation Details

### Data Models

#### CompanyRequest
```python
class CompanyRequest(BaseModel):
    name: Optional[str]
    ticker: Optional[str]
    cik: Optional[str]
    sec: Optional[bool] = False
    yahoo: Optional[bool] = False
```

#### SubmitJobsRequest
```python
class SubmitJobsRequest(BaseModel):
    companies: List[CompanyRequest]
    n_years: Optional[int] = 1
    form_type: Optional[str] = "10-K"
    organization_id: str
    user_id: Optional[str] = None
```

### Key Functions

#### initialize_job
```python
def init_job_in_redis(job_id, companies, n_years, form_type, organization_id, user_id)
```
- Sets up Redis structures for tracking job state
- Creates placeholder entries for each company/source
- Establishes multi-tenant key prefixing

#### enqueue_job
```python
def enqueue_job(job_id, companies, n_years, form_type, organization_id, user_id)
```
- Prepares job payload with company and source details
- Intelligently enables sources based on available company identifiers
- Pushes serialized job to Redis queue for worker pickup

### Redis Key Structure

To support multi-tenancy and organized job tracking:
```
org:{organization_id}:job:{job_id}:meta     # Job metadata
org:{organization_id}:job:{job_id}:status   # Status for each company/source
org:{organization_id}:job:{job_id}:result   # Results for each company/source
org:{organization_id}:job:{job_id}:overall_status  # Overall job status
```

## Usage Examples

### Client Submission Example (Python)
```python
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
```

### cURL Examples
```bash
# Submit a job
curl -X POST http://localhost:8100/submit \
  -H "Content-Type: application/json" \
  -d '{"companies": [{"ticker": "AAPL"}, {"ticker": "MSFT"}], "n_years": 2, "form_type": "10-K", "organization_id": "acme-corp"}'

# Check job status
curl "http://localhost:8100/status/<job_id>?organization_id=acme-corp"

# Get job results
curl "http://localhost:8100/results/<job_id>?organization_id=acme-corp"
```

## Deployment

### Environment Variables
- `REDIS_URL`: Redis connection string (default: redis://localhost:6379/0)
- `LOG_LEVEL`: Logging level (default: INFO)

### Running the API
```bash
# Install dependencies
pip install fastapi uvicorn redis pydantic python-dotenv

# Start the API
uvicorn data_acquisition.orchestrator_api:app --host 0.0.0.0 --port 8100
```

### Docker Deployment
```bash
# Build the container
docker build -f api/Dockerfile -t salescience-api:latest .

# Run with environment variables
docker run -p 8100:8100 \
  -e REDIS_URL=redis://redis:6379/0 \
  -e LOG_LEVEL=INFO \
  salescience-api:latest
```

## Dependencies
- `fastapi`: Modern, fast web framework
- `pydantic`: Data validation and settings management
- `redis`: Redis client for job queue and result storage
- `uvicorn`: ASGI server for running the API
- `python-dotenv`: Loading environment variables from .env files