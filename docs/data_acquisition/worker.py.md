# Asynchronous Worker Documentation

## Overview
The `worker.py` module implements the core worker component of the data acquisition pipeline. It processes data acquisition jobs from a Redis queue, interacts with various data sources like SEC and Yahoo Finance, and manages results storage for client retrieval.

## System Architecture Context

The worker functions as the execution engine for data acquisition tasks, sitting between the job queue and data sources:

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│ Orchestrator│     │ Redis       │     │ Async       │     │ Data Sources│
│ API         │────▶│ Job Queue   │────▶│ Worker      │────▶│ (SEC/Yahoo) │
└─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
                                              │
                                              ▼
                                        ┌─────────────┐
                                        │ Redis       │
                                        │ Result Store│
                                        └─────────────┘
```

## Key Components

### Main Processing Functions

1. **`process_sec_job`**: Handles SEC filing acquisition for a single company
   - Fetches multiple years of filings for specified form types
   - Stores results in Redis with organization-specific namespacing
   - Publishes data to message bus topics for further processing

2. **`process_yahoo_job`**: Retrieves Yahoo Finance data for a single company
   - Fetches company profile, financial metrics, and stock data
   - Normalizes and stores results with multi-tenant isolation

3. **`process_xbrl_job`**: Acquires structured XBRL financial data
   - Retrieves standardized financial metrics (revenue, income, assets, etc.)
   - Processes multiple XBRL concepts in parallel

4. **`main`**: Main worker event loop
   - Polls Redis queue for new jobs
   - Dispatches tasks to appropriate processing functions
   - Manages concurrency with semaphores
   - Handles errors and ensures worker resilience

## Design Principles

### Asynchronous Processing
Uses asyncio for non-blocking I/O operations, maximizing throughput and resource utilization.

### Horizontal Scalability
Multiple worker instances can run concurrently to handle increased load.

### Fault Tolerance
Comprehensive error handling ensures individual job failures don't affect the overall system.

### Concurrency Control
Configurable concurrency limits prevent overloading external APIs.

### Observability
Prometheus metrics and structured logging provide detailed operational visibility.

### Multi-Tenancy
Organization prefixing for Redis keys ensures proper data isolation.

## Job Processing Flow

1. Job arrives in the Redis queue
2. Worker parses job parameters (companies, form types, years, etc.)
3. For each company, appropriate processing tasks are created
4. Tasks are executed concurrently, respecting concurrency limits
5. Results are stored in Redis with organization-specific keys
6. Status updates are tracked at both individual and overall job levels
7. Results are optionally published to message bus topics

## Configuration Options

The worker can be configured through environment variables:

- `REDIS_URL`: Redis connection string (default: "redis://localhost:6379/0")
- `WORKER_CONCURRENCY`: Maximum parallel tasks (default: 10)
- `SEC_TOPIC`: Message bus topic for SEC data (default: "data.sec")
- `YAHOO_TOPIC`: Message bus topic for Yahoo data (default: "data.yahoo")
- `METRICS_PORT`: Prometheus metrics port (default: 8000)

## Monitoring and Metrics

When Prometheus is enabled, the worker exports the following metrics:

- `worker_jobs_total`: Counter of total jobs processed by source
- `worker_job_failures`: Counter of failed jobs by source
- `worker_job_duration_seconds`: Histogram of job processing times
- `worker_current_jobs`: Gauge of currently active jobs

## Error Handling

The worker implements robust error handling:

1. **Per-Filing Errors**: Errors in individual filings are contained to that filing
2. **Per-Company Errors**: Issues with a specific company don't affect other companies
3. **Per-Job Errors**: Problems in one job don't affect other jobs
4. **Worker Resilience**: The main loop catches all exceptions to ensure continuous operation

## Usage

The worker is designed to be run as a long-lived process, typically in a containerized environment:

```bash
python -m data_acquisition.worker
```

For more detailed logs:

```bash
LOGGING_LEVEL=DEBUG python -m data_acquisition.worker
```

## Dependencies

- `redis`: Redis client for job queue and result storage
- `asyncio`: Asynchronous I/O and concurrency
- `sec_client`: SEC EDGAR data source implementation 
- `yahoo_client`: Yahoo Finance data source implementation
- `prometheus_client`: Optional metrics collection
- `utils`: Utility functions for key management and logging