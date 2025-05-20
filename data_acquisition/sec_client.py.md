# SEC Client Documentation

## Overview
The `sec_client.py` module provides comprehensive access to SEC EDGAR filings and financial data through the sec-api.io service. It implements a robust interface for retrieving various filing types, with extensive error handling, fallback strategies, and metadata enrichment.

## Main Components

### BaseDataSource Abstract Base Class
Defines the interface for all data sources in the system, establishing a consistent data retrieval pattern.

### MessageBusPublisher Class
Implements a publish-subscribe pattern using Redis Streams for event-driven architecture, allowing decoupling of data acquisition from processing and storage.

### SECDataSource Class
The primary class for interacting with SEC filing data, offering methods for retrieving various types of SEC filings and financial data.

### Key Utility Functions
- `get_job_redis_key`: Creates Redis keys with organization prefixes for multi-tenant isolation.
- `get_cik_for_ticker`: Resolves stock ticker symbols to SEC Central Index Keys (CIKs).
- `process_sec_batch_job`: Handles batch processing of SEC data acquisition jobs.
- `run_sec_batch_worker`: Starts a worker process for processing SEC batch jobs from a Redis queue.

## Data Acquisition Capabilities

The module offers comprehensive access to SEC data:

1. **Annual Reports (10-K)**: Retrieve full annual reports for companies
2. **Quarterly Reports (10-Q)**: Access quarterly financial information
3. **Material Events (8-K)**: Get notifications of significant company events
4. **Insider Trading (Form 4)**: Track insider trading activities
5. **XBRL Financial Data**: Access structured financial data points
6. **Multi-Year Historical Data**: Retrieve data across multiple years
7. **Batch Processing**: Process multiple companies in parallel

## Implementation Details

### Error Handling and Resilience
- Multiple fallback strategies for document retrieval
- Comprehensive error reporting and logging
- Automatic retries and alternative source attempts

### Performance Optimization
- Optional Redis Queue (RQ) integration for distributed processing
- Prometheus metrics for operational monitoring
- Efficient HTTP client with proper timeouts

### Security Features
- Environment variable configuration for sensitive values
- Proper logging with sensitive data masking
- User-Agent compliance for SEC API requirements

## Usage Examples

### Fetching a Single 10-K Filing
```python
sec = SECDataSource()
result = sec.fetch({'ticker': 'AAPL', 'form_type': '10-K', 'year': 2022})
```

### Retrieving Multiple Years of Filings
```python
sec = SECDataSource()
results = sec.fetch_last_n_years({'ticker': 'MSFT'}, n_years=5, form_type='10-K')
```

### Getting XBRL Financial Data
```python
sec = SECDataSource()
revenue_data = sec.fetch_xbrl_fact({'ticker': 'GOOGL'}, 'us-gaap:Revenues')
```

### Running a Batch Worker
```python
if __name__ == '__main__':
    run_sec_batch_worker(queue_name='sec_batch')
```

## Configuration

The module uses the following environment variables:
- `SEC_API_KEY`: API key for sec-api.io (required)
- `REDIS_URL`: Redis connection URL (defaults to localhost)
- `SEC_USER_AGENT`: User agent for SEC API requests (required by SEC)

## Dependencies
- `httpx`: Modern HTTP client (async support)
- `redis`: Redis client for queuing and message bus
- `dotenv`: Environment variable loading (optional)
- `prometheus_client`: Metrics collection (optional)
- `rq`: Redis Queue for job processing (optional)

## Container Support
The module includes CLI support for containerized deployment with Prometheus metrics exposure, making it suitable for Kubernetes environments.

```bash
python -m data_acquisition.sec_client --queue=sec_jobs --metrics-port=8001 --log-level=INFO
```