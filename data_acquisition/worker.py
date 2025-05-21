"""
Asynchronous Worker for Data Acquisition Pipeline
------------------------------------------------

This module implements the asynchronous worker component of the Salescience
data acquisition pipeline. It's responsible for processing data acquisition
jobs from a Redis queue, fetching data from various sources (SEC, Yahoo Finance),
and storing the results for retrieval by clients.

System Architecture Context:
---------------------------
The worker sits between the job queue and the data sources, acting as the
execution engine for data acquisition tasks:

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

Key Responsibilities:
-------------------
1. Job Queue Processing: Continuously poll Redis queue for new jobs
2. Parallel Execution: Process multiple data acquisition tasks concurrently
3. Source Integration: Interact with various data sources (SEC, Yahoo, etc.)
4. Result Storage: Store results in Redis for retrieval by clients
5. Message Publishing: Publish data to message bus topics for subscribers
6. Metrics & Monitoring: Track performance metrics with Prometheus
7. Error Handling: Implement robust error handling and recovery
8. Multi-tenancy: Maintain proper isolation between organizations

Design Principles:
----------------
1. Asynchronous Execution: Uses asyncio for non-blocking I/O operations,
   maximizing throughput and resource utilization.

2. Horizontal Scalability: Multiple worker instances can run concurrently,
   scaling horizontally to handle increased load.

3. Fault Tolerance: Implements comprehensive error handling to ensure that
   individual job failures don't affect the entire system.

4. Worker Concurrency: Controls parallelism through configurable concurrency
   limits to prevent overloading external APIs.

5. Observability: Integrates Prometheus metrics and structured logging for
   comprehensive monitoring and debugging.

6. Fallback Strategies: Implements fallback approaches when primary methods
   fail, enhancing reliability and resilience.

This worker is designed to be run as a long-lived process, typically in a
container orchestration environment like Kubernetes, where it can be scaled
horizontally based on workload demands.
"""

import json
import asyncio
import logging
import redis
import datetime
import time
import traceback
import uuid
from typing import Dict, Any, List, Optional

# Import data source implementations
from data_acquisition.sec_client import SECDataSource
from data_acquisition.yahoo import YahooDataSource

# Import utility functions for Redis key management, logging, and envelope handling
from data_acquisition.utils import get_job_redis_key, log_job_status, normalize_envelope, get_source_key, JsonLogger

# Import centralized configuration
from config import settings

# Import error framework
from data_acquisition.errors import (
    SalescienceError, ErrorContext, 
    SECError, SECNotFoundError, SECRateLimitError,
    YahooError, 
    RedisError, RedisOperationError, RedisConnectionError,
    WorkerError, WorkerProcessingError, WorkerTimeoutError,
    log_error, format_error_response
)

# Try to import optional modules
try:
    from prometheus_client import Counter, Histogram, Gauge, start_http_server
    PROMETHEUS_ENABLED = True
    
    # Define metrics
    WORKER_JOBS_TOTAL = Counter('worker_jobs_total', 'Total number of jobs processed', ['source'])
    WORKER_JOB_FAILURES = Counter('worker_job_failures', 'Total number of failed jobs', ['source'])
    WORKER_JOB_DURATION = Histogram('worker_job_duration_seconds', 'Duration of jobs in seconds', ['source'])
    WORKER_CURRENT_JOBS = Gauge('worker_current_jobs', 'Number of jobs currently being processed')
except ImportError:
    PROMETHEUS_ENABLED = False
    logging.warning("prometheus_client not installed, metrics will not be available")

# Get centralized logging configuration
from data_acquisition.utils import configure_logging

# Get logger specific to this module
logger = configure_logging("async_worker")

# Initialize JSON logger
json_logger = JsonLogger("worker_json")

# Get Redis configuration from centralized settings
REDIS_URL = settings.redis.url

# Initialize Redis client with configuration from settings
redis_client = redis.Redis.from_url(
    REDIS_URL, 
    decode_responses=True, 
    password=settings.redis.password,
    ssl=settings.redis.ssl,
    socket_timeout=settings.redis.socket_timeout,
    socket_connect_timeout=settings.redis.socket_connect_timeout,
    retry_on_timeout=settings.redis.retry_on_timeout
)

# Optional message bus publisher
try:
    from data_acquisition.sec_client import MessageBusPublisher
    publisher = MessageBusPublisher(REDIS_URL)
    PUBLISH_ENABLED = True
except ImportError:
    logger.warning("Message bus publisher not configured, data will not be published to message bus")
    PUBLISH_ENABLED = False


async def process_sec_job(job_id: str, company: Dict[str, Any], n_years: int, form_type: str, organization_id: str, user_id: str = None):
    """
    Process an SEC data acquisition job for a single company.
    
    Args:
        job_id: Unique job identifier
        company: Company information (ticker or CIK)
        n_years: Number of years of filings to fetch
        form_type: Type of form to fetch (10-K, 10-Q, etc.)
        organization_id: Organization identifier
        user_id: User identifier
    """
    # Generate a request_id for tracing
    request_id = str(uuid.uuid4())
    
    # Create error context
    context = ErrorContext(
        request_id=request_id,
        organization_id=organization_id,
        user_id=user_id,
        company=company,
        source_type="SEC"
    )
    
    start_time = time.time()
    if PROMETHEUS_ENABLED:
        WORKER_CURRENT_JOBS.inc()
        WORKER_JOBS_TOTAL.labels(source='SEC').inc()

    logger.info(f"[{request_id}][SEC] Processing batch job {job_id} for {company} (org={organization_id}, user={user_id})")
    json_logger.log_json(
        level="info",
        action="sec_job_start",
        job_id=job_id,
        request_id=request_id,
        company=company,
        n_years=n_years,
        form_type=form_type,
        organization_id=organization_id,
        user_id=user_id
    )
    
    try:
        # Initialize SEC data source
        sec = SECDataSource()
        ticker = company.get('ticker') or company.get('name')
        cik = company.get('cik')
        
        # Fetch filings for last N years
        params = {
            'ticker': ticker,
            'cik': cik,
            'form_type': form_type
        }
        
        # Try to fetch all years at once using fetch_last_n_years with request_id
        try:
            logger.info(f"[{request_id}][SEC] Fetching {n_years} years of {form_type} for {ticker or cik} (org={organization_id})")
            # Pass request_id to enable proper tracing
            results = sec.fetch_last_n_years(params, n_years, form_type, request_id)
            
            # Process each result
            for idx, envelope in enumerate(results):
                year = envelope.get('metadata', {}).get('year')
                key = get_source_key(company, 'SEC', idx)
                
                # Normalize response
                envelope = normalize_envelope(envelope)
                
                # Add request_id to metadata for tracing
                if 'metadata' not in envelope:
                    envelope['metadata'] = {}
                envelope['metadata']['request_id'] = request_id
                
                # Publish to message bus if enabled
                if PUBLISH_ENABLED:
                    try:
                        publisher.publish(settings.message_bus.sec_topic, envelope)
                        logger.info(f"[{request_id}][SEC] Published {ticker or cik} filing for year {year} to {settings.message_bus.sec_topic}")
                    except Exception as pub_exc:
                        error = SalescienceError(f"Failed to publish to message bus", context=context, cause=pub_exc)
                        log_error(error, logger)
                
                # Store result and status in Redis using organization-prefixed keys
                try:
                    # Store with request_id for tracing
                    redis_client.hset(get_job_redis_key(organization_id, job_id, "result"), key, json.dumps(envelope))
                    redis_client.hset(get_job_redis_key(organization_id, job_id, "status"), key, envelope.get("status", "error"))
                    
                    # Add request_id to audit log
                    redis_client.hset(
                        get_job_redis_key(organization_id, job_id, "audit"), 
                        f"{key}:{timestamp_now()}", 
                        json.dumps({
                            "action": "store_result",
                            "request_id": request_id,
                            "status": envelope.get("status", "error")
                        })
                    )
                    
                    # Add TTL for Redis keys
                    redis_client.expire(get_job_redis_key(organization_id, job_id, "result"), settings.job_expiry_sec)
                    redis_client.expire(get_job_redis_key(organization_id, job_id, "status"), settings.job_expiry_sec)
                    redis_client.expire(get_job_redis_key(organization_id, job_id, "audit"), settings.job_expiry_sec)
                    
                except Exception as redis_exc:
                    error = RedisOperationError(
                        f"Failed to store results in Redis: {redis_exc}", 
                        context=context.with_context(year=year, idx=idx),
                        cause=redis_exc
                    )
                    log_error(error, logger)
                    raise error
                
                logger.info(f"[{request_id}][SEC] Processed filing {idx+1}/{n_years} for {ticker or cik} with status {envelope.get('status')} (org={organization_id})")
        
        except SECError as sec_error:
            # Log with specific SEC error information
            log_error(sec_error, logger)
            # Include error code in log message for easier troubleshooting
            logger.warning(f"[{request_id}][SEC] SEC error in batch fetch ({sec_error.error_code}), falling back to individual fetches: {sec_error.message}")
            
            # Store error in audit log
            try:
                redis_client.hset(
                    get_job_redis_key(organization_id, job_id, "audit"), 
                    f"batch:{timestamp_now()}", 
                    json.dumps({
                        "action": "batch_fetch_error",
                        "request_id": request_id,
                        "error_code": sec_error.error_code,
                        "error": sec_error.message
                    })
                )
            except Exception as e:
                logger.warning(f"[{request_id}][SEC] Failed to store audit log: {e}")
            
            # Fall back to individual fetches
            await _process_sec_individual_fetches(
                sec, job_id, company, n_years, form_type, organization_id, 
                user_id, request_id, context
            )
        
        except SalescienceError as s_error:
            # Log with structured error information
            log_error(s_error, logger)
            logger.warning(f"[{request_id}][SEC] Error in batch fetch ({s_error.error_code}), falling back to individual fetches: {s_error.message}")
            
            # Store error in audit log
            try:
                redis_client.hset(
                    get_job_redis_key(organization_id, job_id, "audit"), 
                    f"batch:{timestamp_now()}", 
                    json.dumps({
                        "action": "batch_fetch_error",
                        "request_id": request_id,
                        "error_code": s_error.error_code,
                        "error": s_error.message
                    })
                )
            except Exception as e:
                logger.warning(f"[{request_id}][SEC] Failed to store audit log: {e}")
            
            # Fall back to individual fetches
            await _process_sec_individual_fetches(
                sec, job_id, company, n_years, form_type, organization_id, 
                user_id, request_id, context
            )
            
        except Exception as batch_error:
            # Wrap unexpected errors in WorkerProcessingError
            error = WorkerProcessingError(
                f"Unexpected error in batch fetch: {batch_error}", 
                context=context,
                cause=batch_error
            )
            log_error(error, logger)
            logger.warning(f"[{request_id}][SEC] Error in batch fetch, falling back to individual fetches")
            
            # Store error in audit log
            try:
                redis_client.hset(
                    get_job_redis_key(organization_id, job_id, "audit"), 
                    f"batch:{timestamp_now()}", 
                    json.dumps({
                        "action": "batch_fetch_error",
                        "request_id": request_id,
                        "error_code": "WORKER_PROCESSING_ERROR",
                        "error": str(batch_error)
                    })
                )
            except Exception as e:
                logger.warning(f"[{request_id}][SEC] Failed to store audit log: {e}")
            
            # Fall back to individual fetches
            await _process_sec_individual_fetches(
                sec, job_id, company, n_years, form_type, organization_id, 
                user_id, request_id, context
            )
        
        # Update overall status
        all_status = redis_client.hvals(get_job_redis_key(organization_id, job_id, "status"))
        if all(s == "success" for s in all_status):
            redis_client.set(get_job_redis_key(organization_id, job_id, "overall_status"), "complete")
        elif any(s == "processing" for s in all_status):
            redis_client.set(get_job_redis_key(organization_id, job_id, "overall_status"), "processing")
        elif any(s == "error" for s in all_status):
            redis_client.set(get_job_redis_key(organization_id, job_id, "overall_status"), "error")
        else:
            redis_client.set(get_job_redis_key(organization_id, job_id, "overall_status"), "not_found")
        
        logger.info(f"[{request_id}][SEC] Completed batch job {job_id} for {company} (org={organization_id}, user={user_id})")
        json_logger.log_json(
            level="info",
            action="sec_job_complete",
            job_id=job_id,
            request_id=request_id,
            company=company,
            organization_id=organization_id,
            user_id=user_id,
            duration=time.time() - start_time
        )
    except Exception as e:
        # Catch-all handler for job-level errors
        logger.error(f"[{request_id}][SEC] Error processing batch job {job_id} for {company} (org={organization_id}, user={user_id}): {e}", exc_info=True)
        
        # Ensure ticker is defined for error handling
        ticker = company.get('ticker') or company.get('cik') or company.get('name')
        
        # Handle error for all years
        for idx in range(n_years):
            key = f"{ticker}:SEC:{idx}"
            redis_client.hset(get_job_redis_key(organization_id, job_id, "status"), key, "error")
            
            # Create proper error envelope with request_id
            error_envelope = {
                'content': None,
                'content_type': None,
                'source': 'sec',
                'status': 'error',
                'error': str(e),
                'metadata': {
                    'ticker': ticker,
                    'year': datetime.datetime.now().year - idx,
                    'form_type': form_type,
                    'request_id': request_id
                }
            }
            
            redis_client.hset(get_job_redis_key(organization_id, job_id, "result"), key, json.dumps(error_envelope))
        
        # Set overall status
        redis_client.set(get_job_redis_key(organization_id, job_id, "overall_status"), "error")
        
        # Store in audit log
        try:
            redis_client.hset(
                get_job_redis_key(organization_id, job_id, "audit"), 
                f"job:{timestamp_now()}", 
                json.dumps({
                    "action": "job_error",
                    "request_id": request_id,
                    "error": str(e)
                })
            )
        except Exception as audit_error:
            logger.warning(f"[{request_id}][SEC] Failed to store audit log: {audit_error}")
        
        json_logger.log_json(
            level="error",
            action="sec_job_error",
            job_id=job_id,
            request_id=request_id,
            company=company,
            organization_id=organization_id,
            user_id=user_id,
            error=str(e),
            duration=time.time() - start_time
        )
        
        if PROMETHEUS_ENABLED:
            WORKER_JOB_FAILURES.labels(source='SEC').inc()
    finally:
        if PROMETHEUS_ENABLED:
            WORKER_CURRENT_JOBS.dec()
            WORKER_JOB_DURATION.labels(source='SEC').observe(time.time() - start_time)


async def _process_sec_individual_fetches(sec, job_id, company, n_years, form_type, organization_id, user_id, request_id, context):
    """
    Helper function to process individual year SEC fetches when batch fetch fails.
    
    Args:
        sec: SECDataSource instance
        job_id: Unique job identifier
        company: Company information (ticker or CIK)
        n_years: Number of years of filings to fetch
        form_type: Type of form to fetch
        organization_id: Organization identifier
        user_id: User identifier
        request_id: Request ID for tracing
        context: Error context for error handling
    """
    ticker = company.get('ticker') or company.get('name')
    cik = company.get('cik')
    
    for idx in range(n_years):
        year = datetime.datetime.now().year - idx
        key = get_source_key(company, 'SEC', idx)
        
        # Update status to processing
        redis_client.hset(get_job_redis_key(organization_id, job_id, "status"), key, "processing")
        log_job_status(job_id, "processing", organization_id, {
            'company': ticker or cik,
            'source': 'SEC',
            'year': year,
            'year_idx': idx,
            'request_id': request_id
        })
        
        # Store processing status in audit log
        try:
            redis_client.hset(
                get_job_redis_key(organization_id, job_id, "audit"), 
                f"{key}:{timestamp_now()}", 
                json.dumps({
                    "action": "processing_year",
                    "request_id": request_id,
                    "year": year
                })
            )
        except Exception as e:
            logger.warning(f"[{request_id}][SEC] Failed to store audit log: {e}")
        
        try:
            # Generate a unique sub-request ID for this year's fetch for detailed tracing
            year_request_id = f"{request_id}-{year}"
            logger.info(f"[{year_request_id}][SEC] Fetching {form_type} for {ticker or cik} for year {year} (org={organization_id})")
            
            # Fetch data for this year with request ID for tracing
            result = sec.fetch({
                'ticker': ticker,
                'cik': cik,
                'form_type': form_type,
                'year': year
            }, year_request_id)
            
            # Normalize response
            envelope = normalize_envelope(result)
            
            # Add request_id to metadata for tracing
            if 'metadata' not in envelope:
                envelope['metadata'] = {}
            envelope['metadata']['request_id'] = year_request_id
            
            # Publish to message bus if enabled
            if PUBLISH_ENABLED:
                try:
                    publisher.publish(settings.message_bus.sec_topic, envelope)
                    logger.info(f"[{year_request_id}][SEC] Published {ticker or cik} filing for year {year} to {settings.message_bus.sec_topic}")
                except Exception as pub_exc:
                    # Create specific error with context
                    error = SalescienceError(
                        f"Failed to publish filing to message bus", 
                        context=context.with_context(year=year), 
                        cause=pub_exc
                    )
                    log_error(error, logger)
            
            # Store result and status in Redis using organization-prefixed keys
            redis_client.hset(get_job_redis_key(organization_id, job_id, "result"), key, json.dumps(envelope))
            redis_client.hset(get_job_redis_key(organization_id, job_id, "status"), key, envelope.get("status", "error"))
            
            # Add to audit log
            try:
                redis_client.hset(
                    get_job_redis_key(organization_id, job_id, "audit"), 
                    f"{key}:{timestamp_now()}", 
                    json.dumps({
                        "action": "fetch_complete",
                        "request_id": year_request_id,
                        "status": envelope.get("status", "error"),
                        "year": year
                    })
                )
            except Exception as e:
                logger.warning(f"[{year_request_id}][SEC] Failed to store audit log: {e}")
            
            logger.info(f"[{year_request_id}][SEC] Successfully processed {ticker or cik} for year {year} (org={organization_id})")
            
        except Exception as e:
            # Create specific error with context for logging
            year_error = WorkerProcessingError(
                f"Error processing SEC filing for {ticker or cik} for year {year}: {e}",
                context=context.with_context(year=year),
                cause=e
            )
            log_error(year_error, logger)
            
            # Store error in Redis - create proper error envelope
            error_envelope = {
                'content': None,
                'content_type': None,
                'source': 'sec',
                'status': 'error',
                'error': str(e),
                'metadata': {
                    'ticker': ticker,
                    'cik': cik,
                    'year': year,
                    'form_type': form_type,
                    'request_id': request_id
                }
            }
            
            # Store error result and status
            redis_client.hset(get_job_redis_key(organization_id, job_id, "result"), key, json.dumps(error_envelope))
            redis_client.hset(get_job_redis_key(organization_id, job_id, "status"), key, "error")
            
            # Add to audit log
            try:
                redis_client.hset(
                    get_job_redis_key(organization_id, job_id, "audit"), 
                    f"{key}:{timestamp_now()}", 
                    json.dumps({
                        "action": "fetch_error",
                        "request_id": request_id,
                        "error": str(e),
                        "year": year
                    })
                )
            except Exception as audit_error:
                logger.warning(f"[{request_id}][SEC] Failed to store audit log: {audit_error}")
            
            if PROMETHEUS_ENABLED:
                WORKER_JOB_FAILURES.labels(source='SEC').inc()


async def process_yahoo_job(job_id: str, company: Dict[str, Any], organization_id: str, user_id: str = None):
    """
    Process a Yahoo Finance data acquisition job for a single company.
    
    Args:
        job_id: Unique job identifier
        company: Company information (ticker)
        organization_id: Organization identifier
        user_id: User identifier
    """
    # Generate a request_id for tracing
    request_id = str(uuid.uuid4())
    
    # Create error context
    context = ErrorContext(
        request_id=request_id,
        organization_id=organization_id,
        user_id=user_id,
        company=company,
        source_type="Yahoo"
    )
    
    start_time = time.time()
    if PROMETHEUS_ENABLED:
        WORKER_CURRENT_JOBS.inc()
        WORKER_JOBS_TOTAL.labels(source='Yahoo').inc()
    
    logger.info(f"[{request_id}][Yahoo] Processing job {job_id} for {company} (org={organization_id}, user={user_id})")
    json_logger.log_json(
        level="info",
        action="yahoo_job_start",
        job_id=job_id,
        request_id=request_id,
        company=company,
        organization_id=organization_id,
        user_id=user_id
    )
    
    try:
        # Initialize Yahoo data source
        yahoo = YahooDataSource()
        ticker = company.get('ticker') or company.get('name')
        
        # Set key for this company/source
        key = get_source_key(company, 'Yahoo')
        
        # Update status to processing
        redis_client.hset(get_job_redis_key(organization_id, job_id, "status"), key, "processing")
        
        # Store processing status in audit log
        try:
            redis_client.hset(
                get_job_redis_key(organization_id, job_id, "audit"), 
                f"{key}:{timestamp_now()}", 
                json.dumps({
                    "action": "processing",
                    "request_id": request_id
                })
            )
        except Exception as e:
            logger.warning(f"[{request_id}][Yahoo] Failed to store audit log: {e}")
        
        log_job_status(job_id, "processing", organization_id, {
            'company': ticker,
            'source': 'Yahoo',
            'request_id': request_id
        })
        
        # Fetch data with request_id for tracing
        try:
            logger.info(f"[{request_id}][Yahoo] Fetching data for ticker {ticker}")
            result = yahoo.fetch({'ticker': ticker}, request_id)
            
            # Normalize response
            envelope = normalize_envelope(result)
            
            # Add request_id to metadata for tracing
            if 'metadata' not in envelope:
                envelope['metadata'] = {}
            envelope['metadata']['request_id'] = request_id
            
            # Publish to message bus if enabled
            if PUBLISH_ENABLED:
                try:
                    publisher.publish(settings.message_bus.yahoo_topic, envelope)
                    logger.info(f"[{request_id}][Yahoo] Published {ticker} data to {settings.message_bus.yahoo_topic}")
                except Exception as pub_exc:
                    # Create specific error with context
                    error = SalescienceError(
                        f"Failed to publish Yahoo data to message bus", 
                        context=context, 
                        cause=pub_exc
                    )
                    log_error(error, logger)
            
            # Store result and status in Redis using organization-prefixed keys
            redis_client.hset(get_job_redis_key(organization_id, job_id, "result"), key, json.dumps(envelope))
            redis_client.hset(get_job_redis_key(organization_id, job_id, "status"), key, envelope.get("status", "error"))
            
            # Add to audit log
            try:
                redis_client.hset(
                    get_job_redis_key(organization_id, job_id, "audit"), 
                    f"{key}:{timestamp_now()}", 
                    json.dumps({
                        "action": "fetch_complete",
                        "request_id": request_id,
                        "status": envelope.get("status", "error")
                    })
                )
            except Exception as e:
                logger.warning(f"[{request_id}][Yahoo] Failed to store audit log: {e}")
            
        except (YahooError, SalescienceError) as yahoo_error:
            # Handle specific Yahoo errors with proper context
            log_error(yahoo_error, logger)
            logger.error(f"[{request_id}][Yahoo] Specific error fetching data for ticker {ticker}: {yahoo_error.error_code}")
            
            # Create error envelope
            error_envelope = {
                'content': None,
                'content_type': None,
                'source': 'yahoo',
                'status': 'error',
                'error': str(yahoo_error),
                'error_code': yahoo_error.error_code,
                'metadata': {
                    'ticker': ticker,
                    'request_id': request_id
                }
            }
            
            # Store the error result
            redis_client.hset(get_job_redis_key(organization_id, job_id, "result"), key, json.dumps(error_envelope))
            redis_client.hset(get_job_redis_key(organization_id, job_id, "status"), key, "error")
            
            # Add to audit log
            try:
                redis_client.hset(
                    get_job_redis_key(organization_id, job_id, "audit"), 
                    f"{key}:{timestamp_now()}", 
                    json.dumps({
                        "action": "fetch_error",
                        "request_id": request_id,
                        "error_code": yahoo_error.error_code,
                        "error": str(yahoo_error)
                    })
                )
            except Exception as e:
                logger.warning(f"[{request_id}][Yahoo] Failed to store audit log: {e}")
                
            if PROMETHEUS_ENABLED:
                WORKER_JOB_FAILURES.labels(source='Yahoo').inc()
                
        except Exception as e:
            # Handle unexpected errors
            error = WorkerProcessingError(
                f"Unexpected error fetching Yahoo data for ticker {ticker}: {e}",
                context=context,
                cause=e
            )
            log_error(error, logger)
            
            # Create error envelope
            error_envelope = {
                'content': None,
                'content_type': None,
                'source': 'yahoo',
                'status': 'error',
                'error': str(e),
                'metadata': {
                    'ticker': ticker,
                    'request_id': request_id
                }
            }
            
            # Store the error result
            redis_client.hset(get_job_redis_key(organization_id, job_id, "result"), key, json.dumps(error_envelope))
            redis_client.hset(get_job_redis_key(organization_id, job_id, "status"), key, "error")
            
            # Add to audit log
            try:
                redis_client.hset(
                    get_job_redis_key(organization_id, job_id, "audit"), 
                    f"{key}:{timestamp_now()}", 
                    json.dumps({
                        "action": "fetch_error",
                        "request_id": request_id,
                        "error": str(e)
                    })
                )
            except Exception as audit_error:
                logger.warning(f"[{request_id}][Yahoo] Failed to store audit log: {audit_error}")
                
            if PROMETHEUS_ENABLED:
                WORKER_JOB_FAILURES.labels(source='Yahoo').inc()
        
        # Update overall status
        all_status = redis_client.hvals(get_job_redis_key(organization_id, job_id, "status"))
        if all(s == "success" for s in all_status):
            redis_client.set(get_job_redis_key(organization_id, job_id, "overall_status"), "complete")
        elif any(s == "processing" for s in all_status):
            redis_client.set(get_job_redis_key(organization_id, job_id, "overall_status"), "processing")
        elif any(s == "error" for s in all_status):
            redis_client.set(get_job_redis_key(organization_id, job_id, "overall_status"), "error")
        else:
            redis_client.set(get_job_redis_key(organization_id, job_id, "overall_status"), "not_found")
        
        # Add TTL for Redis keys
        redis_client.expire(get_job_redis_key(organization_id, job_id, "result"), settings.job_expiry_sec)
        redis_client.expire(get_job_redis_key(organization_id, job_id, "status"), settings.job_expiry_sec)
        redis_client.expire(get_job_redis_key(organization_id, job_id, "audit"), settings.job_expiry_sec)
        
        logger.info(f"[{request_id}][Yahoo] Completed job {job_id} for {ticker} (org={organization_id}, user={user_id})")
        json_logger.log_json(
            level="info",
            action="yahoo_job_complete",
            job_id=job_id,
            request_id=request_id,
            company=company,
            organization_id=organization_id,
            user_id=user_id,
            duration=time.time() - start_time
        )
    except Exception as e:
        # Catch-all for any unexpected errors at the job level
        logger.error(f"[{request_id}][Yahoo] Error processing job {job_id} for {company} (org={organization_id}, user={user_id}): {e}", exc_info=True)
        
        # Ensure ticker is defined for error handling
        ticker = company.get('ticker') or company.get('name')
        key = f"{ticker}:Yahoo"
        
        # Create error envelope with request_id
        error_envelope = {
            'content': None,
            'content_type': None,
            'source': 'yahoo',
            'status': 'error',
            'error': str(e),
            'metadata': {
                'ticker': ticker,
                'request_id': request_id
            }
        }
        
        # Store error result and status
        redis_client.hset(get_job_redis_key(organization_id, job_id, "result"), key, json.dumps(error_envelope))
        redis_client.hset(get_job_redis_key(organization_id, job_id, "status"), key, "error")
        redis_client.set(get_job_redis_key(organization_id, job_id, "overall_status"), "error")
        
        # Add to audit log
        try:
            redis_client.hset(
                get_job_redis_key(organization_id, job_id, "audit"), 
                f"job:{timestamp_now()}", 
                json.dumps({
                    "action": "job_error",
                    "request_id": request_id,
                    "error": str(e)
                })
            )
        except Exception as audit_error:
            logger.warning(f"[{request_id}][Yahoo] Failed to store audit log: {audit_error}")
        
        json_logger.log_json(
            level="error",
            action="yahoo_job_error",
            job_id=job_id,
            request_id=request_id,
            company=company,
            organization_id=organization_id,
            user_id=user_id,
            error=str(e),
            duration=time.time() - start_time
        )
        
        if PROMETHEUS_ENABLED:
            WORKER_JOB_FAILURES.labels(source='Yahoo').inc()
    finally:
        if PROMETHEUS_ENABLED:
            WORKER_CURRENT_JOBS.dec()
            WORKER_JOB_DURATION.labels(source='Yahoo').observe(time.time() - start_time)


async def process_xbrl_job(job_id: str, company: Dict[str, Any], concepts: List[str], organization_id: str, user_id: str = None):
    """
    Process an XBRL data acquisition job for a single company.
    
    Args:
        job_id: Unique job identifier
        company: Company information (ticker or CIK)
        concepts: List of XBRL concepts to fetch (e.g., ['us-gaap:Revenues'])
        organization_id: Organization identifier
        user_id: User identifier
    """
    if not concepts:
        logger.warning(f"[XBRL] No concepts provided for job {job_id}, skipping")
        return
        
    start_time = time.time()
    if PROMETHEUS_ENABLED:
        WORKER_CURRENT_JOBS.inc()
        WORKER_JOBS_TOTAL.labels(source='XBRL').inc()
    
    logger.info(f"[XBRL] Processing job {job_id} for {company} with {len(concepts)} concepts (org={organization_id}, user={user_id})")
    
    try:
        # Initialize SEC data source
        sec = SECDataSource()
        ticker = company.get('ticker') or company.get('name')
        cik = company.get('cik')
        
        # Process each concept
        for concept in concepts:
            key = f"{ticker or cik}:XBRL:{concept}"
            
            # Update status to processing
            redis_client.hset(get_job_redis_key(organization_id, job_id, "status"), key, "processing")
            
            try:
                # Fetch XBRL fact
                result = sec.fetch_xbrl_fact({'ticker': ticker, 'cik': cik}, concept)
                
                # Normalize response
                envelope = normalize_envelope(result)
                
                # Publish to message bus if enabled
                if PUBLISH_ENABLED:
                    try:
                        # Create XBRL topic using prefix from centralized settings
                        xbrl_topic = f"{settings.message_bus.xbrl_topic_prefix}.{concept.replace(':', '_')}"
                        publisher.publish(xbrl_topic, envelope)
                        logger.info(f"[XBRL] Published {ticker or cik} data for concept {concept} to {xbrl_topic}")
                    except Exception as pub_exc:
                        logger.warning(f"[XBRL] Failed to publish envelope to message bus: {pub_exc}")
                
                # Store result
                redis_client.hset(get_job_redis_key(organization_id, job_id, "result"), key, json.dumps(envelope))
                redis_client.hset(get_job_redis_key(organization_id, job_id, "status"), key, envelope.get("status", "error"))
                
                logger.info(f"[XBRL] Successfully processed concept {concept} for {ticker or cik}")
            except Exception as e:
                logger.error(f"[XBRL] Error processing concept {concept} for {ticker or cik}: {e}")
                error_envelope = {
                    'error': str(e),
                    'status': 'error',
                    'source': 'xbrl',
                    'content': None,
                    'content_type': None,
                    'metadata': {
                        'ticker': ticker,
                        'cik': cik,
                        'concept': concept
                    }
                }
                redis_client.hset(get_job_redis_key(organization_id, job_id, "result"), key, json.dumps(error_envelope))
                redis_client.hset(get_job_redis_key(organization_id, job_id, "status"), key, "error")
                
                if PROMETHEUS_ENABLED:
                    WORKER_JOB_FAILURES.labels(source='XBRL').inc()
        
        # Update overall status
        all_status = redis_client.hvals(get_job_redis_key(organization_id, job_id, "status"))
        if all(s == "success" for s in all_status):
            redis_client.set(get_job_redis_key(organization_id, job_id, "overall_status"), "complete")
        elif any(s == "processing" for s in all_status):
            redis_client.set(get_job_redis_key(organization_id, job_id, "overall_status"), "processing")
        elif any(s == "error" for s in all_status):
            redis_client.set(get_job_redis_key(organization_id, job_id, "overall_status"), "error")
        else:
            redis_client.set(get_job_redis_key(organization_id, job_id, "overall_status"), "not_found")
            
        logger.info(f"[XBRL] Completed job {job_id} for {company} (org={organization_id}, user={user_id})")
    except Exception as e:
        logger.error(f"[XBRL] Error processing job {job_id} for {company}: {e}")
        # Update overall status
        redis_client.set(get_job_redis_key(organization_id, job_id, "overall_status"), "error")
        
        if PROMETHEUS_ENABLED:
            WORKER_JOB_FAILURES.labels(source='XBRL').inc()
    finally:
        if PROMETHEUS_ENABLED:
            WORKER_CURRENT_JOBS.dec()
            WORKER_JOB_DURATION.labels(source='XBRL').observe(time.time() - start_time)


async def main():
    """
    Main worker loop to process jobs from the Redis queue.
    
    This function implements the core event loop of the worker process,
    continuously polling the Redis queue for new jobs, parsing them,
    and dispatching them for asynchronous processing.
    
    The worker flow follows these steps:
    1. Initialize the worker (connect to Redis, start metrics server)
    2. Poll the Redis queue for new jobs with a blocking operation
    3. Parse the job data and extract parameters
    4. For each company in the job, create appropriate tasks based on requested sources
    5. Process all tasks concurrently, respecting the concurrency limit
    6. Handle and log any errors that occur during processing
    7. Repeat until terminated
    
    The worker uses a semaphore to control concurrency, ensuring that it
    doesn't exceed the configured limit of concurrent tasks. This prevents
    overwhelming external APIs with too many requests and manages resource
    usage effectively.
    
    Error handling is implemented at multiple levels:
    - Connection errors for Redis are handled at startup
    - Job parsing errors are caught and logged
    - Individual task errors are contained within their respective functions
    - The main loop catches any unexpected exceptions to ensure the worker
      continues running even if a job fails catastrophically
    
    The worker is designed to run indefinitely until explicitly terminated,
    making it suitable for containerized environments and long-running services.
    """
    # Log worker startup with concurrency settings from centralized configuration
    logger.info(f"Async worker started with concurrency limit {settings.worker.concurrency}")
    
    # Create a semaphore to limit concurrent tasks based on configured concurrency
    # This ensures we don't overwhelm external APIs or exhaust system resources
    # The concurrency limit is defined in the centralized configuration system
    semaphore = asyncio.Semaphore(settings.worker.concurrency)
    
    # Validate Redis connection at startup
    # If Redis is unavailable, the worker cannot function and should exit
    try:
        redis_client.ping()
        logger.info("Successfully connected to Redis")
    except Exception as e:
        logger.critical(f"Failed to connect to Redis: {e}")
        return  # Exit the function, which will terminate the worker
    
    # Initialize Prometheus metrics server for monitoring
    # This provides real-time visibility into worker performance
    if PROMETHEUS_ENABLED:
        try:
            # Use metrics port from centralized configuration
            start_http_server(settings.metrics_port)
            logger.info(f"Started Prometheus metrics server on port {settings.metrics_port}")
        except Exception as e:
            logger.error(f"Failed to start Prometheus metrics server: {e}")
            # Continue running even if metrics server fails to start
    
    # Main processing loop
    # This loop runs indefinitely, continuously processing jobs
    while True:
        try:
            # Poll for jobs using Redis BLPOP
            # BLPOP is a blocking operation that waits for a job to be available
            # The timeout parameter (5 seconds) allows periodic health checks
            job_data = redis_client.blpop(settings.queue_name, timeout=5)
            logger.debug(f"Polled Redis: job_data={job_data}")
            
            # If no job is available within the timeout period, sleep briefly and try again
            if not job_data:
                await asyncio.sleep(1)
                continue
            
            # Parse job data
            # The result of BLPOP is a tuple (queue_name, job_data)
            _, job_str = job_data
            job = json.loads(job_str)
            
            # Extract job parameters
            job_id = job.get("job_id")
            companies = job.get("companies", [])
            n_years = job.get("n_years", 1)
            form_type = job.get("form_type", "10-K")
            organization_id = job.get("organization_id")
            user_id = job.get("user_id")
            concepts = job.get("concepts", [])
            
            logger.info(f"Processing job {job_id} with {len(companies)} companies (org={organization_id}, user={user_id})")
            
            # Create tasks for each company and data source
            # This allows processing multiple companies and sources in parallel
            tasks = []
            for company in companies:
                # Check which data sources should be processed for this company
                # Each company can optionally specify which sources to use
                # Default behavior is to enable SEC and Yahoo if not specified
                process_sec = company.get('sec', True)  # Default to True if not specified
                process_yahoo = company.get('yahoo', True)  # Default to True if not specified
                process_xbrl = company.get('xbrl', False)  # Default to False if not specified
                
                company_identifier = company.get('ticker') or company.get('name')
                logger.debug(f"Company {company_identifier} flags: sec={process_sec}, yahoo={process_yahoo}, xbrl={process_xbrl}")
                
                # Create coroutine tasks for each requested data source
                if process_sec:
                    tasks.append(process_sec_job(job_id, company, n_years, form_type, organization_id, user_id))
                
                if process_yahoo:
                    tasks.append(process_yahoo_job(job_id, company, organization_id, user_id))
                
                if process_xbrl and concepts:
                    tasks.append(process_xbrl_job(job_id, company, concepts, organization_id, user_id))
            
            # Execute all tasks concurrently, with concurrency limited by the semaphore
            # asyncio.gather runs all coroutines in parallel and collects their results
            async with semaphore:
                await asyncio.gather(*tasks)
                
            logger.info(f"Completed job {job_id} (org={organization_id}, user={user_id})")
            
        except Exception as e:
            # Catch-all exception handler to ensure the worker keeps running
            # even if a catastrophic error occurs processing a job
            logger.error(f"Error processing job: {e}", exc_info=True)
            
            # Log detailed traceback for debugging
            # This provides important context for troubleshooting issues
            traceback_str = traceback.format_exc()
            logger.error(f"Traceback: {traceback_str}")
            
            # Brief pause before continuing to prevent tight error loops
            # This helps avoid overwhelming the logs if there's a persistent error
            await asyncio.sleep(1)


if __name__ == "__main__":
    """
    Entry point for the worker process when run directly as a script.
    
    This block is executed when the worker is started as a standalone
    process either for development, testing, or in a containerized 
    environment (see Dockerfile.worker).
    
    It handles:
    1. Starting the main async event loop
    2. Graceful shutdown on keyboard interrupts (SIGINT)
    3. Logging any critical exceptions that might cause the worker to crash
    
    In production, the worker would typically be run in a container with
    appropriate health checks and restart policies to ensure high availability.
    """
    # Run the async worker using asyncio.run, which:
    # - Creates a new event loop
    # - Runs the main coroutine until completion
    # - Closes the event loop and all pending tasks
    try:
        logger.info("Starting worker process")
        asyncio.run(main())
    except KeyboardInterrupt:
        # Handle clean shutdown on Ctrl+C or Docker stop
        logger.info("Worker stopped by user (SIGINT/SIGTERM)")
    except Exception as e:
        # Log any unhandled exceptions that might crash the worker
        # This provides critical information for debugging production issues
        logger.critical(f"Worker crashed: {e}", exc_info=True)
        # Exit with non-zero status to indicate failure
        # This allows container orchestration systems to detect the failure
        import sys
        sys.exit(1)