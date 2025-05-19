import os
import json
import asyncio
import logging
import redis
import datetime
import time
import traceback
from typing import Dict, Any, List, Optional

# Import required modules 
from data_acquisition.sec_client import SECDataSource
from data_acquisition.yahoo_client import YahooDataSource
from data_acquisition.utils import get_job_redis_key, log_job_status, normalize_envelope, get_source_key, JsonLogger

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

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("async_worker")

# Initialize JSON logger
json_logger = JsonLogger("worker_json")

# Redis connection
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
redis_client = redis.Redis.from_url(REDIS_URL, decode_responses=True)

# Concurrency limits
WORKER_CONCURRENCY = int(os.getenv("WORKER_CONCURRENCY", "10"))

# Message bus configuration
SEC_TOPIC = os.getenv("SEC_TOPIC", "data.sec")  # Topic for SEC data
YAHOO_TOPIC = os.getenv("YAHOO_TOPIC", "data.yahoo")  # Topic for Yahoo data

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
    start_time = time.time()
    if PROMETHEUS_ENABLED:
        WORKER_CURRENT_JOBS.inc()
        WORKER_JOBS_TOTAL.labels(source='SEC').inc()

    logger.info(f"[SEC] Processing batch job {job_id} for {company} (org={organization_id}, user={user_id})")
    json_logger.log_json(
        level="info",
        action="sec_job_start",
        job_id=job_id,
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
        
        # Try to fetch all years at once using fetch_last_n_years
        try:
            logger.info(f"[SEC] Fetching {n_years} years of {form_type} for {ticker or cik} (org={organization_id})")
            results = sec.fetch_last_n_years(params, n_years, form_type)
            
            # Process each result
            for idx, envelope in enumerate(results):
                year = envelope.get('metadata', {}).get('year')
                key = get_source_key(company, 'SEC', idx)
                
                # Normalize response
                envelope = normalize_envelope(envelope)
                
                # Publish to message bus if enabled
                if PUBLISH_ENABLED:
                    try:
                        publisher.publish(SEC_TOPIC, envelope)
                        logger.info(f"[SEC] Published {ticker or cik} filing for year {year} to {SEC_TOPIC}")
                    except Exception as pub_exc:
                        logger.warning(f"[SEC] Failed to publish envelope to message bus: {pub_exc} (org={organization_id}, job={job_id})")
                
                # Store result and status in Redis using organization-prefixed keys
                redis_client.hset(get_job_redis_key(organization_id, job_id, "result"), key, json.dumps(envelope))
                redis_client.hset(get_job_redis_key(organization_id, job_id, "status"), key, envelope.get("status", "error"))
                
                logger.info(f"[SEC] Processed filing {idx+1}/{n_years} for {ticker or cik} with status {envelope.get('status')} (org={organization_id})")
        
        except Exception as batch_error:
            logger.error(f"[SEC] Error in batch fetch: {batch_error}, falling back to individual fetches")
            
            # Fall back to individual fetches
            for idx in range(n_years):
                year = datetime.datetime.now().year - idx
                key = get_source_key(company, 'SEC', idx)
                
                # Update status to processing
                redis_client.hset(get_job_redis_key(organization_id, job_id, "status"), key, "processing")
                log_job_status(job_id, "processing", organization_id, {
                    'company': ticker or cik,
                    'source': 'SEC',
                    'year': year,
                    'year_idx': idx
                })
                
                try:
                    logger.info(f"[SEC] Fetching {form_type} for {ticker or cik} for year {year} (org={organization_id})")
                    
                    # Fetch data for this year
                    result = sec.fetch({
                        'ticker': ticker,
                        'cik': cik,
                        'form_type': form_type,
                        'year': year
                    })
                    
                    # Normalize response
                    envelope = normalize_envelope(result)
                    
                    # Publish to message bus if enabled
                    if PUBLISH_ENABLED:
                        try:
                            publisher.publish(SEC_TOPIC, envelope)
                            logger.info(f"[SEC] Published {ticker or cik} filing for year {year} to {SEC_TOPIC}")
                        except Exception as pub_exc:
                            logger.warning(f"[SEC] Failed to publish envelope to message bus: {pub_exc} (org={organization_id}, job={job_id})")
                    
                    # Store result and status in Redis using organization-prefixed keys
                    redis_client.hset(get_job_redis_key(organization_id, job_id, "result"), key, json.dumps(envelope))
                    redis_client.hset(get_job_redis_key(organization_id, job_id, "status"), key, envelope.get("status", "error"))
                    
                    logger.info(f"[SEC] Successfully processed {ticker or cik} for year {year} (org={organization_id})")
                except Exception as e:
                    logger.error(f"[SEC] Error processing {ticker or cik} for year {year}: {e}")
                    # Store error in Redis
                    error_envelope = {
                        'error': str(e),
                        'status': 'error',
                        'source': 'sec',
                        'content': None,
                        'content_type': None,
                        'metadata': {
                            'ticker': ticker,
                            'cik': cik,
                            'year': year,
                            'form_type': form_type
                        }
                    }
                    redis_client.hset(get_job_redis_key(organization_id, job_id, "result"), key, json.dumps(error_envelope))
                    redis_client.hset(get_job_redis_key(organization_id, job_id, "status"), key, "error")
                    
                    if PROMETHEUS_ENABLED:
                        WORKER_JOB_FAILURES.labels(source='SEC').inc()
        
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
        
        logger.info(f"[SEC] Completed batch job {job_id} for {company} (org={organization_id}, user={user_id})")
        json_logger.log_json(
            level="info",
            action="sec_job_complete",
            job_id=job_id,
            company=company,
            organization_id=organization_id,
            user_id=user_id,
            duration=time.time() - start_time
        )
    except Exception as e:
        logger.error(f"[SEC] Error processing batch job {job_id} for {company} (org={organization_id}, user={user_id}): {e}", exc_info=True)
        for idx in range(n_years):
            key = f"{ticker or cik or company.get('name')}:SEC:{idx}"
            redis_client.hset(get_job_redis_key(organization_id, job_id, "status"), key, "error")
            redis_client.hset(get_job_redis_key(organization_id, job_id, "result"), key, json.dumps({"error": str(e)}))
        redis_client.set(get_job_redis_key(organization_id, job_id, "overall_status"), "error")
        
        json_logger.log_json(
            level="error",
            action="sec_job_error",
            job_id=job_id,
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


async def process_yahoo_job(job_id: str, company: Dict[str, Any], organization_id: str, user_id: str = None):
    """
    Process a Yahoo Finance data acquisition job for a single company.
    
    Args:
        job_id: Unique job identifier
        company: Company information (ticker)
        organization_id: Organization identifier
        user_id: User identifier
    """
    start_time = time.time()
    if PROMETHEUS_ENABLED:
        WORKER_CURRENT_JOBS.inc()
        WORKER_JOBS_TOTAL.labels(source='Yahoo').inc()
    
    logger.info(f"[Yahoo] Processing job {job_id} for {company} (org={organization_id}, user={user_id})")
    json_logger.log_json(
        level="info",
        action="yahoo_job_start",
        job_id=job_id,
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
        log_job_status(job_id, "processing", organization_id, {
            'company': ticker,
            'source': 'Yahoo'
        })
        
        # Fetch data
        result = yahoo.fetch({'ticker': ticker})
        
        # Normalize response
        envelope = normalize_envelope(result)
        
        # Publish to message bus if enabled
        if PUBLISH_ENABLED:
            try:
                publisher.publish(YAHOO_TOPIC, envelope)
                logger.info(f"[Yahoo] Published {ticker} data to {YAHOO_TOPIC}")
            except Exception as pub_exc:
                logger.warning(f"[Yahoo] Failed to publish envelope to message bus: {pub_exc} (org={organization_id}, job={job_id})")
        
        # Store result and status in Redis using organization-prefixed keys
        redis_client.hset(get_job_redis_key(organization_id, job_id, "result"), key, json.dumps(envelope))
        redis_client.hset(get_job_redis_key(organization_id, job_id, "status"), key, envelope.get("status", "error"))
        
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
        
        logger.info(f"[Yahoo] Completed job {job_id} for {ticker} (org={organization_id}, user={user_id})")
        json_logger.log_json(
            level="info",
            action="yahoo_job_complete",
            job_id=job_id,
            company=company,
            organization_id=organization_id,
            user_id=user_id,
            duration=time.time() - start_time
        )
    except Exception as e:
        logger.error(f"[Yahoo] Error processing job {job_id} for {company} (org={organization_id}, user={user_id}): {e}")
        key = f"{ticker or company.get('name')}:Yahoo"
        redis_client.hset(get_job_redis_key(organization_id, job_id, "status"), key, "error")
        redis_client.hset(get_job_redis_key(organization_id, job_id, "result"), key, json.dumps({"error": str(e)}))
        redis_client.set(get_job_redis_key(organization_id, job_id, "overall_status"), "error")
        
        json_logger.log_json(
            level="error",
            action="yahoo_job_error",
            job_id=job_id,
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
                        publisher.publish(f"data.xbrl.{concept.replace(':', '_')}", envelope)
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
    """
    logger.info(f"Async worker started with concurrency limit {WORKER_CONCURRENCY}")
    semaphore = asyncio.Semaphore(WORKER_CONCURRENCY)
    
    # Check Redis connection
    try:
        redis_client.ping()
        logger.info("Successfully connected to Redis")
    except Exception as e:
        logger.critical(f"Failed to connect to Redis: {e}")
        return
    
    # Start Prometheus metrics server if enabled
    if PROMETHEUS_ENABLED:
        try:
            metrics_port = int(os.getenv("METRICS_PORT", "8000"))
            start_http_server(metrics_port)
            logger.info(f"Started Prometheus metrics server on port {metrics_port}")
        except Exception as e:
            logger.error(f"Failed to start Prometheus metrics server: {e}")
    
    while True:
        try:
            # Poll for jobs
            job_data = redis_client.blpop("data_jobs", timeout=5)
            logger.debug(f"Polled Redis: job_data={job_data}")
            
            if not job_data:
                await asyncio.sleep(1)
                continue
            
            # Parse job data
            _, job_str = job_data
            job = json.loads(job_str)
            job_id = job.get("job_id")
            companies = job.get("companies", [])
            n_years = job.get("n_years", 1)
            form_type = job.get("form_type", "10-K")
            organization_id = job.get("organization_id")
            user_id = job.get("user_id")
            concepts = job.get("concepts", [])
            
            logger.info(f"Processing job {job_id} with {len(companies)} companies (org={organization_id}, user={user_id})")
            
            # Schedule all jobs for this batch
            tasks = []
            for company in companies:
                # Check for source flags
                process_sec = company.get('sec', True)  # Default to True if not specified
                process_yahoo = company.get('yahoo', True)  # Default to True if not specified
                process_xbrl = company.get('xbrl', False)  # Default to False if not specified
                
                logger.debug(f"Company {company.get('ticker') or company.get('name')} flags: sec={process_sec}, yahoo={process_yahoo}, xbrl={process_xbrl}")
                
                # Add SEC job if needed
                if process_sec:
                    tasks.append(process_sec_job(job_id, company, n_years, form_type, organization_id, user_id))
                
                # Add Yahoo job if needed
                if process_yahoo:
                    tasks.append(process_yahoo_job(job_id, company, organization_id, user_id))
                
                # Add XBRL job if needed
                if process_xbrl and concepts:
                    tasks.append(process_xbrl_job(job_id, company, concepts, organization_id, user_id))
            
            # Run all jobs concurrently, respecting the semaphore
            async with semaphore:
                await asyncio.gather(*tasks)
                
            logger.info(f"Completed job {job_id} (org={organization_id}, user={user_id})")
            
        except Exception as e:
            logger.error(f"Error processing job: {e}", exc_info=True)
            # Log detailed traceback for debugging
            traceback_str = traceback.format_exc()
            logger.error(f"Traceback: {traceback_str}")
            await asyncio.sleep(1)


if __name__ == "__main__":
    # Run the async worker
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Worker stopped by user")
    except Exception as e:
        logger.critical(f"Worker crashed: {e}", exc_info=True)