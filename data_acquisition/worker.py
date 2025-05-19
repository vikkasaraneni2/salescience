"""
Async/Concurrent Worker for Data Acquisition
-------------------------------------------
This script runs an async worker that processes data acquisition jobs concurrently across all sources and companies.

Features:
- Uses asyncio to process multiple jobs in parallel (configurable concurrency limit)
- Handles SEC multi-year fetches and Yahoo fetches as coroutines
- Updates Redis with status/results for each job
- Publishes SEC envelopes to a Redis stream (message bus)
- Production-ready: logging, error handling, env config

How to run:
1. Ensure Redis is running and accessible (see README for Kubernetes setup)
2. Install dependencies:
    pip install redis python-dotenv httpx yfinance
3. Start the worker:
    python data_acquisition/worker.py

Environment variables:
- REDIS_URL (e.g., redis://redis:6379/0)
- WORKER_CONCURRENCY (default: 10)

"""
import os
import logging
import asyncio
from typing import Dict, Any
import redis
from dotenv import load_dotenv
import json

# Import real data source classes
from data_acquisition.sec_client import SECDataSource, MessageBusPublisher
from data_acquisition.yahoo import YahooDataSource

# Load environment variables from .env if present

load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("async_worker")

# Redis connection
# Load from environment variables
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
redis_client = redis.Redis.from_url(REDIS_URL, decode_responses=True)

# Concurrency limit
WORKER_CONCURRENCY = int(os.getenv("WORKER_CONCURRENCY", 10))

SEC_TOPIC = "raw_sec_filings"

def get_job_redis_key(organization_id: str, job_id: str, suffix: str) -> str:
    """
    Helper to build org-prefixed Redis keys for job metadata, status, and results.
    """
    return f"org:{organization_id}:job:{job_id}:{suffix}"

def process_sec_batch_job(job_id: str, company: Dict[str, Any], n_years: int, form_type: str, organization_id: str = None, user_id: str = None):
    """
    Fetches the last n_years of filings for a company from the SEC, publishes each envelope to the message bus,
    and updates Redis with status/results for each year. Handles all status cases.
    """
    try:
        logger.info(f"[SEC] Processing batch job {job_id} for {company} (n_years={n_years}, form_type={form_type}, org={organization_id}, user={user_id})")
        sec = SECDataSource()
        publisher = MessageBusPublisher()
        params = {k: v for k, v in company.items() if k in ("cik", "ticker")}
        envelopes = sec.fetch_last_n_years(params, n_years=n_years, form_type=form_type)
        logger.info(f"[SEC] envelopes type: {type(envelopes)}, value: {envelopes}")
        if not isinstance(envelopes, list):
            envelopes = [envelopes]
        for idx, envelope in enumerate(envelopes):
            key = f"{company.get('ticker') or company.get('cik') or company.get('name')}:SEC:{idx}"
            # Publish to message bus
            try:
                publisher.publish(SEC_TOPIC, envelope)
            except Exception as pub_exc:
                logger.warning(f"[SEC] Failed to publish envelope to message bus: {pub_exc} (org={organization_id}, job={job_id})")
            # Store result and status in Redis using organization-prefixed keys
            redis_client.hset(get_job_redis_key(organization_id, job_id, "result"), key, str(envelope))
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
        logger.info(f"[SEC] Completed batch job {job_id} for {company} (org={organization_id}, user={user_id})")
    except Exception as e:
        logger.error(f"[SEC] Error processing batch job {job_id} for {company} (org={organization_id}, user={user_id}): {e}", exc_info=True)
        for idx in range(n_years):
            key = f"{company.get('ticker') or company.get('cik') or company.get('name')}:SEC:{idx}"
            redis_client.hset(get_job_redis_key(organization_id, job_id, "status"), key, "error")
            redis_client.hset(get_job_redis_key(organization_id, job_id, "result"), key, str({"error": str(e)}))
        redis_client.set(get_job_redis_key(organization_id, job_id, "overall_status"), "error")

def process_company_source(job_id: str, company: Dict[str, Any], source: str, job_key: str, organization_id: str = None, user_id: str = None):
    """
    Process a single company/source data acquisition job.
    Fetches real data from Yahoo and stores the result in Redis.
    """
    try:
        logger.info(f"Processing job {job_id} for {job_key} (org={organization_id}, user={user_id})")
        redis_client.hset(get_job_redis_key(organization_id, job_id, "status"), job_key, "processing")
        # Fetch real data from the appropriate source
        if source == "Yahoo":
            yahoo = YahooDataSource()
            params = {k: v for k, v in company.items() if k == "ticker"}
            envelope = yahoo.fetch(params)
        else:
            raise ValueError(f"Unknown source: {source}")
        # Store result and set status to 'success'
        redis_client.hset(get_job_redis_key(organization_id, job_id, "result"), job_key, str(envelope))
        redis_client.hset(get_job_redis_key(organization_id, job_id, "status"), job_key, "success")
        # Update overall status
        all_status = redis_client.hvals(get_job_redis_key(organization_id, job_id, "status"))
        if all(s == "success" for s in all_status):
            redis_client.set(get_job_redis_key(organization_id, job_id, "overall_status"), "complete")
        else:
            redis_client.set(get_job_redis_key(organization_id, job_id, "overall_status"), "processing")
        logger.info(f"Completed job {job_id} for {job_key} (org={organization_id}, user={user_id})")
    except Exception as e:
        logger.error(f"Error processing job {job_id} for {job_key} (org={organization_id}, user={user_id}): {e}")
        redis_client.hset(get_job_redis_key(organization_id, job_id, "status"), job_key, "error")
        redis_client.hset(get_job_redis_key(organization_id, job_id, "result"), job_key, str({"error": str(e)}))
        redis_client.set(get_job_redis_key(organization_id, job_id, "overall_status"), "error")

async def process_sec_job(job_id: str, company: Dict[str, Any], n_years: int, form_type: str, organization_id: str):
    """
    Async coroutine to process a SEC multi-year fetch job for a single company.
    Updates Redis and publishes to the message bus.
    """
    try:
        logger.info(f"[SEC] Processing job {job_id} for {company} (n_years={n_years}, form_type={form_type}, org={organization_id})")
        sec = SECDataSource()
        publisher = MessageBusPublisher()
        params = {k: v for k, v in company.items() if k in ("cik", "ticker")}
        envelopes = sec.fetch_last_n_years(params, n_years=n_years, form_type=form_type)
        logger.info(f"[SEC] envelopes type: {type(envelopes)}, value: {envelopes}")
        if not isinstance(envelopes, list):
            envelopes = [envelopes]
        for idx, envelope in enumerate(envelopes):
            key = f"{company.get('ticker') or company.get('cik') or company.get('name')}:SEC:{idx}"
            try:
                publisher.publish(SEC_TOPIC, envelope)
            except Exception as pub_exc:
                logger.warning(f"[SEC] Failed to publish envelope to message bus: {pub_exc}")
            # Store result and status in org-prefixed Redis keys
            redis_client.hset(get_job_redis_key(organization_id, job_id, "result"), key, str(envelope))
            redis_client.hset(get_job_redis_key(organization_id, job_id, "status"), key, envelope.get("status", "error"))
        all_status = redis_client.hvals(get_job_redis_key(organization_id, job_id, "status"))
        if all(s == "success" for s in all_status):
            redis_client.set(get_job_redis_key(organization_id, job_id, "overall_status"), "complete")
        elif any(s == "processing" for s in all_status):
            redis_client.set(get_job_redis_key(organization_id, job_id, "overall_status"), "processing")
        elif any(s == "error" for s in all_status):
            redis_client.set(get_job_redis_key(organization_id, job_id, "overall_status"), "error")
        else:
            redis_client.set(get_job_redis_key(organization_id, job_id, "overall_status"), "not_found")
        logger.info(f"[SEC] Completed job {job_id} for {company} (org={organization_id})")
    except Exception as e:
        logger.error(f"[SEC] Error processing job {job_id} for {company} (org={organization_id}): {e}", exc_info=True)
        for idx in range(n_years):
            key = f"{company.get('ticker') or company.get('cik') or company.get('name')}:SEC:{idx}"
            redis_client.hset(get_job_redis_key(organization_id, job_id, "status"), key, "error")
            redis_client.hset(get_job_redis_key(organization_id, job_id, "result"), key, str({"error": str(e)}))
        redis_client.set(get_job_redis_key(organization_id, job_id, "overall_status"), "error")

async def process_yahoo_job(job_id: str, company: Dict[str, Any], organization_id: str):
    """
    Async coroutine to process a Yahoo fetch job for a single company.
    Updates Redis with result using org-prefixed keys.
    """
    try:
        key = f"{company.get('ticker') or company.get('cik') or company.get('name')}:Yahoo"
        logger.info(f"[Yahoo] Processing job {job_id} for {key} (org={organization_id})")
        yahoo = YahooDataSource()
        params = {k: v for k, v in company.items() if k == "ticker"}
        envelope = yahoo.fetch(params)
        redis_client.hset(get_job_redis_key(organization_id, job_id, "result"), key, str(envelope))
        redis_client.hset(get_job_redis_key(organization_id, job_id, "status"), key, "success")
        all_status = redis_client.hvals(get_job_redis_key(organization_id, job_id, "status"))
        if all(s == "success" for s in all_status):
            redis_client.set(get_job_redis_key(organization_id, job_id, "overall_status"), "complete")
        else:
            redis_client.set(get_job_redis_key(organization_id, job_id, "overall_status"), "processing")
        logger.info(f"[Yahoo] Completed job {job_id} for {key} (org={organization_id})")
    except Exception as e:
        logger.error(f"[Yahoo] Error processing job {job_id} for {key} (org={organization_id}): {e}")
        redis_client.hset(get_job_redis_key(organization_id, job_id, "status"), key, "error")
        redis_client.hset(get_job_redis_key(organization_id, job_id, "result"), key, str({"error": str(e)}))
        redis_client.set(get_job_redis_key(organization_id, job_id, "overall_status"), "error")

async def worker_main():
    """
    Main async worker loop. Pulls jobs from a Redis list/queue and processes them concurrently.
    Uses org-prefixed Redis keys for all job data.
    """
    logger.info(f"Async worker started with concurrency limit {WORKER_CONCURRENCY}")
    semaphore = asyncio.Semaphore(WORKER_CONCURRENCY)
    while True:
        job_data = redis_client.blpop("data_jobs", timeout=5)
        logger.info(f"Polled Redis: job_data={job_data}")
        if not job_data:
            await asyncio.sleep(1)
            continue
        _, job_json = job_data
        logger.info(f"Dequeued job: {job_json}")
        job = json.loads(job_json)
        job_id = job["job_id"]
        companies = job["companies"]
        n_years = job.get("n_years", 1)
        form_type = job.get("form_type", "10-K")
        organization_id = job["organization_id"]  # Extract org context from job payload
        user_id = job.get("user_id")
        
        # Schedule all jobs for this batch
        tasks = []
        for company in companies:
            # Process SEC data if the sec flag is True or if ticker/cik is present (fallback for backward compatibility)
            sec_enabled = company.get('sec', False) or (company.get('ticker') is not None or company.get('cik') is not None)
            if sec_enabled:
                logger.info(f"[SEC] Scheduling SEC data fetch for {company} (job={job_id})")
                tasks.append(process_sec_job(job_id, company, n_years, form_type, organization_id))
            else:
                logger.info(f"[SEC] Skipping SEC data fetch for {company} (sec flag not set, job={job_id})")
                
            # Process Yahoo data if the yahoo flag is True or if ticker is present (fallback for backward compatibility)
            yahoo_enabled = company.get('yahoo', False) or company.get('ticker') is not None
            if yahoo_enabled:
                logger.info(f"[Yahoo] Scheduling Yahoo data fetch for {company} (job={job_id})")
                tasks.append(process_yahoo_job(job_id, company, organization_id))
            else:
                logger.info(f"[Yahoo] Skipping Yahoo data fetch for {company} (yahoo flag not set, job={job_id})")
                
            # If neither sec nor yahoo is enabled but we have a company name, default to SEC (backward compatibility)
            if not sec_enabled and not yahoo_enabled and company.get('name') is not None:
                logger.info(f"[Default] Using SEC as default source for {company} (job={job_id})")
                tasks.append(process_sec_job(job_id, company, n_years, form_type, organization_id))
                
        # Run all jobs concurrently, respecting the semaphore
        if tasks:
            async with semaphore:
                await asyncio.gather(*tasks)
        else:
            logger.warning(f"No tasks scheduled for job {job_id} - check company flags and data")
            # Update job status to error if no tasks were scheduled
            redis_client.set(get_job_redis_key(organization_id, job_id, "overall_status"), "error")
            for company in companies:
                key = f"{company.get('ticker') or company.get('cik') or company.get('name')}:Error"
                redis_client.hset(get_job_redis_key(organization_id, job_id, "status"), key, "error")
                redis_client.hset(get_job_redis_key(organization_id, job_id, "result"), key, 
                                 str({"error": "No valid data sources specified - set 'sec' or 'yahoo' to true"}))

if __name__ == "__main__":
    asyncio.run(worker_main()) 