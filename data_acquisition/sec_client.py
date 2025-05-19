# data_acquisition/sec_client.py
# -------------------------------------------------------------
# Abstract Base Class for Data Acquisition Sources (Placed here per project constraints)
# -------------------------------------------------------------
# This class defines the interface (contract) that all data source classes
# in the data acquisition layer must implement. By using Python's abc module,
# we ensure that every data source provides a consistent fetch method, making
# it easy to add, test, or swap sources in the future.
#
# Why use an abstract base class (ABC)?
# - Enforces a contract: All sources must implement the same method signature.
# - Guarantees consistency: Downstream code can always expect the same output structure.
# - Extensible: New sources can be added without changing the rest of the system.
# - Testable: Each source can be tested in isolation.

from abc import ABC, abstractmethod
from typing import Dict, Any
import os
from dotenv import load_dotenv
load_dotenv()

# Now you can safely access environment variables
REDIS_URL = os.environ.get('REDIS_URL', 'redis://localhost:6379/0')
SEC_API_KEY = os.environ.get("SEC_API_KEY")

# This BaseDataSource is now imported from base.py
# class BaseDataSource(ABC):
#     @abstractmethod
#     def fetch(self, params: Dict[str, Any]) -> Dict[str, Any]:
#         """
#         Fetch raw data and metadata from the source.
#         """
#         pass

# -----------------------------------------------------------------------------
# Chunk 1: Logging and Error Reporting
# -----------------------------------------------------------------------------
# We use Python's built-in logging module for structured, configurable logging.
# This is essential for production systems: logs can be sent to files, stdout,
# or centralized logging systems (e.g., ELK, Datadog, CloudWatch).
#
# Usage:
#   - Use logger.info() for normal operations.
#   - Use logger.warning() for recoverable issues.
#   - Use logger.error() for errors/exceptions.
#   - Use logger.debug() for verbose, development-only logs.

import logging

# Configure the logger (can be further configured in main/entrypoint)
logger = logging.getLogger("sec_data_acquisition")
logger.setLevel(logging.INFO)  # Change to DEBUG for more verbosity
handler = logging.StreamHandler()
formatter = logging.Formatter('[%(asctime)s] %(levelname)s %(name)s: %(message)s')
handler.setFormatter(formatter)
if not logger.hasHandlers():
    logger.addHandler(handler)

# -----------------------------------------------------------------------------
# SEC EDGAR Data Source Implementation
# -----------------------------------------------------------------------------
# This module implements a data source for SEC EDGAR filings by inheriting from
# the BaseDataSource interface. It fetches filings (e.g., 10-K, 10-Q) for a given
# company using the SEC's public API.
#
# Why this design?
# - Encapsulates all SEC-specific logic in one place.
# - Makes it easy to update, test, or remove SEC as a source independently.
# - Follows the architectural contract for data acquisition sources.
#
# Requirements:
#   pip install httpx

# Use the BaseDataSource from base.py
from data_acquisition.base import BaseDataSource
import httpx
from typing import Dict, Any
import datetime
import asyncio
import redis
import json

class MessageBusPublisher:
    """
    Message bus publisher using Redis Streams.
    This class provides a simple interface to publish data acquisition envelopes
    to a Redis stream (topic) for downstream processing, storage, or analytics.
    """
    def __init__(self, redis_url=REDIS_URL):
        # Initialize the Redis client
        self.client = redis.Redis.from_url(redis_url)
    
    def publish(self, topic: str, envelope: dict):
        """
        Publish the envelope as a message to the given topic (Redis stream).
        Args:
            topic (str): The name of the Redis stream (e.g., 'raw_sec_filings').
            envelope (dict): The standardized data envelope to publish.
        """
        try:
            # Serialize the envelope as JSON and publish to the stream
            self.client.xadd(topic, {'data': json.dumps(envelope)})
            logger.info(f"Published envelope to topic '{topic}'")
        except Exception as e:
            # Log or handle publishing errors as needed
            logger.error(f"Error publishing to message bus: {e}")

class SECDataSource(BaseDataSource):
    def fetch(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Fetches a filing from sec-api.io and retrieves the actual filing document (HTML, TXT, or PDF).
        """
        cik = params.get('cik')
        ticker = params.get('ticker')
        form_type = params.get('form_type', '10-K')
        year = params.get('year')
        import httpx
        if not SEC_API_KEY:
            logger.error("SEC_API_KEY is not set in the environment.")
            raise ValueError("SEC_API_KEY is not set in the environment.")
        # Robust ticker-to-CIK resolution
        if not cik:
            if ticker:
                cik = get_cik_for_ticker(ticker)
            else:
                raise ValueError("Parameter 'cik' or 'ticker' is required for SECDataSource.fetch")
        # Fetch filings metadata from sec-api.io
        filings_url = f"https://api.sec-api.io/filings?cik={cik}&formType={form_type}&token={SEC_API_KEY}"
        try:
            resp = httpx.get(filings_url)
            if resp.status_code != 200:
                logger.error(f"sec-api.io filings endpoint returned {resp.status_code}: {resp.text}")
                raise ValueError(f"sec-api.io filings error: {resp.status_code}")
            filings_data = resp.json()
            filings = filings_data.get('filings', [])
            # Optionally filter by year if provided
            if year:
                filings = [f for f in filings if f.get('filedAt', '').startswith(str(year))]
            if not filings:
                raise ValueError(f"No {form_type} filings found for CIK {cik} (year={year}) via sec-api.io.")
            filing_info = filings[0]  # Get the latest (or first matching) filing
            # Get the document URL (HTML or TXT)
            doc_url = None
            for doc in filing_info.get('documents', []):
                if doc.get('type', '').lower() == 'complete submission text file':
                    doc_url = doc.get('url')
                    content_type = 'txt'
                    break
                elif doc.get('type', '').lower() == '10-k' and doc.get('url', '').endswith('.htm'):
                    doc_url = doc.get('url')
                    content_type = 'html'
            if not doc_url:
                # Fallback: use the first document
                if filing_info.get('documents'):
                    doc_url = filing_info['documents'][0].get('url')
                    content_type = 'txt' if doc_url and doc_url.endswith('.txt') else 'html'
            if not doc_url:
                raise ValueError(f"No document URL found for filing {filing_info}")
            # Fetch the actual filing document
            doc_resp = httpx.get(doc_url)
            if doc_resp.status_code != 200:
                raise ValueError(f"Could not fetch filing document from {doc_url}")
            document_content = doc_resp.text
            return {
                'content': document_content,
                'content_type': content_type,
                'source': 'sec',
                'metadata': {
                    'cik': cik,
                    'ticker': ticker,
                    'form_type': form_type,
                    'year': year,
                    'retrieved_at': datetime.datetime.utcnow().isoformat() + 'Z',
                    'filing_info': filing_info,
                    'filing_url': doc_url
                }
            }
        except Exception as e:
            logger.error(f"Error fetching filing from sec-api.io: {e}")
            raise ValueError(f"Error fetching filing from sec-api.io: {e}")

    def fetch_last_n_years(self, params: Dict[str, Any], n_years: int = 5, form_type: str = '10-K') -> list:
        """
        Fetches the last N years of filings for a company (by ticker or CIK) using sec-api.io.
        """
        cik = params.get('cik')
        ticker = params.get('ticker')
        import httpx
        if not SEC_API_KEY:
            logger.error("SEC_API_KEY is not set in the environment.")
            raise ValueError("SEC_API_KEY is not set in the environment.")
        if not cik:
            if ticker:
                cik = get_cik_for_ticker(ticker)
            else:
                raise ValueError("Parameter 'cik' or 'ticker' is required for SECDataSource.fetch_last_n_years")
        filings_url = f"https://api.sec-api.io/filings?cik={cik}&formType={form_type}&token={SEC_API_KEY}"
        try:
            resp = httpx.get(filings_url)
            if resp.status_code != 200:
                logger.error(f"sec-api.io filings endpoint returned {resp.status_code}: {resp.text}")
                raise ValueError(f"sec-api.io filings error: {resp.status_code}")
            filings_data = resp.json()
            filings = filings_data.get('filings', [])
            results = []
            seen_years = set()
            for filing in filings:
                try:
                    filed_at = filing.get('filedAt', '')
                    year = filed_at[:4]
                    if year not in seen_years:
                        doc_url = None
                        content_type = None
                        for doc in filing.get('documents', []):
                            if doc.get('type', '').lower() == 'complete submission text file':
                                doc_url = doc.get('url')
                                content_type = 'txt'
                                break
                            elif doc.get('type', '').lower() == '10-k' and doc.get('url', '').endswith('.htm'):
                                doc_url = doc.get('url')
                                content_type = 'html'
                        if not doc_url and filing.get('documents'):
                            doc_url = filing['documents'][0].get('url')
                            content_type = 'txt' if doc_url and doc_url.endswith('.txt') else 'html'
                        if doc_url:
                            doc_resp = httpx.get(doc_url)
                            if doc_resp.status_code == 200:
                                document_content = doc_resp.text
                                envelope = {
                                    'content': document_content,
                                    'content_type': content_type,
                                    'source': 'sec',
                                    'status': 'success',
                                    'metadata': {
                                        'cik': cik,
                                        'ticker': ticker,
                                        'form_type': form_type,
                                        'year': year,
                                        'retrieved_at': datetime.datetime.utcnow().isoformat() + 'Z',
                                        'filing_info': filing,
                                        'filing_url': doc_url
                                    }
                                }
                            else:
                                envelope = {
                                    'content': None,
                                    'content_type': None,
                                    'source': 'sec',
                                    'status': 'not_found',
                                    'metadata': {
                                        'cik': cik,
                                        'ticker': ticker,
                                        'form_type': form_type,
                                        'year': year,
                                        'retrieved_at': datetime.datetime.utcnow().isoformat() + 'Z',
                                        'filing_info': filing,
                                        'filing_url': doc_url
                                    }
                                }
                        else:
                            envelope = {
                                'content': None,
                                'content_type': None,
                                'source': 'sec',
                                'status': 'not_found',
                                'metadata': {
                                    'cik': cik,
                                    'ticker': ticker,
                                    'form_type': form_type,
                                    'year': year,
                                    'retrieved_at': datetime.datetime.utcnow().isoformat() + 'Z',
                                    'filing_info': filing
                                }
                            }
                        results.append(envelope)
                        seen_years.add(year)
                    if len(results) >= n_years:
                        break
                except Exception as filing_exc:
                    logger.warning(f"[SEC] Malformed filing data for {cik}: {filing_exc}")
                    envelope = {
                        'content': None,
                        'content_type': None,
                        'source': 'sec',
                        'status': 'error',
                        'metadata': {
                            'cik': cik,
                            'ticker': ticker,
                            'form_type': form_type,
                            'year': None,
                            'retrieved_at': datetime.datetime.utcnow().isoformat() + 'Z',
                            'error': str(filing_exc)
                        }
                    }
                    results.append(envelope)
            if not results:
                logger.warning(f"[SEC] No {form_type} filings found for the last {n_years} years for CIK {cik} via sec-api.io.")
            return results
        except Exception as e:
            logger.error(f"Error fetching filings from sec-api.io: {e}")
            raise ValueError(f"Error fetching filings from sec-api.io: {e}")

    def fetch_last_n_years_and_publish(self, params: Dict[str, Any], n_years: int, form_type: str, publisher: MessageBusPublisher, topic: str):
        """
        Fetches the last N years of filings and publishes each envelope to the message bus.
        Args:
            params (Dict[str, Any]): Parameters for fetching filings (ticker or cik, etc.)
            n_years (int): Number of years of filings to fetch
            form_type (str): SEC form type (e.g., '10-K')
            publisher (MessageBusPublisher): Instance of the message bus publisher
            topic (str): Name of the Redis stream/topic to publish to
        """
        try:
            logger.info(f"Starting fetch_last_n_years_and_publish for params={params}, n_years={n_years}, form_type={form_type}")
            envelopes = self.fetch_last_n_years(params, n_years, form_type)
            for envelope in envelopes:
                publisher.publish(topic, envelope)
            logger.info(f"Successfully published {len(envelopes)} envelopes for params={params}")
        except Exception as e:
            logger.error(f"Error in fetch_last_n_years_and_publish: {e}")

    def fetch_xbrl_fact(self, params: Dict[str, Any], concept: str) -> Dict[str, Any]:
        """
        Fetches a specific XBRL fact (e.g., us-gaap:Revenues) for a company using sec-api.io.
        Args:
            params (Dict[str, Any]): Must include 'cik' or 'ticker'.
            concept (str): XBRL concept/tag (e.g., 'us-gaap:Revenues').
        Returns:
            Dict[str, Any]: Envelope with XBRL fact data and metadata.
        Example usage:
            sec = SECDataSource()
            result = sec.fetch_xbrl_fact({'ticker': 'AAPL'}, 'us-gaap:Revenues')
        """
        cik = params.get('cik')
        ticker = params.get('ticker')
        import httpx
        if not SEC_API_KEY:
            logger.error("SEC_API_KEY is not set in the environment.")
            raise ValueError("SEC_API_KEY is not set in the environment.")
        if not cik:
            if ticker:
                cik = get_cik_for_ticker(ticker)
            else:
                raise ValueError("Parameter 'cik' or 'ticker' is required for fetch_xbrl_fact")
        xbrl_url = f"https://api.sec-api.io/xbrl/companyconcept?cik={cik}&concept={concept}&token={SEC_API_KEY}"
        try:
            resp = httpx.get(xbrl_url)
            if resp.status_code != 200:
                logger.error(f"sec-api.io XBRL endpoint returned {resp.status_code}: {resp.text}")
                raise ValueError(f"sec-api.io XBRL error: {resp.status_code}")
            data = resp.json()
            if not data or 'facts' not in data:
                raise ValueError(f"No XBRL facts found for CIK {cik}, concept {concept} via sec-api.io.")
            return {
                'content': data['facts'],
                'content_type': 'json',
                'source': 'sec-xbrl',
                'metadata': {
                    'cik': cik,
                    'ticker': ticker,
                    'concept': concept,
                    'retrieved_at': datetime.datetime.utcnow().isoformat() + 'Z',
                    'xbrl_url': xbrl_url
                }
            }
        except Exception as e:
            logger.error(f"Error fetching XBRL fact from sec-api.io: {e}")
            raise ValueError(f"Error fetching XBRL fact from sec-api.io: {e}")

# Example usage:
# sec_source = SECDataSource()
# result = sec_source.fetch({'cik': '0000320193'})  # Apple Inc. CIK
# print(result)

def get_cik_for_ticker(ticker: str) -> str:
    """
    Resolves a ticker symbol to a CIK using the sec-api.io mapping endpoint.
    Returns the CIK as a zero-padded string, or raises ValueError if not found.
    """
    import httpx
    if not SEC_API_KEY:
        logger.error("SEC_API_KEY is not set in the environment.")
        raise ValueError("SEC_API_KEY is not set in the environment.")
    
    # Fixed: Use the correct API endpoint format
    url = f"https://api.sec-api.io/mapping/ticker/{ticker}?token={SEC_API_KEY}"
    
    try:
        print(f"DEBUG: Fetching CIK for ticker '{ticker}' from API")
        resp = httpx.get(url)
        print(f"DEBUG: API Response status: {resp.status_code}")
        
        if resp.status_code != 200:
            logger.error(f"sec-api.io returned status {resp.status_code}: {resp.text}")
            raise ValueError(f"sec-api.io error: {resp.status_code}")
        
        data = resp.json()
        print(f"DEBUG: API Response: {data}")
        
        # The API returns a list of results
        if isinstance(data, list) and len(data) > 0:
            # Get CIK from first result
            cik = data[0].get("cik")
            if cik:
                # Zero-pad the CIK to 10 digits
                return str(cik).zfill(10)
        
        logger.error(f"Ticker '{ticker}' not found in sec-api.io mapping response: {data}")
        raise ValueError(f"Ticker '{ticker}' not found in sec-api.io mapping.")
    except Exception as e:
        logger.error(f"Error fetching CIK for ticker '{ticker}' from sec-api.io: {e}")
        raise ValueError(f"Error fetching CIK for ticker '{ticker}' from sec-api.io: {e}")

# -----------------------------------------------------------------------------
# Chunk 3: Prometheus Metrics for Observability
# -----------------------------------------------------------------------------
# We use the prometheus_client library to track key metrics for SEC batch jobs.
# Metrics include job counts, failures, and durations. These metrics can be scraped
# by Prometheus and visualized in Grafana or used for alerting.
#
# To use: Start the metrics server (see main guard below), then configure Prometheus
# to scrape the metrics endpoint (default: http://localhost:8001/metrics).
#
# You can extend these metrics to track per-source, per-status, or per-company stats.

from prometheus_client import Counter, Histogram, start_http_server

SEC_JOB_COUNT = Counter('sec_job_count', 'Total number of SEC batch jobs processed')
SEC_JOB_FAILURES = Counter('sec_job_failures', 'Total number of failed SEC batch jobs')
SEC_JOB_DURATION = Histogram('sec_job_duration_seconds', 'Duration of SEC batch jobs in seconds')

# -----------------------------------------------------------------------------
# Chunk 3: Job Queue Integration (Redis Queue/RQ)
# -----------------------------------------------------------------------------
# This section adds a worker loop that listens for jobs on a Redis queue (RQ).
# When a job arrives, it processes the job using the batch/async fetch and publish logic.
# Logging is included for job start, success, and failure.

from rq import Queue, Worker
import redis as redis_lib

# Define the job handler function

def process_sec_batch_job(batch_params, n_years, form_type, topic):
    """
    Job handler for SEC batch jobs. Fetches filings for all companies in batch_params
    and publishes each envelope to the message bus. Tracks Prometheus metrics for
    job count, failures, and duration.
    """
    SEC_JOB_COUNT.inc()
    sec_source = SECDataSource()
    publisher = MessageBusPublisher()
    import asyncio
    import time
    start_time = time.time()
    try:
        logger.info(f"[RQ Worker] Starting SEC batch job for {len(batch_params)} companies, n_years={n_years}, form_type={form_type}")
        asyncio.run(sec_source.fetch_last_n_years_and_publish(batch_params, n_years, form_type, publisher, topic))
        logger.info(f"[RQ Worker] Successfully processed SEC batch job for {len(batch_params)} companies.")
    except Exception as e:
        SEC_JOB_FAILURES.inc()
        logger.error(f"[RQ Worker] Error processing SEC batch job: {e}")
    finally:
        duration = time.time() - start_time
        SEC_JOB_DURATION.observe(duration)

# Worker entrypoint (to be run as a script or service)
def run_sec_batch_worker():
    """
    Starts an RQ worker that listens for SEC batch jobs on the 'sec_batch' queue.
    Jobs should be enqueued with process_sec_batch_job as the function.
    """
    redis_conn = redis_lib.Redis.from_url(REDIS_URL)
    q = Queue('sec_batch', connection=redis_conn)
    worker = Worker([q], connection=redis_conn)
    logger.info("[RQ Worker] Starting SEC batch worker, listening on 'sec_batch' queue...")
    worker.work()

# Example usage (enqueue a job from another script):
# from rq import Queue
# import redis
# redis_conn = redis.Redis.from_url(REDIS_URL)
# q = Queue('sec_batch', connection=redis_conn)
# q.enqueue(process_sec_batch_job, batch_params, 5, '10-K', 'raw_sec_filings')

# To run the worker:
# python -c "from data_acquisition.sec_client import run_sec_batch_worker; run_sec_batch_worker()"

# -----------------------------------------------------------------------------
# Chunk 4: Containerization - Entrypoint and Container-Ready Execution
# -----------------------------------------------------------------------------
# This section makes the file fully container-ready. It provides a main entrypoint
# so the worker can be run as the container's CMD. All configuration is via environment
# variables, and the worker can be started with a single command.
#
# To build and run the container (assuming a Dockerfile exists):
#   docker build -t sec-batch-worker .
#   docker run --env REDIS_URL=redis://redis:6379/0 --env SEC_API_KEY="your-api-key" sec-batch-worker
#
# The worker will start and listen for jobs on the 'sec_batch' queue.

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description="SEC Batch Worker Entrypoint")
    parser.add_argument('--queue', type=str, default='sec_batch', help='RQ queue name to listen on (default: sec_batch)')
    parser.add_argument('--metrics-port', type=int, default=8001, help='Port to serve Prometheus metrics on (default: 8001)')
    args = parser.parse_args()
    # Start Prometheus metrics server
    start_http_server(args.metrics_port)
    logger.info(f"Prometheus metrics server started on port {args.metrics_port}")
    # Optionally allow queue name override
    def run_worker_with_queue(queue_name):
        redis_conn = redis_lib.Redis.from_url(REDIS_URL)
        q = Queue(queue_name, connection=redis_conn)
        worker = Worker([q], connection=redis_conn)
        logger.info(f"[RQ Worker] Starting SEC batch worker, listening on '{queue_name}' queue...")
        worker.work()
    run_worker_with_queue(args.queue)
