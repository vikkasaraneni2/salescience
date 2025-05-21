"""
SEC Worker Module for Batch Processing
-------------------------------------

This module handles SEC batch job processing and worker functionality, providing
distributed processing capabilities for SEC data acquisition. It depends on the
core SEC components from data_sources.sec for actual data retrieval.

Key responsibilities:
- Batch processing of SEC filing requests
- Worker management for distributed job processing
- Integration with Redis Queue (RQ) for job distribution
- Prometheus metrics collection for monitoring
- CLI entry point for containerized deployments

This module implements the worker pattern, separating job orchestration from
the actual data source implementation, enabling scalable processing.
"""

import asyncio
import logging
import time
import uuid
from typing import Dict, Any, List, Optional

# Redis and job queue imports
import redis
from config import settings

# Import core SEC components from data_sources
from data_sources.sec.client import SECClient
from data_sources.sec.filings import SECFilingsHandler
from data_sources.sec.xbrl import XBRLHandler

# Import shared utilities and error handling
from data_acquisition.utils import configure_logging
from data_acquisition.errors import (
    SalescienceError, ErrorContext,
    SECError, log_error
)

# Get logger specific to this module
logger = configure_logging("sec_worker")

# Configuration from centralized settings
REDIS_URL = settings.redis.url

# Optional: Prometheus metrics for observability
try:
    from prometheus_client import Counter, Histogram, start_http_server
    # Define standard metrics for SEC job processing
    SEC_JOB_COUNT = Counter('sec_job_count', 'Total number of SEC batch jobs processed')
    SEC_JOB_FAILURES = Counter('sec_job_failures', 'Total number of failed SEC batch jobs')
    SEC_JOB_DURATION = Histogram('sec_job_duration_seconds', 'Duration of SEC batch jobs in seconds')
    PROMETHEUS_ENABLED = True
except ImportError:
    logger.warning("prometheus_client not installed, metrics will not be available")
    PROMETHEUS_ENABLED = False

# Optional: Redis Queue (RQ) for job processing
try:
    from rq import Queue, Worker
    RQ_ENABLED = True
except ImportError:
    logger.warning("rq not installed, job queue processing will not be available")
    RQ_ENABLED = False


class MessageBusPublisher:
    """
    Message bus publisher using Redis Streams for event-driven architecture.
    
    This class provides a simple interface to publish data acquisition envelopes
    to a Redis stream (topic) for downstream processing, storage, or analytics.
    It implements a lightweight publish-subscribe pattern to decouple data acquisition
    from processing and storage concerns.
    
    Redis Streams are used as the underlying transport mechanism, providing:
    - Durable message storage with configurable retention
    - Consumer groups for work distribution and parallel processing
    - Message acknowledgments for reliable delivery
    - Time-based message ordering
    - Efficient appends-only data structure optimized for high throughput
    
    Usage patterns:
    1. One-to-many broadcasting of acquisition results
    2. Work distribution across multiple processing nodes
    3. Event sourcing for system state reconstruction
    4. Audit trail of all acquired data
    """
    def __init__(self, redis_url=REDIS_URL):
        # Initialize the Redis client using centralized configuration
        # Default uses REDIS_URL from settings but allows override for testing
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
            import json
            self.client.xadd(topic, {'data': json.dumps(envelope)})
            logger.info(f"Published envelope to topic '{topic}'")
        except Exception as e:
            # Log or handle publishing errors as needed
            logger.error(f"Error publishing to message bus: {e}")


def process_sec_batch_job(batch_params, n_years, form_type, topic):
    """
    Job handler for SEC batch jobs. Fetches filings for all companies in batch_params
    and publishes each envelope to the message bus. Tracks Prometheus metrics for
    job count, failures, and duration.
    
    This function serves as the entry point for distributed batch processing of SEC filings
    for multiple companies. It's designed to be executed as an RQ (Redis Queue) worker task,
    allowing for scalable, parallel processing of data acquisition jobs.
    
    Key features:
    - Handles batches of companies (multiple tickers/CIKs) in a single job
    - Publishes results to a Redis stream for downstream processing
    - Includes instrumentation with Prometheus metrics if enabled
    - Provides detailed logging of job status and performance
    - Implements robust error handling at both the batch and individual company levels
    
    Args:
        batch_params: List of company parameter dictionaries, each with 'ticker' or 'cik'
        n_years: Number of years of filings to fetch per company
        form_type: SEC form type to fetch (e.g., '10-K', '10-Q')
        topic: Redis stream name to publish results to
    """
    if PROMETHEUS_ENABLED:
        SEC_JOB_COUNT.inc()
    
    # Initialize SEC filings handler and publisher
    sec_filings = SECFilingsHandler()
    publisher = MessageBusPublisher()
    start_time = time.time()
    
    try:
        logger.info(f"[RQ Worker] Starting SEC batch job for {len(batch_params)} companies, n_years={n_years}, form_type={form_type}")
        
        # Process each company
        for company_params in batch_params:
            try:
                # Use the SEC filings handler to fetch filings
                envelopes = sec_filings.fetch_last_n_years(company_params, n_years, form_type)
                for envelope in envelopes:
                    publisher.publish(topic, envelope)
                logger.info(f"[RQ Worker] Processed {len(envelopes)} filings for {company_params.get('ticker') or company_params.get('cik')}")
            except Exception as company_exc:
                logger.error(f"[RQ Worker] Error processing company {company_params}: {company_exc}")
                
        logger.info(f"[RQ Worker] Successfully processed SEC batch job for {len(batch_params)} companies")
    except Exception as e:
        if PROMETHEUS_ENABLED:
            SEC_JOB_FAILURES.inc()
        logger.error(f"[RQ Worker] Error processing SEC batch job: {e}")
    finally:
        duration = time.time() - start_time
        if PROMETHEUS_ENABLED:
            SEC_JOB_DURATION.observe(duration)
        logger.info(f"[RQ Worker] SEC batch job completed in {duration:.2f} seconds")


def run_sec_batch_worker(queue_name='sec_batch'):
    """
    Starts an RQ worker that listens for SEC batch jobs on the specified queue.
    Jobs should be enqueued with process_sec_batch_job as the function.
    
    This function initializes and runs a worker process that continuously monitors
    a Redis queue for SEC data acquisition jobs. It's designed to be used either
    as a long-running service in a containerized environment (e.g., Kubernetes pod)
    or as a standalone process for development and testing.
    
    Worker process flow:
    1. Connects to the Redis server specified by REDIS_URL
    2. Creates an RQ Worker instance listening on the specified queue
    3. Enters a loop to process jobs as they arrive in the queue
    4. For each job, executes the job function (e.g., process_sec_batch_job)
    5. Handles failures, retries, and monitoring
    
    Args:
        queue_name: Name of the Redis queue to listen on (default: 'sec_batch')
        
    Note:
        This function will block indefinitely while processing jobs. It's designed
        to be run in its own process or container. To stop the worker, send a SIGTERM
        signal, which RQ handles gracefully to finish current jobs before shutting down.
    """
    if not RQ_ENABLED:
        logger.error("RQ is not installed. Cannot run worker.")
        return
    
    try:
        redis_conn = redis.Redis.from_url(REDIS_URL)
        q = Queue(queue_name, connection=redis_conn)
        worker = Worker([q], connection=redis_conn)
        logger.info(f"[RQ Worker] Starting SEC batch worker, listening on '{queue_name}' queue...")
        worker.work()
    except Exception as e:
        logger.error(f"[RQ Worker] Error starting worker: {e}")


# Containerization support - CLI entry point for Docker/Kubernetes deployment
if __name__ == '__main__':
    import argparse
    
    # Set up command-line argument parsing for worker configuration
    # All default values come from the centralized configuration system
    parser = argparse.ArgumentParser(description="SEC Batch Worker Entrypoint")
    
    # Queue name from centralized configuration (settings.queue_name)
    parser.add_argument('--queue', type=str, default=settings.queue_name, 
                        help=f'RQ queue name to listen on (default: {settings.queue_name} from config)')
    
    # Metrics port from centralized configuration (settings.metrics_port)
    parser.add_argument('--metrics-port', type=int, default=settings.metrics_port, 
                        help=f'Port to serve Prometheus metrics on (default: {settings.metrics_port} from config)')
    
    # Log level from centralized configuration (settings.log_level)
    parser.add_argument('--log-level', type=str, default=settings.log_level, 
                        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                        help=f'Logging level (default: {settings.log_level} from config)')
    args = parser.parse_args()
    
    # Configure logging level from command line argument
    numeric_level = getattr(logging, args.log_level.upper(), None)
    if numeric_level:
        logging.getLogger().setLevel(numeric_level)
        logger.setLevel(numeric_level)
    
    # Start Prometheus metrics server if enabled for monitoring in containerized environments
    # Uses both library availability check (PROMETHEUS_ENABLED) and feature flag (settings.prometheus_enabled)
    if PROMETHEUS_ENABLED and settings.prometheus_enabled:
        try:
            # Start HTTP server for Prometheus metrics using port from centralized configuration
            # This enables monitoring systems to scrape operational metrics
            start_http_server(args.metrics_port)
            logger.info(f"Prometheus metrics server started on port {args.metrics_port} (from centralized configuration)")
        except Exception as e:
            logger.error(f"Failed to start Prometheus metrics server: {e}")
            logger.error("Check settings.metrics_port in centralized configuration")
    
    # Start worker process - this will block and process jobs from the queue
    if RQ_ENABLED:
        logger.info(f"Starting SEC data acquisition worker, listening on queue '{args.queue}'")
        run_sec_batch_worker(args.queue)
    else:
        logger.error("RQ is not installed. Cannot run worker. Please install 'rq' package.")
        # Exit with error code to signal failure in containerized environments
        import sys
        sys.exit(1)