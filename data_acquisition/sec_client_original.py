import httpx            # HTTP client library (modern alternative to requests)
import logging          # Standard logging system
import datetime         # Date and time utilities
import asyncio          # Asynchronous I/O, event loop, and concurrency tools
import redis            # Redis client for message bus and job queue implementation
import json             # JSON parsing and serialization
import time             # Time access and conversions
import random           # For jitter in backoff strategies
import uuid             # For generating request IDs
from typing import Dict, Any, List, Optional, Tuple  # Type annotations
from abc import ABC, abstractmethod           # Abstract base classes
from config import settings  # Centralized configuration
from data_acquisition.errors import (
    SalescienceError, ErrorContext,
    SECError, SECAuthenticationError, SECRateLimitError, SECNotFoundError, SECParsingError,
    MissingConfigurationError, DataSourceConnectionError, DataSourceTimeoutError,
    ValidationError, DataSourceNotFoundError,
    log_error, format_error_response
)

# Get centralized logging configuration
from data_acquisition.utils import configure_logging

# Get logger specific to this module
logger = configure_logging("sec_client")

# Core configuration constants from centralized settings
# These constants maintain consistent naming throughout the codebase
# while sourcing their values from the centralized configuration system
SEC_API_KEY = settings.api_keys.sec
REDIS_URL = settings.redis.url

# SEC API endpoint configuration from centralized settings
# Using standardized URLs ensures consistent communication with external services
SEC_API_BASE_URL = settings.service_urls.sec_api_base
SEC_EDGAR_BASE_URL = settings.service_urls.sec_edgar_base

# Optional: Prometheus metrics for observability
# Controlled by settings.prometheus_enabled in the centralized configuration
try:
    from prometheus_client import Counter, Histogram, start_http_server
    # Define standard metrics for SEC job processing
    SEC_JOB_COUNT = Counter('sec_job_count', 'Total number of SEC batch jobs processed')
    SEC_JOB_FAILURES = Counter('sec_job_failures', 'Total number of failed SEC batch jobs')
    SEC_JOB_DURATION = Histogram('sec_job_duration_seconds', 'Duration of SEC batch jobs in seconds')
    # Local flag to track successful import - will be combined with settings.prometheus_enabled
    PROMETHEUS_ENABLED = True
except ImportError:
    logger.warning("prometheus_client not installed, metrics will not be available")
    PROMETHEUS_ENABLED = False
    # Even if prometheus is enabled in settings, we can't use it without the library

# Optional: Redis Queue (RQ) for job processing 
# Uses REDIS_URL from centralized settings when enabled
try:
    from rq import Queue, Worker
    # Feature flag to indicate RQ functionality is available
    RQ_ENABLED = True
except ImportError:
    logger.warning("rq not installed, job queue processing will not be available")
    RQ_ENABLED = False
    # Distributed job processing won't be available without RQ


class BaseDataSource(ABC):
    """Abstract base class for all data sources."""
    
    @abstractmethod
    def fetch(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Fetch raw data and metadata from the source.

        Args:
            params (Dict[str, Any]):
                A dictionary of parameters required to fetch the data (e.g., ticker, date, API keys).

        Returns:
            Dict[str, Any]:
                A dictionary containing:
                    - 'content': The raw data (could be bytes, str, etc. depending on the source)
                    - 'content_type': A string indicating the type of content (e.g., 'pdf', 'html', 'text', 'url')
                    - 'source': The name of the data source (e.g., 'sec', 'yahoo')
                    - 'metadata': Additional metadata (e.g., ticker, date, retrieval timestamp)
        """
        pass


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
            self.client.xadd(topic, {'data': json.dumps(envelope)})
            logger.info(f"Published envelope to topic '{topic}'")
        except Exception as e:
            # Log or handle publishing errors as needed
            logger.error(f"Error publishing to message bus: {e}")


def get_job_redis_key(organization_id: Optional[str], job_id: str, key_type: str) -> str:
    """
    Creates a Redis key with organization prefix for proper namespace isolation.
    
    This function enforces a consistent key naming convention across the application,
    ensuring proper multi-tenancy support and namespace isolation between different
    organizations using the system. It's used for job status tracking, results storage,
    and other Redis-based data structures.
    
    Key format: "org:{organization_id}:job:{job_id}:{key_type}"
    If no organization is provided: "job:{job_id}:{key_type}"
    
    Args:
        organization_id: The organization ID for namespacing (tenant identifier)
        job_id: The unique job identifier (typically a UUID)
        key_type: Type of key (status, result, overall_status, etc.)
    
    Returns:
        Properly formatted Redis key with organization prefix
        
    Example:
        get_job_redis_key("acme-corp", "550e8400-e29b-41d4-a716-446655440000", "status")
        # Returns: "org:acme-corp:job:550e8400-e29b-41d4-a716-446655440000:status"
    """
    if not organization_id:
        logger.warning(f"No organization_id provided for Redis key (job={job_id}, type={key_type})")
        return f"job:{job_id}:{key_type}"
    return f"org:{organization_id}:job:{job_id}:{key_type}"


def get_cik_for_ticker(ticker: str, request_id: Optional[str] = None) -> str:
    """
    Resolves a ticker symbol to a CIK (Central Index Key) using the sec-api.io mapping endpoint.
    
    The CIK is a unique identifier assigned by the SEC to companies and individuals who
    file disclosures with the SEC. This function provides the essential mapping between
    common stock ticker symbols (e.g., AAPL) and their corresponding SEC identifiers,
    which are required for accessing SEC EDGAR filings and data.
    
    The function performs proper error handling and logging, and returns the CIK in the
    standard SEC format (10-digit zero-padded string).
    
    Args:
        ticker: The stock ticker symbol (e.g., 'AAPL', 'MSFT', 'GOOGL')
        request_id: Optional request ID for tracing and logging
        
    Returns:
        Zero-padded CIK string (e.g., '0000320193' for Apple Inc.)
        
    Raises:
        MissingConfigurationError: If SEC_API_KEY is not set
        SECNotFoundError: If ticker can't be resolved to a CIK
        SECAuthenticationError: If authentication fails (401)
        SECRateLimitError: If rate limit is exceeded (429)
        SECError: For other SEC API errors
        DataSourceConnectionError: For network/connection issues
        DataSourceTimeoutError: If the request times out
    """
    # Create error context with available information
    context = ErrorContext(
        request_id=request_id,
        company={"ticker": ticker},
        source_type="SEC"
    )
    
    # Check for API key
    if not SEC_API_KEY:
        error_msg = "SEC_API_KEY is not set in the centralized configuration (settings.api_keys.sec)."
        logger.error(f"[{request_id}] {error_msg}")
        raise MissingConfigurationError(error_msg, context=context)
    
    # Correct endpoint format based on SEC-API.io documentation
    url = f"{SEC_API_BASE_URL}/mapping/ticker/{ticker}?token={SEC_API_KEY}"
    
    logger.info(f"[{request_id}] Resolving ticker '{ticker}' to CIK via sec-api.io")
    logger.debug(f"[{request_id}] SEC API request URL: {url}")
    
    # Implement retry with exponential backoff
    max_retries = settings.worker.max_retries
    base_delay = settings.worker.retry_delay_sec
    attempt = 0
    
    while attempt < max_retries:
        try:
            resp = httpx.get(url, timeout=settings.sec_request_timeout_sec)
            
            # Log response details for debugging
            logger.debug(f"[{request_id}] SEC API status code: {resp.status_code}")
            
            # Handle different response status codes with specific errors
            if resp.status_code == 401 or resp.status_code == 403:
                error_msg = f"SEC API authentication failed: {resp.text}"
                logger.error(f"[{request_id}] {error_msg}")
                raise SECAuthenticationError(error_msg, context=context)
                
            elif resp.status_code == 429:
                error_msg = f"SEC API rate limit exceeded: {resp.text}"
                logger.error(f"[{request_id}] {error_msg}")
                raise SECRateLimitError(error_msg, context=context)
                
            elif resp.status_code != 200:
                error_msg = f"SEC API returned status {resp.status_code}: {resp.text}"
                logger.error(f"[{request_id}] {error_msg}")
                raise SECError(error_msg, context=context)
            
            # Parse the response
            try:
                data = resp.json()
                logger.debug(f"[{request_id}] SEC API response: {data}")
            except json.JSONDecodeError as e:
                error_msg = f"Failed to parse SEC API response: {e}"
                logger.error(f"[{request_id}] {error_msg}")
                raise SECParsingError(error_msg, context=context, cause=e)
            
            # The API returns a list of results
            if isinstance(data, list) and len(data) > 0:
                # Get CIK from first result
                cik = data[0].get("cik")
                if cik:
                    padded_cik = str(cik).zfill(10)
                    logger.info(f"[{request_id}] Resolved ticker '{ticker}' to CIK '{padded_cik}'")
                    return padded_cik
            
            # If we got no matches or no CIK field, raise an error
            error_msg = f"Ticker '{ticker}' not found in SEC API response"
            logger.error(f"[{request_id}] {error_msg}: {data}")
            raise SECNotFoundError(error_msg, context=context)
            
        except httpx.TimeoutException as e:
            attempt += 1
            error_msg = f"SEC API request timed out: {e}"
            logger.warning(f"[{request_id}] {error_msg}. Attempt {attempt}/{max_retries}")
            
            if attempt >= max_retries:
                raise DataSourceTimeoutError(error_msg, context=context, cause=e)
                
            # Exponential backoff with jitter
            jitter = 0.1 * base_delay * (random.random() * 2 - 1)
            delay = min(base_delay * (2 ** (attempt - 1)) + jitter, 30.0)
            logger.info(f"[{request_id}] Retrying in {delay:.2f}s")
            time.sleep(delay)
            
        except httpx.RequestError as e:
            attempt += 1
            error_msg = f"SEC API connection error: {e}"
            logger.warning(f"[{request_id}] {error_msg}. Attempt {attempt}/{max_retries}")
            
            if attempt >= max_retries:
                raise DataSourceConnectionError(error_msg, context=context, cause=e)
                
            # Exponential backoff with jitter
            jitter = 0.1 * base_delay * (random.random() * 2 - 1)
            delay = min(base_delay * (2 ** (attempt - 1)) + jitter, 30.0)
            logger.info(f"[{request_id}] Retrying in {delay:.2f}s")
            time.sleep(delay)
            
        except (SECError, SalescienceError):
            # Re-raise SalescienceErrors without retrying
            raise
            
        except Exception as e:
            error_msg = f"Unexpected error fetching CIK for ticker '{ticker}' from SEC API: {e}"
            logger.error(f"[{request_id}] {error_msg}")
            raise SECError(error_msg, context=context, cause=e)


class SECDataSource(BaseDataSource):
    """
    Data source for SEC filings using the sec-api.io service.
    
    This class provides comprehensive access to SEC EDGAR filings and data,
    implementing the BaseDataSource interface for standardized data acquisition.
    It offers methods for retrieving various types of SEC filings and financial data,
    with robust error handling, logging, and telemetry.
    
    Key capabilities:
    - Fetching annual reports (10-K)
    - Fetching quarterly reports (10-Q)
    - Retrieving material events (8-K)
    - Accessing insider trading filings (Form 4)
    - Extracting structured financial data (XBRL)
    - Multi-year historical data retrieval
    - Support for both ticker symbol and CIK-based queries
    - Integration with message bus for event-driven architectures
    
    The class implements advanced features for reliability:
    - Multiple fallback strategies for document retrieval
    - Comprehensive error handling and reporting
    - Detailed logging for operational visibility
    - Optional Prometheus metrics for monitoring
    """
    
    def __init__(self):
        """Initialize the SEC data source."""
        self.name = "SEC"
        
        # Verify API key is available from centralized configuration
        if not SEC_API_KEY:
            logger.warning("SEC_API_KEY not set in settings.api_keys.sec. SEC data source will not function.")
            
    def _validate_and_resolve_company_params(self, params: Dict[str, Any], request_id: Optional[str] = None) -> Tuple[str, Optional[str]]:
        """
        Validate and resolve company parameters.
        
        Args:
            params: Dictionary with company identification parameters
            request_id: Optional request ID for tracing
            
        Returns:
            Tuple of (cik, ticker) where cik is the resolved 10-digit CIK
            
        Raises:
            ValidationError: If required parameters are missing
            SECError: If CIK resolution fails
        """
        # Create error context
        context = ErrorContext(
            request_id=request_id,
            company=params,
            source_type="SEC"
        )
        
        # Extract ticker and CIK from params
        ticker = params.get('ticker')
        cik = params.get('cik')
        
        logger.info(f"[{request_id}] Validating company parameters: ticker={ticker}, cik={cik}")
        
        # Validate that we have at least ticker or CIK
        if not ticker and not cik:
            error_msg = "Parameter 'ticker' or 'cik' is required"
            logger.error(f"[{request_id}] {error_msg}")
            raise ValidationError(error_msg, context=context)
            
        # Resolve ticker to CIK if needed
        if not cik and ticker:
            try:
                logger.info(f"[{request_id}] Resolving ticker {ticker} to CIK")
                cik = get_cik_for_ticker(ticker, request_id)
                logger.info(f"[{request_id}] Ticker {ticker} resolved to CIK {cik}")
            except Exception as e:
                error_msg = f"Failed to resolve ticker {ticker} to CIK"
                logger.error(f"[{request_id}] {error_msg}: {e}")
                raise SECError(error_msg, context=context, cause=e)
        
        # Ensure CIK is properly formatted
        if cik:
            cik = str(cik).zfill(10)
        
        return cik, ticker
    
    def fetch(self, params: Dict[str, Any], request_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Fetch SEC filing data based on provided parameters.
        
        Args:
            params: A dictionary with fetch parameters:
                - cik: (Optional) Central Index Key (CIK) for the company
                - ticker: (Optional) Stock ticker symbol (will be resolved to CIK if provided)
                - form_type: (Optional) Form type to fetch (default: '10-K')
                - year: (Optional) Year of filing to fetch
        
        Returns:
            A dictionary with the following keys:
                - content: The raw filing content
                - content_type: Content type (html, txt, etc.)
                - source: 'sec'
                - metadata: Additional filing metadata
                
        Raises:
            ValueError: If required parameters are missing or API errors occur
        """
        # Generate request_id if not provided
        if not request_id:
            request_id = str(uuid.uuid4())
            
        # Extract form_type and year parameters
        form_type = params.get('form_type', '10-K')
        year = params.get('year')
        
        # Validate form type - we support many filing types
        valid_form_types = [
            '10-K', '10-Q', '8-K', '20-F', '40-F',   # Most common reports
            'DEF 14A', 'DEFA14A', 'DEFM14A',         # Proxy statements
            'S-1', 'S-3', 'S-4', 'F-1',              # Registration statements
            '4', '13F', '13G', '13D',                # Ownership filings
            'SD', 'CORRESP', 'FWP',                  # Specialized filings
            '10-K/A', '10-Q/A', '8-K/A'              # Amended filings
        ]
        
        logger.info(f"[{request_id}] Fetching SEC filing: form_type={form_type}, year={year}")
        
        # Create initial error context
        context = ErrorContext(
            request_id=request_id,
            source_type="SEC",
            form_type=form_type,
            year=year
        )
        
        # Verify API key is available from centralized configuration
        if not SEC_API_KEY:
            error_msg = "SEC_API_KEY is not set in the centralized configuration (settings.api_keys.sec)."
            logger.error(f"[{request_id}] {error_msg}")
            raise MissingConfigurationError(error_msg, context=context)
            
        # Log user agent and contact info for SEC compliance
        headers = {
            "User-Agent": settings.sec_user_agent,
            "Accept": "application/json",
            "X-Request-ID": request_id  # Add request ID to headers for tracing
        }
        
        logger.debug(f"[{request_id}] Using SEC API key: {SEC_API_KEY[:5]}...{SEC_API_KEY[-5:]}")
        
        # Validate and resolve company parameters
        try:
            cik, ticker = self._validate_and_resolve_company_params(params, request_id)
            # Update context with resolved company information
            context.company = {"cik": cik, "ticker": ticker}
        except (ValidationError, SECError) as e:
            logger.error(f"[{request_id}] Failed to validate company parameters: {e}")
            raise
        
        try:
            # Construct the SEC API query for filings
            # Using the Query API format from SEC-API.io documentation
            query = {
                "query": {
                    "query_string": {
                        "query": f"formType:\"{form_type}\" AND cik:{cik.lstrip('0')}"
                    }
                },
                "from": "0",
                "size": "10",
                "sort": [{"filedAt": {"order": "desc"}}]
            }
            
            # Add year filtering if specified
            if year:
                query["query"]["query_string"]["query"] += f" AND filedAt:[{year}-01-01 TO {year}-12-31]"
            
            # SEC-API.io Query API endpoint (base URL with no /query path)
            query_url = f"{SEC_API_BASE_URL}?token={SEC_API_KEY}"
            
            logger.debug(f"SEC API query URL: {query_url}")
            logger.debug(f"SEC API query payload: {query}")
            
            # Send the request to the SEC API
            resp = httpx.post(query_url, json=query, headers=headers, timeout=15.0)
            
            # Log response details
            logger.debug(f"SEC API query status: {resp.status_code}")
            
            if resp.status_code != 200:
                error_msg = f"SEC API query returned status {resp.status_code}: {resp.text}"
                logger.error(error_msg)
                
                # Map error based on status code
                if resp.status_code == 401 or resp.status_code == 403:
                    raise SECAuthenticationError(f"SEC API authentication failed: {resp.text}", context=context)
                elif resp.status_code == 429:
                    raise SECRateLimitError(f"SEC API rate limit exceeded: {resp.text}", context=context)
                else:
                    raise SECError(f"SEC API query error: {resp.status_code} - {resp.text}", context=context)
            
            # Process the response
            query_result = resp.json()
            logger.debug(f"SEC API query response: {query_result}")
            
            filings = query_result.get('filings', [])
            if not filings:
                error_msg = f"No {form_type} filings found for CIK {cik} (year={year}) via SEC API."
                logger.warning(error_msg)
                raise SECNotFoundError(error_msg, context=context)
            
            # Get the most recent filing
            filing = filings[0]
            logger.info(f"Found filing: accessionNo={filing.get('accessionNo')}, filedAt={filing.get('filedAt')}")
            
            # Get the document URL
            accession_no = filing.get('accessionNo', '').replace('-', '')
            cik_no_leading_zeros = cik.lstrip('0')
            
            # Construct URLs to try for the filing document
            urls_to_try = []
            
            # First, try to use URLs from the filing metadata (most reliable)
            if filing.get('linkToFilingDetails'):
                urls_to_try.append(filing.get('linkToFilingDetails'))
            
            if filing.get('linkToTxt'):
                urls_to_try.append(filing.get('linkToTxt'))
                
            if filing.get('linkToHtml'):
                urls_to_try.append(filing.get('linkToHtml'))
                
            # If there are document format files, add their URLs
            if 'documentFormatFiles' in filing and filing['documentFormatFiles']:
                for doc in filing['documentFormatFiles']:
                    if doc.get('documentUrl') and (doc.get('type') == filing.get('formType') or 
                                                doc.get('type') == '10-K' or 
                                                doc.get('description', '').lower() == 'complete submission text file'):
                        urls_to_try.append(doc.get('documentUrl'))
            
            # Add fallback URLs in case the metadata URLs are not available
            if filing.get('primaryDocument'):
                # Primary URL from SEC EDGAR
                urls_to_try.append(f"{SEC_EDGAR_BASE_URL}/{cik_no_leading_zeros}/{accession_no}/{filing.get('primaryDocument')}")
                # Alternative URL format
                urls_to_try.append(f"{SEC_EDGAR_BASE_URL}/{cik_no_leading_zeros}/{accession_no.replace('-', '')}/{filing.get('primaryDocument')}")
            
            # Complete submission text file
            urls_to_try.append(f"{SEC_EDGAR_BASE_URL}/{cik_no_leading_zeros}/{accession_no}/{accession_no}.txt")
            urls_to_try.append(f"{SEC_EDGAR_BASE_URL}/{cik_no_leading_zeros}/{accession_no.replace('-', '')}/{accession_no.replace('-', '')}.txt")
            
            # Try each URL until we get a successful response
            document_content = None
            content_type = None
            doc_url = None
            
            for url in urls_to_try:
                logger.debug(f"Trying to fetch filing document from: {url}")
                try:
                    doc_resp = httpx.get(url, headers=headers, timeout=15.0)
                    if doc_resp.status_code == 200:
                        document_content = doc_resp.text
                        doc_url = url
                        content_type = 'html' if url.endswith('.htm') or url.endswith('.html') else 'txt'
                        logger.info(f"Successfully fetched filing document from {url}")
                        break
                    else:
                        logger.debug(f"Failed to fetch from {url}: {doc_resp.status_code}")
                except Exception as e:
                    logger.debug(f"Error fetching from {url}: {e}")
            
            if not document_content:
                error_msg = "Could not fetch filing document from any source URL"
                logger.error(error_msg)
                raise ValueError(error_msg)
            
            # Return the filing data
            return {
                'content': document_content,
                'content_type': content_type,
                'source': 'sec',
                'status': 'success',
                'metadata': {
                    'cik': cik,
                    'ticker': ticker,
                    'form_type': form_type,
                    'year': year or filing.get('filedAt', '').split('-')[0],
                    'retrieved_at': datetime.datetime.utcnow().isoformat() + 'Z',
                    'filing_info': filing,
                    'filing_url': doc_url
                }
            }
            
        except Exception as e:
            error_msg = f"Error fetching filing from SEC API: {e}"
            logger.error(error_msg)
            raise ValueError(error_msg)
    
    def fetch_quarterly_reports(self, params: Dict[str, Any], n_quarters: int = 4, request_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Fetches the most recent quarterly reports (10-Q) for a company.
        
        Args:
            params (Dict[str, Any]): Must include 'cik' or 'ticker'.
            n_quarters (int): Number of recent quarters to fetch (default 4)
            request_id: Optional request ID for tracing
            
        Returns:
            List[Dict[str, Any]]: List of envelopes with 10-Q data and metadata.
            
        Example usage:
            sec = SECDataSource()
            results = sec.fetch_quarterly_reports({'ticker': 'AAPL'})
        """
        # Generate request_id if not provided
        if not request_id:
            request_id = str(uuid.uuid4())
            
        logger.info(f"[{request_id}] Fetching quarterly reports for {n_quarters} quarters")
        
        # This function will use the fetch_last_n_years logic but with form_type='10-Q'
        return self.fetch_last_n_years(params, n_quarters, form_type='10-Q', request_id=request_id)
    
    def fetch_material_events(self, params: Dict[str, Any], limit: int = 10, request_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Fetches the most recent Form 8-K (material events) filings for a company.
        
        Args:
            params (Dict[str, Any]): Must include 'cik' or 'ticker'.
            limit (int): Maximum number of filings to return (default 10)
            
        Returns:
            List[Dict[str, Any]]: List of envelopes with 8-K data and metadata.
            
        Example usage:
            sec = SECDataSource()
            results = sec.fetch_material_events({'ticker': 'AAPL'})
        """
        # Generate request_id if not provided
        if not request_id:
            request_id = str(uuid.uuid4())
            
        # Check SEC API key first
        if not SEC_API_KEY:
            error_msg = "SEC_API_KEY is not set in the centralized configuration (settings.api_keys.sec)."
            logger.error(f"[{request_id}] {error_msg}")
            # Create error context
            context = ErrorContext(
                request_id=request_id,
                source_type="SEC-8K"
            )
            raise MissingConfigurationError(error_msg, context=context)
        
        # Validate and resolve company parameters
        try:
            cik, ticker = self._validate_and_resolve_company_params(params, request_id)
        except (ValidationError, SECError) as e:
            logger.error(f"[{request_id}] Failed to validate company parameters for material events: {e}")
            raise
                
        # Prepare SEC API query for Form 8-K filings
        query_url = f"{SEC_API_BASE_URL}?token={SEC_API_KEY}"
        query = {
            "query": {
                "query_string": {
                    "query": f"formType:\"8-K\" AND cik:{cik.lstrip('0')}"
                }
            },
            "from": "0",
            "size": str(limit),
            "sort": [{"filedAt": {"order": "desc"}}]
        }
        
        headers = {
            "User-Agent": settings.sec_user_agent,
            "Accept": "application/json",
        }
        
        try:
            logger.info(f"Fetching Form 8-K (material events) filings for CIK {cik}")
            resp = httpx.post(query_url, json=query, headers=headers)
            
            if resp.status_code != 200:
                logger.error(f"SEC API Form 8-K query returned {resp.status_code}: {resp.text}")
                raise ValueError(f"SEC API Form 8-K query error: {resp.status_code}")
                
            data = resp.json()
            filings = data.get('filings', [])
            
            if not filings:
                logger.warning(f"No Form 8-K filings found for CIK {cik}")
                return []
                
            results = []
            
            # Process each Form 8-K filing
            for filing in filings:
                # Get document URLs
                doc_url = None
                content_type = None
                
                # Find the Form 8-K document URL
                if 'documentFormatFiles' in filing:
                    for doc in filing['documentFormatFiles']:
                        if doc.get('type') == '8-K' and doc.get('documentUrl'):
                            doc_url = doc.get('documentUrl')
                            content_type = 'html' if doc_url.endswith('.htm') or doc_url.endswith('.html') else 'txt'
                            break
                
                # Fallback to linkToFilingDetails
                if not doc_url and filing.get('linkToFilingDetails'):
                    doc_url = filing.get('linkToFilingDetails')
                    content_type = 'html'
                
                # If we found a document URL, fetch it
                if doc_url:
                    try:
                        doc_resp = httpx.get(doc_url, headers=headers)
                        
                        if doc_resp.status_code == 200:
                            content = doc_resp.text
                            
                            # Create envelope
                            envelope = {
                                'content': content,
                                'content_type': content_type,
                                'source': 'sec-8k',
                                'status': 'success',
                                'metadata': {
                                    'cik': cik,
                                    'ticker': ticker,
                                    'form_type': '8-K',
                                    'filing_date': filing.get('filedAt'),
                                    'accession_no': filing.get('accessionNo'),
                                    'filing_info': filing,
                                    'filing_url': doc_url,
                                    'retrieved_at': datetime.datetime.utcnow().isoformat() + 'Z'
                                }
                            }
                            
                            results.append(envelope)
                        else:
                            logger.warning(f"Failed to fetch Form 8-K document from {doc_url}: {doc_resp.status_code}")
                    except Exception as e:
                        logger.error(f"Error fetching Form 8-K document from {doc_url}: {e}")
            
            logger.info(f"Successfully fetched {len(results)} Form 8-K filings for CIK {cik}")
            return results
            
        except Exception as e:
            logger.error(f"Error fetching Form 8-K filings from SEC API: {e}")
            raise ValueError(f"Error fetching Form 8-K filings from SEC API: {e}")
    
    def _validate_and_resolve_params(self, params: Dict[str, Any], request_id: Optional[str] = None) -> Tuple[str, Optional[str]]:
    """
    Validate and resolve company parameters.
    
    Args:
        params: Dictionary with company identification parameters
        request_id: Optional request ID for tracing
        
    Returns:
        Tuple of (cik, ticker) where cik is the resolved 10-digit CIK
        
    Raises:
        ValidationError: If required parameters are missing
        SECError: If CIK resolution fails
    """
    # Create error context
    context = ErrorContext(
        request_id=request_id,
        company=params,
        source_type="SEC"
    )
    
    # Extract ticker and CIK from params
    ticker = params.get('ticker')
    cik = params.get('cik')
    
    logger.info(f"[{request_id}] Validating company parameters: ticker={ticker}, cik={cik}")
    
    # Validate that we have at least ticker or CIK
    if not ticker and not cik:
        error_msg = "Parameter 'ticker' or 'cik' is required"
        logger.error(f"[{request_id}] {error_msg}")
        raise ValidationError(error_msg, context=context)
        
    # Resolve ticker to CIK if needed
    if not cik and ticker:
        try:
            logger.info(f"[{request_id}] Resolving ticker {ticker} to CIK")
            cik = get_cik_for_ticker(ticker, request_id)
            logger.info(f"[{request_id}] Ticker {ticker} resolved to CIK {cik}")
        except Exception as e:
            error_msg = f"Failed to resolve ticker {ticker} to CIK"
            logger.error(f"[{request_id}] {error_msg}: {e}")
            raise SECError(error_msg, context=context, cause=e)
    
    # Ensure CIK is properly formatted
    if cik:
        cik = str(cik).zfill(10)
    
    return cik, ticker

def _build_sec_api_query(self, cik: str, form_type: str, start_year: int, end_year: int, max_results: int = 10) -> Dict[str, Any]:
    """
    Build SEC API query for filings.
    
    Args:
        cik: Company CIK (10-digit padded)
        form_type: SEC form type to fetch
        start_year: Start year for query range
        end_year: End year for query range
        max_results: Maximum number of results to return
        
    Returns:
        Dictionary with SEC API query parameters
    """
    # Build the query with date range filter
    query = {
        "query": {
            "query_string": {
                "query": f"formType:\"{form_type}\" AND cik:{cik.lstrip('0')} AND filedAt:[{start_year}-01-01 TO {end_year}-12-31]"
            }
        },
        "from": "0",
        "size": str(max_results),
        "sort": [{"filedAt": {"order": "desc"}}]
    }
    
    return query

def _execute_sec_api_query(self, query: Dict[str, Any], request_id: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Execute SEC API query and handle responses.
    
    Args:
        query: SEC API query to execute
        request_id: Optional request ID for tracing
        
    Returns:
        List of filing metadata
        
    Raises:
        SECAuthenticationError: If authentication fails
        SECRateLimitError: If rate limit is exceeded
        SECError: For other SEC API errors
        SECNotFoundError: If no filings are found
    """
    # Create error context
    context = ErrorContext(
        request_id=request_id,
        source_type="SEC",
        query=query
    )
    
    # Set up API URL
    query_url = f"{SEC_API_BASE_URL}?token={SEC_API_KEY}"
    
    # Set up headers
    headers = {
        "User-Agent": settings.sec_user_agent,
        "Accept": "application/json",
    }
    
    logger.info(f"[{request_id}] Executing SEC API query for filings")
    logger.debug(f"[{request_id}] SEC API query: {query}")
    
    try:
        # Execute the query
        resp = httpx.post(query_url, json=query, headers=headers, timeout=settings.sec_request_timeout_sec)
        
        # Log response details
        logger.debug(f"[{request_id}] SEC API status code: {resp.status_code}")
        
        # Handle different status codes
        if resp.status_code == 401 or resp.status_code == 403:
            error_msg = f"SEC API authentication failed: {resp.text}"
            logger.error(f"[{request_id}] {error_msg}")
            raise SECAuthenticationError(error_msg, context=context)
            
        elif resp.status_code == 429:
            error_msg = f"SEC API rate limit exceeded: {resp.text}"
            logger.error(f"[{request_id}] {error_msg}")
            raise SECRateLimitError(error_msg, context=context)
            
        elif resp.status_code != 200:
            error_msg = f"SEC API returned status {resp.status_code}: {resp.text}"
            logger.error(f"[{request_id}] {error_msg}")
            raise SECError(error_msg, context=context)
        
        # Parse the response
        try:
            data = resp.json()
            logger.debug(f"[{request_id}] SEC API response: {data}")
        except json.JSONDecodeError as e:
            error_msg = f"Failed to parse SEC API response"
            logger.error(f"[{request_id}] {error_msg}: {e}")
            raise SECParsingError(error_msg, context=context, cause=e)
        
        # Extract filings
        filings = data.get('filings', [])
        
        # Check if we got any filings
        if not filings:
            error_msg = f"No filings found in SEC API response"
            logger.warning(f"[{request_id}] {error_msg}")
            raise SECNotFoundError(error_msg, context=context)
        
        return filings
        
    except httpx.TimeoutException as e:
        error_msg = f"SEC API request timed out"
        logger.error(f"[{request_id}] {error_msg}: {e}")
        raise DataSourceTimeoutError(error_msg, context=context, cause=e)
        
    except httpx.RequestError as e:
        error_msg = f"SEC API connection error"
        logger.error(f"[{request_id}] {error_msg}: {e}")
        raise DataSourceConnectionError(error_msg, context=context, cause=e)
        
    except (SECError, SalescienceError):
        # Re-raise SalescienceErrors
        raise
        
    except Exception as e:
        error_msg = f"Unexpected error executing SEC API query"
        logger.error(f"[{request_id}] {error_msg}: {e}")
        raise SECError(error_msg, context=context, cause=e)

def _get_document_url(self, filing: Dict[str, Any], cik: str, form_type: str, request_id: Optional[str] = None) -> Tuple[List[str], str]:
    """
    Get document URLs from filing metadata with fallback strategies.
    
    Args:
        filing: Filing metadata
        cik: Company CIK
        form_type: SEC form type
        request_id: Optional request ID for tracing
        
    Returns:
        Tuple of (urls_to_try, content_type) where:
            - urls_to_try is a list of URLs to try in order
            - content_type is the expected content type
    """
    urls_to_try = []
    content_type = 'txt'  # Default to text
    
    # Extract accession number for fallback URLs
    accession_no = filing.get('accessionNo', '').replace('-', '')
    cik_no_leading_zeros = cik.lstrip('0')
    
    logger.debug(f"[{request_id}] Extracting document URLs for filing: {accession_no}")
    
    # 1. Try URLs from filing metadata (most reliable)
    if filing.get('linkToFilingDetails'):
        urls_to_try.append(filing.get('linkToFilingDetails'))
        content_type = 'html'
    
    if filing.get('linkToTxt'):
        urls_to_try.append(filing.get('linkToTxt'))
        content_type = 'txt'
        
    if filing.get('linkToHtml'):
        urls_to_try.append(filing.get('linkToHtml'))
        content_type = 'html'
        
    # 2. Check document format files
    if 'documentFormatFiles' in filing and filing['documentFormatFiles']:
        for doc in filing['documentFormatFiles']:
            # Look for the main form document
            if doc.get('documentUrl') and (doc.get('type') == form_type or 
                                           doc.get('type') == '10-K' or 
                                           doc.get('description', '').lower() == 'complete submission text file'):
                urls_to_try.append(doc.get('documentUrl'))
                content_type = 'html' if doc.get('documentUrl', '').endswith(('.htm', '.html')) else 'txt'
    
    # 3. Fallback URLs based on SEC EDGAR structure
    if filing.get('primaryDocument'):
        # Primary URL from SEC EDGAR
        urls_to_try.append(f"{SEC_EDGAR_BASE_URL}/{cik_no_leading_zeros}/{accession_no}/{filing.get('primaryDocument')}")
        # Alternative URL format
        urls_to_try.append(f"{SEC_EDGAR_BASE_URL}/{cik_no_leading_zeros}/{accession_no.replace('-', '')}/{filing.get('primaryDocument')}")
    
    # 4. Complete submission text file
    urls_to_try.append(f"{SEC_EDGAR_BASE_URL}/{cik_no_leading_zeros}/{accession_no}/{accession_no}.txt")
    urls_to_try.append(f"{SEC_EDGAR_BASE_URL}/{cik_no_leading_zeros}/{accession_no.replace('-', '')}/{accession_no.replace('-', '')}.txt")
    
    logger.debug(f"[{request_id}] Found {len(urls_to_try)} possible URLs for filing {accession_no}")
    
    # Set content type based on URL extensions
    content_type = 'html' if urls_to_try and urls_to_try[0].endswith(('.htm', '.html')) else 'txt'
    
    return urls_to_try, content_type

def _fetch_document_content(self, urls: List[str], headers: Dict[str, str], request_id: Optional[str] = None) -> Tuple[Optional[str], Optional[str], str]:
    """
    Fetch document content from URLs.
    
    Args:
        urls: List of URLs to try in order
        headers: HTTP headers for request
        request_id: Optional request ID for tracing
        
    Returns:
        Tuple of (document_content, successful_url, content_type) where:
            - document_content is the fetched content
            - successful_url is the URL that worked
            - content_type is determined from the URL
    """
    # Create error context
    context = ErrorContext(
        request_id=request_id,
        source_type="SEC",
        urls=urls
    )
    
    # Try each URL until we get a successful response
    for url in urls:
        try:
            logger.debug(f"[{request_id}] Trying to fetch document from: {url}")
            
            # Determine content type from URL
            content_type = 'html' if url.endswith(('.htm', '.html')) else 'txt'
            
            # Fetch the document
            doc_resp = httpx.get(url, headers=headers, timeout=settings.sec_request_timeout_sec)
            
            if doc_resp.status_code == 200:
                document_content = doc_resp.text
                logger.info(f"[{request_id}] Successfully fetched document from {url}")
                return document_content, url, content_type
            else:
                logger.debug(f"[{request_id}] Failed to fetch from {url}: HTTP {doc_resp.status_code}")
                
        except Exception as e:
            logger.debug(f"[{request_id}] Error fetching from {url}: {e}")
    
    # If we get here, all URLs failed
    error_msg = "Could not fetch document from any source URL"
    logger.error(f"[{request_id}] {error_msg}")
    raise SECNotFoundError(error_msg, context=context)

def _create_filing_envelope(self, content: Optional[str], status: str, metadata: Dict[str, Any], 
                           error: Optional[str] = None, request_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Create response envelope for a filing.
    
    Args:
        content: Document content (None for error cases)
        status: Response status (success, error, not_found)
        metadata: Filing metadata
        error: Optional error message
        request_id: Optional request ID for tracing
        
    Returns:
        Standardized response envelope
    """
    # Create base envelope structure
    envelope = {
        'content': content,
        'content_type': metadata.get('content_type', 'txt'),
        'source': 'sec',
        'status': status,
        'metadata': {
            'cik': metadata.get('cik'),
            'ticker': metadata.get('ticker'),
            'form_type': metadata.get('form_type'),
            'year': metadata.get('year'),
            'retrieved_at': datetime.datetime.utcnow().isoformat() + 'Z',
        }
    }
    
    # Add additional metadata if available
    if 'filing_info' in metadata:
        envelope['metadata']['filing_info'] = metadata['filing_info']
        
    if 'filing_url' in metadata:
        envelope['metadata']['filing_url'] = metadata['filing_url']
    
    # Add error information if provided
    if error:
        envelope['error'] = error
        envelope['status'] = 'error'  # Ensure status is error when error message exists
    
    logger.debug(f"[{request_id}] Created filing envelope with status: {status}")
    
    return envelope

def _process_single_filing(self, filing: Dict[str, Any], cik: str, ticker: Optional[str], 
                           form_type: str, headers: Dict[str, str], request_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Process a single filing from metadata to final envelope.
    
    Args:
        filing: Filing metadata
        cik: Company CIK
        ticker: Company ticker
        form_type: SEC form type
        headers: HTTP headers for requests
        request_id: Optional request ID for tracing
        
    Returns:
        Filing result envelope
    """
    try:
        # Extract filing year
        filed_at = filing.get('filedAt', '')
        year = filed_at[:4] if filed_at else None
        
        logger.info(f"[{request_id}] Processing {form_type} filing for year {year}")
        
        # Get document URLs
        urls_to_try, content_type = self._get_document_url(filing, cik, form_type, request_id)
        
        # Fetch document content
        document_content, doc_url, content_type = self._fetch_document_content(urls_to_try, headers, request_id)
        
        # Create success envelope
        return self._create_filing_envelope(
            content=document_content,
            status="success",
            metadata={
                'cik': cik,
                'ticker': ticker,
                'form_type': form_type,
                'year': year,
                'filing_info': filing,
                'filing_url': doc_url,
                'content_type': content_type
            },
            request_id=request_id
        )
        
    except (SECError, SalescienceError) as e:
        # Create error envelope for specific known errors
        return self._create_filing_envelope(
            content=None,
            status="error",
            metadata={
                'cik': cik,
                'ticker': ticker,
                'form_type': form_type,
                'year': year if 'year' in locals() else None,
                'filing_info': filing
            },
            error=str(e),
            request_id=request_id
        )
        
    except Exception as e:
        # Create error envelope for unexpected errors
        error_msg = f"Unexpected error processing filing: {e}"
        logger.error(f"[{request_id}] {error_msg}")
        
        return self._create_filing_envelope(
            content=None,
            status="error",
            metadata={
                'cik': cik,
                'ticker': ticker,
                'form_type': form_type,
                'year': year if 'year' in locals() else None,
                'filing_info': filing
            },
            error=error_msg,
            request_id=request_id
        )

def fetch_last_n_years(self, params: Dict[str, Any], n_years: int = 5, form_type: str = '10-K', request_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Fetch filings for the last N years for a specific company.
        
        This method retrieves SEC filings (default: 10-K annual reports) for the specified
        number of years, attempting to get one filing per year. It handles all the 
        complexity of finding and retrieving these documents from multiple potential
        sources, with fallback options for reliability.
        
        Args:
            params: Base parameters for fetch method, must include either 'cik' or 'ticker'
            n_years: Number of years to fetch, defaults to 5 years of historical data
            form_type: Form type to fetch (e.g., '10-K', '10-Q', '8-K', etc.)
            
        Returns:
            List of fetch results, with one envelope per year where available.
            Each envelope contains filing content and comprehensive metadata.
            If a filing isn't available for a particular year, the envelope will
            have status='error' or status='not_found' with appropriate error details.
        """
        results = []
        current_year = datetime.datetime.now().year
        
        # Generate request_id if not provided
        if not request_id:
            request_id = str(uuid.uuid4())
            
        # Create error context for the overall operation
        context = ErrorContext(
            request_id=request_id,
            source_type="SEC",
            form_type=form_type,
            years=n_years
        )
        
        # Validate and resolve company parameters
        try:
            cik, ticker = self._validate_and_resolve_company_params(params, request_id)
        except (ValidationError, SECError) as e:
            logger.error(f"[{request_id}] Failed to validate company parameters: {e}")
            
            # Return error envelope for the validation failure
            error_envelope = {
                'content': None,
                'content_type': None,
                'source': 'sec',
                'status': 'error',
                'error': str(e),
                'metadata': {
                    'cik': params.get('cik'),
                    'ticker': params.get('ticker'),
                    'form_type': form_type,
                    'retrieved_at': datetime.datetime.utcnow().isoformat() + 'Z'
                }
            }
            return [error_envelope] * n_years
            
        # Update the context with resolved company information
        context.company = {"cik": cik, "ticker": ticker}
        
        filings_url = f"{SEC_API_BASE_URL}?token={SEC_API_KEY}"
        try:
            # Build query for all filings in the time range
            end_year = current_year
            start_year = current_year - n_years + 1
            
            query = {
                "query": {
                    "query_string": {
                        "query": f"formType:\"{form_type}\" AND cik:{cik.lstrip('0')} AND filedAt:[{start_year}-01-01 TO {end_year}-12-31]"
                    }
                },
                "from": "0",
                "size": str(n_years * 2),  # Get a few extras in case some years have multiple filings
                "sort": [{"filedAt": {"order": "desc"}}]
            }
            
            headers = {
                "User-Agent": settings.sec_user_agent,
                "Accept": "application/json",
            }
            
            logger.info(f"Fetching multiple {form_type} filings for CIK {cik} from {start_year} to {end_year}")
            resp = httpx.post(filings_url, json=query, headers=headers)
            
            if resp.status_code != 200:
                logger.error(f"SEC API query returned status {resp.status_code}: {resp.text}")
                raise ValueError(f"SEC API query error: {resp.status_code}")
            
            query_result = resp.json()
            filings = query_result.get('filings', [])
            
            # Group filings by year
            seen_years = set()
            for filing in filings:
                try:
                    filed_at = filing.get('filedAt', '')
                    year = filed_at[:4]
                    
                    # Skip if we already have this year
                    if year in seen_years:
                        continue
                    
                    # Process filing
                    doc_url = None
                    content_type = None
                    
                    # First try to get URL from the filing metadata fields
                    if filing.get('linkToFilingDetails'):
                        doc_url = filing.get('linkToFilingDetails')
                        content_type = 'html'
                    elif filing.get('linkToHtml'):
                        doc_url = filing.get('linkToHtml')
                        content_type = 'html'
                    elif filing.get('linkToTxt'):
                        doc_url = filing.get('linkToTxt')
                        content_type = 'txt'
                    
                    # Check documentFormatFiles
                    if not doc_url and 'documentFormatFiles' in filing:
                        for doc in filing['documentFormatFiles']:
                            if doc.get('type') == form_type and doc.get('documentUrl'):
                                doc_url = doc.get('documentUrl')
                                content_type = 'html' if doc_url.endswith('.htm') or doc_url.endswith('.html') else 'txt'
                                break
                            elif doc.get('description', '').lower() == 'complete submission text file':
                                doc_url = doc.get('documentUrl')
                                content_type = 'txt'
                                break
                    
                    # Try to find the main document
                    if not doc_url:
                        for doc in filing.get('documents', []):
                            if doc.get('type', '').lower() == 'complete submission text file':
                                doc_url = doc.get('url')
                                content_type = 'txt'
                                break
                            elif doc.get('type', '').lower() == form_type.lower() and doc.get('url', '').endswith(('.htm', '.html')):
                                doc_url = doc.get('url')
                                content_type = 'html'
                                break
                    
                    # Fallback to first document if needed
                    if not doc_url and filing.get('documents'):
                        doc_url = filing['documents'][0].get('url')
                        content_type = 'txt' if doc_url and doc_url.endswith('.txt') else 'html'
                    
                    # Last resort: try to construct URL based on accession number
                    if not doc_url:
                        accession_no = filing.get('accessionNo', '').replace('-', '')
                        cik_no_leading_zeros = cik.lstrip('0')
                        txt_url = f"https://www.sec.gov/Archives/edgar/data/{cik_no_leading_zeros}/{accession_no}/{accession_no}.txt"
                        doc_url = txt_url
                        content_type = 'txt'
                    
                    # Fetch document if URL found
                    if doc_url:
                        try:
                            doc_resp = httpx.get(doc_url, headers=headers)
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
                                results.append(envelope)
                                seen_years.add(year)
                                logger.info(f"Successfully fetched {form_type} for {year}")
                            else:
                                logger.warning(f"Failed to fetch filing document: {doc_resp.status_code}")
                                envelope = {
                                    'content': None,
                                    'content_type': None,
                                    'source': 'sec',
                                    'status': 'error',
                                    'error': f"Failed to fetch document: HTTP {doc_resp.status_code}",
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
                                results.append(envelope)
                        except Exception as doc_exc:
                            logger.error(f"Error fetching filing document: {doc_exc}")
                            envelope = {
                                'content': None,
                                'content_type': None,
                                'source': 'sec',
                                'status': 'error',
                                'error': str(doc_exc),
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
                            results.append(envelope)
                    else:
                        logger.warning(f"No document URL found for filing {filing.get('accessionNo')}")
                        envelope = {
                            'content': None,
                            'content_type': None,
                            'source': 'sec',
                            'status': 'not_found',
                            'error': "No document URL found",
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
                    
                    # Stop if we have enough years
                    if len(seen_years) >= n_years:
                        break
                        
                except Exception as filing_exc:
                    logger.warning(f"Error processing filing: {filing_exc}")
                    envelope = {
                        'content': None,
                        'content_type': None,
                        'source': 'sec',
                        'status': 'error',
                        'error': str(filing_exc),
                        'metadata': {
                            'cik': cik,
                            'ticker': ticker,
                            'form_type': form_type,
                            'year': None,
                            'retrieved_at': datetime.datetime.utcnow().isoformat() + 'Z'
                        }
                    }
                    results.append(envelope)
            
            # If we didn't get enough years, fill with error results
            missing_years = n_years - len(results)
            if missing_years > 0:
                logger.warning(f"Missing {missing_years} years of filings")
                for i in range(missing_years):
                    envelope = {
                        'content': None,
                        'content_type': None,
                        'source': 'sec',
                        'status': 'not_found',
                        'error': f"Filing not available",
                        'metadata': {
                            'cik': cik,
                            'ticker': ticker,
                            'form_type': form_type,
                            'year': None,
                            'retrieved_at': datetime.datetime.utcnow().isoformat() + 'Z'
                        }
                    }
                    results.append(envelope)
            
            return results
                
        except Exception as e:
            logger.error(f"Error fetching multiple filings: {e}")
            # Return error envelopes for all years
            for i in range(n_years):
                year = current_year - i
                envelope = {
                    'content': None,
                    'content_type': None,
                    'source': 'sec',
                    'status': 'error',
                    'error': str(e),
                    'metadata': {
                        'cik': cik,
                        'ticker': ticker,
                        'form_type': form_type,
                        'year': year,
                        'retrieved_at': datetime.datetime.utcnow().isoformat() + 'Z'
                    }
                }
                results.append(envelope)
            
            return results
    
    async def fetch_last_n_years_and_publish(self, params: Dict[str, Any], n_years: int, form_type: str, publisher: MessageBusPublisher, topic: str, request_id: Optional[str] = None):
        """
        Fetches the last N years of filings and publishes each envelope to the message bus.
        
        This asynchronous method facilitates integration with event-driven architectures
        by fetching multiple years of SEC filings and publishing them to a Redis-based
        message bus. It handles the entire flow from data retrieval to publication,
        with proper error logging.
        
        Args:
            params (Dict[str, Any]): Parameters for fetching filings (ticker or cik, etc.)
            n_years (int): Number of years of filings to fetch
            form_type (str): SEC form type (e.g., '10-K')
            publisher (MessageBusPublisher): Instance of the message bus publisher
            topic (str): Name of the Redis stream/topic to publish to
            request_id: Optional request ID for tracing
            
        Note:
            This method is designed to be awaited in an asyncio event loop. It will
            perform the fetch operation synchronously (which can be slow for multiple years),
            but it allows the caller to continue with other async operations.
        """
        # Generate request_id if not provided
        if not request_id:
            request_id = str(uuid.uuid4())
            
        try:
            # Create error context for the operation
            context = ErrorContext(
                request_id=request_id,
                source_type="SEC-Publish",
                form_type=form_type,
                years=n_years
            )
            
            logger.info(f"[{request_id}] Starting fetch_last_n_years_and_publish for params={params}, n_years={n_years}, form_type={form_type}")
            
            # Fetch the filings with the same request_id for tracing
            envelopes = self.fetch_last_n_years(params, n_years, form_type, request_id=request_id)
            
            # Publish each envelope to the message bus
            for envelope in envelopes:
                # Add request_id to envelope metadata if not present
                if 'metadata' not in envelope:
                    envelope['metadata'] = {}
                if 'request_id' not in envelope['metadata']:
                    envelope['metadata']['request_id'] = request_id
                
                publisher.publish(topic, envelope)
                
            logger.info(f"[{request_id}] Successfully published {len(envelopes)} envelopes to topic '{topic}'")
            
        except (ValidationError, SECError) as e:
            # Log specific error types with context
            logger.error(f"[{request_id}] Error in fetch_last_n_years_and_publish: {e}")
            log_error(e)
        except Exception as e:
            # Log unexpected errors
            logger.error(f"[{request_id}] Unexpected error in fetch_last_n_years_and_publish: {e}")
            # If we have a context, wrap the error
            if 'context' in locals():
                error = SECError(f"Unexpected error fetching and publishing filings: {e}", context=context, cause=e)
                log_error(error)
    
    def fetch_insider_filings(self, params: Dict[str, Any], limit: int = 10, request_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Fetches the most recent Form 4 (insider trading) filings for a company.
        
        Form 4 filings report changes in ownership of company securities by insiders
        (directors, officers, and beneficial owners of more than 10% of a class of securities).
        This method retrieves these filings to provide visibility into insider trading
        activities, which can be valuable signals for company performance and sentiment.
        
        Args:
            params (Dict[str, Any]): Must include 'cik' or 'ticker'.
            limit (int): Maximum number of filings to return (default 10)
            
        Returns:
            List[Dict[str, Any]]: List of envelopes with Form 4 data and metadata.
            Each envelope contains:
                - content: The raw filing document (HTML or XML)
                - content_type: The format of the content ('html' or 'xml')
                - source: 'sec-form4'
                - status: 'success' or error status
                - metadata: Comprehensive filing metadata including filing date,
                  accession number, CIK, ticker, and source URL
            
        Example usage:
            sec = SECDataSource()
            result = sec.fetch_insider_filings({'ticker': 'AAPL'})
        """
        # Generate request_id if not provided
        if not request_id:
            request_id = str(uuid.uuid4())
        
        # Create error context
        context = ErrorContext(
            request_id=request_id,
            source_type="SEC-Form4"
        )
            
        # Check SEC API key first
        if not SEC_API_KEY:
            error_msg = "SEC_API_KEY is not set in the centralized configuration (settings.api_keys.sec)."
            logger.error(f"[{request_id}] {error_msg}")
            raise MissingConfigurationError(error_msg, context=context)
        
        # Validate and resolve company parameters
        try:
            cik, ticker = self._validate_and_resolve_company_params(params, request_id)
            context.company = {"cik": cik, "ticker": ticker}
        except (ValidationError, SECError) as e:
            logger.error(f"[{request_id}] Failed to validate company parameters for insider filings: {e}")
            raise
                
        # Prepare SEC API query for Form 4 filings
        query_url = f"{SEC_API_BASE_URL}?token={SEC_API_KEY}"
        query = {
            "query": {
                "query_string": {
                    "query": f"formType:\"4\" AND cik:{cik.lstrip('0')}"
                }
            },
            "from": "0",
            "size": str(limit),
            "sort": [{"filedAt": {"order": "desc"}}]
        }
        
        headers = {
            "User-Agent": settings.sec_user_agent,
            "Accept": "application/json",
        }
        
        try:
            logger.info(f"Fetching Form 4 (insider trading) filings for CIK {cik}")
            resp = httpx.post(query_url, json=query, headers=headers)
            
            if resp.status_code != 200:
                logger.error(f"SEC API Form 4 query returned {resp.status_code}: {resp.text}")
                raise ValueError(f"SEC API Form 4 query error: {resp.status_code}")
                
            data = resp.json()
            filings = data.get('filings', [])
            
            if not filings:
                logger.warning(f"No Form 4 filings found for CIK {cik}")
                return []
                
            results = []
            
            # Process each Form 4 filing
            for filing in filings:
                # Get document URLs
                doc_url = None
                content_type = None
                
                # Find the Form 4 document URL
                if 'documentFormatFiles' in filing:
                    for doc in filing['documentFormatFiles']:
                        if doc.get('type') == '4' and doc.get('documentUrl'):
                            doc_url = doc.get('documentUrl')
                            content_type = 'html' if doc_url.endswith('.htm') or doc_url.endswith('.html') else 'xml'
                            break
                
                # Fallback to linkToFilingDetails
                if not doc_url and filing.get('linkToFilingDetails'):
                    doc_url = filing.get('linkToFilingDetails')
                    content_type = 'html'
                
                # If we found a document URL, fetch it
                if doc_url:
                    try:
                        doc_resp = httpx.get(doc_url, headers=headers)
                        
                        if doc_resp.status_code == 200:
                            content = doc_resp.text
                            
                            # Create envelope
                            envelope = {
                                'content': content,
                                'content_type': content_type,
                                'source': 'sec-form4',
                                'status': 'success',
                                'metadata': {
                                    'cik': cik,
                                    'ticker': ticker,
                                    'form_type': '4',
                                    'filing_date': filing.get('filedAt'),
                                    'accession_no': filing.get('accessionNo'),
                                    'filing_info': filing,
                                    'filing_url': doc_url,
                                    'retrieved_at': datetime.datetime.utcnow().isoformat() + 'Z'
                                }
                            }
                            
                            results.append(envelope)
                        else:
                            logger.warning(f"Failed to fetch Form 4 document from {doc_url}: {doc_resp.status_code}")
                    except Exception as e:
                        logger.error(f"Error fetching Form 4 document from {doc_url}: {e}")
            
            logger.info(f"Successfully fetched {len(results)} Form 4 filings for CIK {cik}")
            return results
            
        except Exception as e:
            logger.error(f"Error fetching Form 4 filings from SEC API: {e}")
            raise ValueError(f"Error fetching Form 4 filings from SEC API: {e}")
    
    def fetch_xbrl_fact(self, params: Dict[str, Any], concept: str, request_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Fetches a specific XBRL fact (e.g., us-gaap:Revenues) for a company using sec-api.io.
        
        XBRL (eXtensible Business Reporting Language) is a structured data format that standardizes
        financial reporting elements. This method accesses specific financial data points
        in XBRL format, allowing for consistent extraction of standardized financial metrics
        across companies and time periods. This is particularly valuable for quantitative
        analysis and financial modeling.
        
        Args:
            params (Dict[str, Any]): Must include 'cik' or 'ticker'.
            concept (str): XBRL concept/tag (e.g., 'us-gaap:Revenues', 'us-gaap:NetIncomeLoss',
                          'us-gaap:Assets', 'us-gaap:Liabilities').
            
        Returns:
            Dict[str, Any]: Envelope with XBRL fact data and metadata.
            The returned envelope contains:
                - content: A dictionary with the XBRL fact data, typically including
                  values across multiple reporting periods
                - content_type: 'json'
                - source: 'sec-xbrl'
                - status: 'success' or error status
                - metadata: Information about the request including CIK, ticker,
                  concept name, and retrieval timestamp
            
        Example usage:
            sec = SECDataSource()
            result = sec.fetch_xbrl_fact({'ticker': 'AAPL'}, 'us-gaap:Revenues')
            
        Note:
            Common XBRL concepts include:
            - us-gaap:Revenues - Total revenue
            - us-gaap:NetIncomeLoss - Net income/loss
            - us-gaap:Assets - Total assets
            - us-gaap:Liabilities - Total liabilities
            - us-gaap:StockholdersEquity - Shareholders' equity
            - us-gaap:EarningsPerShareBasic - Basic EPS
            - us-gaap:OperatingIncomeLoss - Operating income
        """
        # Generate request_id if not provided
        if not request_id:
            request_id = str(uuid.uuid4())
        
        # Create error context
        context = ErrorContext(
            request_id=request_id,
            source_type="SEC-XBRL",
            xbrl_concept=concept
        )
            
        # Check SEC API key first
        if not SEC_API_KEY:
            error_msg = "SEC_API_KEY is not set in the centralized configuration (settings.api_keys.sec)."
            logger.error(f"[{request_id}] {error_msg}")
            raise MissingConfigurationError(error_msg, context=context)
        
        # Validate and resolve company parameters
        try:
            cik, ticker = self._validate_and_resolve_company_params(params, request_id)
            context.company = {"cik": cik, "ticker": ticker}
        except (ValidationError, SECError) as e:
            logger.error(f"[{request_id}] Failed to validate company parameters for XBRL concept {concept}: {e}")
            raise
                
        xbrl_url = f"{SEC_API_BASE_URL}/xbrl/companyconcept?cik={cik.lstrip('0')}&concept={concept}&token={SEC_API_KEY}"
        try:
            headers = {
                "User-Agent": settings.sec_user_agent,
                "Accept": "application/json",
            }
            
            logger.info(f"Fetching XBRL fact {concept} for CIK {cik}")
            resp = httpx.get(xbrl_url, headers=headers)
            
            if resp.status_code != 200:
                logger.error(f"SEC API XBRL endpoint returned {resp.status_code}: {resp.text}")
                raise ValueError(f"SEC API XBRL error: {resp.status_code}")
                
            data = resp.json()
            if not data or 'facts' not in data:
                raise ValueError(f"No XBRL facts found for CIK {cik}, concept {concept}")
                
            return {
                'content': data['facts'],
                'content_type': 'json',
                'source': 'sec-xbrl',
                'status': 'success',
                'metadata': {
                    'cik': cik,
                    'ticker': ticker,
                    'concept': concept,
                    'retrieved_at': datetime.datetime.utcnow().isoformat() + 'Z',
                    'xbrl_url': xbrl_url
                }
            }
        except Exception as e:
            logger.error(f"Error fetching XBRL fact from SEC API: {e}")
            raise ValueError(f"Error fetching XBRL fact from SEC API: {e}")


# Job queue integration for distributed processing
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
    
    sec_source = SECDataSource()
    publisher = MessageBusPublisher()
    start_time = time.time()
    
    try:
        logger.info(f"[RQ Worker] Starting SEC batch job for {len(batch_params)} companies, n_years={n_years}, form_type={form_type}")
        
        # Process each company
        for company_params in batch_params:
            try:
                # Fetch and publish filings
                envelopes = sec_source.fetch_last_n_years(company_params, n_years, form_type)
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