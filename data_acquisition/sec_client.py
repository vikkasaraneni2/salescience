import os
import httpx
import logging
import datetime
import asyncio
import redis
import json
from typing import Dict, Any, List, Optional
from abc import ABC, abstractmethod
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s %(name)s: %(message)s'
)
logger = logging.getLogger("sec_client")

# Environment variables
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    logger.warning("dotenv not installed, relying on existing environment variables")

# Get API key and Redis URL from environment
SEC_API_KEY = os.getenv("SEC_API_KEY")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

# SEC API configuration
SEC_API_BASE_URL = "https://api.sec-api.io"
SEC_EDGAR_BASE_URL = "https://www.sec.gov/Archives/edgar/data"

# Optional: Prometheus metrics for observability
try:
    from prometheus_client import Counter, Histogram, start_http_server
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


def get_job_redis_key(organization_id: Optional[str], job_id: str, key_type: str) -> str:
    """
    Creates a Redis key with organization prefix for proper namespace isolation.
    
    Args:
        organization_id: The organization ID for namespacing
        job_id: The unique job identifier
        key_type: Type of key (status, result, overall_status, etc.)
    
    Returns:
        Properly formatted Redis key with organization prefix
    """
    if not organization_id:
        logger.warning(f"No organization_id provided for Redis key (job={job_id}, type={key_type})")
        return f"job:{job_id}:{key_type}"
    return f"org:{organization_id}:job:{job_id}:{key_type}"


def get_cik_for_ticker(ticker: str) -> str:
    """
    Resolves a ticker symbol to a CIK using the sec-api.io mapping endpoint.
    Returns the CIK as a zero-padded string, or raises ValueError if not found.
    
    Args:
        ticker: The stock ticker symbol (e.g., 'AAPL')
        
    Returns:
        Zero-padded CIK string (e.g., '0000320193')
        
    Raises:
        ValueError: If ticker can't be resolved or API error occurs
    """
    if not SEC_API_KEY:
        error_msg = "SEC_API_KEY is not set in the environment."
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    # Correct endpoint format based on SEC-API.io documentation
    url = f"{SEC_API_BASE_URL}/mapping/ticker/{ticker}?token={SEC_API_KEY}"
    
    logger.info(f"Resolving ticker '{ticker}' to CIK via sec-api.io")
    logger.debug(f"SEC API request URL: {url}")
    
    try:
        resp = httpx.get(url, timeout=10.0)
        
        # Log response details for debugging
        logger.debug(f"SEC API status code: {resp.status_code}")
        logger.debug(f"SEC API response headers: {resp.headers}")
        
        if resp.status_code != 200:
            error_msg = f"SEC API returned status {resp.status_code}: {resp.text}"
            logger.error(error_msg)
            raise ValueError(f"sec-api.io error: {resp.status_code}")
        
        # Parse the response
        data = resp.json()
        logger.debug(f"SEC API response: {data}")
        
        # The API returns a list of results
        if isinstance(data, list) and len(data) > 0:
            # Get CIK from first result
            cik = data[0].get("cik")
            if cik:
                padded_cik = str(cik).zfill(10)
                logger.info(f"Resolved ticker '{ticker}' to CIK '{padded_cik}'")
                return padded_cik
        
        # If we got no matches or no CIK field, raise an error
        error_msg = f"Ticker '{ticker}' not found in SEC API response: {data}"
        logger.error(error_msg)
        raise ValueError(f"Ticker '{ticker}' not found in SEC API response.")
        
    except Exception as e:
        error_msg = f"Error fetching CIK for ticker '{ticker}' from SEC API: {e}"
        logger.error(error_msg)
        raise ValueError(error_msg)


class SECDataSource(BaseDataSource):
    """
    Data source for SEC filings using the sec-api.io service.
    """
    
    def __init__(self):
        """Initialize the SEC data source."""
        self.name = "SEC"
        
        # Verify API key is available
        if not SEC_API_KEY:
            logger.warning("SEC_API_KEY not set. SEC data source will not function.")
    
    def fetch(self, params: Dict[str, Any]) -> Dict[str, Any]:
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
        # Extract parameters
        ticker = params.get('ticker')
        cik = params.get('cik')
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
        
        logger.info(f"Fetching SEC filing: ticker={ticker}, cik={cik}, form_type={form_type}, year={year}")
        
        # Verify API key is available
        if not SEC_API_KEY:
            error_msg = "SEC_API_KEY is not set in the environment."
            logger.error(error_msg)
            raise ValueError(error_msg)
            
        # Log user agent and contact info for SEC compliance
        headers = {
            "User-Agent": os.getenv("SEC_USER_AGENT", "salescience/1.0"),
            "Accept": "application/json",
        }
        
        logger.debug(f"Using SEC API key: {SEC_API_KEY[:5]}...{SEC_API_KEY[-5:]}")
        
        # Robust ticker-to-CIK resolution
        if not cik:
            if ticker:
                logger.info(f"Resolving ticker {ticker} to CIK")
                cik = get_cik_for_ticker(ticker)
                logger.info(f"Ticker {ticker} resolved to CIK {cik}")
            else:
                raise ValueError("Parameter 'cik' or 'ticker' is required for SECDataSource.fetch")
        
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
                raise ValueError(f"SEC API query error: {resp.status_code}")
            
            # Process the response
            query_result = resp.json()
            logger.debug(f"SEC API query response: {query_result}")
            
            filings = query_result.get('filings', [])
            if not filings:
                error_msg = f"No {form_type} filings found for CIK {cik} (year={year}) via SEC API."
                logger.warning(error_msg)
                raise ValueError(error_msg)
            
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
    
    def fetch_quarterly_reports(self, params: Dict[str, Any], n_quarters: int = 4) -> List[Dict[str, Any]]:
        """
        Fetches the most recent quarterly reports (10-Q) for a company.
        
        Args:
            params (Dict[str, Any]): Must include 'cik' or 'ticker'.
            n_quarters (int): Number of recent quarters to fetch (default 4)
            
        Returns:
            List[Dict[str, Any]]: List of envelopes with 10-Q data and metadata.
            
        Example usage:
            sec = SECDataSource()
            results = sec.fetch_quarterly_reports({'ticker': 'AAPL'})
        """
        # This function will use the fetch_last_n_years logic but with form_type='10-Q'
        return self.fetch_last_n_years(params, n_quarters, form_type='10-Q')
    
    def fetch_material_events(self, params: Dict[str, Any], limit: int = 10) -> List[Dict[str, Any]]:
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
        cik = params.get('cik')
        ticker = params.get('ticker')
        
        if not SEC_API_KEY:
            logger.error("SEC_API_KEY is not set in the environment.")
            raise ValueError("SEC_API_KEY is not set in the environment.")
            
        if not cik:
            if ticker:
                cik = get_cik_for_ticker(ticker)
            else:
                raise ValueError("Parameter 'cik' or 'ticker' is required for fetch_material_events")
                
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
            "User-Agent": os.getenv("SEC_USER_AGENT", "salescience/1.0"),
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
    
    def fetch_last_n_years(self, params: Dict[str, Any], n_years: int = 5, form_type: str = '10-K') -> List[Dict[str, Any]]:
        """
        Fetch filings for the last N years.
        
        Args:
            params: Base parameters for fetch method
            n_years: Number of years to fetch
            form_type: Form type to fetch
            
        Returns:
            List of fetch results, one per year
        """
        results = []
        current_year = datetime.datetime.now().year
        
        # Extract parameters
        cik = params.get('cik')
        ticker = params.get('ticker')
        
        if not cik:
            if ticker:
                logger.info(f"Resolving ticker {ticker} to CIK for multi-year fetch")
                cik = get_cik_for_ticker(ticker)
                logger.info(f"Ticker {ticker} resolved to CIK {cik}")
            else:
                raise ValueError("Parameter 'cik' or 'ticker' is required for fetch_last_n_years")
        
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
                "User-Agent": os.getenv("SEC_USER_AGENT", "salescience/1.0"),
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
    
    async def fetch_last_n_years_and_publish(self, params: Dict[str, Any], n_years: int, form_type: str, publisher: MessageBusPublisher, topic: str):
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
    
    def fetch_insider_filings(self, params: Dict[str, Any], limit: int = 10) -> List[Dict[str, Any]]:
        """
        Fetches the most recent Form 4 (insider trading) filings for a company.
        
        Args:
            params (Dict[str, Any]): Must include 'cik' or 'ticker'.
            limit (int): Maximum number of filings to return (default 10)
            
        Returns:
            List[Dict[str, Any]]: List of envelopes with Form 4 data and metadata.
            
        Example usage:
            sec = SECDataSource()
            result = sec.fetch_insider_filings({'ticker': 'AAPL'})
        """
        cik = params.get('cik')
        ticker = params.get('ticker')
        
        if not SEC_API_KEY:
            logger.error("SEC_API_KEY is not set in the environment.")
            raise ValueError("SEC_API_KEY is not set in the environment.")
            
        if not cik:
            if ticker:
                cik = get_cik_for_ticker(ticker)
            else:
                raise ValueError("Parameter 'cik' or 'ticker' is required for fetch_insider_filings")
                
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
            "User-Agent": os.getenv("SEC_USER_AGENT", "salescience/1.0"),
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
        
        if not SEC_API_KEY:
            logger.error("SEC_API_KEY is not set in the environment.")
            raise ValueError("SEC_API_KEY is not set in the environment.")
            
        if not cik:
            if ticker:
                cik = get_cik_for_ticker(ticker)
            else:
                raise ValueError("Parameter 'cik' or 'ticker' is required for fetch_xbrl_fact")
                
        xbrl_url = f"{SEC_API_BASE_URL}/xbrl/companyconcept?cik={cik.lstrip('0')}&concept={concept}&token={SEC_API_KEY}"
        try:
            headers = {
                "User-Agent": os.getenv("SEC_USER_AGENT", "salescience/1.0"),
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


# Job queue integration
def process_sec_batch_job(batch_params, n_years, form_type, topic):
    """
    Job handler for SEC batch jobs. Fetches filings for all companies in batch_params
    and publishes each envelope to the message bus. Tracks Prometheus metrics for
    job count, failures, and duration.
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
    
    Args:
        queue_name: Name of the Redis queue to listen on
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


# Containerization support
if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description="SEC Batch Worker Entrypoint")
    parser.add_argument('--queue', type=str, default='sec_batch', help='RQ queue name to listen on (default: sec_batch)')
    parser.add_argument('--metrics-port', type=int, default=8001, help='Port to serve Prometheus metrics on (default: 8001)')
    args = parser.parse_args()
    
    # Start Prometheus metrics server if enabled
    if PROMETHEUS_ENABLED:
        try:
            start_http_server(args.metrics_port)
            logger.info(f"Prometheus metrics server started on port {args.metrics_port}")
        except Exception as e:
            logger.error(f"Failed to start Prometheus metrics server: {e}")
    
    # Start worker
    if RQ_ENABLED:
        run_sec_batch_worker(args.queue)
    else:
        logger.error("RQ is not installed. Cannot run worker.")