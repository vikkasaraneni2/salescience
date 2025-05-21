"""
SEC EDGAR API Client
-------------------

Focused client for communicating with the SEC EDGAR API to fetch financial filings
and company data. This module provides core SEC API functionality without mixing
concerns with message publishing, worker management, or other responsibilities.

Key responsibilities:
1. SEC API authentication and communication
2. Company identifier resolution (ticker ↔ CIK)
3. Filing retrieval and standardized data envelope creation
4. Error handling specific to SEC API interactions
5. Request retry logic and resilience patterns

This client is designed to be used by specialized modules that handle specific
filing types or processing workflows.
"""

import httpx
import logging
import datetime
import time
import random
import uuid
from typing import Dict, Any, Optional, Tuple

from config import settings
from data_sources.base import BaseDataSource
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

# Core configuration from centralized settings
SEC_API_KEY = settings.api_keys.sec
SEC_API_BASE_URL = settings.service_urls.sec_api_base
SEC_EDGAR_BASE_URL = settings.service_urls.sec_edgar_base


def get_cik_for_ticker(ticker: str, request_id: Optional[str] = None, max_retries: int = 3) -> str:
    """
    Resolves a ticker symbol to a CIK (Central Index Key) using the sec-api.io mapping endpoint.
    
    The CIK is a unique identifier assigned by the SEC to companies and individuals who
    file disclosures with the SEC. This function provides the essential mapping between
    common stock ticker symbols (e.g., AAPL) and their corresponding SEC identifiers.
    
    Args:
        ticker: The stock ticker symbol (e.g., 'AAPL', 'MSFT', 'GOOGL')
        request_id: Optional request ID for tracing and logging
        max_retries: Maximum number of retry attempts for resilience
        
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
    
    # Prepare request with proper headers
    headers = {
        "User-Agent": settings.sec_user_agent,
        "Accept": "application/json",
        "X-Request-ID": request_id or str(uuid.uuid4())
    }
    
    # Build the sec-api.io mapping endpoint URL
    url = f"{SEC_API_BASE_URL}/mapping/ticker/{ticker}?token={SEC_API_KEY}"
    
    # Retry logic with exponential backoff
    attempt = 0
    delay = 1.0
    
    while attempt < max_retries:
        try:
            logger.info(f"[{request_id}] Resolving ticker '{ticker}' to CIK via sec-api.io (attempt {attempt+1}/{max_retries})")
            
            # Set timeout for API call
            resp = httpx.get(url, headers=headers, timeout=10.0)
            
            # Handle different response scenarios
            if resp.status_code == 200:
                try:
                    data = resp.json()
                    cik = data.get('cik')
                    
                    if cik:
                        # Ensure CIK is properly formatted as 10-digit zero-padded string
                        padded_cik = str(cik).zfill(10)
                        logger.info(f"[{request_id}] Successfully resolved ticker '{ticker}' to CIK '{padded_cik}'")
                        return padded_cik
                    else:
                        error_msg = f"Response from sec-api.io mapping API did not contain CIK for ticker '{ticker}'"
                        logger.error(f"[{request_id}] {error_msg}")
                        raise SECNotFoundError(error_msg, context=context)
                        
                except (ValueError, KeyError) as e:
                    error_msg = f"Invalid JSON response from sec-api.io mapping API for ticker '{ticker}': {e}"
                    logger.error(f"[{request_id}] {error_msg}")
                    raise SECParsingError(error_msg, context=context, cause=e)
                    
            elif resp.status_code == 401:
                error_msg = f"Authentication failed for sec-api.io mapping API (ticker: {ticker}). Check SEC_API_KEY."
                logger.error(f"[{request_id}] {error_msg}")
                raise SECAuthenticationError(error_msg, context=context)
                
            elif resp.status_code == 404:
                error_msg = f"Ticker '{ticker}' not found in SEC database"
                logger.error(f"[{request_id}] {error_msg}")
                raise SECNotFoundError(error_msg, context=context)
                
            elif resp.status_code == 429:
                error_msg = f"Rate limit exceeded for sec-api.io mapping API (ticker: {ticker})"
                logger.warning(f"[{request_id}] {error_msg}")
                raise SECRateLimitError(error_msg, context=context)
                
            else:
                error_msg = f"SEC mapping API returned unexpected status {resp.status_code} for ticker '{ticker}': {resp.text}"
                logger.error(f"[{request_id}] {error_msg}")
                raise SECError(error_msg, context=context)
                
        except (httpx.TimeoutException, httpx.ConnectTimeout, httpx.ReadTimeout) as e:
            error_msg = f"Timeout occurred while resolving ticker '{ticker}' to CIK"
            logger.warning(f"[{request_id}] {error_msg} (attempt {attempt+1}/{max_retries}): {e}")
            
            if attempt >= max_retries - 1:
                raise DataSourceTimeoutError(error_msg, context=context, cause=e)
                
        except (httpx.NetworkError, httpx.ConnectError) as e:
            error_msg = f"Network error while resolving ticker '{ticker}' to CIK"
            logger.warning(f"[{request_id}] {error_msg} (attempt {attempt+1}/{max_retries}): {e}")
            
            if attempt >= max_retries - 1:
                raise DataSourceConnectionError(error_msg, context=context, cause=e)
                
        except (SECAuthenticationError, SECNotFoundError, SECRateLimitError, SECError, SECParsingError):
            # These are SEC-specific errors that shouldn't be retried
            raise
            
        except Exception as e:
            error_msg = f"Unexpected error while resolving ticker '{ticker}' to CIK"
            logger.error(f"[{request_id}] {error_msg} (attempt {attempt+1}/{max_retries}): {e}")
            
            if attempt >= max_retries - 1:
                raise SECError(error_msg, context=context, cause=e)
        
        # Exponential backoff with jitter for retries
        attempt += 1
        if attempt < max_retries:
            jitter = 0.1 * delay * (random.random() * 2 - 1)
            delay = min(delay * 2 + jitter, 30.0)
            
            logger.warning(f"[{request_id}] Retrying ticker resolution in {delay:.2f}s")
            time.sleep(delay)


class SECClient(BaseDataSource):
    """
    Core SEC EDGAR API client for fetching financial filings and data.
    
    This client provides focused functionality for communicating with the SEC EDGAR API,
    handling authentication, company identifier resolution, and basic filing retrieval.
    It follows the single responsibility principle by focusing solely on SEC API
    communication without mixing concerns like message publishing or job management.
    
    Key capabilities:
    - Company identifier resolution (ticker ↔ CIK)
    - Filing search and retrieval
    - Standardized data envelope creation
    - Proper error handling and retry logic
    - Request tracing for distributed debugging
    
    Usage:
        client = SECClient()
        filing = client.fetch({
            'ticker': 'AAPL',
            'form_type': '10-K',
            'year': 2023
        })
    """
    
    def __init__(self):
        """Initialize the SEC client."""
        self.name = "SEC"
        
        # Verify API key is available from centralized configuration
        if not SEC_API_KEY:
            logger.warning("SEC_API_KEY not set in settings.api_keys.sec. SEC client will not function.")
    
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
        
        This is the core method for retrieving SEC filings. It handles company
        identifier resolution, constructs appropriate SEC API queries, and returns
        standardized data envelopes.
        
        Args:
            params: A dictionary with fetch parameters:
                - cik: (Optional) Central Index Key (CIK) for the company
                - ticker: (Optional) Stock ticker symbol (will be resolved to CIK if provided)
                - form_type: (Optional) Form type to fetch (default: '10-K')
                - year: (Optional) Year of filing to fetch
            request_id: Optional request ID for tracing
        
        Returns:
            A dictionary with the following keys:
                - content: The raw filing content
                - content_type: Content type (html, txt, etc.)
                - source: 'sec'
                - status: 'success'
                - metadata: Additional filing metadata
                
        Raises:
            MissingConfigurationError: If SEC_API_KEY is not configured
            ValidationError: If required parameters are missing
            SECError: If SEC API call fails
            SECNotFoundError: If no filings are found matching criteria
            SECAuthenticationError: If authentication fails
            SECRateLimitError: If rate limit is exceeded
        """
        # Generate request_id if not provided
        if not request_id:
            request_id = str(uuid.uuid4())
            
        # Extract form_type and year parameters
        form_type = params.get('form_type', '10-K')
        year = params.get('year')
        
        # Validate form type - support common SEC form types
        valid_form_types = [
            '10-K', '10-Q', '8-K', '20-F', '40-F',   # Most common reports
            'DEF 14A', 'DEFA14A', 'DEFM14A',         # Proxy statements
            'S-1', 'S-3', 'S-4', 'F-1',              # Registration statements
            '4', '13F', '13G', '13D',                # Ownership filings
            'SD', 'CORRESP', 'FWP',                  # Specialized filings
            '10-K/A', '10-Q/A', '8-K/A'              # Amended filings
        ]
        
        if form_type not in valid_form_types:
            logger.warning(f"[{request_id}] Unusual form type '{form_type}' - proceeding anyway")
        
        logger.info(f"[{request_id}] Fetching SEC filing: form_type={form_type}, year={year}")
        
        # Create initial error context
        context = ErrorContext(
            request_id=request_id,
            source_type="SEC",
            form_type=form_type,
            year=year
        )
        
        # Verify API key is available
        if not SEC_API_KEY:
            error_msg = "SEC_API_KEY is not set in the centralized configuration (settings.api_keys.sec)."
            logger.error(f"[{request_id}] {error_msg}")
            raise MissingConfigurationError(error_msg, context=context)
            
        # Prepare request headers
        headers = {
            "User-Agent": settings.sec_user_agent,
            "Accept": "application/json",
            "X-Request-ID": request_id
        }
        
        logger.debug(f"[{request_id}] Using SEC API key: {SEC_API_KEY[:8]}...")
        
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
            
            # SEC-API.io Query API endpoint
            query_url = f"{SEC_API_BASE_URL}?token={SEC_API_KEY}"
            
            logger.debug(f"[{request_id}] SEC API query URL: {query_url}")
            logger.debug(f"[{request_id}] SEC API query payload: {query}")
            
            # Send the request to the SEC API
            resp = httpx.post(query_url, json=query, headers=headers, timeout=15.0)
            
            # Log response details
            logger.debug(f"[{request_id}] SEC API query status: {resp.status_code}")
            
            if resp.status_code != 200:
                error_msg = f"SEC API query returned status {resp.status_code}: {resp.text}"
                logger.error(f"[{request_id}] {error_msg}")
                
                # Map error based on status code
                if resp.status_code == 401 or resp.status_code == 403:
                    raise SECAuthenticationError(f"SEC API authentication failed: {resp.text}", context=context)
                elif resp.status_code == 429:
                    raise SECRateLimitError(f"SEC API rate limit exceeded: {resp.text}", context=context)
                else:
                    raise SECError(f"SEC API query error: {resp.status_code} - {resp.text}", context=context)
            
            # Process the response
            query_result = resp.json()
            logger.debug(f"[{request_id}] SEC API query response: {query_result}")
            
            filings = query_result.get('filings', [])
            if not filings:
                error_msg = f"No {form_type} filings found for CIK {cik} (year={year}) via SEC API."
                logger.warning(f"[{request_id}] {error_msg}")
                raise SECNotFoundError(error_msg, context=context)
            
            # Get the most recent filing
            filing = filings[0]
            logger.info(f"[{request_id}] Found filing: accessionNo={filing.get('accessionNo')}, filedAt={filing.get('filedAt')}")
            
            # Get the document URL
            accession_no = filing.get('accessionNo', '').replace('-', '')
            cik_no_leading_zeros = cik.lstrip('0')
            
            # Try to fetch the actual filing document
            doc_url = None
            document_content = None
            content_type = 'html'
            
            # Construct potential document URLs
            urls_to_try = []
            
            # First, try to use URLs from the filing metadata
            if filing.get('linkToFilingDetails'):
                urls_to_try.append(filing['linkToFilingDetails'])
            
            # Fallback to constructing EDGAR URLs
            if accession_no and cik_no_leading_zeros:
                # Primary document format
                urls_to_try.append(f"{SEC_EDGAR_BASE_URL}/{cik_no_leading_zeros}/{accession_no}/{accession_no}.txt")
                # HTML format
                urls_to_try.append(f"{SEC_EDGAR_BASE_URL}/{cik_no_leading_zeros}/{accession_no}/{accession_no}-index.html")
            
            # Try each URL until we get a successful response
            for url in urls_to_try:
                try:
                    logger.debug(f"[{request_id}] Trying document URL: {url}")
                    doc_resp = httpx.get(url, headers=headers, timeout=30.0)
                    
                    if doc_resp.status_code == 200:
                        document_content = doc_resp.text
                        doc_url = url
                        
                        # Determine content type based on URL and response
                        if url.endswith('.txt'):
                            content_type = 'text'
                        elif url.endswith('.html') or 'html' in doc_resp.headers.get('content-type', ''):
                            content_type = 'html'
                        else:
                            content_type = 'text'
                            
                        logger.info(f"[{request_id}] Successfully fetched document from: {url}")
                        break
                        
                except Exception as e:
                    logger.debug(f"[{request_id}] Failed to fetch document from {url}: {e}")
                    continue
            
            if not document_content:
                error_msg = f"Could not fetch document content for filing {accession_no}"
                logger.error(f"[{request_id}] {error_msg}")
                raise SECError(error_msg, context=context)
            
            # Return the standardized filing data envelope
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
                    'filing_url': doc_url,
                    'request_id': request_id
                }
            }
            
        except (SECError, SECAuthenticationError, SECRateLimitError, SECNotFoundError):
            # Re-raise SEC-specific errors without wrapping
            raise
        except httpx.TimeoutException as e:
            error_msg = f"Timeout while fetching SEC filing"
            logger.error(f"[{request_id}] {error_msg}: {e}")
            raise DataSourceTimeoutError(error_msg, context=context, cause=e)
        except httpx.NetworkError as e:
            error_msg = f"Network error while fetching SEC filing"
            logger.error(f"[{request_id}] {error_msg}: {e}")
            raise DataSourceConnectionError(error_msg, context=context, cause=e)
        except Exception as e:
            error_msg = f"Unexpected error fetching filing from SEC API"
            logger.error(f"[{request_id}] {error_msg}: {e}")
            raise SECError(error_msg, context=context, cause=e)