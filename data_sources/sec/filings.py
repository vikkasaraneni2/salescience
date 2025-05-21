"""
SEC Filings Handler
------------------

Specialized handler for different types of SEC filings. This module focuses on
the business logic for retrieving and processing specific filing types while
delegating core API communication to the SECClient.

Key responsibilities:
1. Handling different filing types (10-K, 10-Q, 8-K, Form 4, etc.)
2. Multi-year and multi-quarter retrieval logic
3. Filing-specific document processing and URL resolution
4. Creating specialized data envelopes for different filing types

This handler uses the delegation pattern to leverage the core SECClient
for API communication while providing specialized functionality for
different filing workflows.
"""

import httpx
import datetime
import uuid
from typing import Dict, Any, List, Optional, Tuple

from config import settings
from .client import SECClient
from data_acquisition.errors import (
    SalescienceError, ErrorContext,
    SECError, SECAuthenticationError, SECRateLimitError, SECNotFoundError, SECParsingError,
    MissingConfigurationError, DataSourceConnectionError, DataSourceTimeoutError,
    ValidationError, log_error
)

# Get centralized logging configuration
from data_acquisition.utils import configure_logging

# Get logger specific to this module
logger = configure_logging("sec_filings")

# Configuration from centralized settings
SEC_API_KEY = settings.api_keys.sec
SEC_API_BASE_URL = settings.service_urls.sec_api_base
SEC_EDGAR_BASE_URL = settings.service_urls.sec_edgar_base


class SECFilingsHandler:
    """
    Specialized handler for different types of SEC filings.
    
    This class provides focused functionality for retrieving specific types of
    SEC filings, handling the complexities of multi-year retrievals, document
    format variations, and filing-specific processing logic.
    
    It uses the delegation pattern, relying on SECClient for core API communication
    while implementing specialized business logic for different filing types.
    
    Usage:
        client = SECClient()
        handler = SECFilingsHandler(client)
        
        # Get last 3 years of 10-K filings
        annual_reports = handler.fetch_last_n_years({
            'ticker': 'AAPL',
            'form_type': '10-K'
        }, n_years=3)
        
        # Get recent quarterly reports
        quarterly = handler.fetch_quarterly_reports({
            'ticker': 'AAPL'
        }, n_quarters=4)
    """
    
    def __init__(self, sec_client: Optional[SECClient] = None):
        """
        Initialize the filings handler.
        
        Args:
            sec_client: Optional SECClient instance. If not provided, creates a new one.
        """
        self.client = sec_client or SECClient()
        
        # Verify API key is available
        if not SEC_API_KEY:
            logger.warning("SEC_API_KEY not set in settings.api_keys.sec. SEC filings handler will not function.")
    
    def fetch_last_n_years(self, params: Dict[str, Any], n_years: int = 5, 
                          form_type: str = '10-K', request_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Fetch filings for the last N years for a specific company.
        
        This method retrieves SEC filings (default: 10-K annual reports) for the specified
        number of years, attempting to get one filing per year. It handles all the 
        complexity of finding and retrieving these documents from multiple potential
        sources, with fallback options for reliability.
        
        Args:
            params: Base parameters that must include either 'cik' or 'ticker'
            n_years: Number of years to fetch, defaults to 5 years of historical data
            form_type: Form type to fetch (e.g., '10-K', '10-Q', '8-K', etc.)
            request_id: Optional request ID for tracing
            
        Returns:
            List of filing envelopes, with one envelope per year where available.
            Each envelope contains filing content and comprehensive metadata.
            If a filing isn't available for a particular year, the envelope will
            have status='error' or status='not_found' with appropriate error details.
        """
        results = []
        current_year = datetime.datetime.now().year
        
        # Generate request_id if not provided
        if not request_id:
            request_id = str(uuid.uuid4())
            
        logger.info(f"[{request_id}] Fetching {form_type} filings for last {n_years} years")
        
        # Create error context for the overall operation
        context = ErrorContext(
            request_id=request_id,
            source_type="SEC",
            form_type=form_type,
            years=n_years
        )
        
        # Validate and resolve company parameters using the client
        try:
            cik, ticker = self.client._validate_and_resolve_company_params(params, request_id)
        except (ValidationError, SECError) as e:
            logger.error(f"[{request_id}] Failed to validate company parameters: {e}")
            
            # Return error envelope for the validation failure
            error_envelope = self._create_error_envelope(
                str(e), form_type, params.get('cik'), params.get('ticker'), request_id
            )
            return [error_envelope] * n_years
            
        # Update the context with resolved company information
        context.company = {"cik": cik, "ticker": ticker}
        
        try:
            # Build query for all filings in the time range
            end_year = current_year
            start_year = current_year - n_years + 1
            
            # Use the SEC API query functionality
            query = self._build_sec_api_query(cik, form_type, start_year, end_year, n_years * 2)
            filings = self._execute_sec_api_query(query, request_id)
            
            # Group filings by year
            seen_years = set()
            for filing in filings:
                filing_date = filing.get('filedAt', '')
                if filing_date:
                    filing_year = int(filing_date.split('-')[0])
                    
                    # Only process if we haven't seen this year yet and it's in our target range
                    if filing_year not in seen_years and start_year <= filing_year <= end_year:
                        seen_years.add(filing_year)
                        
                        # Process this filing
                        filing_envelope = self._process_single_filing(
                            filing, cik, ticker, form_type, request_id
                        )
                        results.append(filing_envelope)
                        
                        # If we have enough results, stop
                        if len(results) >= n_years:
                            break
            
            # Fill in missing years with not_found envelopes
            for year in range(start_year, end_year + 1):
                if year not in seen_years:
                    not_found_envelope = self._create_not_found_envelope(
                        form_type, cik, ticker, year, request_id
                    )
                    results.append(not_found_envelope)
            
            # Sort results by year (most recent first)
            results.sort(key=lambda x: x['metadata'].get('year', 0), reverse=True)
            
            # Trim to exactly n_years results
            results = results[:n_years]
            
            logger.info(f"[{request_id}] Successfully processed {len(results)} years of {form_type} filings")
            return results
            
        except Exception as e:
            error_msg = f"Error fetching {form_type} filings for last {n_years} years"
            logger.error(f"[{request_id}] {error_msg}: {e}")
            
            # Return error envelopes for all requested years
            error_envelope = self._create_error_envelope(
                f"{error_msg}: {e}", form_type, cik, ticker, request_id
            )
            return [error_envelope] * n_years
    
    def fetch_quarterly_reports(self, params: Dict[str, Any], n_quarters: int = 4, 
                              request_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Fetches the most recent quarterly reports (10-Q) for a company.
        
        Args:
            params: Dictionary that must include 'cik' or 'ticker'
            n_quarters: Number of recent quarters to fetch (default 4)
            request_id: Optional request ID for tracing
            
        Returns:
            List of envelopes with 10-Q data and metadata.
            
        Example usage:
            handler = SECFilingsHandler()
            results = handler.fetch_quarterly_reports({'ticker': 'AAPL'})
        """
        # Generate request_id if not provided
        if not request_id:
            request_id = str(uuid.uuid4())
            
        logger.info(f"[{request_id}] Fetching quarterly reports for {n_quarters} quarters")
        
        # Use the fetch_last_n_years logic but with form_type='10-Q'
        # For quarters, we treat n_quarters as n_years since we're looking for recent filings
        return self.fetch_last_n_years(params, n_quarters, form_type='10-Q', request_id=request_id)
    
    def fetch_material_events(self, params: Dict[str, Any], limit: int = 10, 
                            request_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Fetches the most recent Form 8-K (material events) filings for a company.
        
        Args:
            params: Dictionary that must include 'cik' or 'ticker'
            limit: Maximum number of filings to return (default 10)
            request_id: Optional request ID for tracing
            
        Returns:
            List of envelopes with 8-K data and metadata.
            
        Example usage:
            handler = SECFilingsHandler()
            results = handler.fetch_material_events({'ticker': 'AAPL'})
        """
        # Generate request_id if not provided
        if not request_id:
            request_id = str(uuid.uuid4())
            
        logger.info(f"[{request_id}] Fetching material events (Form 8-K) filings, limit={limit}")
        
        # Create error context
        context = ErrorContext(
            request_id=request_id,
            source_type="SEC-8K"
        )
        
        # Validate and resolve company parameters using the client
        try:
            cik, ticker = self.client._validate_and_resolve_company_params(params, request_id)
            context.company = {"cik": cik, "ticker": ticker}
        except (ValidationError, SECError) as e:
            logger.error(f"[{request_id}] Failed to validate company parameters for material events: {e}")
            raise
                
        # Prepare SEC API query for Form 8-K filings
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
        
        try:
            filings = self._execute_sec_api_query(query, request_id)
            
            results = []
            
            # Process each Form 8-K filing
            for filing in filings:
                try:
                    filing_envelope = self._process_single_filing(
                        filing, cik, ticker, '8-K', request_id
                    )
                    results.append(filing_envelope)
                except Exception as e:
                    logger.warning(f"[{request_id}] Failed to process 8-K filing {filing.get('accessionNo', 'unknown')}: {e}")
                    # Continue with other filings even if one fails
                    continue
            
            logger.info(f"[{request_id}] Successfully fetched {len(results)} Form 8-K filings for CIK {cik}")
            return results
            
        except Exception as e:
            error_msg = f"Error fetching Form 8-K filings"
            logger.error(f"[{request_id}] {error_msg}: {e}")
            raise SECError(error_msg, context=context, cause=e)
    
    def fetch_insider_filings(self, params: Dict[str, Any], limit: int = 10, 
                            request_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Fetches the most recent Form 4 (insider trading) filings for a company.
        
        Form 4 filings report changes in ownership of company securities by insiders
        (directors, officers, and beneficial owners of more than 10% of a class of securities).
        
        Args:
            params: Dictionary that must include 'cik' or 'ticker'
            limit: Maximum number of filings to return (default 10)
            request_id: Optional request ID for tracing
            
        Returns:
            List of envelopes with Form 4 data and metadata.
            Each envelope contains:
                - content: The raw filing document (HTML or XML)
                - content_type: The format of the content ('html' or 'xml')
                - source: 'sec-form4'
                - status: 'success' or error status
                - metadata: Comprehensive filing metadata
            
        Example usage:
            handler = SECFilingsHandler()
            results = handler.fetch_insider_filings({'ticker': 'AAPL'})
        """
        # Generate request_id if not provided
        if not request_id:
            request_id = str(uuid.uuid4())
        
        logger.info(f"[{request_id}] Fetching insider trading (Form 4) filings, limit={limit}")
        
        # Create error context
        context = ErrorContext(
            request_id=request_id,
            source_type="SEC-Form4"
        )
        
        # Validate and resolve company parameters using the client
        try:
            cik, ticker = self.client._validate_and_resolve_company_params(params, request_id)
            context.company = {"cik": cik, "ticker": ticker}
        except (ValidationError, SECError) as e:
            logger.error(f"[{request_id}] Failed to validate company parameters for insider filings: {e}")
            raise
                
        # Prepare SEC API query for Form 4 filings
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
        
        try:
            filings = self._execute_sec_api_query(query, request_id)
            
            results = []
            
            # Process each Form 4 filing
            for filing in filings:
                try:
                    filing_envelope = self._process_single_filing(
                        filing, cik, ticker, '4', request_id, source_suffix='-form4'
                    )
                    results.append(filing_envelope)
                except Exception as e:
                    logger.warning(f"[{request_id}] Failed to process Form 4 filing {filing.get('accessionNo', 'unknown')}: {e}")
                    # Continue with other filings even if one fails
                    continue
            
            logger.info(f"[{request_id}] Successfully fetched {len(results)} Form 4 filings for CIK {cik}")
            return results
            
        except Exception as e:
            error_msg = f"Error fetching Form 4 filings"
            logger.error(f"[{request_id}] {error_msg}: {e}")
            raise SECError(error_msg, context=context, cause=e)
    
    def _build_sec_api_query(self, cik: str, form_type: str, start_year: int, 
                           end_year: int, max_results: int = 10) -> Dict[str, Any]:
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
        
        # Check API key first
        if not SEC_API_KEY:
            error_msg = "SEC_API_KEY is not set in the centralized configuration (settings.api_keys.sec)."
            logger.error(f"[{request_id}] {error_msg}")
            raise MissingConfigurationError(error_msg, context=context)
        
        # Set up API URL and headers
        query_url = f"{SEC_API_BASE_URL}?token={SEC_API_KEY}"
        headers = {
            "User-Agent": settings.sec_user_agent,
            "Accept": "application/json",
            "X-Request-ID": request_id or str(uuid.uuid4())
        }
        
        logger.debug(f"[{request_id}] Executing SEC API query for filings")
        
        try:
            # Execute the query
            resp = httpx.post(query_url, json=query, headers=headers, timeout=settings.sec_request_timeout_sec)
            
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
            except Exception as e:
                error_msg = f"Failed to parse SEC API response"
                logger.error(f"[{request_id}] {error_msg}: {e}")
                raise SECParsingError(error_msg, context=context, cause=e)
            
            # Extract filings
            filings = data.get('filings', [])
            
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
    
    def _process_single_filing(self, filing: Dict[str, Any], cik: str, ticker: Optional[str], 
                             form_type: str, request_id: Optional[str] = None, 
                             source_suffix: str = "") -> Dict[str, Any]:
        """
        Process a single filing from metadata to final envelope.
        
        Args:
            filing: Filing metadata from SEC API
            cik: Company CIK
            ticker: Company ticker (optional)
            form_type: SEC form type
            request_id: Optional request ID for tracing
            source_suffix: Optional suffix for source field (e.g., '-form4')
            
        Returns:
            Standardized filing envelope
        """
        # Get document URLs for this filing
        urls_to_try, expected_content_type = self._get_document_urls(filing, cik, form_type, request_id)
        
        # Set up headers for document retrieval
        headers = {
            "User-Agent": settings.sec_user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "X-Request-ID": request_id or str(uuid.uuid4())
        }
        
        # Try to fetch the document content
        try:
            document_content, successful_url, content_type = self._fetch_document_content(
                urls_to_try, headers, request_id
            )
            
            # Create successful envelope
            return self._create_filing_envelope(
                content=document_content,
                status='success',
                metadata={
                    'cik': cik,
                    'ticker': ticker,
                    'form_type': form_type,
                    'filing_date': filing.get('filedAt'),
                    'year': filing.get('filedAt', '').split('-')[0] if filing.get('filedAt') else None,
                    'accession_no': filing.get('accessionNo'),
                    'filing_info': filing,
                    'filing_url': successful_url,
                    'content_type': content_type
                },
                request_id=request_id,
                source_suffix=source_suffix
            )
            
        except Exception as e:
            # Create error envelope
            return self._create_filing_envelope(
                content=None,
                status='error',
                metadata={
                    'cik': cik,
                    'ticker': ticker,
                    'form_type': form_type,
                    'filing_date': filing.get('filedAt'),
                    'year': filing.get('filedAt', '').split('-')[0] if filing.get('filedAt') else None,
                    'accession_no': filing.get('accessionNo'),
                    'filing_info': filing,
                    'content_type': expected_content_type
                },
                error=str(e),
                request_id=request_id,
                source_suffix=source_suffix
            )
    
    def _get_document_urls(self, filing: Dict[str, Any], cik: str, form_type: str, 
                         request_id: Optional[str] = None) -> Tuple[List[str], str]:
        """
        Get document URLs from filing metadata with fallback strategies.
        
        Args:
            filing: Filing metadata
            cik: Company CIK
            form_type: SEC form type
            request_id: Optional request ID for tracing
            
        Returns:
            Tuple of (urls_to_try, content_type)
        """
        urls_to_try = []
        content_type = 'txt'  # Default to text
        
        # Extract accession number for fallback URLs
        accession_no = filing.get('accessionNo', '').replace('-', '')
        cik_no_leading_zeros = cik.lstrip('0')
        
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
                if doc.get('documentUrl') and (doc.get('type') == form_type or 
                                             doc.get('type') == '10-K' or 
                                             doc.get('description', '').lower() == 'complete submission text file'):
                    urls_to_try.append(doc.get('documentUrl'))
                    content_type = 'html' if doc.get('documentUrl', '').endswith(('.htm', '.html')) else 'txt'
        
        # 3. Fallback URLs based on SEC EDGAR structure
        if filing.get('primaryDocument'):
            urls_to_try.append(f"{SEC_EDGAR_BASE_URL}/{cik_no_leading_zeros}/{accession_no}/{filing.get('primaryDocument')}")
        
        # 4. Complete submission text file
        urls_to_try.append(f"{SEC_EDGAR_BASE_URL}/{cik_no_leading_zeros}/{accession_no}/{accession_no}.txt")
        
        # Ensure we have at least one URL to try
        if not urls_to_try:
            urls_to_try.append(f"{SEC_EDGAR_BASE_URL}/{cik_no_leading_zeros}/{accession_no}")
        
        return urls_to_try, content_type
    
    def _fetch_document_content(self, urls: List[str], headers: Dict[str, str], 
                              request_id: Optional[str] = None) -> Tuple[str, str, str]:
        """
        Fetch document content from URLs.
        
        Args:
            urls: List of URLs to try in order
            headers: HTTP headers for request
            request_id: Optional request ID for tracing
            
        Returns:
            Tuple of (document_content, successful_url, content_type)
            
        Raises:
            SECNotFoundError: If no URL provides valid content
        """
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
        raise SECNotFoundError(error_msg, context=ErrorContext(
            request_id=request_id,
            source_type="SEC",
            urls=urls
        ))
    
    def _create_filing_envelope(self, content: Optional[str], status: str, metadata: Dict[str, Any], 
                              error: Optional[str] = None, request_id: Optional[str] = None,
                              source_suffix: str = "") -> Dict[str, Any]:
        """
        Create response envelope for a filing.
        
        Args:
            content: Document content (None for error cases)
            status: Response status (success, error, not_found)
            metadata: Filing metadata
            error: Optional error message
            request_id: Optional request ID for tracing
            source_suffix: Optional suffix for source field
            
        Returns:
            Standardized response envelope
        """
        envelope = {
            'content': content,
            'content_type': metadata.get('content_type', 'txt'),
            'source': f'sec{source_suffix}',
            'status': status,
            'metadata': {
                'cik': metadata.get('cik'),
                'ticker': metadata.get('ticker'),
                'form_type': metadata.get('form_type'),
                'year': metadata.get('year'),
                'retrieved_at': datetime.datetime.utcnow().isoformat() + 'Z',
                'request_id': request_id
            }
        }
        
        # Add additional metadata if available
        for key in ['filing_info', 'filing_url', 'filing_date', 'accession_no']:
            if key in metadata:
                envelope['metadata'][key] = metadata[key]
        
        # Add error information if provided
        if error:
            envelope['error'] = error
            envelope['status'] = 'error'
        
        return envelope
    
    def _create_error_envelope(self, error_msg: str, form_type: str, cik: Optional[str], 
                             ticker: Optional[str], request_id: Optional[str] = None) -> Dict[str, Any]:
        """Create error envelope for validation or other failures."""
        return {
            'content': None,
            'content_type': None,
            'source': 'sec',
            'status': 'error',
            'error': error_msg,
            'metadata': {
                'cik': cik,
                'ticker': ticker,
                'form_type': form_type,
                'retrieved_at': datetime.datetime.utcnow().isoformat() + 'Z',
                'request_id': request_id
            }
        }
    
    def _create_not_found_envelope(self, form_type: str, cik: str, ticker: Optional[str], 
                                 year: int, request_id: Optional[str] = None) -> Dict[str, Any]:
        """Create not found envelope for missing filings."""
        return {
            'content': None,
            'content_type': None,
            'source': 'sec',
            'status': 'not_found',
            'error': f'No {form_type} filing found for year {year}',
            'metadata': {
                'cik': cik,
                'ticker': ticker,
                'form_type': form_type,
                'year': year,
                'retrieved_at': datetime.datetime.utcnow().isoformat() + 'Z',
                'request_id': request_id
            }
        }