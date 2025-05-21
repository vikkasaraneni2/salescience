"""
Company Lookup Module for Data Acquisition Pipeline
--------------------------------------------------

This module provides company lookup and resolution functionality for the
data acquisition pipeline. It's responsible for:

1. Resolving ticker symbols to CIK numbers for SEC filings
2. Searching for companies by name, industry, or other attributes
3. Retrieving company metadata from various sources
4. Caching company information for improved performance
5. Providing a consistent interface for company identification

The module enables reliable entity resolution across different data sources,
ensuring that the same company can be identified consistently regardless of
which external API is being used.
"""

import os
import json
import httpx
import logging
import redis
import time
import uuid
import random
import asyncio
from typing import Dict, Any, List, Optional, Union

# Import configuration and utilities
from config import settings
from data_acquisition.utils import timestamp_now, mask_sensitive, JsonLogger

# Get centralized logging configuration
from data_acquisition.utils import configure_logging

# Get logger specific to this module
logger = configure_logging("company_lookup")
json_logger = JsonLogger("company_lookup")

# Initialize Redis client
try:
    redis_client = redis.Redis.from_url(settings.redis.url, decode_responses=True)
    REDIS_AVAILABLE = True
except Exception as e:
    logger.warning(f"Redis not available for company lookup caching: {e}")
    REDIS_AVAILABLE = False


# Import error handling framework 
from data_acquisition.errors import (
    SalescienceError, ErrorContext,
    SECError, SECNotFoundError, SECAuthenticationError, SECRateLimitError, 
    ValidationError, DataSourceNotFoundError, DataSourceConnectionError, DataSourceTimeoutError,
    MissingConfigurationError, ConfigurationError,
    log_error, format_error_response
)

# For backwards compatibility
class CompanyLookupException(SalescienceError):
    """Exception raised for errors in the company lookup process."""
    error_code = "COMPANY_LOOKUP_ERROR"
    http_status = 500


class CompanyLookup:
    """
    Class for resolving company identifiers and retrieving company information.
    
    This class provides a centralized service for company lookup operations,
    supporting various identifier types (ticker, CIK, name) and enhancing them
    with additional metadata from external sources. It implements caching to
    minimize redundant API calls and improve performance.
    """
    
    def __init__(self, cache_ttl_seconds: int = 86400):
        """
        Initialize the company lookup service.
        
        Args:
            cache_ttl_seconds: Time-to-live for cached company data in seconds (default: 24 hours)
        """
        self.cache_ttl = cache_ttl_seconds
        self.sec_api_key = settings.api_keys.sec
        self.sec_api_base_url = settings.service_urls.sec_api_base
        
        # Standard headers for HTTP requests
        self.headers = {
            "User-Agent": settings.sec_user_agent,
            "Accept": "application/json",
        }
        
        logger.info("CompanyLookup service initialized")
        
    def _get_cache_key(self, lookup_type: str, lookup_value: str) -> str:
        """
        Generate a cache key for company lookups.
        
        Args:
            lookup_type: Type of lookup (ticker, cik, name)
            lookup_value: The value being looked up
            
        Returns:
            Properly formatted cache key
        """
        # Normalize lookup value and type for consistent caching
        normalized_value = str(lookup_value).lower().strip()
        normalized_type = lookup_type.lower()
        
        # Use consistent key format for all lookups
        return f"company:lookup:{normalized_type}:{normalized_value}"
    
    def _get_from_cache(self, lookup_type: str, lookup_value: str, request_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Try to get company information from cache.
        
        Args:
            lookup_type: Type of lookup (ticker, cik, name)
            lookup_value: The value being looked up
            request_id: Optional request ID for tracing and logging
            
        Returns:
            Cached company data if available and valid, None otherwise
        """
        if not REDIS_AVAILABLE:
            return None
        
        # Format request ID string for logging if provided
        req_id_str = f"[{request_id}] " if request_id else ""
        
        cache_key = self._get_cache_key(lookup_type, lookup_value)
        
        try:
            cached_data = redis_client.get(cache_key)
            if cached_data:
                company_data = json.loads(cached_data)
                
                # Check if data is stale by comparing cached_at timestamp
                if 'cached_at' in company_data:
                    cached_time = company_data.get('cached_at')
                    logger.debug(f"{req_id_str}Cache hit for {lookup_type}='{lookup_value}' (cached at {cached_time})")
                else:
                    logger.debug(f"{req_id_str}Cache hit for {lookup_type}='{lookup_value}'")
                    
                return company_data
            else:
                logger.debug(f"{req_id_str}Cache miss for {lookup_type}='{lookup_value}'")
                
        except json.JSONDecodeError as e:
            # Handle corrupt cache data by removing it
            logger.warning(f"{req_id_str}Corrupt cache data for {lookup_type}='{lookup_value}': {e}")
            try:
                redis_client.delete(cache_key)
                logger.info(f"{req_id_str}Removed corrupt cache entry for {cache_key}")
            except Exception as del_err:
                logger.warning(f"{req_id_str}Failed to remove corrupt cache entry: {del_err}")
                
        except Exception as e:
            # For Redis connection errors or other issues
            logger.warning(f"{req_id_str}Error retrieving from cache: {e}")
            
        return None
    
    def _save_to_cache(self, lookup_type: str, lookup_value: str, company_data: Dict[str, Any], request_id: Optional[str] = None) -> bool:
        """
        Save company information to cache.
        
        Args:
            lookup_type: Type of lookup (ticker, cik, name)
            lookup_value: The value being looked up
            company_data: Company information to cache
            request_id: Optional request ID for tracing and logging
            
        Returns:
            True if successfully cached, False otherwise
        """
        if not REDIS_AVAILABLE:
            return False
        
        # Skip caching if lookup value is None or empty
        if not lookup_value:
            return False
            
        # Format request ID string for logging if provided
        req_id_str = f"[{request_id}] " if request_id else ""
        
        cache_key = self._get_cache_key(lookup_type, lookup_value)
        
        try:
            # Create a copy to avoid modifying the original data
            cache_data = company_data.copy()
            
            # Add cache timestamp for invalidation management
            cache_data['cached_at'] = timestamp_now()
            
            # Save to Redis with TTL
            redis_client.setex(
                cache_key,
                self.cache_ttl,
                json.dumps(cache_data)
            )
            
            # Log with more detail for debugging cache issues
            company_name = cache_data.get('name', 'unknown')
            logger.debug(f"{req_id_str}Cached company data for {company_name} via {lookup_type}='{lookup_value}'")
            
            return True
            
        except json.JSONDecodeError as e:
            logger.warning(f"{req_id_str}Failed to serialize company data for caching: {e}")
            return False
            
        except Exception as e:
            # For Redis connection errors or other issues
            logger.warning(f"{req_id_str}Error saving to cache: {e}")
            return False
    
    async def get_cik_for_ticker(self, ticker: str, request_id: Optional[str] = None) -> str:
        """
        Resolves a ticker symbol to a CIK (Central Index Key) using the SEC API.
        
        Args:
            ticker: The stock ticker symbol (e.g., 'AAPL', 'MSFT')
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
        # Generate request_id if not provided for tracing purposes
        if not request_id:
            request_id = str(uuid.uuid4())
            
        # Create error context
        context = ErrorContext(
            request_id=request_id,
            company={"ticker": ticker},
            source_type="SEC-Lookup"
        )
        
        # Check cache first
        cached_company = self._get_from_cache('ticker', ticker)
        if cached_company and 'cik' in cached_company:
            logger.info(f"[{request_id}] Cache hit for ticker '{ticker}', returning CIK '{cached_company['cik']}'")
            return cached_company['cik']
        
        # Verify API key is available
        if not self.sec_api_key:
            error_msg = "SEC_API_KEY is not set in the configuration."
            logger.error(f"[{request_id}] {error_msg}")
            raise MissingConfigurationError(error_msg, context=context)
        
        # Correct endpoint format based on SEC-API.io documentation
        url = f"{self.sec_api_base_url}/mapping/ticker/{ticker}?token={self.sec_api_key}"
        
        logger.info(f"[{request_id}] Resolving ticker '{ticker}' to CIK via sec-api.io")
        logger.debug(f"[{request_id}] SEC API request URL: {url}")
        
        # Implement retry with exponential backoff
        max_retries = settings.worker.max_retries
        base_delay = settings.worker.retry_delay_sec
        attempt = 0
        
        while attempt < max_retries:
            try:
                logger.info(f"[{request_id}] Resolving ticker '{ticker}' to CIK (attempt {attempt+1}/{max_retries})")
                
                # Use httpx.AsyncClient for asynchronous requests
                async with httpx.AsyncClient() as client:
                    resp = await client.get(url, headers=self.headers, timeout=settings.sec_request_timeout_sec)
                    
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
                        raise SECError(error_msg, context=context, cause=e)
                    
                    # The API returns a list of results
                    if isinstance(data, list) and len(data) > 0:
                        # Get CIK from first result
                        cik = data[0].get("cik")
                        if cik:
                            padded_cik = str(cik).zfill(10)
                            logger.info(f"[{request_id}] Resolved ticker '{ticker}' to CIK '{padded_cik}'")
                            
                            # Cache the result for future lookups
                            company_data = data[0]
                            company_data['cik'] = padded_cik  # Ensure we store padded CIK
                            self._save_to_cache('ticker', ticker, company_data)
                            
                            # Also cache by CIK for reverse lookup
                            self._save_to_cache('cik', padded_cik, company_data)
                            
                            return padded_cik
                    
                    # If we got no matches or no CIK field, raise an error
                    error_msg = f"Ticker '{ticker}' not found in SEC API response"
                    logger.warning(f"[{request_id}] {error_msg}: {data}")
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
                await asyncio.sleep(delay)
                
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
                await asyncio.sleep(delay)
                
            except (SECAuthenticationError, SECNotFoundError) as e:
                # Don't retry authentication errors or not found errors
                raise
                
            except (SECError, SalescienceError) as e:
                # For other SEC-specific errors, attempt retry if not at max
                attempt += 1
                if attempt >= max_retries:
                    raise
                    
                # Exponential backoff with jitter
                jitter = 0.1 * base_delay * (random.random() * 2 - 1)
                delay = min(base_delay * (2 ** (attempt - 1)) + jitter, 30.0)
                logger.info(f"[{request_id}] Retrying in {delay:.2f}s after error: {e.error_code}")
                await asyncio.sleep(delay)
                
            except Exception as e:
                # For unexpected errors, wrap in SECError and raise
                error_msg = f"Unexpected error resolving ticker '{ticker}' to CIK: {e}"
                logger.error(f"[{request_id}] {error_msg}")
                raise SECError(error_msg, context=context, cause=e)
    
    async def get_ticker_for_cik(self, cik: str, request_id: Optional[str] = None) -> Optional[str]:
        """
        Resolves a CIK number to a ticker symbol using the SEC API.
        
        Args:
            cik: The CIK number (with or without leading zeros)
            request_id: Optional request ID for tracing and logging
            
        Returns:
            Ticker symbol if found, None otherwise
            
        Raises:
            MissingConfigurationError: If SEC_API_KEY is not set
            SECAuthenticationError: If authentication fails (401)
            SECRateLimitError: If rate limit is exceeded (429)
            SECError: For other SEC API errors
            DataSourceConnectionError: For network/connection issues
            DataSourceTimeoutError: If the request times out
        """
        # Generate request_id if not provided for tracing purposes
        if not request_id:
            request_id = str(uuid.uuid4())
            
        # Ensure CIK is properly formatted with leading zeros
        padded_cik = str(cik).zfill(10)
        
        # Create error context
        context = ErrorContext(
            request_id=request_id,
            company={"cik": padded_cik},
            source_type="SEC-Lookup"
        )
        
        # Check cache first
        cached_company = self._get_from_cache('cik', padded_cik)
        if cached_company and 'ticker' in cached_company:
            logger.info(f"[{request_id}] Cache hit for CIK '{padded_cik}', returning ticker '{cached_company['ticker']}'")
            return cached_company['ticker']
        
        # Verify API key is available
        if not self.sec_api_key:
            error_msg = "SEC_API_KEY is not set in the configuration."
            logger.error(f"[{request_id}] {error_msg}")
            raise MissingConfigurationError(error_msg, context=context)
        
        # SEC-API.io endpoint for CIK to ticker mapping
        url = f"{self.sec_api_base_url}/mapping/cik/{padded_cik.lstrip('0')}?token={self.sec_api_key}"
        
        logger.info(f"[{request_id}] Resolving CIK '{padded_cik}' to ticker via sec-api.io")
        
        # Implement retry with exponential backoff
        max_retries = settings.worker.max_retries
        base_delay = settings.worker.retry_delay_sec
        attempt = 0
        
        while attempt < max_retries:
            try:
                logger.info(f"[{request_id}] Resolving CIK '{padded_cik}' to ticker (attempt {attempt+1}/{max_retries})")
                
                async with httpx.AsyncClient() as client:
                    resp = await client.get(url, headers=self.headers, timeout=settings.sec_request_timeout_sec)
                    
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
                        raise SECError(error_msg, context=context, cause=e)
                    
                    # Check if we have a ticker in the response
                    if isinstance(data, dict) and 'ticker' in data:
                        ticker = data['ticker']
                        logger.info(f"[{request_id}] Resolved CIK '{padded_cik}' to ticker '{ticker}'")
                        
                        # Cache the result for future lookups
                        self._save_to_cache('cik', padded_cik, data)
                        self._save_to_cache('ticker', ticker, data)
                        
                        return ticker
                        
                    # No ticker found, but this is not an error - just return None
                    logger.warning(f"[{request_id}] No ticker found for CIK '{padded_cik}'")
                    return None
                    
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
                await asyncio.sleep(delay)
                
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
                await asyncio.sleep(delay)
                
            except (SECAuthenticationError, SECNotFoundError) as e:
                # Don't retry authentication errors or not found errors
                raise
                
            except (SECError, SalescienceError) as e:
                # For other SEC-specific errors, attempt retry if not at max
                attempt += 1
                if attempt >= max_retries:
                    raise
                    
                # Exponential backoff with jitter
                jitter = 0.1 * base_delay * (random.random() * 2 - 1)
                delay = min(base_delay * (2 ** (attempt - 1)) + jitter, 30.0)
                logger.info(f"[{request_id}] Retrying in {delay:.2f}s after error: {e.error_code}")
                await asyncio.sleep(delay)
                
            except Exception as e:
                # For unexpected errors, wrap in SECError and raise
                error_msg = f"Unexpected error resolving CIK '{padded_cik}' to ticker: {e}"
                logger.error(f"[{request_id}] {error_msg}")
                raise SECError(error_msg, context=context, cause=e)
    
    async def search_companies(self, query: str, limit: int = 10, request_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Search for companies by name, ticker, or other attributes.
        
        Args:
            query: Search query string
            limit: Maximum number of results to return
            request_id: Optional request ID for tracing and logging
            
        Returns:
            List of matching company records with metadata
            
        Raises:
            MissingConfigurationError: If SEC_API_KEY is not set
            SECAuthenticationError: If authentication fails (401)
            SECRateLimitError: If rate limit is exceeded (429)
            SECError: For other SEC API errors
            DataSourceConnectionError: For network/connection issues
            DataSourceTimeoutError: If the request times out
        """
        # Generate request_id if not provided for tracing purposes
        if not request_id:
            request_id = str(uuid.uuid4())
            
        # Create error context
        context = ErrorContext(
            request_id=request_id,
            source_type="SEC-Search",
            query=query
        )
        
        # Verify API key is available
        if not self.sec_api_key:
            error_msg = "SEC_API_KEY is not set in the configuration."
            logger.error(f"[{request_id}] {error_msg}")
            raise MissingConfigurationError(error_msg, context=context)
        
        # Check cache first
        cache_key = f"company:search:{query.lower()}"
        if REDIS_AVAILABLE:
            try:
                cached_results = redis_client.get(cache_key)
                if cached_results:
                    logger.info(f"[{request_id}] Cache hit for search query '{query}'")
                    return json.loads(cached_results)
            except Exception as e:
                logger.warning(f"[{request_id}] Error retrieving search results from cache: {e}")
        
        # Use SEC-API.io for company search
        url = f"{self.sec_api_base_url}/search?token={self.sec_api_key}"
        
        # Prepare search parameters
        payload = {
            "query": query,
            "limit": limit
        }
        
        logger.info(f"[{request_id}] Searching for companies with query: '{query}'")
        
        # Implement retry with exponential backoff
        max_retries = settings.worker.max_retries
        base_delay = settings.worker.retry_delay_sec
        attempt = 0
        
        while attempt < max_retries:
            try:
                logger.info(f"[{request_id}] Searching companies with query '{query}' (attempt {attempt+1}/{max_retries})")
                
                async with httpx.AsyncClient() as client:
                    resp = await client.post(
                        url, 
                        json=payload, 
                        headers={
                            **self.headers,
                            "X-Request-ID": request_id
                        }, 
                        timeout=settings.sec_request_timeout_sec
                    )
                    
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
                        logger.debug(f"[{request_id}] SEC API search response received")
                    except json.JSONDecodeError as e:
                        error_msg = f"Failed to parse SEC API response: {e}"
                        logger.error(f"[{request_id}] {error_msg}")
                        raise SECError(error_msg, context=context, cause=e)
                    
                    results = data.get('results', [])
                    
                    # Process and enhance search results
                    companies = []
                    for result in results:
                        company = {
                            'cik': result.get('cik', '').zfill(10),
                            'ticker': result.get('ticker'),
                            'name': result.get('name'),
                            'sic': result.get('sic'),
                            'sic_description': result.get('sicDescription'),
                            'exchange': result.get('exchange'),
                            'state_of_incorporation': result.get('stateOfIncorporation'),
                            'fiscal_year_end': result.get('fiscalYearEnd')
                        }
                        companies.append(company)
                    
                    logger.info(f"[{request_id}] Found {len(companies)} companies matching query '{query}'")
                    
                    # Cache the results
                    if REDIS_AVAILABLE:
                        try:
                            redis_client.setex(
                                cache_key,
                                self.cache_ttl,
                                json.dumps(companies)
                            )
                            logger.debug(f"[{request_id}] Cached search results for query '{query}'")
                        except Exception as e:
                            logger.warning(f"[{request_id}] Error caching search results: {e}")
                    
                    # Log structured information about the search
                    json_logger.log_json(
                        level="info",
                        action="company_search",
                        message=f"Company search for '{query}' found {len(companies)} results",
                        request_id=request_id,
                        query=query,
                        result_count=len(companies)
                    )
                    
                    return companies
                    
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
                await asyncio.sleep(delay)
                
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
                await asyncio.sleep(delay)
                
            except (SECAuthenticationError, SECNotFoundError) as e:
                # Don't retry authentication errors or not found errors
                raise
                
            except (SECError, SalescienceError) as e:
                # For other SEC-specific errors, attempt retry if not at max
                attempt += 1
                if attempt >= max_retries:
                    raise
                    
                # Exponential backoff with jitter
                jitter = 0.1 * base_delay * (random.random() * 2 - 1)
                delay = min(base_delay * (2 ** (attempt - 1)) + jitter, 30.0)
                logger.info(f"[{request_id}] Retrying in {delay:.2f}s after error: {e.error_code}")
                await asyncio.sleep(delay)
                
            except Exception as e:
                # For unexpected errors, wrap in SECError and raise
                error_msg = f"Unexpected error searching for companies with query '{query}': {e}"
                logger.error(f"[{request_id}] {error_msg}")
                raise SECError(error_msg, context=context, cause=e)
    
    async def get_company_info(self, identifier: str, identifier_type: str = 'auto', request_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Get comprehensive company information by identifier.
        
        This method accepts various identifier types and returns a complete
        company profile with metadata from multiple sources.
        
        Args:
            identifier: Company identifier (ticker, CIK, or name)
            identifier_type: Type of identifier ('ticker', 'cik', 'name', or 'auto' for automatic detection)
            request_id: Optional request ID for tracing and logging
            
        Returns:
            Dictionary with company information from multiple sources
            
        Raises:
            ValidationError: If identifier_type is invalid
            SECNotFoundError: If company not found
            SECError: For other SEC API errors
            DataSourceConnectionError: For network/connection issues
            DataSourceTimeoutError: If the request times out
        """
        # Generate request_id if not provided for tracing purposes
        if not request_id:
            request_id = str(uuid.uuid4())
            
        logger.info(f"[{request_id}] Getting company info for {identifier_type}='{identifier}'")
        
        # Create error context with what we know so far
        context = ErrorContext(
            request_id=request_id,
            source_type="Company-Info"
        )
        
        # Auto-detect identifier type if not specified
        if identifier_type == 'auto':
            if identifier.isdigit() or (identifier.startswith('0') and len(identifier) == 10):
                identifier_type = 'cik'
                logger.info(f"[{request_id}] Auto-detected identifier type as 'cik' for '{identifier}'")
            elif len(identifier) <= 5 and identifier.isalpha():
                identifier_type = 'ticker'
                logger.info(f"[{request_id}] Auto-detected identifier type as 'ticker' for '{identifier}'")
            else:
                identifier_type = 'name'
                logger.info(f"[{request_id}] Auto-detected identifier type as 'name' for '{identifier}'")
        
        # Update context with detected type
        if identifier_type == 'ticker':
            context.company = {"ticker": identifier}
        elif identifier_type == 'cik':
            context.company = {"cik": identifier}
        elif identifier_type == 'name':
            context.company = {"name": identifier}
        
        # Check cache first
        cache_key = self._get_cache_key(identifier_type, identifier)
        cached_company = self._get_from_cache(identifier_type, identifier, request_id)
        if cached_company and cached_company.get('full_profile', False):
            logger.info(f"[{request_id}] Cache hit for {identifier_type}='{identifier}'")
            return cached_company
        
        # Process based on identifier type
        try:
            if identifier_type == 'ticker':
                # For ticker, get CIK first
                logger.info(f"[{request_id}] Resolving ticker '{identifier}' to CIK")
                cik = await self.get_cik_for_ticker(identifier, request_id)
                company_info = await self._fetch_company_profile(cik, request_id)
                
                # Ensure ticker is set
                company_info['ticker'] = identifier
                
            elif identifier_type == 'cik':
                # Ensure CIK is properly formatted
                padded_cik = str(identifier).zfill(10)
                logger.info(f"[{request_id}] Fetching company profile for CIK '{padded_cik}'")
                company_info = await self._fetch_company_profile(padded_cik, request_id)
                
                # Try to get ticker if not in profile
                if 'ticker' not in company_info:
                    try:
                        logger.info(f"[{request_id}] Resolving CIK '{padded_cik}' to ticker")
                        ticker = await self.get_ticker_for_cik(padded_cik, request_id)
                        if ticker:
                            company_info['ticker'] = ticker
                    except Exception as e:
                        logger.warning(f"[{request_id}] Could not resolve ticker for CIK {padded_cik}: {e}")
                        
            elif identifier_type == 'name':
                # Search by name and use first result
                logger.info(f"[{request_id}] Searching companies by name '{identifier}'")
                results = await self.search_companies(identifier, limit=1, request_id=request_id)
                if not results:
                    error_msg = f"No company found with name '{identifier}'"
                    logger.warning(f"[{request_id}] {error_msg}")
                    raise SECNotFoundError(error_msg, context=context)
                    
                result = results[0]
                cik = result.get('cik')
                logger.info(f"[{request_id}] Found company with name '{identifier}', fetching full profile")
                company_info = await self._fetch_company_profile(cik, request_id)
                
                # Add search result data if not in profile
                for key, value in result.items():
                    if key not in company_info:
                        company_info[key] = value
            else:
                error_msg = f"Invalid identifier_type: {identifier_type}"
                logger.error(f"[{request_id}] {error_msg}")
                raise ValidationError(error_msg, context=context)
            
            # Mark as full profile and cache
            company_info['full_profile'] = True
            
            # Cache under all known identifiers
            cache_success = False
            if 'cik' in company_info:
                cache_result = self._save_to_cache('cik', company_info['cik'], company_info)
                cache_success = cache_success or cache_result
                
            if 'ticker' in company_info:
                cache_result = self._save_to_cache('ticker', company_info['ticker'], company_info)
                cache_success = cache_success or cache_result
                
            if 'name' in company_info:
                cache_result = self._save_to_cache('name', company_info['name'], company_info)
                cache_success = cache_success or cache_result
                
            if cache_success:
                logger.debug(f"[{request_id}] Cached company information for {company_info.get('name', 'unknown')}")
            
            # Log structured information about the company info retrieval
            json_logger.log_json(
                level="info",
                action="company_info_retrieved",
                message=f"Retrieved company info for {company_info.get('name', '')}",
                request_id=request_id,
                company_name=company_info.get('name'),
                company_ticker=company_info.get('ticker'),
                company_cik=company_info.get('cik'),
                identifier_type=identifier_type,
                identifier=identifier
            )
            
            return company_info
            
        except (SECNotFoundError, SECError, DataSourceConnectionError, DataSourceTimeoutError, ValidationError) as e:
            # Let specific errors propagate with proper context
            raise
            
        except Exception as e:
            # For unexpected errors, wrap in appropriate error type
            error_msg = f"Error retrieving company info for {identifier_type}='{identifier}': {e}"
            logger.error(f"[{request_id}] {error_msg}")
            raise SECError(error_msg, context=context, cause=e)
    
    async def _fetch_company_profile(self, cik: str, request_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Fetch detailed company profile from SEC API.
        
        Args:
            cik: Company CIK number (with or without leading zeros)
            request_id: Optional request ID for tracing and logging
            
        Returns:
            Company profile dictionary
            
        Raises:
            MissingConfigurationError: If SEC_API_KEY is not set
            SECAuthenticationError: If authentication fails (401)
            SECRateLimitError: If rate limit is exceeded (429)
            SECNotFoundError: If company profile not found
            SECError: For other SEC API errors
            DataSourceConnectionError: For network/connection issues
            DataSourceTimeoutError: If the request times out
        """
        # Generate request_id if not provided for tracing purposes
        if not request_id:
            request_id = str(uuid.uuid4())
        
        # Ensure CIK is properly formatted
        padded_cik = str(cik).zfill(10)
        
        # Create error context
        context = ErrorContext(
            request_id=request_id,
            company={"cik": padded_cik},
            source_type="SEC-Profile"
        )
        
        # Verify API key is available
        if not self.sec_api_key:
            error_msg = "SEC_API_KEY is not set in the configuration."
            logger.error(f"[{request_id}] {error_msg}")
            raise MissingConfigurationError(error_msg, context=context)
        
        # SEC-API.io endpoint for company profile
        url = f"{self.sec_api_base_url}/company/{padded_cik.lstrip('0')}?token={self.sec_api_key}"
        
        logger.info(f"[{request_id}] Fetching company profile for CIK '{padded_cik}'")
        
        # Implement retry with exponential backoff
        max_retries = settings.worker.max_retries
        base_delay = settings.worker.retry_delay_sec
        attempt = 0
        
        while attempt < max_retries:
            try:
                logger.info(f"[{request_id}] Fetching company profile for CIK '{padded_cik}' (attempt {attempt+1}/{max_retries})")
                
                async with httpx.AsyncClient() as client:
                    resp = await client.get(
                        url, 
                        headers={
                            **self.headers,
                            "X-Request-ID": request_id
                        }, 
                        timeout=settings.sec_request_timeout_sec
                    )
                    
                    # Handle different response status codes with specific errors
                    if resp.status_code == 401 or resp.status_code == 403:
                        error_msg = f"SEC API authentication failed: {resp.text}"
                        logger.error(f"[{request_id}] {error_msg}")
                        raise SECAuthenticationError(error_msg, context=context)
                        
                    elif resp.status_code == 429:
                        error_msg = f"SEC API rate limit exceeded: {resp.text}"
                        logger.error(f"[{request_id}] {error_msg}")
                        raise SECRateLimitError(error_msg, context=context)
                        
                    elif resp.status_code == 404:
                        error_msg = f"Company profile not found for CIK '{padded_cik}'"
                        logger.warning(f"[{request_id}] {error_msg}: {resp.text}")
                        raise SECNotFoundError(error_msg, context=context)
                        
                    elif resp.status_code != 200:
                        error_msg = f"SEC API returned status {resp.status_code}: {resp.text}"
                        logger.error(f"[{request_id}] {error_msg}")
                        raise SECError(error_msg, context=context)
                    
                    # Parse the response
                    try:
                        profile = resp.json()
                        logger.debug(f"[{request_id}] SEC API profile response received")
                    except json.JSONDecodeError as e:
                        error_msg = f"Failed to parse SEC API response: {e}"
                        logger.error(f"[{request_id}] {error_msg}")
                        raise SECError(error_msg, context=context, cause=e)
                    
                    # Ensure consistent field names and format
                    standardized_profile = {
                        'cik': padded_cik,
                        'name': profile.get('name'),
                        'ticker': profile.get('ticker'),
                        'exchange': profile.get('exchange'),
                        'sic': profile.get('sic'),
                        'sic_description': profile.get('sicDescription'),
                        'industry': profile.get('industry'),
                        'sector': profile.get('sector'),
                        'state_of_incorporation': profile.get('stateOfIncorporation'),
                        'fiscal_year_end': profile.get('fiscalYearEnd'),
                        'company_type': profile.get('entityType'),
                        'filing_address': profile.get('addresses', {}).get('business'),
                        'business_address': profile.get('addresses', {}).get('mailing'),
                        'phone': profile.get('phone'),
                        'website': profile.get('website'),
                        'employee_count': profile.get('employeeCount'),
                        'description': profile.get('description')
                    }
                    
                    company_name = standardized_profile.get('name', 'Unknown')
                    logger.info(f"[{request_id}] Successfully fetched company profile for {company_name}")
                    
                    # Log structured information about the profile fetch
                    json_logger.log_json(
                        level="info",
                        action="company_profile_fetched",
                        message=f"Fetched company profile for {company_name} (CIK: {padded_cik})",
                        request_id=request_id,
                        company_name=company_name,
                        company_cik=padded_cik,
                        company_ticker=standardized_profile.get('ticker')
                    )
                    
                    return standardized_profile
                    
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
                await asyncio.sleep(delay)
                
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
                await asyncio.sleep(delay)
                
            except (SECAuthenticationError, SECNotFoundError) as e:
                # Don't retry authentication errors or not found errors
                raise
                
            except (SECError, SalescienceError) as e:
                # For other SEC-specific errors, attempt retry if not at max
                attempt += 1
                if attempt >= max_retries:
                    raise
                    
                # Exponential backoff with jitter
                jitter = 0.1 * base_delay * (random.random() * 2 - 1)
                delay = min(base_delay * (2 ** (attempt - 1)) + jitter, 30.0)
                logger.info(f"[{request_id}] Retrying in {delay:.2f}s after error: {e.error_code}")
                await asyncio.sleep(delay)
                
            except Exception as e:
                # For unexpected errors, wrap in SECError and raise
                error_msg = f"Unexpected error fetching company profile for CIK '{padded_cik}': {e}"
                logger.error(f"[{request_id}] {error_msg}")
                raise SECError(error_msg, context=context, cause=e)
    
    async def resolve_company(self, company_data: Dict[str, Any], request_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Resolves and enriches company information from partial data.
        
        This is a utility method for standardizing company references across
        the data acquisition pipeline, ensuring that all services work with
        consistent company identifiers regardless of what was initially provided.
        
        Args:
            company_data: Partial company data (must include at least one identifier)
            request_id: Optional request ID for tracing and logging
            
        Returns:
            Enriched company data with all available identifiers and metadata
            
        Raises:
            ValidationError: If no valid identifier is provided
            SECNotFoundError: If company not found
            SECError: For other SEC API errors
            DataSourceConnectionError: For network/connection issues
            DataSourceTimeoutError: If the request times out
        """
        # Generate request_id if not provided for tracing purposes
        if not request_id:
            request_id = str(uuid.uuid4())
            
        # Extract available identifiers
        ticker = company_data.get('ticker')
        cik = company_data.get('cik')
        name = company_data.get('name')
        
        # Create error context with what we know so far
        context = ErrorContext(
            request_id=request_id,
            source_type="Company-Resolution",
            company={
                "ticker": ticker,
                "cik": cik,
                "name": name
            }
        )
        
        # Log the resolution attempt with available identifiers
        logger.info(f"[{request_id}] Resolving company with identifiers: " + 
                   f"ticker='{ticker or 'None'}', cik='{cik or 'None'}', name='{name or 'None'}'")
        
        # Ensure we have at least one identifier
        if not ticker and not cik and not name:
            error_msg = "Company resolution requires at least one identifier (ticker, cik, or name)"
            logger.error(f"[{request_id}] {error_msg}")
            raise ValidationError(error_msg, context=context)
        
        # Determine identifier and type to use for resolution
        if ticker:
            identifier = ticker
            identifier_type = 'ticker'
            logger.info(f"[{request_id}] Using ticker '{ticker}' for company resolution")
        elif cik:
            identifier = cik
            identifier_type = 'cik'
            logger.info(f"[{request_id}] Using CIK '{cik}' for company resolution")
        else:
            identifier = name
            identifier_type = 'name'
            logger.info(f"[{request_id}] Using name '{name}' for company resolution")
            
        # Get full company information
        try:
            company_info = await self.get_company_info(identifier, identifier_type, request_id)
            
            # Merge with original data, prioritizing original values
            result = company_info.copy()
            for key, value in company_data.items():
                if value:  # Only override if value is not None or empty
                    result[key] = value
                    
            json_logger.log_json(
                level="info",
                action="company_resolved",
                message=f"Resolved company {identifier} ({identifier_type})",
                request_id=request_id,
                company_name=result.get('name'),
                company_ticker=result.get('ticker'),
                company_cik=result.get('cik')
            )
            
            logger.info(f"[{request_id}] Successfully resolved company '{result.get('name')}' " +
                       f"(ticker: {result.get('ticker', 'N/A')}, CIK: {result.get('cik', 'N/A')})")
            
            return result
            
        except (SECNotFoundError, ValidationError) as e:
            # Add resolution context and re-raise for caller to handle
            logger.warning(f"[{request_id}] Could not resolve company: {e}")
            raise
            
        except Exception as e:
            # For unexpected errors, wrap in appropriate error type
            error_msg = f"Error resolving company with {identifier_type}='{identifier}': {e}"
            logger.error(f"[{request_id}] {error_msg}")
            raise SECError(error_msg, context=context, cause=e)


# Singleton instance for easy access
company_lookup = CompanyLookup()

# If this module is executed directly, run a simple test
if __name__ == "__main__":
    import asyncio
    
    async def test_lookup():
        # Generate a test request ID
        test_request_id = str(uuid.uuid4())
        print(f"Test request ID: {test_request_id}")
        
        try:
            # Test ticker to CIK resolution
            ticker = "AAPL"
            print(f"\n1. Testing ticker to CIK resolution for {ticker}...")
            cik = await company_lookup.get_cik_for_ticker(ticker, request_id=test_request_id)
            print(f"CIK for {ticker}: {cik}")
            
            # Test company info retrieval
            print(f"\n2. Testing company info retrieval for {ticker}...")
            company = await company_lookup.get_company_info(ticker, request_id=test_request_id)
            print(f"Company info for {ticker}:")
            print(json.dumps(company, indent=2))
            
            # Test company search
            search_term = "Apple"
            print(f"\n3. Testing company search for '{search_term}'...")
            results = await company_lookup.search_companies(search_term, limit=3, request_id=test_request_id)
            print(f"Search results for '{search_term}':")
            print(json.dumps(results, indent=2))
            
            # Test company resolution
            print(f"\n4. Testing company resolution with partial data...")
            partial_data = {"ticker": "MSFT"}
            resolved = await company_lookup.resolve_company(partial_data, request_id=test_request_id)
            print(f"Resolved company data:")
            print(json.dumps(resolved, indent=2))
            
            print("\nAll tests completed successfully!")
            
        except SalescienceError as e:
            print(f"\nSalescienceError during test: {e.error_code} - {e.message}")
            print(f"Context: {e.context}")
            if e.cause:
                print(f"Caused by: {type(e.cause).__name__}: {e.cause}")
        except Exception as e:
            print(f"\nUnexpected error during test: {type(e).__name__}: {e}")
    
    # Run the async test
    asyncio.run(test_lookup())