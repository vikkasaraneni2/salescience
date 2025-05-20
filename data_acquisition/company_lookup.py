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
from typing import Dict, Any, List, Optional, Union

# Import configuration and utilities
from data_acquisition.config import settings
from data_acquisition.utils import timestamp_now, mask_sensitive, JsonLogger

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("company_lookup")
json_logger = JsonLogger("company_lookup")

# Initialize Redis client
try:
    redis_client = redis.Redis.from_url(settings.REDIS_URL, decode_responses=True)
    REDIS_AVAILABLE = True
except Exception as e:
    logger.warning(f"Redis not available for company lookup caching: {e}")
    REDIS_AVAILABLE = False


class CompanyLookupException(Exception):
    """Exception raised for errors in the company lookup process."""
    pass


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
        self.sec_api_key = settings.SEC_API_KEY
        self.sec_api_base_url = settings.SEC_API_BASE_URL
        
        # Standard headers for HTTP requests
        self.headers = {
            "User-Agent": settings.SEC_USER_AGENT,
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
        return f"company:lookup:{lookup_type}:{lookup_value.lower()}"
    
    def _get_from_cache(self, lookup_type: str, lookup_value: str) -> Optional[Dict[str, Any]]:
        """
        Try to get company information from cache.
        
        Args:
            lookup_type: Type of lookup (ticker, cik, name)
            lookup_value: The value being looked up
            
        Returns:
            Cached company data if available and valid, None otherwise
        """
        if not REDIS_AVAILABLE:
            return None
        
        cache_key = self._get_cache_key(lookup_type, lookup_value)
        
        try:
            cached_data = redis_client.get(cache_key)
            if cached_data:
                company_data = json.loads(cached_data)
                logger.debug(f"Cache hit for {lookup_type}={lookup_value}")
                return company_data
        except Exception as e:
            logger.warning(f"Error retrieving from cache: {e}")
            
        return None
    
    def _save_to_cache(self, lookup_type: str, lookup_value: str, company_data: Dict[str, Any]) -> bool:
        """
        Save company information to cache.
        
        Args:
            lookup_type: Type of lookup (ticker, cik, name)
            lookup_value: The value being looked up
            company_data: Company information to cache
            
        Returns:
            True if successfully cached, False otherwise
        """
        if not REDIS_AVAILABLE:
            return False
        
        cache_key = self._get_cache_key(lookup_type, lookup_value)
        
        try:
            # Add cache timestamp for invalidation management
            company_data['cached_at'] = timestamp_now()
            
            # Save to Redis with TTL
            redis_client.setex(
                cache_key,
                self.cache_ttl,
                json.dumps(company_data)
            )
            logger.debug(f"Cached company data for {lookup_type}={lookup_value}")
            return True
        except Exception as e:
            logger.warning(f"Error saving to cache: {e}")
            return False
    
    async def get_cik_for_ticker(self, ticker: str) -> str:
        """
        Resolves a ticker symbol to a CIK (Central Index Key) using the SEC API.
        
        Args:
            ticker: The stock ticker symbol (e.g., 'AAPL', 'MSFT')
            
        Returns:
            Zero-padded CIK string (e.g., '0000320193' for Apple Inc.)
            
        Raises:
            CompanyLookupException: If ticker can't be resolved or API error occurs
        """
        # Check cache first
        cached_company = self._get_from_cache('ticker', ticker)
        if cached_company and 'cik' in cached_company:
            return cached_company['cik']
        
        # Verify API key is available
        if not self.sec_api_key:
            error_msg = "SEC_API_KEY is not set in the environment."
            logger.error(error_msg)
            raise CompanyLookupException(error_msg)
        
        # Correct endpoint format based on SEC-API.io documentation
        url = f"{self.sec_api_base_url}/mapping/ticker/{ticker}?token={self.sec_api_key}"
        
        logger.info(f"Resolving ticker '{ticker}' to CIK via sec-api.io")
        logger.debug(f"SEC API request URL: {url}")
        
        try:
            # Use httpx.AsyncClient for asynchronous requests
            async with httpx.AsyncClient() as client:
                resp = await client.get(url, headers=self.headers, timeout=settings.SEC_REQUEST_TIMEOUT_SEC)
                
                # Log response details for debugging
                logger.debug(f"SEC API status code: {resp.status_code}")
                
                if resp.status_code != 200:
                    error_msg = f"SEC API returned status {resp.status_code}: {resp.text}"
                    logger.error(error_msg)
                    raise CompanyLookupException(f"sec-api.io error: {resp.status_code}")
                
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
                        
                        # Cache the result for future lookups
                        company_data = data[0]
                        company_data['cik'] = padded_cik  # Ensure we store padded CIK
                        self._save_to_cache('ticker', ticker, company_data)
                        
                        # Also cache by CIK for reverse lookup
                        self._save_to_cache('cik', padded_cik, company_data)
                        
                        return padded_cik
                
                # If we got no matches or no CIK field, raise an error
                error_msg = f"Ticker '{ticker}' not found in SEC API response: {data}"
                logger.error(error_msg)
                raise CompanyLookupException(f"Ticker '{ticker}' not found in SEC API response.")
                
        except httpx.TimeoutException:
            error_msg = f"Timeout while fetching CIK for ticker '{ticker}' from SEC API"
            logger.error(error_msg)
            raise CompanyLookupException(error_msg)
        except httpx.RequestError as e:
            error_msg = f"Request error fetching CIK for ticker '{ticker}' from SEC API: {e}"
            logger.error(error_msg)
            raise CompanyLookupException(error_msg)
        except Exception as e:
            error_msg = f"Error fetching CIK for ticker '{ticker}' from SEC API: {e}"
            logger.error(error_msg)
            raise CompanyLookupException(error_msg)
    
    async def get_ticker_for_cik(self, cik: str) -> Optional[str]:
        """
        Resolves a CIK number to a ticker symbol using the SEC API.
        
        Args:
            cik: The CIK number (with or without leading zeros)
            
        Returns:
            Ticker symbol if found, None otherwise
            
        Raises:
            CompanyLookupException: If API error occurs
        """
        # Ensure CIK is properly formatted with leading zeros
        padded_cik = str(cik).zfill(10)
        
        # Check cache first
        cached_company = self._get_from_cache('cik', padded_cik)
        if cached_company and 'ticker' in cached_company:
            return cached_company['ticker']
        
        # Verify API key is available
        if not self.sec_api_key:
            error_msg = "SEC_API_KEY is not set in the environment."
            logger.error(error_msg)
            raise CompanyLookupException(error_msg)
        
        # SEC-API.io endpoint for CIK to ticker mapping
        url = f"{self.sec_api_base_url}/mapping/cik/{padded_cik.lstrip('0')}?token={self.sec_api_key}"
        
        logger.info(f"Resolving CIK '{padded_cik}' to ticker via sec-api.io")
        
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.get(url, headers=self.headers, timeout=settings.SEC_REQUEST_TIMEOUT_SEC)
                
                if resp.status_code != 200:
                    error_msg = f"SEC API returned status {resp.status_code}: {resp.text}"
                    logger.error(error_msg)
                    raise CompanyLookupException(f"sec-api.io error: {resp.status_code}")
                
                data = resp.json()
                
                if isinstance(data, dict) and 'ticker' in data:
                    ticker = data['ticker']
                    logger.info(f"Resolved CIK '{padded_cik}' to ticker '{ticker}'")
                    
                    # Cache the result
                    self._save_to_cache('cik', padded_cik, data)
                    self._save_to_cache('ticker', ticker, data)
                    
                    return ticker
                    
                logger.warning(f"No ticker found for CIK '{padded_cik}'")
                return None
                
        except Exception as e:
            error_msg = f"Error fetching ticker for CIK '{padded_cik}' from SEC API: {e}"
            logger.error(error_msg)
            raise CompanyLookupException(error_msg)
    
    async def search_companies(self, query: str, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Search for companies by name, ticker, or other attributes.
        
        Args:
            query: Search query string
            limit: Maximum number of results to return
            
        Returns:
            List of matching company records with metadata
            
        Raises:
            CompanyLookupException: If API error occurs
        """
        # Verify API key is available
        if not self.sec_api_key:
            error_msg = "SEC_API_KEY is not set in the environment."
            logger.error(error_msg)
            raise CompanyLookupException(error_msg)
        
        # Check cache first
        cache_key = f"company:search:{query.lower()}"
        if REDIS_AVAILABLE:
            try:
                cached_results = redis_client.get(cache_key)
                if cached_results:
                    return json.loads(cached_results)
            except Exception as e:
                logger.warning(f"Error retrieving search results from cache: {e}")
        
        # Use SEC-API.io for company search
        # This is a simplified implementation - in a real system you might use
        # multiple data sources and merge results
        url = f"{self.sec_api_base_url}/search?token={self.sec_api_key}"
        
        # Prepare search parameters
        payload = {
            "query": query,
            "limit": limit
        }
        
        logger.info(f"Searching for companies with query: '{query}'")
        
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.post(url, json=payload, headers=self.headers, timeout=settings.SEC_REQUEST_TIMEOUT_SEC)
                
                if resp.status_code != 200:
                    error_msg = f"SEC API search returned status {resp.status_code}: {resp.text}"
                    logger.error(error_msg)
                    raise CompanyLookupException(f"sec-api.io search error: {resp.status_code}")
                
                data = resp.json()
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
                
                logger.info(f"Found {len(companies)} companies matching query '{query}'")
                
                # Cache the results
                if REDIS_AVAILABLE:
                    try:
                        redis_client.setex(
                            cache_key,
                            self.cache_ttl,
                            json.dumps(companies)
                        )
                    except Exception as e:
                        logger.warning(f"Error caching search results: {e}")
                
                return companies
                
        except Exception as e:
            error_msg = f"Error searching for companies with query '{query}': {e}"
            logger.error(error_msg)
            raise CompanyLookupException(error_msg)
    
    async def get_company_info(self, identifier: str, identifier_type: str = 'auto') -> Dict[str, Any]:
        """
        Get comprehensive company information by identifier.
        
        This method accepts various identifier types and returns a complete
        company profile with metadata from multiple sources.
        
        Args:
            identifier: Company identifier (ticker, CIK, or name)
            identifier_type: Type of identifier ('ticker', 'cik', 'name', or 'auto' for automatic detection)
            
        Returns:
            Dictionary with company information from multiple sources
            
        Raises:
            CompanyLookupException: If company not found or error occurs
        """
        # Auto-detect identifier type if not specified
        if identifier_type == 'auto':
            if identifier.isdigit() or (identifier.startswith('0') and len(identifier) == 10):
                identifier_type = 'cik'
            elif len(identifier) <= 5 and identifier.isalpha():
                identifier_type = 'ticker'
            else:
                identifier_type = 'name'
        
        # Check cache first
        cached_company = self._get_from_cache(identifier_type, identifier)
        if cached_company and cached_company.get('full_profile', False):
            return cached_company
        
        # Process based on identifier type
        if identifier_type == 'ticker':
            # For ticker, get CIK first
            cik = await self.get_cik_for_ticker(identifier)
            company_info = await self._fetch_company_profile(cik)
            
            # Ensure ticker is set
            company_info['ticker'] = identifier
            
        elif identifier_type == 'cik':
            # Ensure CIK is properly formatted
            padded_cik = str(identifier).zfill(10)
            company_info = await self._fetch_company_profile(padded_cik)
            
            # Try to get ticker if not in profile
            if 'ticker' not in company_info:
                try:
                    ticker = await self.get_ticker_for_cik(padded_cik)
                    if ticker:
                        company_info['ticker'] = ticker
                except Exception as e:
                    logger.warning(f"Could not resolve ticker for CIK {padded_cik}: {e}")
                    
        elif identifier_type == 'name':
            # Search by name and use first result
            results = await self.search_companies(identifier, limit=1)
            if not results:
                raise CompanyLookupException(f"No company found with name '{identifier}'")
                
            result = results[0]
            cik = result.get('cik')
            company_info = await self._fetch_company_profile(cik)
            
            # Add search result data if not in profile
            for key, value in result.items():
                if key not in company_info:
                    company_info[key] = value
        else:
            raise CompanyLookupException(f"Invalid identifier_type: {identifier_type}")
        
        # Mark as full profile and cache
        company_info['full_profile'] = True
        
        # Cache under all known identifiers
        if 'cik' in company_info:
            self._save_to_cache('cik', company_info['cik'], company_info)
            
        if 'ticker' in company_info:
            self._save_to_cache('ticker', company_info['ticker'], company_info)
            
        if 'name' in company_info:
            self._save_to_cache('name', company_info['name'], company_info)
            
        return company_info
    
    async def _fetch_company_profile(self, cik: str) -> Dict[str, Any]:
        """
        Fetch detailed company profile from SEC API.
        
        Args:
            cik: Company CIK number (with or without leading zeros)
            
        Returns:
            Company profile dictionary
            
        Raises:
            CompanyLookupException: If API error occurs
        """
        # Ensure CIK is properly formatted
        padded_cik = str(cik).zfill(10)
        
        # Verify API key is available
        if not self.sec_api_key:
            error_msg = "SEC_API_KEY is not set in the environment."
            logger.error(error_msg)
            raise CompanyLookupException(error_msg)
        
        # SEC-API.io endpoint for company profile
        url = f"{self.sec_api_base_url}/company/{padded_cik.lstrip('0')}?token={self.sec_api_key}"
        
        logger.info(f"Fetching company profile for CIK '{padded_cik}'")
        
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.get(url, headers=self.headers, timeout=settings.SEC_REQUEST_TIMEOUT_SEC)
                
                if resp.status_code != 200:
                    error_msg = f"SEC API returned status {resp.status_code}: {resp.text}"
                    logger.error(error_msg)
                    raise CompanyLookupException(f"sec-api.io error: {resp.status_code}")
                
                profile = resp.json()
                
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
                
                logger.info(f"Successfully fetched company profile for {standardized_profile.get('name')}")
                return standardized_profile
                
        except Exception as e:
            error_msg = f"Error fetching company profile for CIK '{padded_cik}': {e}"
            logger.error(error_msg)
            raise CompanyLookupException(error_msg)
    
    async def resolve_company(self, company_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Resolves and enriches company information from partial data.
        
        This is a utility method for standardizing company references across
        the data acquisition pipeline, ensuring that all services work with
        consistent company identifiers regardless of what was initially provided.
        
        Args:
            company_data: Partial company data (must include at least one identifier)
            
        Returns:
            Enriched company data with all available identifiers and metadata
            
        Raises:
            CompanyLookupException: If company cannot be resolved
        """
        # Extract available identifiers
        ticker = company_data.get('ticker')
        cik = company_data.get('cik')
        name = company_data.get('name')
        
        # Ensure we have at least one identifier
        if not ticker and not cik and not name:
            raise CompanyLookupException("Company resolution requires at least one identifier (ticker, cik, or name)")
        
        # Determine identifier and type to use for resolution
        if ticker:
            identifier = ticker
            identifier_type = 'ticker'
        elif cik:
            identifier = cik
            identifier_type = 'cik'
        else:
            identifier = name
            identifier_type = 'name'
            
        # Get full company information
        try:
            company_info = await self.get_company_info(identifier, identifier_type)
            
            # Merge with original data, prioritizing original values
            result = company_info.copy()
            for key, value in company_data.items():
                if value:  # Only override if value is not None or empty
                    result[key] = value
                    
            json_logger.log_json(
                level="info",
                action="company_resolved",
                message=f"Resolved company {identifier} ({identifier_type})",
                company_name=result.get('name'),
                company_ticker=result.get('ticker'),
                company_cik=result.get('cik')
            )
            
            return result
            
        except Exception as e:
            error_msg = f"Error resolving company with {identifier_type}='{identifier}': {e}"
            logger.error(error_msg)
            raise CompanyLookupException(error_msg)


# Singleton instance for easy access
company_lookup = CompanyLookup()

# If this module is executed directly, run a simple test
if __name__ == "__main__":
    import asyncio
    
    async def test_lookup():
        try:
            # Test ticker to CIK resolution
            ticker = "AAPL"
            cik = await company_lookup.get_cik_for_ticker(ticker)
            print(f"CIK for {ticker}: {cik}")
            
            # Test company info retrieval
            company = await company_lookup.get_company_info(ticker)
            print(f"Company info for {ticker}:")
            print(json.dumps(company, indent=2))
            
            # Test company search
            results = await company_lookup.search_companies("Apple", limit=3)
            print(f"Search results for 'Apple':")
            print(json.dumps(results, indent=2))
            
        except Exception as e:
            print(f"Error during test: {e}")
    
    # Run the async test
    asyncio.run(test_lookup())