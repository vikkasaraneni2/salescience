"""
Yahoo Finance Data Source for Data Acquisition Pipeline
-----------------------------------------------------

This module implements a Yahoo Finance data source for retrieving financial
data about companies. It provides:

1. Stock Quote Data: Current price, volume, market cap, etc.
2. Summary Information: Basic company information and performance stats
3. Historical Price Data: Daily/weekly/monthly historical pricing
4. Key Statistics: Financial ratios and company fundamentals
5. Recommendations: Analyst ratings and recommendations

The module implements the BaseDataSource interface for seamless integration
with the data acquisition pipeline, ensuring consistent data handling and
error reporting across all data sources.
"""

import os
import json
import httpx
import logging
import datetime
import asyncio
import time
from typing import Dict, Any, List, Optional

# Import from internal modules
from config import settings
from data_acquisition.sec_client import BaseDataSource, MessageBusPublisher
from data_acquisition.utils import normalize_envelope, timestamp_now

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("yahoo")

# API configuration
YAHOO_API_KEY = settings.YAHOO_API_KEY
YAHOO_API_BASE_URL = settings.YAHOO_API_BASE_URL
REQUEST_TIMEOUT = settings.YAHOO_REQUEST_TIMEOUT_SEC

# Fallback to yfinance if direct API is not available
try:
    import yfinance as yf
    YFINANCE_AVAILABLE = True
except ImportError:
    logger.warning("yfinance package not installed, fallback to direct API only")
    YFINANCE_AVAILABLE = False


class YahooDataSource(BaseDataSource):
    """
    Yahoo Finance data source for retrieving financial data about companies.
    
    This class implements the BaseDataSource interface for the data acquisition
    pipeline, providing access to Yahoo Finance data through their public API
    or through the yfinance package as a fallback.
    
    It offers methods for retrieving various types of financial data and stock
    information.
    """
    
    def __init__(self):
        """Initialize the Yahoo Finance data source."""
        self.name = "Yahoo"
        self.base_url = YAHOO_API_BASE_URL
        self.api_key = YAHOO_API_KEY
        
        # Standard headers for all requests
        self.headers = {
            "User-Agent": settings.SEC_USER_AGENT,
            "Accept": "application/json",
        }
        
        if self.api_key:
            self.headers["X-API-KEY"] = self.api_key
            
        logger.info("Yahoo Finance data source initialized")
    
    def fetch(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Fetch Yahoo Finance data for a company.
        
        This method implements the BaseDataSource interface method for retrieving
        financial data from Yahoo Finance. It supports fetching company data
        based on ticker symbol.
        
        Args:
            params: Dictionary with parameters:
                - ticker: The stock ticker symbol (required)
                - modules: List of data modules to fetch (default: summaryProfile,price,defaultKeyStatistics)
                
        Returns:
            Dictionary containing:
                - content: JSON data from Yahoo Finance
                - content_type: 'json'
                - source: 'yahoo'
                - metadata: Additional information about the request
                
        Raises:
            ValueError: If required parameters are missing or API errors occur
        """
        # Extract parameters
        ticker = params.get('ticker')
        modules = params.get('modules', 'summaryProfile,price,defaultKeyStatistics')
        
        # Validate parameters
        if not ticker:
            error_msg = "Parameter 'ticker' is required for YahooDataSource.fetch"
            logger.error(error_msg)
            raise ValueError(error_msg)
            
        logger.info(f"Fetching Yahoo Finance data for ticker: {ticker}")
        
        try:
            # First try the direct API approach
            return self._fetch_via_api(ticker, modules)
        except Exception as api_error:
            # Log the API error
            logger.warning(f"Error fetching data via direct API: {api_error}")
            
            # Fall back to yfinance if available
            if YFINANCE_AVAILABLE:
                logger.info(f"Falling back to yfinance for ticker: {ticker}")
                try:
                    return self._fetch_via_yfinance(ticker)
                except Exception as yf_error:
                    # Both approaches failed, return error
                    logger.error(f"Error fetching data via yfinance: {yf_error}")
                    return {
                        'content': None,
                        'content_type': 'json',
                        'source': 'yahoo',
                        'status': 'error',
                        'error': f"API error: {api_error}; yfinance error: {yf_error}",
                        'metadata': {
                            'ticker': ticker,
                            'modules': modules,
                            'retrieved_at': timestamp_now()
                        }
                    }
            else:
                # Only API approach was attempted, return that error
                return {
                    'content': None,
                    'content_type': 'json',
                    'source': 'yahoo',
                    'status': 'error',
                    'error': f"API error: {api_error}; yfinance not available",
                    'metadata': {
                        'ticker': ticker,
                        'modules': modules,
                        'retrieved_at': timestamp_now()
                    }
                }
    
    def _fetch_via_api(self, ticker: str, modules: str) -> Dict[str, Any]:
        """
        Fetch Yahoo Finance data using the direct API.
        
        Args:
            ticker: Stock ticker symbol
            modules: Comma-separated list of data modules to fetch
            
        Returns:
            Data envelope with Yahoo Finance data
            
        Raises:
            ValueError: If API error occurs
        """
        # Construct the Yahoo Finance API URL
        url = f"{self.base_url}/quoteSummary/{ticker}"
        
        # Set up query parameters
        query_params = {
            "modules": modules
        }
        
        logger.debug(f"Yahoo Finance API request URL: {url}")
        logger.debug(f"Query parameters: {query_params}")
        
        # Make the request
        resp = httpx.get(
            url, 
            params=query_params,
            headers=self.headers,
            timeout=REQUEST_TIMEOUT
        )
        
        logger.debug(f"Yahoo Finance API status code: {resp.status_code}")
        
        if resp.status_code != 200:
            error_msg = f"Yahoo Finance API returned status {resp.status_code}: {resp.text}"
            logger.error(error_msg)
            raise ValueError(f"Yahoo Finance API error: {resp.status_code}")
            
        # Parse the response
        data = resp.json()
        
        # Check if the response contains an error
        if "quoteSummary" not in data or data.get("quoteSummary", {}).get("error"):
            error = data.get("quoteSummary", {}).get("error", "Unknown error")
            error_msg = f"Yahoo Finance API returned an error: {error}"
            logger.error(error_msg)
            raise ValueError(error_msg)
            
        # Extract the quote summary result
        result = data.get("quoteSummary", {}).get("result", [])
        
        if not result:
            error_msg = f"No data found for ticker '{ticker}'"
            logger.warning(error_msg)
            return {
                'content': None,
                'content_type': 'json',
                'source': 'yahoo',
                'status': 'not_found',
                'error': error_msg,
                'metadata': {
                    'ticker': ticker,
                    'modules': modules,
                    'retrieved_at': timestamp_now()
                }
            }
            
        # Extract and format the relevant data
        company_data = result[0]
        
        return {
            'content': company_data,
            'content_type': 'json',
            'source': 'yahoo',
            'status': 'success',
            'metadata': {
                'ticker': ticker,
                'modules': modules,
                'retrieved_at': timestamp_now()
            }
        }
    
    def _fetch_via_yfinance(self, ticker: str) -> Dict[str, Any]:
        """
        Fetch Yahoo Finance data using the yfinance package.
        
        This is a fallback method when the direct API approach fails.
        
        Args:
            ticker: Stock ticker symbol
            
        Returns:
            Data envelope with Yahoo Finance data
            
        Raises:
            ValueError: If yfinance error occurs
        """
        if not YFINANCE_AVAILABLE:
            raise ValueError("yfinance package is not installed")
            
        # Use yfinance to fetch data
        yf_ticker = yf.Ticker(ticker)
        info = yf_ticker.info
        
        # yfinance returns an empty dict for invalid tickers
        if not info or 'regularMarketPrice' not in info:
            raise ValueError(f"Ticker '{ticker}' not found or not valid on Yahoo Finance")
            
        return {
            'content': info,  # This is a dict with all available company/stock info
            'content_type': 'json',
            'source': 'yahoo',
            'status': 'success',
            'metadata': {
                'ticker': ticker,
                'source_package': 'yfinance',
                'retrieved_at': timestamp_now()
            }
        }
    
    async def fetch_historical_prices(self, ticker: str, period: str = "1y", interval: str = "1d") -> Dict[str, Any]:
        """
        Fetch historical stock prices for a company.
        
        This method retrieves historical stock price data from Yahoo Finance
        for the specified ticker, period, and interval.
        
        Args:
            ticker: Stock ticker symbol
            period: Time period for historical data (1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y, 10y, max)
            interval: Data interval (1m, 2m, 5m, 15m, 30m, 60m, 90m, 1h, 1d, 5d, 1wk, 1mo, 3mo)
            
        Returns:
            Dictionary containing historical price data and metadata
            
        Raises:
            ValueError: If API error occurs
        """
        if not ticker:
            error_msg = "Ticker symbol is required for historical prices"
            logger.error(error_msg)
            raise ValueError(error_msg)
            
        logger.info(f"Fetching historical prices for {ticker} ({period}, {interval})")
        
        try:
            # Try API approach first
            historical_data = await self._fetch_historical_via_api(ticker, period, interval)
            return historical_data
        except Exception as api_error:
            logger.warning(f"Error fetching historical data via API: {api_error}")
            
            # Fall back to yfinance if available
            if YFINANCE_AVAILABLE:
                logger.info(f"Falling back to yfinance for historical data: {ticker}")
                try:
                    return self._fetch_historical_via_yfinance(ticker, period, interval)
                except Exception as yf_error:
                    logger.error(f"Error fetching historical data via yfinance: {yf_error}")
                    return {
                        'content': None,
                        'content_type': 'json',
                        'source': 'yahoo-historical',
                        'status': 'error',
                        'error': f"API error: {api_error}; yfinance error: {yf_error}",
                        'metadata': {
                            'ticker': ticker,
                            'period': period,
                            'interval': interval,
                            'retrieved_at': timestamp_now()
                        }
                    }
            else:
                # Only API approach was attempted
                return {
                    'content': None,
                    'content_type': 'json',
                    'source': 'yahoo-historical',
                    'status': 'error',
                    'error': f"API error: {api_error}; yfinance not available",
                    'metadata': {
                        'ticker': ticker,
                        'period': period,
                        'interval': interval,
                        'retrieved_at': timestamp_now()
                    }
                }
    
    async def _fetch_historical_via_api(self, ticker: str, period: str, interval: str) -> Dict[str, Any]:
        """
        Fetch historical prices using the direct API.
        
        Args:
            ticker: Stock ticker symbol
            period: Time period for historical data
            interval: Data interval
            
        Returns:
            Data envelope with historical price data
            
        Raises:
            ValueError: If API error occurs
        """
        # Construct the API URL for historical data
        url = f"{self.base_url}/chart/{ticker}"
        
        # Set up query parameters
        query_params = {
            "range": period,
            "interval": interval,
            "includePrePost": "false",
            "events": "div,split"
        }
        
        # Make the request
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                url,
                params=query_params,
                headers=self.headers,
                timeout=REQUEST_TIMEOUT
            )
            
            if resp.status_code != 200:
                error_msg = f"Yahoo Finance API returned status {resp.status_code}: {resp.text}"
                logger.error(error_msg)
                raise ValueError(f"Yahoo Finance API error: {resp.status_code}")
                
            data = resp.json()
            
            # Check for errors in response
            if "chart" not in data or data.get("chart", {}).get("error"):
                error = data.get("chart", {}).get("error", "Unknown error")
                error_msg = f"Yahoo Finance API returned an error: {error}"
                logger.error(error_msg)
                raise ValueError(error_msg)
                
            # Extract the result
            result = data.get("chart", {}).get("result", [])
            
            if not result:
                error_msg = f"No historical data found for ticker '{ticker}'"
                logger.warning(error_msg)
                return {
                    'content': None,
                    'content_type': 'json',
                    'source': 'yahoo-historical',
                    'status': 'not_found',
                    'error': error_msg,
                    'metadata': {
                        'ticker': ticker,
                        'period': period,
                        'interval': interval,
                        'retrieved_at': timestamp_now()
                    }
                }
                
            # Format the historical data
            historical_data = result[0]
            
            return {
                'content': historical_data,
                'content_type': 'json',
                'source': 'yahoo-historical',
                'status': 'success',
                'metadata': {
                    'ticker': ticker,
                    'period': period,
                    'interval': interval,
                    'retrieved_at': timestamp_now()
                }
            }
    
    def _fetch_historical_via_yfinance(self, ticker: str, period: str, interval: str) -> Dict[str, Any]:
        """
        Fetch historical prices using yfinance as a fallback.
        
        Args:
            ticker: Stock ticker symbol
            period: Time period for historical data
            interval: Data interval
            
        Returns:
            Data envelope with historical price data
            
        Raises:
            ValueError: If yfinance error occurs
        """
        if not YFINANCE_AVAILABLE:
            raise ValueError("yfinance package is not installed")
            
        # Use yfinance to fetch historical data
        yf_ticker = yf.Ticker(ticker)
        hist = yf_ticker.history(period=period, interval=interval)
        
        # Check if we got any data
        if hist.empty:
            raise ValueError(f"No historical data found for ticker '{ticker}'")
            
        # Convert to dictionary (JSON-serializable)
        hist_dict = hist.reset_index().to_dict(orient='records')
        
        return {
            'content': hist_dict,
            'content_type': 'json',
            'source': 'yahoo-historical',
            'status': 'success',
            'metadata': {
                'ticker': ticker,
                'period': period,
                'interval': interval,
                'source_package': 'yfinance',
                'retrieved_at': timestamp_now()
            }
        }
    
    async def fetch_recommendations(self, ticker: str) -> Dict[str, Any]:
        """
        Fetch analyst recommendations for a company.
        
        This method retrieves analyst recommendations and target price
        data from Yahoo Finance for the specified ticker.
        
        Args:
            ticker: Stock ticker symbol
            
        Returns:
            Dictionary containing recommendation data and metadata
            
        Raises:
            ValueError: If API error occurs
        """
        logger.info(f"Fetching recommendations for {ticker}")
        
        try:
            # Construct the API URL for recommendations
            url = f"{self.base_url}/quoteSummary/{ticker}"
            
            # Set up query parameters
            query_params = {
                "modules": "recommendationTrend,upgradeDowngradeHistory"
            }
            
            # Make the request
            async with httpx.AsyncClient() as client:
                resp = await client.get(
                    url,
                    params=query_params,
                    headers=self.headers,
                    timeout=REQUEST_TIMEOUT
                )
                
                if resp.status_code != 200:
                    error_msg = f"Yahoo Finance API returned status {resp.status_code}: {resp.text}"
                    logger.error(error_msg)
                    raise ValueError(f"Yahoo Finance API error: {resp.status_code}")
                    
                data = resp.json()
                
                # Check for errors in response
                if "quoteSummary" not in data or data.get("quoteSummary", {}).get("error"):
                    error = data.get("quoteSummary", {}).get("error", "Unknown error")
                    error_msg = f"Yahoo Finance API returned an error: {error}"
                    logger.error(error_msg)
                    raise ValueError(error_msg)
                    
                # Extract the result
                result = data.get("quoteSummary", {}).get("result", [])
                
                if not result:
                    error_msg = f"No recommendation data found for ticker '{ticker}'"
                    logger.warning(error_msg)
                    return {
                        'content': None,
                        'content_type': 'json',
                        'source': 'yahoo-recommendations',
                        'status': 'not_found',
                        'error': error_msg,
                        'metadata': {
                            'ticker': ticker,
                            'retrieved_at': timestamp_now()
                        }
                    }
                    
                # Extract recommendation data
                recommendation_data = result[0]
                
                return {
                    'content': recommendation_data,
                    'content_type': 'json',
                    'source': 'yahoo-recommendations',
                    'status': 'success',
                    'metadata': {
                        'ticker': ticker,
                        'retrieved_at': timestamp_now()
                    }
                }
                
        except Exception as e:
            # Fall back to yfinance if available
            if YFINANCE_AVAILABLE:
                try:
                    logger.info(f"Falling back to yfinance for recommendations: {ticker}")
                    yf_ticker = yf.Ticker(ticker)
                    
                    # Get recommendations from yfinance
                    try:
                        recommendations = yf_ticker.recommendations
                        if recommendations is not None and not recommendations.empty:
                            # Convert to dictionary (JSON-serializable)
                            rec_dict = recommendations.reset_index().to_dict(orient='records')
                            
                            return {
                                'content': rec_dict,
                                'content_type': 'json',
                                'source': 'yahoo-recommendations',
                                'status': 'success',
                                'metadata': {
                                    'ticker': ticker,
                                    'source_package': 'yfinance',
                                    'retrieved_at': timestamp_now()
                                }
                            }
                    except Exception as rec_error:
                        logger.warning(f"Error fetching recommendations via yfinance: {rec_error}")
                        
                    # If recommendations failed, try analyst data
                    try:
                        analysts = yf_ticker.analyst_recommendations
                        if analysts is not None and not analysts.empty:
                            # Convert to dictionary (JSON-serializable)
                            analysts_dict = analysts.reset_index().to_dict(orient='records')
                            
                            return {
                                'content': analysts_dict,
                                'content_type': 'json',
                                'source': 'yahoo-recommendations',
                                'status': 'success',
                                'metadata': {
                                    'ticker': ticker,
                                    'source_package': 'yfinance',
                                    'retrieved_at': timestamp_now()
                                }
                            }
                    except Exception as analyst_error:
                        logger.warning(f"Error fetching analyst data via yfinance: {analyst_error}")
                    
                    # Both attempts failed
                    error_msg = f"Error fetching recommendations from Yahoo Finance: {e}"
                    logger.error(error_msg)
                    
                except Exception as yf_error:
                    error_msg = f"API error: {e}; yfinance error: {yf_error}"
                    logger.error(error_msg)
            else:
                error_msg = f"Error fetching recommendations from Yahoo Finance: {e}"
                logger.error(error_msg)
                
            return {
                'content': None,
                'content_type': 'json',
                'source': 'yahoo-recommendations',
                'status': 'error',
                'error': error_msg,
                'metadata': {
                    'ticker': ticker,
                    'retrieved_at': timestamp_now()
                }
            }
    
    async def fetch_all_data(self, ticker: str, publisher: Optional[MessageBusPublisher] = None,
                         topic: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Fetch all available data for a company from Yahoo Finance.
        
        This method aggregates multiple data types from Yahoo Finance into
        a comprehensive dataset. It optionally publishes results to a message bus.
        
        Args:
            ticker: Stock ticker symbol
            publisher: Optional MessageBusPublisher instance for event publishing
            topic: Optional topic name for publishing events
            
        Returns:
            List of data envelopes with different types of Yahoo Finance data
            
        Example usage:
            yahoo = YahooDataSource()
            publisher = MessageBusPublisher(redis_url)
            results = await yahoo.fetch_all_data('AAPL', publisher, 'data.yahoo')
        """
        logger.info(f"Fetching all Yahoo Finance data for {ticker}")
        results = []
        
        try:
            # Async tasks for all data types
            tasks = [
                self.fetch_async({'ticker': ticker, 'modules': 'summaryProfile,price,defaultKeyStatistics'}),
                self.fetch_historical_prices(ticker),
                self.fetch_recommendations(ticker)
            ]
            
            # Execute all tasks concurrently
            data_sets = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Process results
            for i, data in enumerate(data_sets):
                # Skip exceptions and process only successful results
                if isinstance(data, Exception):
                    logger.error(f"Error fetching data set {i} for {ticker}: {data}")
                    continue
                    
                # Normalize the envelope
                envelope = normalize_envelope(data)
                
                # Add to results
                results.append(envelope)
                
                # Publish to message bus if enabled
                if publisher and topic:
                    try:
                        publisher.publish(topic, envelope)
                        logger.info(f"Published {ticker} envelope to topic {topic}")
                    except Exception as pub_exc:
                        logger.warning(f"Failed to publish {ticker} envelope to message bus: {pub_exc}")
            
            logger.info(f"Successfully fetched {len(results)} data sets for {ticker}")
            return results
            
        except Exception as e:
            error_msg = f"Error fetching all data for {ticker}: {e}"
            logger.error(error_msg)
            return [{
                'content': None,
                'content_type': 'json',
                'source': 'yahoo-aggregated',
                'status': 'error',
                'error': str(e),
                'metadata': {
                    'ticker': ticker,
                    'retrieved_at': timestamp_now()
                }
            }]
    
    async def fetch_async(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Asynchronous version of fetch method for use in async contexts.
        
        This method provides the same functionality as fetch() but in an
        asynchronous implementation for use with asyncio.
        
        Args:
            params: Same parameters as fetch() method
            
        Returns:
            Same return value as fetch() method
        """
        # Extract parameters
        ticker = params.get('ticker')
        modules = params.get('modules', 'summaryProfile,price,defaultKeyStatistics')
        
        # Validate parameters
        if not ticker:
            error_msg = "Parameter 'ticker' is required for fetch_async"
            logger.error(error_msg)
            raise ValueError(error_msg)
            
        logger.info(f"Async fetching Yahoo Finance data for ticker: {ticker}")
        
        try:
            # Try direct API first
            async with httpx.AsyncClient() as client:
                # Construct the Yahoo Finance API URL
                url = f"{self.base_url}/quoteSummary/{ticker}"
                
                # Set up query parameters
                query_params = {
                    "modules": modules
                }
                
                resp = await client.get(
                    url,
                    params=query_params,
                    headers=self.headers,
                    timeout=REQUEST_TIMEOUT
                )
                
                if resp.status_code != 200:
                    error_msg = f"Yahoo Finance API returned status {resp.status_code}: {resp.text}"
                    logger.error(error_msg)
                    raise ValueError(f"Yahoo Finance API error: {resp.status_code}")
                    
                data = resp.json()
                
                # Check if the response contains an error
                if "quoteSummary" not in data or data.get("quoteSummary", {}).get("error"):
                    error = data.get("quoteSummary", {}).get("error", "Unknown error")
                    error_msg = f"Yahoo Finance API returned an error: {error}"
                    logger.error(error_msg)
                    raise ValueError(error_msg)
                    
                # Extract the quote summary result
                result = data.get("quoteSummary", {}).get("result", [])
                
                if not result:
                    error_msg = f"No data found for ticker '{ticker}'"
                    logger.warning(error_msg)
                    return {
                        'content': None,
                        'content_type': 'json',
                        'source': 'yahoo',
                        'status': 'not_found',
                        'error': error_msg,
                        'metadata': {
                            'ticker': ticker,
                            'modules': modules,
                            'retrieved_at': timestamp_now()
                        }
                    }
                    
                # Extract and format the relevant data
                company_data = result[0]
                
                return {
                    'content': company_data,
                    'content_type': 'json',
                    'source': 'yahoo',
                    'status': 'success',
                    'metadata': {
                        'ticker': ticker,
                        'modules': modules,
                        'retrieved_at': timestamp_now()
                    }
                }
        except Exception as api_error:
            logger.warning(f"API error in fetch_async: {api_error}")
            
            # Fall back to yfinance if available
            if YFINANCE_AVAILABLE:
                try:
                    # Since yfinance is synchronous, run it in a separate thread
                    # to avoid blocking the event loop
                    loop = asyncio.get_running_loop()
                    result = await loop.run_in_executor(
                        None, 
                        lambda: self._fetch_via_yfinance(ticker)
                    )
                    return result
                except Exception as yf_error:
                    logger.error(f"yfinance error in fetch_async: {yf_error}")
                    return {
                        'content': None,
                        'content_type': 'json',
                        'source': 'yahoo',
                        'status': 'error',
                        'error': f"API error: {api_error}; yfinance error: {yf_error}",
                        'metadata': {
                            'ticker': ticker,
                            'modules': modules,
                            'retrieved_at': timestamp_now()
                        }
                    }
            else:
                return {
                    'content': None,
                    'content_type': 'json',
                    'source': 'yahoo',
                    'status': 'error',
                    'error': f"API error: {api_error}; yfinance not available",
                    'metadata': {
                        'ticker': ticker,
                        'modules': modules,
                        'retrieved_at': timestamp_now()
                    }
                }


# If this module is executed directly, run a simple test
if __name__ == "__main__":
    import asyncio
    
    async def test_yahoo():
        try:
            yahoo = YahooDataSource()
            ticker = "AAPL"
            
            # Test basic fetch
            print(f"Fetching data for {ticker}...")
            result = yahoo.fetch({'ticker': ticker})
            print(f"Fetch status: {result.get('status')}")
            
            # Test historical prices
            print(f"Fetching historical prices for {ticker}...")
            historical = await yahoo.fetch_historical_prices(ticker)
            print(f"Historical data status: {historical.get('status')}")
            
            # Test recommendations
            print(f"Fetching recommendations for {ticker}...")
            recommendations = await yahoo.fetch_recommendations(ticker)
            print(f"Recommendations status: {recommendations.get('status')}")
            
            # Test fetch all data
            print(f"Fetching all data for {ticker}...")
            all_data = await yahoo.fetch_all_data(ticker)
            print(f"Retrieved {len(all_data)} data sets")
            
        except Exception as e:
            print(f"Error during test: {e}")
    
    # Run the async test
    asyncio.run(test_yahoo())