# data_acquisition/yahoo.py
# -----------------------------------------------------------------------------
# Yahoo Finance Data Source Implementation
# -----------------------------------------------------------------------------
# This module implements a data source for Yahoo Finance by inheriting from the
# BaseDataSource interface. It uses the yfinance package to fetch stock/company
# information. This class can be extended to support batch fetching, async, or
# additional endpoints as needed.
#
# Why this design?
# - Encapsulates all Yahoo-specific logic in one place.
# - Makes it easy to update, test, or remove Yahoo as a source independently.
# - Follows the architectural contract for data acquisition sources.
#
# Requirements:
#   pip install yfinance

from data_acquisition.base import BaseDataSource
import yfinance as yf
from typing import Dict, Any
import datetime

class YahooDataSource(BaseDataSource):
    def fetch(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Fetches stock/company info from Yahoo Finance using yfinance.

        Args:
            params (Dict[str, Any]):
                Must include 'ticker' (e.g., 'AAPL').
                Can be extended to include other options (e.g., period, interval).

        Returns:
            Dict[str, Any]:
                Standardized envelope with:
                    - 'content': The info dict from yfinance (JSON-serializable)
                    - 'content_type': 'json'
                    - 'source': 'yahoo'
                    - 'metadata': Includes ticker and retrieval timestamp
        Raises:
            ValueError: If the ticker is invalid or not found.
        """
        ticker = params.get('ticker')
        if not ticker:
            raise ValueError("'ticker' parameter is required for YahooDataSource.fetch")
        yf_ticker = yf.Ticker(ticker)
        info = yf_ticker.info
        # yfinance returns an empty dict for invalid tickers
        if not info or 'regularMarketPrice' not in info:
            raise ValueError(f"Ticker '{ticker}' not found or not valid on Yahoo Finance.")
        return {
            'content': info,  # This is a dict with all available company/stock info
            'content_type': 'json',
            'source': 'yahoo',
            'metadata': {
                'ticker': ticker,
                'retrieved_at': datetime.datetime.utcnow().isoformat() + 'Z'
            }
        }

# Example usage:
# yahoo_source = YahooDataSource()
# result = yahoo_source.fetch({'ticker': 'AAPL'})
# print(result)
#
# To extend for batch fetching, implement a fetch_batch method that takes a list of tickers.
# For async support, use an async HTTP client or run yfinance in a thread pool. 