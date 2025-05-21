"""
SEC Data Source Package
----------------------

Focused SEC EDGAR API integration for fetching financial filings and data.

This package provides a clean, modular interface for SEC data acquisition with
clear separation of concerns between API communication, filing handling, and
XBRL data processing.

Components:
- SECClient: Core SEC API communication client
- SECFilingsHandler: Specialized handler for different filing types  
- XBRLHandler: Specialized handler for XBRL data extraction
- get_cik_for_ticker: Utility function for ticker to CIK resolution

Backward Compatibility:
- SECDataSource: Alias for SECClient for legacy compatibility
- All major functions from the original sec_client module are available
"""

import warnings
from typing import Dict, Any, Optional

# Import all core components
from .client import SECClient, get_cik_for_ticker
from .filings import SECFilingsHandler  
from .xbrl import XBRLHandler

# Import base class from parent data_sources
from ..base import BaseDataSource


# Convenience class that combines all SEC functionality
class SEC:
    """
    Convenience class that provides access to all SEC functionality in one place.
    
    This class acts as a facade, providing easy access to client, filings, and XBRL
    functionality without needing to import multiple classes.
    
    Usage:
        sec = SEC()
        
        # Access client directly
        data = sec.client.fetch({'ticker': 'AAPL'})
        
        # Access filings handler  
        reports = sec.filings.fetch_last_n_years({'ticker': 'AAPL'}, 3)
        
        # Access XBRL handler
        facts = sec.xbrl.fetch_xbrl_fact({'ticker': 'AAPL'}, 'us-gaap:Revenues')
    """
    
    def __init__(self):
        self.client = SECClient()
        self.filings = SECFilingsHandler(self.client)
        self.xbrl = XBRLHandler(self.client)


# Backward compatibility aliases and classes
class SECDataSource(SECClient):
    """
    Backward compatibility alias for SECClient.
    
    This class maintains compatibility with existing code that imports
    SECDataSource from data_acquisition.sec_client.
    """
    
    def __init__(self):
        warnings.warn(
            "SECDataSource is deprecated. Use SECClient from data_sources.sec instead.",
            DeprecationWarning,
            stacklevel=2
        )
        super().__init__()


# Convenience functions for common operations
def fetch_annual_reports(params: Dict[str, Any], n_years: int = 5, request_id: Optional[str] = None):
    """
    Convenience function to fetch annual reports (10-K) for a company.
    
    Args:
        params: Company parameters (ticker or cik)
        n_years: Number of years to fetch (default: 5)
        request_id: Optional request ID for tracing
        
    Returns:
        List of annual report envelopes
    """
    handler = SECFilingsHandler()
    return handler.fetch_last_n_years(params, n_years, '10-K', request_id)


def fetch_quarterly_reports(params: Dict[str, Any], n_quarters: int = 4, request_id: Optional[str] = None):
    """
    Convenience function to fetch quarterly reports (10-Q) for a company.
    
    Args:
        params: Company parameters (ticker or cik)
        n_quarters: Number of quarters to fetch (default: 4)
        request_id: Optional request ID for tracing
        
    Returns:
        List of quarterly report envelopes
    """
    handler = SECFilingsHandler()
    return handler.fetch_quarterly_reports(params, n_quarters, request_id)


def fetch_xbrl_concept(params: Dict[str, Any], concept: str, request_id: Optional[str] = None):
    """
    Convenience function to fetch XBRL concept data for a company.
    
    Args:
        params: Company parameters (ticker or cik)
        concept: XBRL concept (e.g., 'us-gaap:Revenues')
        request_id: Optional request ID for tracing
        
    Returns:
        XBRL concept data envelope
    """
    handler = XBRLHandler()
    return handler.fetch_xbrl_fact(params, concept, request_id)


# All public exports
__all__ = [
    # Core classes
    'SECClient', 
    'SECFilingsHandler', 
    'XBRLHandler',
    'SEC',
    
    # Utility functions  
    'get_cik_for_ticker',
    
    # Convenience functions
    'fetch_annual_reports',
    'fetch_quarterly_reports', 
    'fetch_xbrl_concept',
    
    # Backward compatibility
    'SECDataSource',
    'BaseDataSource'
]