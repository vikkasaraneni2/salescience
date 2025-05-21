"""
Data Sources Package
-------------------

This package contains focused, single-responsibility data source clients
for fetching data from external APIs and services.

Each data source module follows the BaseDataSource interface and provides
clean, focused functionality without mixing concerns.

Available Data Sources:
- sec: SEC EDGAR API integration for financial filings and XBRL data
- base: Base classes and interfaces for all data sources

Usage:
    # Import specific components
    from data_sources.sec import SECClient, SECFilingsHandler
    
    # Import convenience facade
    from data_sources.sec import SEC
    sec = SEC()
    
    # Use backward compatibility imports
    from data_sources.sec import SECDataSource  # with deprecation warning
"""

import warnings
from typing import Dict, Any, Optional

# Import base classes
from .base import BaseDataSource

# Import SEC module components for convenience
from . import sec

# Re-export major components for convenience
from .sec import (
    SECClient, 
    SECFilingsHandler, 
    XBRLHandler, 
    SEC,
    get_cik_for_ticker,
    fetch_annual_reports,
    fetch_quarterly_reports,
    fetch_xbrl_concept,
    SECDataSource  # With deprecation warning
)


def get_data_source(source_type: str):
    """
    Factory function to get a data source instance by type.
    
    Args:
        source_type: Type of data source ('sec', 'yahoo', etc.)
        
    Returns:
        Data source instance
        
    Raises:
        ValueError: If source_type is not supported
    """
    if source_type.lower() == 'sec':
        return SEC()
    else:
        raise ValueError(f"Unsupported data source type: {source_type}")


# Version information
__version__ = "1.0.0"

# All public exports
__all__ = [
    # Base classes
    'BaseDataSource',
    
    # SEC module
    'sec',
    
    # SEC components (for convenience)
    'SECClient',
    'SECFilingsHandler', 
    'XBRLHandler',
    'SEC',
    'get_cik_for_ticker',
    'fetch_annual_reports',
    'fetch_quarterly_reports',
    'fetch_xbrl_concept',
    
    # Utility functions
    'get_data_source',
    
    # Backward compatibility
    'SECDataSource',
    
    # Package info
    '__version__'
]