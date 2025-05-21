"""
Backward Compatibility Layer for sec_client Module
-------------------------------------------------

This module provides backward compatibility for code that imports from
data_acquisition.sec_client. It imports the components from the new
data_sources.sec package and re-exports them with deprecation warnings.

This allows existing code to continue working while encouraging migration
to the new modular structure.
"""

import warnings
from typing import Dict, Any, Optional, List

# Import all components from the new structure
from data_sources.sec import (
    SECClient,
    SECFilingsHandler, 
    XBRLHandler,
    SEC,
    get_cik_for_ticker,
    fetch_annual_reports,
    fetch_quarterly_reports,
    fetch_xbrl_concept,
    BaseDataSource
)

# Import workers components that were moved
from workers.sec_worker import (
    MessageBusPublisher,
    process_sec_batch_job,
    run_sec_batch_worker
)

# Import utility functions that may have been in the original
from data_acquisition.utils import get_job_redis_key


def _deprecated_import_warning(item_name: str, new_location: str):
    """Helper to show deprecation warnings for imports."""
    warnings.warn(
        f"Importing {item_name} from data_acquisition.sec_client is deprecated. "
        f"Use 'from {new_location} import {item_name}' instead.",
        DeprecationWarning,
        stacklevel=3
    )


# Legacy class alias with deprecation warning
class SECDataSource(SECClient):
    """
    Legacy class alias for backward compatibility.
    
    This class maintains the same interface as the original SECDataSource
    but delegates to the new SECClient implementation.
    """
    
    def __init__(self):
        _deprecated_import_warning("SECDataSource", "data_sources.sec")
        super().__init__()
    
    # Legacy method aliases if needed
    def fetch_last_n_years(self, params: Dict[str, Any], n_years: int = 5, 
                          form_type: str = '10-K', request_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Legacy method that routes to SECFilingsHandler."""
        handler = SECFilingsHandler()
        return handler.fetch_last_n_years(params, n_years, form_type, request_id)
    
    def fetch_quarterly_reports(self, params: Dict[str, Any], n_quarters: int = 4, 
                               request_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Legacy method that routes to SECFilingsHandler."""
        handler = SECFilingsHandler()
        return handler.fetch_quarterly_reports(params, n_quarters, request_id)
    
    def fetch_material_events(self, params: Dict[str, Any], limit: int = 10, 
                             request_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Legacy method that routes to SECFilingsHandler."""
        handler = SECFilingsHandler()
        return handler.fetch_material_events(params, limit, request_id)
    
    def fetch_insider_filings(self, params: Dict[str, Any], limit: int = 10, 
                             request_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Legacy method that routes to SECFilingsHandler.""" 
        handler = SECFilingsHandler()
        return handler.fetch_insider_filings(params, limit, request_id)
    
    def fetch_xbrl_fact(self, params: Dict[str, Any], concept: str, 
                       request_id: Optional[str] = None) -> Dict[str, Any]:
        """Legacy method that routes to XBRLHandler."""
        handler = XBRLHandler()
        return handler.fetch_xbrl_fact(params, concept, request_id)


# Module-level deprecation notice
warnings.warn(
    "The data_acquisition.sec_client module is deprecated and will be removed in a future version. "
    "Please update your imports to use the new modular structure:\n"
    "  - from data_sources.sec import SECClient, SECFilingsHandler, XBRLHandler\n"
    "  - from workers.sec_worker import MessageBusPublisher, process_sec_batch_job",
    DeprecationWarning,
    stacklevel=2
)

# Export everything for backward compatibility
__all__ = [
    # Core classes
    'SECDataSource',
    'SECClient', 
    'SECFilingsHandler',
    'XBRLHandler',
    'SEC',
    'BaseDataSource',
    
    # Worker functionality
    'MessageBusPublisher',
    'process_sec_batch_job', 
    'run_sec_batch_worker',
    
    # Utility functions
    'get_cik_for_ticker',
    'get_job_redis_key',
    
    # Convenience functions
    'fetch_annual_reports',
    'fetch_quarterly_reports',
    'fetch_xbrl_concept'
]