"""
SEC Client Module Redirect
--------------------------

This file redirects imports from the legacy data_acquisition.sec_client
to the new modular structure with appropriate deprecation warnings.

The functionality has been refactored into:
- data_sources.sec: Core SEC data source components
- workers.sec_worker: Worker and batch processing functionality
"""

# Import everything from the compatibility layer
from .sec_client_compat import *