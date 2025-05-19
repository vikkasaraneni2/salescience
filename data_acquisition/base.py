# data_acquisition/base.py
# -----------------------------------------------------------------------------
# Abstract Base Class for Data Acquisition Sources
# -----------------------------------------------------------------------------
# This file defines the foundational interface (contract) for all data source
# classes in the data acquisition layer. By using Python's abc module, we ensure
# that every data source provides a consistent fetch method, making it easy to add,
# test, or swap sources in the future.
#
# -----------------------------
# 1. Architectural Principles
# -----------------------------
# - Separation of Concerns: This layer is ONLY responsible for fetching and returning
#   raw data + metadata. No normalization, parsing, or business logic is allowed here.
#   This keeps the layer simple, testable, and easy to extend for new sources or data types.
# - Extensibility: New data sources can be added by simply implementing this interface.
# - Maintainability: Downstream code can always expect the same output structure.
# - Testability: Each source can be tested in isolation, and the interface can be mocked.
#
# -----------------------------
# 2. Interface: Abstract Base Class (ABC)
# -----------------------------
# - We use Python's abc module to define an abstract base class (ABC).
# - All data sources must inherit from this class and implement the fetch method.
# - The fetch method enforces a contract for what data must be returned and how.
#
# 
# - Guarantees consistency across all data sources.
# - Makes it easy to add new sources without changing the rest of the system.
# - Supports robust error handling, logging, and traceability.
# - Enables parallel development and onboarding of new team members.

from abc import ABC, abstractmethod
from typing import Dict, Any

class BaseDataSource(ABC):
    @abstractmethod
    def fetch(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Fetch raw data and metadata from the source.

        Args:
            params (Dict[str, Any]):
                A dictionary of parameters required to fetch the data (e.g., ticker, date, API keys).
                This allows the fetch method to be flexible and support a wide range of data sources.

        Returns:
            Dict[str, Any]:
                A dictionary containing:
                    - 'content': The raw data (could be bytes, str, etc. depending on the source)
                        # Example: PDF bytes, HTML string, plaintext, or a URL to a file
                    - 'content_type': A string indicating the type of content (e.g., 'pdf', 'html', 'text', 'url')
                        # This allows downstream normalization logic to handle different formats appropriately.
                    - 'source': The name of the data source (e.g., 'sec', 'yahoo')
                        # Provides traceability and context for debugging, logging, and analysis.
                    - 'metadata': Additional metadata (e.g., ticker, date, retrieval timestamp)
                        # Useful for tracking, auditing, and downstream processing.

        Why this structure?
            - 'content' and 'content_type' allow downstream normalization logic to handle different formats.
            - 'source' and 'metadata' provide traceability and context for debugging, logging, and analysis.
            - This envelope pattern makes it easy to add new data types or sources without breaking the contract.
            - By returning a dict, we allow for future extensibility (e.g., adding new fields as needed).

        Example usage:
            sec_source = SECDataSource()
            result = sec_source.fetch({'ticker': 'AAPL', 'year': 2023})
            # result might be:
            # {
            #   'content': b'%PDF-1.4...',
            #   'content_type': 'pdf',
            #   'source': 'sec',
            #   'metadata': {'ticker': 'AAPL', 'year': 2023, 'retrieved_at': '2024-06-01T12:00:00Z'}
            # }
        """
        pass 