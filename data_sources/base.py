"""
Base Data Source Interface
--------------------------

This module defines the core interfaces for all data sources in the acquisition pipeline.
These interfaces establish a consistent contract that all data sources must implement.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any


class BaseDataSource(ABC):
    """
    Abstract base class for all data sources in the acquisition pipeline.
    
    This interface defines the contract that all data sources must implement,
    ensuring consistent behavior and standardized data formats. Data sources
    retrieve information from external APIs, databases, or other systems and
    transform it into a standardized envelope format for further processing.
    """
    
    @abstractmethod
    def fetch(self, params: Dict[str, Any], request_id: str = None) -> Dict[str, Any]:
        """
        Fetch raw data and metadata from the source.

        Args:
            params: A dictionary of parameters required to fetch the data 
                   (e.g., ticker, date, API keys).
            request_id: Optional request ID for tracing

        Returns:
            A dictionary containing:
                - 'content': The raw data (could be bytes, str, etc. depending on the source)
                - 'content_type': A string indicating the type of content (e.g., 'pdf', 'html', 'text', 'url')
                - 'source': The name of the data source (e.g., 'sec', 'yahoo')
                - 'metadata': Additional metadata (e.g., ticker, date, retrieval timestamp)
        """
        pass