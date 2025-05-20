"""
Base Interfaces for Data Acquisition Pipeline
--------------------------------------------

This module defines the core interfaces (abstract base classes) for the
data acquisition pipeline. These interfaces establish a consistent contract
that all data sources must implement, ensuring uniform behavior and
standardized data formats throughout the system.

Key benefits of these interfaces:
1. Consistent API: All data sources expose the same methods
2. Standard Data Format: Uniform envelope structure across sources
3. Pluggable Architecture: Easy to add new data sources
4. Testability: Interfaces can be mocked for unit testing
5. Separation of Concerns: Clean boundaries between components

The BaseDataSource interface is the cornerstone of the data acquisition
layer, defining how data is retrieved from external sources and
standardized for processing by downstream components.

System Architecture Context
--------------------------
The BaseDataSource interface sits at the foundation of a multi-tier data pipeline:
  
  ┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
  │ Data Acquisition│     │ Processing &    │     │ Storage &       │
  │ (This Layer)    │────▶│ Normalization   │────▶│ Retrieval       │
  └─────────────────┘     └─────────────────┘     └─────────────────┘
         ▲                                               │
         │                                               │
         └───────────────────────────────────────────────┘

Within the Data Acquisition layer, specific implementations (SEC, Yahoo, etc.)
inherit from this base class, providing a unified interface for the rest of the system.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional


class BaseDataSource(ABC):
    """
    Abstract base class for all data sources in the acquisition pipeline.
    
    This interface defines the contract that all data sources must implement,
    ensuring consistent behavior and standardized data formats. Data sources
    retrieve information from external APIs, databases, or other systems and
    transform it into a standardized envelope format for further processing.
    
    All concrete data source implementations (SEC, Yahoo, etc.) must inherit 
    from this base class and implement its methods.
    """
    
    @abstractmethod
    def fetch(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Fetch raw data and metadata from the source.
        
        This method is the primary entry point for data acquisition. It takes
        source-specific parameters and returns a standardized data envelope
        containing the retrieved content and associated metadata.
        
        Args:
            params (Dict[str, Any]):
                A dictionary of parameters required to fetch the data.
                The specific parameters depend on the data source implementation.
                Common examples include:
                - 'ticker': Stock ticker symbol (e.g., 'AAPL')
                - 'cik': SEC Central Index Key
                - 'form_type': Type of SEC filing (e.g., '10-K', '10-Q')
                - 'year': Year of data to retrieve
        
        Returns:
            Dict[str, Any]:
                A standardized data envelope containing:
                - 'content': The raw data content
                - 'content_type': Content format (e.g., 'html', 'json', 'pdf', 'text')
                - 'source': Data source identifier (e.g., 'sec', 'yahoo')
                - 'status': Status of the operation ('success', 'error', 'not_found')
                - 'metadata': Additional context and provenance information
                - 'error': Error message if status is 'error' (optional)
                
        Raises:
            ValueError: If required parameters are missing or invalid
            
        Implementation guidelines:
        1. Always return a valid envelope, even when errors occur
        2. Include comprehensive metadata for tracking and debugging
        3. Handle API rate limits and retries appropriately
        4. Follow source-specific best practices and terms of service
        
        Example usage:
            sec_source = SECDataSource()
            result = sec_source.fetch({'ticker': 'AAPL', 'year': 2023})
            # result might be:
            # {
            #   'content': b'%PDF-1.4...',
            #   'content_type': 'pdf',
            #   'source': 'sec',
            #   'status': 'success', 
            #   'metadata': {'ticker': 'AAPL', 'year': 2023, 'retrieved_at': '2024-06-01T12:00:00Z'}
            # }
        """
        pass


class DataSourceRegistry:
    """
    Registry for data sources in the acquisition pipeline.
    
    This class maintains a centralized registry of all available data sources,
    allowing dynamic lookup and instantiation of data sources by name. It
    supports the pluggable architecture of the data acquisition pipeline by
    enabling runtime discovery and configuration of data sources.
    """
    
    def __init__(self):
        """Initialize an empty registry."""
        self._sources = {}
        
    def register(self, name: str, source_class):
        """
        Register a data source class with the registry.
        
        Args:
            name: Unique identifier for the data source
            source_class: Class that implements BaseDataSource
            
        Raises:
            ValueError: If name is already registered or class doesn't
                       implement BaseDataSource
        """
        # Verify the class inherits from BaseDataSource
        if not issubclass(source_class, BaseDataSource):
            raise ValueError(f"Class {source_class.__name__} must implement BaseDataSource")
            
        # Check for duplicate registrations
        if name in self._sources:
            raise ValueError(f"Data source '{name}' is already registered")
            
        # Register the data source
        self._sources[name] = source_class
        
    def get_source(self, name: str) -> BaseDataSource:
        """
        Get an instance of the specified data source.
        
        Args:
            name: The name of the data source to instantiate
            
        Returns:
            An initialized data source instance
            
        Raises:
            KeyError: If the requested data source is not registered
        """
        if name not in self._sources:
            raise KeyError(f"Data source '{name}' is not registered")
            
        # Create and return a new instance of the data source
        return self._sources[name]()
        
    def list_sources(self) -> List[str]:
        """
        Get a list of all registered data source names.
        
        Returns:
            List of data source names
        """
        return list(self._sources.keys())
        
    def has_source(self, name: str) -> bool:
        """
        Check if a data source is registered.
        
        Args:
            name: The name of the data source to check
            
        Returns:
            True if the data source is registered, False otherwise
        """
        return name in self._sources


# Create singleton instance for easy access
registry = DataSourceRegistry()

# Export key components
__all__ = ['BaseDataSource', 'DataSourceRegistry', 'registry']