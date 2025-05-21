# BaseDataSource Interface Documentation

## Overview
The `base.py` module defines the foundational interface for all data source classes in the data acquisition layer using Python's Abstract Base Class (ABC) pattern. This establishes a consistent contract that all data sources must implement, ensuring a unified approach to data acquisition throughout the system.

## Architecture Principles

### Separation of Concerns
This layer is **ONLY** responsible for fetching and returning raw data + metadata. No normalization, parsing, or business logic is allowed here. This keeps the layer:
- Simple
- Testable
- Easy to extend for new sources/data types

### Extensibility
New data sources can be added by simply implementing this interface with source-specific logic.

### Maintainability
Downstream code can always expect the same output structure regardless of the data source.

### Testability
Each source can be tested in isolation, and the interface can be mocked for higher-level testing.

## Interface Design Benefits

### Clean Abstraction
The abstract base class pattern provides a clear separation between what a component does (interface) and how it does it (implementation).

### Interface Consistency
All data sources expose the same method signatures, ensuring downstream components have a unified way to interact with any source.

### Output Standardization
The structured envelope pattern ensures that data from different sources all follow the same format, making downstream processing simpler.

### Error Handling
By standardizing the error reporting approach, consistent error handling and recovery strategies can be implemented at the orchestration layer.

### Loose Coupling
The system can add, remove, or replace data sources without modifying code that depends on those sources, as long as the interface is respected.

## System Architecture Context

The BaseDataSource interface sits at the foundation of a multi-tier data pipeline:

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│ Data Acquisition│     │ Processing &    │     │ Storage &       │
│ (This Layer)    │────▶│ Normalization   │────▶│ Retrieval       │
└─────────────────┘     └─────────────────┘     └─────────────────┘
         ▲                                               │
         │                                               │
         └───────────────────────────────────────────────┘
```

Within the Data Acquisition layer, specific implementations (SEC, Yahoo, etc.) inherit from this base class, providing a unified interface for the rest of the system.

## Interface Details

### BaseDataSource ABC

```python
class BaseDataSource(ABC):
    @abstractmethod
    def fetch(self, params: Dict[str, Any]) -> Dict[str, Any]:
        # Implementation required in concrete classes
        pass
```

### fetch() Method

**Purpose**: Retrieves raw data and metadata from a specific source

**Parameters**:
- `params`: Dictionary of parameters required for the specific data source
   - E.g., ticker, date, API keys, etc.
   - Allows flexibility across different source types

**Returns**: Standard envelope dictionary containing:
- `content`: Raw data (could be bytes, str, etc.)
- `content_type`: Format indicator ('pdf', 'html', 'text', 'url', etc.)
- `source`: Name of data source ('sec', 'yahoo', etc.)
- `metadata`: Additional context information (ticker, date, timestamps, etc.)

## Implementation Example

```python
from data_acquisition.base import BaseDataSource

class SECDataSource(BaseDataSource):
    def fetch(self, params: Dict[str, Any]) -> Dict[str, Any]:
        # SEC-specific implementation...
        return {
            'content': retrieved_data,
            'content_type': 'html',
            'source': 'sec',
            'metadata': {
                'ticker': params.get('ticker'),
                'year': params.get('year'),
                'retrieved_at': '2023-06-01T12:00:00Z'
            }
        }
```

## Usage Example

```python
sec_source = SECDataSource()
result = sec_source.fetch({'ticker': 'AAPL', 'year': 2023})

# Result might be:
# {
#   'content': '<html>...</html>',
#   'content_type': 'html',
#   'source': 'sec',
#   'metadata': {'ticker': 'AAPL', 'year': 2023, 'retrieved_at': '2023-06-01T12:00:00Z'}
# }
```