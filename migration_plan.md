# Migration Plan for Centralized Configuration

This document outlines the changes needed to migrate each file from direct environment variable access to using the new centralized configuration system.

## 1. Overview

We've created a `config.py` file with a Pydantic-based settings system that offers:
- Categorized settings (Redis, API Keys, etc.)
- Type validation
- Default values
- Environment variable mapping
- .env file support
- Cached settings instance

Now we need to update all files that currently access environment variables directly to use this centralized configuration instead.

## 2. Files to Update

The following files need to be updated:

1. `/data_acquisition/orchestrator_api.py`
2. `/data_acquisition/sec_client.py`
3. `/data_acquisition/test_worker.py`
4. `/data_acquisition/worker.py`
5. `/storage/storage_interface.py`
6. `/storage/postgres_storage.py`
7. `/storage/vector_store.py`

## 3. Changes Required for Each File

### 3.1 `/data_acquisition/orchestrator_api.py`

**Current usage:**
- Loads `.env` file with `load_dotenv()`
- Uses `os.getenv("REDIS_URL", "redis://localhost:6379/0")` for Redis connection

**Required changes:**
1. Import the settings:
   ```python
   from config import settings
   ```
2. Remove `load_dotenv()` call
3. Replace `REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")` with:
   ```python
   REDIS_URL = settings.redis.url
   ```
4. Replace `redis_client = redis.Redis.from_url(REDIS_URL, decode_responses=True)` with:
   ```python
   redis_client = redis.Redis.from_url(settings.redis.url, decode_responses=True)
   ```

### 3.2 `/data_acquisition/sec_client.py`

**Current usage:**
- Loads `.env` file with `load_dotenv()`
- Uses multiple environment variables:
  - `SEC_API_KEY = os.getenv("SEC_API_KEY")`
  - `REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")`
  - Hard-coded API URLs: `SEC_API_BASE_URL = "https://api.sec-api.io"`
  - Multiple `os.getenv("SEC_USER_AGENT", "salescience/1.0")` instances
  - `metrics_port = int(os.getenv("METRICS_PORT", "8000"))`

**Required changes:**
1. Import the settings:
   ```python
   from config import settings
   ```
2. Remove `load_dotenv()` call and related error handling
3. Replace `SEC_API_KEY = os.getenv("SEC_API_KEY")` with:
   ```python
   SEC_API_KEY = settings.api_keys.sec
   ```
4. Replace `REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")` with:
   ```python
   REDIS_URL = settings.redis.url
   ```
5. Replace the hard-coded URLs:
   ```python
   SEC_API_BASE_URL = settings.service_urls.sec_api_base
   SEC_EDGAR_BASE_URL = settings.service_urls.sec_edgar_base
   ```
6. Replace all instances of `os.getenv("SEC_USER_AGENT", "salescience/1.0")` with:
   ```python
   settings.api_keys.sec_user_agent
   ```
7. Replace metrics port reference:
   ```python
   start_http_server(settings.metrics_port)
   ```

### 3.3 `/data_acquisition/test_worker.py`

**Current usage:**
- Loads `.env` file with `load_dotenv()`
- Uses hard-coded Redis URL: `REDIS_URL = "redis://localhost:6379/0"`

**Required changes:**
1. Import the settings:
   ```python
   from config import settings
   ```
2. Remove `load_dotenv()` call
3. Replace `REDIS_URL = "redis://localhost:6379/0"` with:
   ```python
   REDIS_URL = settings.redis.url
   ```

### 3.4 `/data_acquisition/worker.py`

**Current usage:**
- Uses multiple environment variables:
  - `REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")`
  - `WORKER_CONCURRENCY = int(os.getenv("WORKER_CONCURRENCY", "10"))`
  - `SEC_TOPIC = os.getenv("SEC_TOPIC", "data.sec")`
  - `YAHOO_TOPIC = os.getenv("YAHOO_TOPIC", "data.yahoo")`
  - `metrics_port = int(os.getenv("METRICS_PORT", "8000"))`

**Required changes:**
1. Import the settings:
   ```python
   from config import settings
   ```
2. Replace Redis URL reference:
   ```python
   REDIS_URL = settings.redis.url
   ```
3. Replace worker concurrency:
   ```python
   WORKER_CONCURRENCY = settings.worker.concurrency
   ```
4. Replace topic names:
   ```python
   SEC_TOPIC = settings.worker.sec_topic
   YAHOO_TOPIC = settings.worker.yahoo_topic
   ```
5. Replace metrics port:
   ```python
   metrics_port = settings.metrics_port
   ```

### 3.5 `/storage/storage_interface.py`

**Current usage:**
- Loads `.env` file with `load_dotenv()`
- Uses `REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")`

**Required changes:**
1. Import the settings:
   ```python
   from config import settings
   ```
2. Remove `load_dotenv()` call
3. Replace `REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")` with:
   ```python
   REDIS_URL = settings.redis.url
   ```

### 3.6 `/storage/postgres_storage.py`

**Current usage:**
- Loads `.env` file with `load_dotenv()`
- Uses various PostgreSQL environment variables in the `__init__` method:
  - `self.host = host or os.getenv("POSTGRES_HOST", "localhost")`
  - `self.database = database or os.getenv("POSTGRES_DB", "salescience")`
  - `self.user = user or os.getenv("POSTGRES_USER", "postgres")`
  - `self.password = password or os.getenv("POSTGRES_PASSWORD")`

**Required changes:**
1. Import the settings:
   ```python
   from config import settings
   ```
2. Remove `load_dotenv()` call
3. Update `__init__` method to use settings:
   ```python
   def __init__(self, host: str = None, database: str = None, user: str = None, password: str = None):
       """
       Initialize the PostgreSQL storage with connection parameters.
       
       Args:
           host: Database host address (overrides settings)
           database: Database name (overrides settings)
           user: Database user (overrides settings)
           password: Database password (overrides settings)
       """
       # Need to add database settings to config.py first:
       # In config.py, add:
       # class DatabaseSettings(BaseSettings):
       #    host: str = Field(default="localhost", env="POSTGRES_HOST")
       #    name: str = Field(default="salescience", env="POSTGRES_DB")
       #    user: str = Field(default="postgres", env="POSTGRES_USER")
       #    password: Optional[str] = Field(default=None, env="POSTGRES_PASSWORD")
       #
       # Then in Settings class:
       # database: DatabaseSettings = DatabaseSettings()
       
       self.host = host or settings.database.host
       self.database = database or settings.database.name
       self.user = user or settings.database.user
       self.password = password or settings.database.password
       
       # Rest of method unchanged
   ```

### 3.7 `/storage/vector_store.py`

**Current usage:**
- Loads `.env` file with `load_dotenv()`
- Uses `self.backend = backend or os.getenv("VECTOR_STORE_BACKEND", "faiss")`

**Required changes:**
1. Import the settings:
   ```python
   from config import settings
   ```
2. Remove `load_dotenv()` call
3. Replace backend initialization:
   ```python
   # Need to add vector store settings to config.py first:
   # In config.py, add to EmbeddingSettings:
   # backend: str = Field(default="faiss", env="VECTOR_STORE_BACKEND")
   
   self.backend = backend or settings.embedding.backend
   ```

## 4. Implementation Steps

1. First, update the `config.py` file to add any missing settings (database, vector_store_backend, etc.)
2. Update each file one by one, starting with simpler files to test the approach
3. For each file:
   - Add the import for settings
   - Remove direct environment variable calls
   - Replace with appropriate settings references
   - Test functionality to ensure behavior remains the same

## 5. Benefits of This Migration

- **Type Safety**: The Pydantic model ensures all settings have correct types
- **Documentation**: All settings are documented in the config.py file
- **Validation**: Invalid settings are detected early with meaningful error messages
- **Central Management**: Settings are managed in one place
- **Default Values**: Sensible defaults are defined centrally
- **Environment Isolation**: Environment-specific validation can be applied
- **Reusability**: Settings logic can be shared across the application
- **Testability**: Configuration can be mocked easily in tests

## 6. Update for config.py

The existing `config.py` should be extended to include the following settings that are currently missing:

1. Add `sec_user_agent` to API Keys settings
2. Add a DatabaseSettings class for PostgreSQL configuration
3. Add `backend` to EmbeddingSettings for vector store

```python
# Additions to config.py

class ApiKeySettings(BaseSettings):
    # ... existing code ...
    sec_user_agent: str = Field(
        default="salescience/1.0",
        env="SEC_USER_AGENT",
        description="User agent for SEC API requests"
    )

class DatabaseSettings(BaseSettings):
    """Database connection settings."""
    
    host: str = Field(
        default="localhost",
        env="POSTGRES_HOST",
        description="PostgreSQL database host"
    )
    
    name: str = Field(
        default="salescience",
        env="POSTGRES_DB",
        description="PostgreSQL database name"
    )
    
    user: str = Field(
        default="postgres",
        env="POSTGRES_USER",
        description="PostgreSQL database user"
    )
    
    password: Optional[str] = Field(
        default=None,
        env="POSTGRES_PASSWORD",
        description="PostgreSQL database password"
    )
    
    port: int = Field(
        default=5432,
        env="POSTGRES_PORT",
        description="PostgreSQL database port"
    )

# Update EmbeddingSettings
class EmbeddingSettings(BaseSettings):
    # ... existing code ...
    backend: str = Field(
        default="faiss",
        env="VECTOR_STORE_BACKEND",
        description="Vector store backend to use (faiss, pinecone, pgvector, chroma)"
    )

# Add to Settings class
class Settings(BaseSettings):
    # ... existing code ...
    database: DatabaseSettings = DatabaseSettings()
```