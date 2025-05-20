# Test Worker Script Documentation

## Overview
`test_worker.py` is a script designed to test the functionality of the worker system that processes Redis queue jobs. It validates that jobs can be properly added to and retrieved from the Redis queue, which is an essential part of the data acquisition pipeline.

## Functionality
The script performs the following actions:
1. Connects to a Redis instance
2. Clears existing jobs from the queue
3. Creates and adds a test job to the queue
4. Simulates a worker by retrieving and processing the job from the queue

## Dependencies
- `redis`: For Redis queue operations
- `json`: For serializing/deserializing job data
- `os`: For environment interaction
- `dotenv`: For loading environment variables

## Configuration
- Redis connection URL is set to `redis://localhost:6379/0` (local development instance)
- Test job data includes sample company information (Apple/AAPL), retrieval parameters, and user identification

## Test Job Structure
```json
{
    "job_id": "test_job_1",
    "companies": [{"name": "Apple", "ticker": "AAPL"}],
    "n_years": 1,
    "form_type": "10-K",
    "organization_id": "test_org",
    "user_id": "test_user"
}
```

## Usage
Run this script to test if the Redis queue is properly configured and if jobs can be added and retrieved correctly:

```bash
python test_worker.py
```

## Expected Output
1. Confirmation message that a test job was added to the queue
2. Confirmation message that the job was successfully retrieved
3. Details of the retrieved job (ID, companies list)

## Error Handling
If the job cannot be retrieved from the queue (e.g., Redis connection issues), the script will output "Failed to retrieve job from queue".

## Notes
- This script is intended for testing purposes only
- It uses a local Redis instance by default
- The script does not perform actual data fetching or processing - it only tests the job queueing mechanism