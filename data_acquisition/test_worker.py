"""
Test script to check if worker can process Redis jobs correctly.
"""
import redis
import json
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Redis connection
REDIS_URL = "redis://localhost:6379/0"
redis_client = redis.Redis.from_url(REDIS_URL, decode_responses=True)

# Clear any existing jobs
redis_client.delete("data_jobs")

# Add a test job to the queue
test_job = {
    "job_id": "test_job_1",
    "companies": [{"name": "Apple", "ticker": "AAPL"}],
    "n_years": 1,
    "form_type": "10-K",
    "organization_id": "test_org",
    "user_id": "test_user"
}

redis_client.rpush("data_jobs", json.dumps(test_job))
print(f"Added test job to queue: {test_job}")

# Check if we can pop the job from the queue (simulate worker)
job_data = redis_client.blpop("data_jobs", timeout=1)
if job_data:
    _, job_json = job_data
    print(f"Successfully retrieved job: {job_json}")
    job = json.loads(job_json)
    print(f"Job ID: {job['job_id']}")
    print(f"Companies: {job['companies']}")
else:
    print("Failed to retrieve job from queue")