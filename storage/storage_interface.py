import redis
import os
from dotenv import load_dotenv

load_dotenv()

# Redis connection
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
redis_client = redis.Redis.from_url(REDIS_URL, decode_responses=True)

def get_status(job_id: str):
    """Get the status of a job"""
    overall_status = redis_client.get(f"job:{job_id}:overall_status") or "unknown"
    return overall_status

def get_results(job_id: str):
    """Get the results of a job"""
    results = redis_client.hgetall(f"job:{job_id}:result")
    return results
