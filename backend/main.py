from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
import requests
import uuid

# For local development, use localhost. For Kubernetes, use the service name (interpreter).
INTERPRETER_URL = "http://localhost:8000/interpret_query"  # Use localhost for local dev; change to 'http://interpreter:8000/interpret_query' in K8s
ORCHESTRATOR_URL = "http://localhost:8100/submit"         # Update as needed

app = FastAPI()

class UserQuery(BaseModel):
    query: str
    company: str
    organization_id: str
    user_id: str

class JobSubmissionResponse(BaseModel):
    job_ids: List[str]
    job_specs: List[Dict[str, Any]]

@app.post("/submit_user_query", response_model=JobSubmissionResponse)
def submit_user_query(user_query: UserQuery, background_tasks: BackgroundTasks):
    # 1. Call the query interpreter
    interp_resp = requests.post(INTERPRETER_URL, json=user_query.dict())
    if interp_resp.status_code != 200:
        raise HTTPException(status_code=500, detail="Interpreter error")
    job_spec = interp_resp.json()["job_spec"]

    # 2. Submit jobs to orchestrator for each SEC form type (can extend for Yahoo, etc.)
    job_ids = []
    job_specs = []
    for form_type in job_spec["sec_form_types"]:
        job_payload = {
            "companies": [{"name": job_spec["company"], "sec": True}],  # Set sec flag to True for SEC form types
            "n_years": _years_from_range(job_spec["date_range"]),
            "form_type": form_type,
            "organization_id": job_spec["organization_id"],
            "user_id": job_spec["user_id"]
        }
        orch_resp = requests.post(ORCHESTRATOR_URL, json=job_payload)
        if orch_resp.status_code != 200:
            continue
        job_id = orch_resp.json()["job_ids"][0]
        job_ids.append(job_id)
        job_specs.append(job_payload)
        # Optionally, start background polling for results
        # background_tasks.add_task(poll_for_results, job_id, job_spec["organization_id"])
    return JobSubmissionResponse(job_ids=job_ids, job_specs=job_specs)

def _years_from_range(date_range: List[str]) -> int:
    if not date_range or len(date_range) != 2:
        return 1
    start_year = int(date_range[0][:4])
    end_year = int(date_range[1][:4])
    return max(1, end_year - start_year + 1)

# Optionally, add endpoints to poll status/results, or use background tasks to notify users
