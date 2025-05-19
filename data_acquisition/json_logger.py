import logging
import json
import sys
from datetime import datetime
from typing import Optional, Dict, Any

SENSITIVE_KEYS = {"api_key", "password", "token", "secret", "OPENAI_API_KEY", "SEC_API_KEY", "POSTGRES_PASSWORD"}

def mask_sensitive(data: dict) -> dict:
    """
    Mask or omit sensitive fields in a dictionary for safe logging.
    """
    masked = {}
    for k, v in data.items():
        if k.lower() in SENSITIVE_KEYS:
            masked[k] = "***MASKED***"
        else:
            masked[k] = v
    return masked

class JsonLogger:
    def __init__(self, name: str):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.INFO)
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter('%(message)s'))
        if not self.logger.hasHandlers():
            self.logger.addHandler(handler)

    def log_json(self, level: str, action: str, message: str, organization_id: Optional[str] = None, job_id: Optional[str] = None, status: Optional[str] = None, extra: Optional[Dict[str, Any]] = None):
        log_entry = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": level,
            "action": action,
            "organization_id": organization_id,
            "job_id": job_id,
            "status": status,
            "message": message,
        }
        if extra:
            log_entry.update(mask_sensitive(extra))
        log_line = json.dumps({k: v for k, v in log_entry.items() if v is not None})
        if level == "error":
            self.logger.error(log_line)
        elif level == "warning":
            self.logger.warning(log_line)
        elif level == "info":
            self.logger.info(log_line)
        else:
            self.logger.debug(log_line)

# Usage example:
# logger = JsonLogger("orchestrator_api")
# logger.log_json("info", action="submit_job", message="Job submitted", organization_id="acme", job_id="1234", status="queued", extra={"user_id": "u1"}) 