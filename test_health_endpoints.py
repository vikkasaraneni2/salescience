#!/usr/bin/env python3
"""
Quick test script for health endpoints
"""

import asyncio
import httpx
import json
from datetime import datetime

async def test_health_endpoints():
    """Test all health endpoints"""
    base_url = "http://localhost:8100"  # Adjust if your API runs on different port
    
    endpoints = [
        "/health",
        "/readiness", 
        "/status"
    ]
    
    async with httpx.AsyncClient() as client:
        for endpoint in endpoints:
            try:
                print(f"\n=== Testing {endpoint} ===")
                response = await client.get(f"{base_url}{endpoint}")
                
                print(f"Status Code: {response.status_code}")
                print(f"Response Headers: {dict(response.headers)}")
                
                if response.headers.get("content-type", "").startswith("application/json"):
                    data = response.json()
                    print(f"Response Body: {json.dumps(data, indent=2)}")
                else:
                    print(f"Response Body: {response.text}")
                    
            except Exception as e:
                print(f"Error testing {endpoint}: {e}")

if __name__ == "__main__":
    print("Testing Salescience Health Endpoints")
    print(f"Test started at: {datetime.now().isoformat()}")
    
    try:
        asyncio.run(test_health_endpoints())
    except KeyboardInterrupt:
        print("\nTest interrupted by user")
    except Exception as e:
        print(f"Test failed: {e}")
    
    print(f"\nTest completed at: {datetime.now().isoformat()}")