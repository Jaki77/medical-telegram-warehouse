"""
Dagster job for API pipeline (Task 4)
"""
from dagster import job
from dagster.ops.api_ops import (
    start_fastapi, 
    test_api_endpoints, 
    stop_fastapi
)

@job(
    description="Orchestrate API testing pipeline",
    tags={"pipeline": "api", "task": "4"},
    config={
        "ops": {
            "start_fastapi": {
                "config": {
                    "api_process": None  # Will be set during execution
                }
            }
        }
    }
)
def api_pipeline():
    """
    Job to test the FastAPI application
    
    This job:
    1. Starts the FastAPI application
    2. Tests API endpoints
    3. Stops the FastAPI application
    """
    # Start FastAPI
    api_startup_results = start_fastapi()
    
    # Test API endpoints
    api_test_results = test_api_endpoints(api_startup_results)
    
    # Stop FastAPI
    stop_fastapi(api_test_results)