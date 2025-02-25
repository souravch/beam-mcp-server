"""
End-to-end tests for the Direct runner implementation.
"""

import os
import asyncio
import pytest
from fastapi.testclient import TestClient
import yaml

from src.server.app import create_app
from src.server.models import JobState, RunnerType, JobType

# Load test configuration
with open('config/test_config.yaml', 'r') as f:
    TEST_CONFIG = yaml.safe_load(f)

@pytest.fixture
def test_client():
    """Create a test client with direct runner configuration."""
    app = create_app()
    return TestClient(app)

@pytest.fixture
def test_pipeline_path():
    """Get the test pipeline path."""
    return os.path.abspath('examples/pipelines/wordcount.py')

@pytest.mark.asyncio
async def test_direct_runner_workflow(test_client, test_pipeline_path):
    """Test the complete workflow using the Direct runner."""
    
    # Test manifest endpoint
    response = test_client.get("/api/v1/manifest")
    assert response.status_code == 200
    manifest = response.json()
    assert manifest["data"]["name"] == "beam-mcp"
    
    # Test runners endpoint
    response = test_client.get("/api/v1/runners")
    assert response.status_code == 200
    runners = response.json()["data"]["runners"]
    direct_runner = next((r for r in runners if r["runner_type"] == "direct"), None)
    assert direct_runner is not None
    assert direct_runner["name"] == "Direct Runner"
    
    # Create a job
    job_params = {
        "job_name": "test-wordcount",
        "runner_type": RunnerType.DIRECT,
        "job_type": JobType.BATCH,
        "code_path": test_pipeline_path,
        "pipeline_options": {
            "temp_location": "/tmp/beam-test",
            "save_main_session": True
        }
    }
    
    response = test_client.post(
        "/api/v1/jobs",
        json=job_params,
        headers={
            "MCP-Session-ID": "test-session",
            "MCP-Transaction-ID": "test-transaction"
        }
    )
    assert response.status_code == 200
    job = response.json()["data"]
    job_id = job["job_id"]
    assert job["state"] == JobState.RUNNING
    
    # Wait for job completion (with timeout)
    max_attempts = 30
    attempts = 0
    while attempts < max_attempts:
        response = test_client.get(f"/api/v1/jobs/{job_id}")
        assert response.status_code == 200
        job = response.json()["data"]
        if job["state"] in [JobState.SUCCEEDED, JobState.FAILED]:
            break
        await asyncio.sleep(1)
        attempts += 1
    
    assert job["state"] == JobState.SUCCEEDED
    
    # Get job metrics
    response = test_client.get(f"/api/v1/jobs/{job_id}/metrics")
    assert response.status_code == 200
    metrics = response.json()["data"]
    assert metrics is not None
    
    # Verify output file exists
    output_files = [f for f in os.listdir('.') if f.startswith('wordcount_output')]
    assert len(output_files) > 0
    
    # Clean up output files
    for f in output_files:
        os.remove(f)

@pytest.mark.asyncio
async def test_direct_runner_job_cancellation(test_client, test_pipeline_path):
    """Test job cancellation with the Direct runner."""
    
    # Create a job
    job_params = {
        "job_name": "test-wordcount-cancel",
        "runner_type": RunnerType.DIRECT,
        "job_type": JobType.BATCH,
        "code_path": test_pipeline_path,
        "pipeline_options": {
            "temp_location": "/tmp/beam-test",
            "save_main_session": True
        }
    }
    
    response = test_client.post(
        "/api/v1/jobs",
        json=job_params,
        headers={
            "MCP-Session-ID": "test-session",
            "MCP-Transaction-ID": "test-transaction"
        }
    )
    assert response.status_code == 200
    job = response.json()["data"]
    job_id = job["job_id"]
    
    # Cancel the job
    response = test_client.delete(f"/api/v1/jobs/{job_id}")
    assert response.status_code == 200
    
    # Verify job was cancelled
    response = test_client.get(f"/api/v1/jobs/{job_id}")
    assert response.status_code == 200
    job = response.json()["data"]
    assert job["state"] == JobState.CANCELLED

@pytest.mark.asyncio
async def test_direct_runner_error_handling(test_client):
    """Test error handling with the Direct runner."""
    
    # Try to create a job with invalid pipeline path
    job_params = {
        "job_name": "test-wordcount-error",
        "runner_type": RunnerType.DIRECT,
        "job_type": JobType.BATCH,
        "code_path": "nonexistent.py",
        "pipeline_options": {
            "temp_location": "/tmp/beam-test",
            "save_main_session": True
        }
    }
    
    response = test_client.post(
        "/api/v1/jobs",
        json=job_params,
        headers={
            "MCP-Session-ID": "test-session",
            "MCP-Transaction-ID": "test-transaction"
        }
    )
    assert response.status_code == 400  # Bad request
    
    # Try to get nonexistent job
    response = test_client.get("/api/v1/jobs/nonexistent")
    assert response.status_code == 404  # Not found 