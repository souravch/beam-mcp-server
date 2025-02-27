"""
End-to-end tests for Apache Beam runners.
"""

import os
import asyncio
import pytest
from fastapi.testclient import TestClient
import yaml
import time
import requests

from src.server.app import create_app
from src.server.models.common import RunnerType, JobType, JobState
from src.server.models.savepoint import SavepointStatus

# Load test configuration
with open('config/test_config.yaml', 'r') as f:
    TEST_CONFIG = yaml.safe_load(f)

@pytest.fixture
def test_client():
    """Create a test client."""
    app = create_app(TEST_CONFIG)
    return TestClient(app)

@pytest.fixture
def test_pipeline_path():
    """Get the test pipeline path."""
    return os.path.abspath('examples/pipelines/wordcount.py')

@pytest.fixture
def flink_available():
    """Check if Flink is available."""
    try:
        response = requests.get('http://localhost:8081/overview')
        return response.status_code == 200
    except:
        return False

@pytest.mark.asyncio
async def test_direct_runner_workflow(test_client, test_pipeline_path):
    """Test the complete workflow using the Direct runner."""
    
    # Test manifest endpoint
    response = test_client.get("/api/v1/manifest")
    assert response.status_code == 200
    response_data = response.json()
    assert response_data["success"] is True
    manifest = response_data["data"]
    assert manifest["name"] == "beam-mcp"
    
    # Test runners endpoint
    response = test_client.get("/api/v1/runners")
    assert response.status_code == 200
    response_data = response.json()
    assert response_data["success"] is True
    runners = response_data["data"]["runners"]
    direct_runner = next((r for r in runners if r["runner_type"] == "direct"), None)
    assert direct_runner is not None
    assert direct_runner["name"] == "Direct Runner"
    
    # Create a job
    job_params = {
        "mcp_resource_type": "job_parameters",
        "job_name": "test-wordcount-direct",
        "code_path": test_pipeline_path,
        "runner_type": "direct",
        "pipeline_options": {
            "temp_location": "/tmp/beam-test",
            "save_main_session": True,
            "project": "local",
            "region": "local"
        },
        "job_type": JobType.BATCH
    }
    
    response = test_client.post(
        "/api/v1/jobs",
        json=job_params,
        headers={
            "MCP-Session-ID": "test-session",
            "MCP-Transaction-ID": "test-transaction"
        }
    )
    if response.status_code != 200:
        print("Error response:", response.json())
    assert response.status_code == 200
    response_data = response.json()
    assert response_data["success"] is True
    job = response_data["data"]
    job_id = job["job_id"]
    assert job["current_state"] == JobState.RUNNING
    
    # Wait for job completion (with timeout)
    attempts = 0
    while attempts < 60:  # 1 minute timeout
        response = test_client.get(f"/api/v1/jobs/{job_id}")
        assert response.status_code == 200
        response_data = response.json()
        assert response_data["success"] is True
        job = response_data["data"]
        if job["current_state"] in [JobState.SUCCEEDED, JobState.FAILED]:
            break
        await asyncio.sleep(1)
        attempts += 1
    
    assert job["current_state"] == JobState.SUCCEEDED
    
    # Get job metrics
    response = test_client.get(f"/api/v1/jobs/{job_id}/metrics")
    assert response.status_code == 200
    response_data = response.json()
    assert response_data["success"] is True
    metrics = response_data["data"]
    assert metrics is not None
    
    # Verify output file exists
    output_files = [f for f in os.listdir('.') if f.startswith('wordcount_output')]
    assert len(output_files) > 0
    
    # Clean up output files
    for f in output_files:
        os.remove(f)

@pytest.mark.asyncio
async def test_flink_runner_workflow(test_client, test_pipeline_path, flink_available):
    """Test the complete workflow using the Flink runner."""
    if not flink_available:
        pytest.skip("Flink is not available")
    
    # Test runners endpoint
    response = test_client.get("/api/v1/runners")
    assert response.status_code == 200
    response_data = response.json()
    assert response_data["success"] is True
    runners = response_data["data"]["runners"]
    flink_runner = next((r for r in runners if r["runner_type"] == "flink"), None)
    assert flink_runner is not None
    assert flink_runner["name"] == "Apache Flink"
    
    # Create a job
    job_params = {
        "mcp_resource_type": "job_parameters",
        "job_name": "test-wordcount-flink",
        "code_path": test_pipeline_path,
        "runner_type": "flink",
        "pipeline_options": {
            "parallelism": 4,
            "checkpointing_interval": 10000,
            "state_backend": "rocksdb",
            "state_backend_path": "/tmp/flink-state",
            "temp_location": "/tmp/flink-test",
            "project": "local",
            "region": "local"
        },
        "job_type": JobType.STREAMING
    }
    
    response = test_client.post(
        "/api/v1/jobs",
        json=job_params,
        headers={
            "MCP-Session-ID": "test-session",
            "MCP-Transaction-ID": "test-transaction"
        }
    )
    if response.status_code != 200:
        print("Error response:", response.json())
    assert response.status_code == 200
    response_data = response.json()
    assert response_data["success"] is True
    job = response_data["data"]
    job_id = job["job_id"]
    assert job["current_state"] == JobState.RUNNING
    
    # Wait for job to be fully running
    attempts = 0
    while attempts < 30:  # 30 seconds timeout
        response = test_client.get(f"/api/v1/jobs/{job_id}")
        assert response.status_code == 200
        response_data = response.json()
        assert response_data["success"] is True
        job = response_data["data"]
        if job["current_state"] == "RUNNING":
            break
        await asyncio.sleep(1)
        attempts += 1

    assert job["current_state"] == "RUNNING"

    # Cancel the job
    response = test_client.delete(f"/api/v1/jobs/{job_id}")
    assert response.status_code == 200
    response_data = response.json()
    assert response_data["success"] is True

    # Wait for job to be cancelled
    attempts = 0
    while attempts < 30:  # 30 seconds timeout
        response = test_client.get(f"/api/v1/jobs/{job_id}")
        assert response.status_code == 200
        response_data = response.json()
        assert response_data["success"] is True
        job = response_data["data"]
        if job["current_state"] == JobState.CANCELLED:
            break
        await asyncio.sleep(1)
        attempts += 1

    assert job["current_state"] == JobState.CANCELLED
    
    # Create a savepoint
    savepoint_params = {
        "mcp_resource_type": "savepoint_request",
        "job_id": job_id,
        "savepoint_path": "/tmp/flink-savepoints",
        "drain": False
    }
    
    response = test_client.post(
        f"/api/v1/jobs/{job_id}/savepoints",
        json=savepoint_params,
        headers={
            "MCP-Session-ID": "test-session",
            "MCP-Transaction-ID": "test-transaction"
        }
    )
    if response.status_code != 200:
        print("Error response:", response.json())
    assert response.status_code == 200
    response_data = response.json()
    assert response_data["success"] is True
    savepoint = response_data["data"]
    assert savepoint["status"] == SavepointStatus.PENDING
    
    # Wait for savepoint completion
    max_attempts = 30
    attempts = 0
    while attempts < max_attempts:
        response = test_client.get(f"/api/v1/jobs/{job_id}/savepoints/{savepoint['savepoint_id']}")
        assert response.status_code == 200
        response_data = response.json()
        assert response_data["success"] is True
        savepoint = response_data["data"]
        if savepoint["status"] in [SavepointStatus.COMPLETED, SavepointStatus.FAILED]:
            break
        await asyncio.sleep(1)
        attempts += 1
    
    assert savepoint["status"] == SavepointStatus.COMPLETED
    
    # Get job metrics
    response = test_client.get(f"/api/v1/jobs/{job_id}/metrics")
    assert response.status_code == 200
    response_data = response.json()
    assert response_data["success"] is True
    metrics = response_data["data"]
    assert metrics is not None
    
    # Cancel the job
    response = test_client.delete(f"/api/v1/jobs/{job_id}")
    assert response.status_code == 200
    response_data = response.json()
    assert response_data["success"] is True
    
    # Wait for job to be cancelled
    attempts = 0
    while attempts < 30:  # 30 seconds timeout
        response = test_client.get(f"/api/v1/jobs/{job_id}")
        assert response.status_code == 200
        response_data = response.json()
        assert response_data["success"] is True
        job = response_data["data"]
        if job["current_state"] == JobState.CANCELLED:
            break
        await asyncio.sleep(1)
        attempts += 1
    
    assert job["current_state"] == JobState.CANCELLED 