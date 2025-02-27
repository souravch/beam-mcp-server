"""
Test for submitting a real job to a local Spark context.

This test submits a WordCount job to a local Spark runner.
It will write the output to a location that can be manually checked.
"""

import os
import pytest
import logging
import uuid
import time
from pathlib import Path
from fastapi.testclient import TestClient

from src.server.app import create_app
from src.server.models.job import JobType
from src.server.models.runner import RunnerType

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Get the absolute path to the example files
EXAMPLES_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src', 'examples'))
INPUT_FILE = os.path.join(EXAMPLES_DIR, 'sample_text.txt')
OUTPUT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'output'))

# Create output directory if it doesn't exist
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Create a test configuration with Spark runner enabled
TEST_CONFIG = {
    "default_runner": "spark",
    "runners": {
        "spark": {
            "enabled": True,
            "options": {
                "spark_master_url": "local[*]",
                "spark_submit_uber_jar": True
            }
        },
        "direct": {
            "enabled": True
        }
    }
}

@pytest.mark.integration
def test_submit_spark_wordcount():
    """Test submitting a real WordCount job to a local Spark context."""
    # Create the FastAPI app and test client with custom configuration
    app = create_app(config=TEST_CONFIG)
    client = TestClient(app)
    
    # First, check available runners
    response = client.get("/api/v1/runners")
    assert response.status_code == 200
    runners_data = response.json()["data"]
    
    # Ensure the Spark runner is available
    spark_runner = None
    for runner in runners_data["runners"]:
        if runner["runner_type"] == "spark":
            spark_runner = runner
            break
    
    if not spark_runner:
        pytest.skip("Spark runner not available in the server")
    
    # Generate a unique job name and output path
    job_name = f"wordcount-test-{uuid.uuid4().hex[:8]}"
    output_path = os.path.join(OUTPUT_DIR, job_name)
    
    # Create job parameters
    job_params = {
        "job_name": job_name,
        "runner_type": RunnerType.SPARK,
        "job_type": JobType.BATCH,
        "code_path": os.path.join(EXAMPLES_DIR, "wordcount.py"),
        "pipeline_options": {
            "input_file": INPUT_FILE,
            "output_path": output_path,
            # Spark-specific options
            "spark_master_url": "local[*]",
            "spark_submit_uber_jar": True
        }
    }
    
    # Submit the job
    logger.info(f"Submitting WordCount job to Spark: {job_name}")
    response = client.post("/api/v1/jobs", json=job_params)
    assert response.status_code == 200
    job_data = response.json()["data"]
    job_id = job_data["job_id"]
    
    logger.info(f"Job submitted successfully: {job_id}")
    
    # Wait for the job to complete (with timeout)
    timeout = 60  # seconds
    start_time = time.time()
    completed = False
    
    while time.time() - start_time < timeout:
        # Check job status
        response = client.get(f"/api/v1/jobs/{job_id}")
        assert response.status_code == 200
        
        status_data = response.json()["data"]
        logger.info(f"Job status: {status_data['status']}")
        
        if status_data["status"] in ["SUCCEEDED", "COMPLETED"]:
            completed = True
            break
        
        if status_data["status"] in ["FAILED", "CANCELLED"]:
            assert False, f"Job failed with status: {status_data['status']}"
        
        # Wait a bit before checking again
        time.sleep(5)
    
    # Verify job completed
    assert completed, f"Job did not complete within {timeout} seconds"
    
    # Check for output files
    output_files = list(Path(output_path).glob("*.txt"))
    assert len(output_files) > 0, "No output files found"
    
    # Print the path to the output files for manual verification
    logger.info("Job completed successfully. Output files:")
    for file in output_files:
        logger.info(f"  - {file}")
        
        # Read and print a sample of the output for verification
        with open(file, 'r') as f:
            sample_lines = []
            for i in range(10):
                try:
                    line = next(f)
                    sample_lines.append(line)
                except StopIteration:
                    break
                    
            logger.info("Sample output (first 10 lines or less):")
            for line in sample_lines:
                logger.info(f"  {line.strip()}")
    
    logger.info(f"You can manually check the full output at: {output_path}")
    
    # Clean up - cancel the job if it's still running
    client.delete(f"/api/v1/jobs/{job_id}")

if __name__ == "__main__":
    # This allows running the test directly
    test_submit_spark_wordcount() 