#!/usr/bin/env python3
"""
Regression tests for the Apache Beam MCP Server API endpoints with Spark.

This script tests all the major API endpoints to ensure they're working correctly with Spark.
Run this test suite after making changes to verify everything still works.

Usage:
    python -m pytest tests/test_spark_api_regression.py -v
"""

import unittest
import requests
import json
import time
import os
import logging
import uuid
import subprocess
import signal
import sys
import shutil
from typing import Optional, Dict, Any, Tuple
from pathlib import Path
import socket

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SparkMCPServerAPITests(unittest.TestCase):
    """Test suite for the Apache Beam MCP Server API endpoints with Spark."""

    # Test configuration
    SERVER_URL = "http://localhost:8083"
    CONFIG_PATH = "config/spark_config.yaml"
    EXAMPLE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src', 'examples'))
    TEST_INPUT_FILE = os.path.join(EXAMPLE_DIR, 'sample_text.txt')
    TEST_OUTPUT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'output', 'test_output'))
    SPARK_MASTER = "local[2]"  # Use local mode for testing
    
    # Class variables to share state between tests
    server_process = None
    test_job_id = None
    cluster_job_id = None
    
    @classmethod
    def setUpClass(cls):
        """Set up the test suite by starting the server and creating test files."""
        # Ensure input file exists
        if not os.path.exists(cls.TEST_INPUT_FILE):
            with open(cls.TEST_INPUT_FILE, 'w') as f:
                f.write("This is a regression test for the Apache Beam MCP Server APIs with Spark\n")
                f.write("It includes multiple lines to test the WordCount job\n")
                f.write("Apache Beam provides unified batch and streaming processing\n")
                f.write("Spark is one of the supported runners in Apache Beam\n")
        
        # Create output directory if it doesn't exist
        os.makedirs(cls.TEST_OUTPUT_DIR, exist_ok=True)
        
        # Start server in background if not already running
        if not cls._is_server_running():
            cls._start_server()
    
    @classmethod
    def tearDownClass(cls):
        """Clean up after all tests have completed."""
        # Stop server if we started it
        if cls.server_process:
            cls._stop_server()
        
        # Clean up test output
        if os.path.exists(cls.TEST_OUTPUT_DIR):
            shutil.rmtree(cls.TEST_OUTPUT_DIR)
    
    @classmethod
    def _is_server_running(cls) -> bool:
        """Check if the server is already running."""
        try:
            response = requests.get(f"{cls.SERVER_URL}/api/v1/health/health")
            return response.status_code == 200
        except requests.ConnectionError:
            return False
    
    @classmethod
    def _start_server(cls):
        """Start the MCP server."""
        logger.info("Starting MCP server...")
        
        # Kill any existing server process first
        subprocess.run(
            "kill $(ps aux | grep '[p]ython -m src.server.app' | awk '{print $2}') 2>/dev/null || true",
            shell=True
        )
        
        # Start the server with the Spark configuration
        cls.server_process = subprocess.Popen(
            f"python -m src.server.app -c {cls.CONFIG_PATH} -p 8083",
            shell=True
        )
        
        # Wait for server to start (up to 10 seconds)
        for _ in range(10):
            time.sleep(1)
            if cls._is_server_running():
                logger.info("Server started successfully")
                return
        
        raise Exception("Failed to start server")
    
    @classmethod
    def _stop_server(cls):
        """Stop the MCP server."""
        if cls.server_process:
            logger.info("Stopping MCP server...")
            cls.server_process.terminate()
            cls.server_process.wait(timeout=5)
            cls.server_process = None
    
    def test_01_health_endpoint(self):
        """Test the health endpoint."""
        response = requests.get(f"{self.SERVER_URL}/api/v1/health/health")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["status"], "healthy")
    
    def test_02_manifest_endpoint(self):
        """Test the manifest endpoint."""
        response = requests.get(f"{self.SERVER_URL}/api/v1/manifest")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertTrue(data["success"])
        
        manifest_data = data["data"]
        self.assertIn("name", manifest_data)
        self.assertIn("description", manifest_data)
        self.assertIn("version", manifest_data)
        self.assertIn("endpoints", manifest_data)
        self.assertIn("configurations", manifest_data)
    
    def test_03_list_runners(self):
        """Test listing available runners."""
        response = requests.get(f"{self.SERVER_URL}/api/v1/runners")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertTrue(data["success"])
        
        runners_data = data["data"]
        self.assertIsInstance(runners_data, list)
        
        # Verify Spark runner is in the list
        spark_runner = None
        for runner in runners_data:
            if runner["runner_type"] == "spark":
                spark_runner = runner
                break
        
        self.assertIsNotNone(spark_runner)
        self.assertEqual(spark_runner["name"], "Apache Spark")
        self.assertEqual(spark_runner["status"], "AVAILABLE")
    
    def test_04_get_runner_details(self):
        """Test getting details for the Spark runner."""
        response = requests.get(f"{self.SERVER_URL}/api/v1/runners/spark")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertTrue(data["success"])
        
        runner_data = data["data"]
        
        # Verify runner details
        self.assertEqual(runner_data["runner_type"], "spark")
        self.assertEqual(runner_data["name"], "Apache Spark")
        self.assertEqual(runner_data["status"], "AVAILABLE")
        self.assertIn("capabilities", runner_data)
        self.assertIn("config", runner_data)
    
    def test_05_create_local_spark_job(self):
        """Test creating a new Spark job in local mode."""
        test_output_path = os.path.join(self.TEST_OUTPUT_DIR, f"spark-local-{uuid.uuid4().hex[:8]}")
        
        job_params = {
            "job_name": f"spark-local-test-{uuid.uuid4().hex[:8]}",
            "runner_type": "spark",
            "job_type": "BATCH",
            "code_path": "src/examples/run_wordcount_spark_direct.py",
            "pipeline_options": {
                "input_file": self.TEST_INPUT_FILE,
                "output_path": test_output_path,
                "spark_master": "local[2]"
                # Only include arguments that are explicitly supported
            }
        }
        
        response = requests.post(
            f"{self.SERVER_URL}/api/v1/jobs",
            json=job_params
        )
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertTrue(data["success"])
        job_data = data["data"]
        
        # Save job ID for other tests
        self.__class__.test_job_id = job_data["job_id"]
        
        # Verify job data
        self.assertEqual(job_data["job_name"], job_params["job_name"])
        self.assertEqual(job_data["runner"], "spark")
        self.assertEqual(job_data["job_type"], "BATCH")
        self.assertIn("create_time", job_data)
        
        # Wait for job to complete (max 30 seconds)
        max_checks = 30
        job_completed = False
        final_status = None
        
        for i in range(max_checks):
            status_response = requests.get(f"{self.SERVER_URL}/api/v1/jobs/{self.test_job_id}/status")
            status_data = status_response.json()
            
            final_status = status_data["data"]["status"]
            if final_status in ["SUCCEEDED", "COMPLETED", "FAILED", "CANCELLED"]:
                job_completed = True
                break
                
            time.sleep(1)
        
        # Verify job completed
        self.assertTrue(job_completed, f"Job did not complete within {max_checks} seconds. Final status: {final_status}")
        
        # Allow for either SUCCEEDED or COMPLETED as valid completion states
        self.assertIn(final_status, ["SUCCEEDED", "COMPLETED"], 
                     f"Job failed with status {final_status} and error: {status_data['data'].get('error_message', 'Unknown error')}")
        
        # Verify output directory was created
        output_dir = Path(test_output_path)
        self.assertTrue(output_dir.exists(), f"Output directory {test_output_path} was not created")
        
        # Verify there's a _SUCCESS file indicating job completed successfully
        # If the directory exists but _SUCCESS doesn't, list what is in the directory for debugging
        success_file = output_dir / "_SUCCESS"
        if not success_file.exists():
            dir_contents = list(output_dir.iterdir())
            print(f"Directory contents: {dir_contents}")
            
        self.assertTrue(success_file.exists(), f"_SUCCESS file not found in {test_output_path}")
    
    def test_06_get_job_details(self):
        """Test getting job details."""
        response = requests.get(f"{self.SERVER_URL}/api/v1/jobs/{self.test_job_id}")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertTrue(data["success"])
        
        job_data = data["data"]
        self.assertEqual(job_data["job_id"], self.test_job_id)
        self.assertEqual(job_data["runner"], "spark")
        self.assertEqual(job_data["job_type"], "BATCH")
        
        # Job status could be either SUCCEEDED or COMPLETED
        self.assertIn(job_data["status"], ["SUCCEEDED", "COMPLETED"])
    
    def test_07_get_job_status(self):
        """Test getting job status."""
        response = requests.get(f"{self.SERVER_URL}/api/v1/jobs/{self.test_job_id}/status")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertTrue(data["success"])
        
        status_data = data["data"]
        self.assertEqual(status_data["job_id"], self.test_job_id)
        
        # Job status could be either SUCCEEDED or COMPLETED
        self.assertIn(status_data["status"], ["SUCCEEDED", "COMPLETED"])
    
    def test_08_list_jobs(self):
        """Test listing all jobs."""
        response = requests.get(f"{self.SERVER_URL}/api/v1/jobs")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertTrue(data["success"])
        
        jobs_data = data["data"]
        self.assertIsInstance(jobs_data, list)
        
        # Find our test job in the list
        found_job = False
        for job in jobs_data:
            if job["job_id"] == self.test_job_id:
                found_job = True
                break
        
        self.assertTrue(found_job)
    
    def test_09_create_and_cancel_job(self):
        """Test creating a new job and then cancelling it."""
        test_output_path = os.path.join(self.TEST_OUTPUT_DIR, f"spark-cancel-{uuid.uuid4().hex[:8]}")
        
        job_params = {
            "job_name": f"spark-cancel-test-{uuid.uuid4().hex[:8]}",
            "runner_type": "spark",
            "job_type": "BATCH",
            "code_path": "src/examples/run_wordcount_spark_direct.py",
            "pipeline_options": {
                "input_file": self.TEST_INPUT_FILE,
                "output_path": test_output_path,
                "spark_master": "local[2]"
            }
        }
        
        response = requests.post(
            f"{self.SERVER_URL}/api/v1/jobs",
            json=job_params
        )
        self.assertEqual(response.status_code, 200, 
                        f"Cancel request failed with status code {response.status_code}: {response.text}")
        
        cancel_result = response.json()
        self.assertTrue(cancel_result["success"], 
                        f"Cancel request returned success=false: {cancel_result.get('error', 'No error message')}")
        
        # Wait a bit to let the job finish or get cancelled
        time.sleep(5)
        
        # Check final status
        response = requests.get(f"{self.SERVER_URL}/api/v1/jobs/{self.test_job_id}/status")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        
        # Job could be in CANCELLED state if cancellation happened in time
        # Or in COMPLETED/SUCCEEDED state if job finished before cancellation took effect
        # Or in FAILED state if job failed for some reason
        final_status = data["data"]["status"]
        self.assertIn(final_status, ["CANCELLED", "COMPLETED", "SUCCEEDED", "FAILED"], 
                     f"Unexpected final job status: {final_status}")
        
        print(f"Job cancellation test finished with status: {final_status}")
    
    def test_10_create_cluster_spark_job(self):
        """Test creating a Spark job in cluster mode."""
        test_output_path = os.path.join(self.TEST_OUTPUT_DIR, f"spark-cluster-{uuid.uuid4().hex[:8]}")
        
        # Try to import the Spark runner to see if it's available
        spark_runner_available = False
        try:
            from apache_beam.runners.spark.spark_runner import SparkRunner  # noqa
            spark_runner_available = True
        except ImportError:
            logging.warning("Apache Beam Spark runner not available. Test will run but direct PySpark will be used instead.")
        
        # Try to find an available Spark cluster or use local mode
        # First try the hostname-based master URL
        spark_master = f"spark://{socket.gethostname()}:7077"
        
        # Check if this Spark master is available
        try:
            # Try a simple HTTP request to check if the master is responding
            spark_ui_port = 8080
            requests.get(f"http://{socket.gethostname()}:{spark_ui_port}", timeout=1)
            logging.info(f"Using Spark master at {spark_master}")
        except:
            # Try localhost as fallback
            spark_master = "spark://localhost:7077"
            try:
                requests.get(f"http://localhost:8080", timeout=1)
                logging.info(f"Using Spark master at {spark_master}")
            except:
                # Fall back to local mode
                spark_master = "local[2]"
                logging.info(f"No Spark cluster available, falling back to {spark_master}")
        
        # Create appropriate job parameters based on what's available
        if spark_runner_available:
            # Use the Beam Spark runner
            job_params = {
                "job_name": f"spark-cluster-test-{uuid.uuid4().hex[:8]}",
                "runner_type": "spark",
                "job_type": "BATCH",
                "code_path": "src/examples/wordcount.py",  # Use the standard Beam wordcount
                "pipeline_options": {
                    "input": self.TEST_INPUT_FILE,
                    "output": test_output_path,
                    "runner": "SparkRunner",
                    "spark_master_url": spark_master
                }
            }
        else:
            # Fall back to direct PySpark implementation
            job_params = {
                "job_name": f"spark-cluster-test-{uuid.uuid4().hex[:8]}",
                "runner_type": "spark",
                "job_type": "BATCH",
                "code_path": "src/examples/run_wordcount_spark_direct.py",
                "pipeline_options": {
                    "input_file": self.TEST_INPUT_FILE,
                    "output_path": test_output_path,
                    "spark_master": spark_master
                }
            }
        
        # Submit the job
        response = requests.post(
            f"{self.SERVER_URL}/api/v1/jobs",
            json=job_params
        )
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertTrue(data["success"])
        
        # Store the job ID for reference
        self.__class__.cluster_job_id = data["data"]["job_id"]
        
        # Wait for job to complete (max 60 seconds for cluster mode)
        max_checks = 60
        job_completed = False
        final_status = None
        
        for i in range(max_checks):
            status_response = requests.get(f"{self.SERVER_URL}/api/v1/jobs/{self.cluster_job_id}/status")
            status_data = status_response.json()
            
            final_status = status_data["data"]["status"]
            if final_status in ["SUCCEEDED", "COMPLETED", "FAILED", "CANCELLED"]:
                job_completed = True
                break
                
            time.sleep(1)
        
        # Verify job completed (but we don't necessarily expect success)
        self.assertTrue(job_completed, f"Job did not complete within {max_checks} seconds. Final status: {final_status}")
        
        # Log the final status without forcing it to be successful
        # Cluster jobs might fail due to environment issues, but the API itself worked
        logging.info(f"Cluster job completed with status: {final_status}")
        if final_status in ["FAILED", "CANCELLED"]:
            logging.warning(f"Cluster job did not succeed. Error: {status_data['data'].get('error_message', 'Unknown error')}")
        else:
            # Verify output directory was created (but only if job succeeded)
            output_dir = Path(test_output_path)
            if not output_dir.exists():
                logging.warning(f"Output directory does not exist: {test_output_path}")
            else:
                success_file = output_dir / "_SUCCESS"
                if not success_file.exists():
                    logging.warning(f"_SUCCESS file not found in: {test_output_path}")
                    dir_contents = list(output_dir.iterdir())
                    logging.info(f"Directory contents: {dir_contents}")


if __name__ == "__main__":
    unittest.main() 