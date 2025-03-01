#!/usr/bin/env python3
"""
Regression tests for the Apache Beam MCP Server API endpoints.

This script tests all the major API endpoints to ensure they're working correctly.
Run this test suite after making changes to verify everything still works.

Usage:
    python -m pytest tests/api_regression_tests.py -v
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
from typing import Optional, Dict, Any, Tuple

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MCPServerAPITests(unittest.TestCase):
    """Test suite for the Apache Beam MCP Server API endpoints."""

    # Test configuration
    SERVER_URL = "http://localhost:8083"
    CONFIG_PATH = "config/flink_wordcount_config.yaml"
    TEST_INPUT_FILE = "/tmp/mcp_regression_test_input.txt"
    TEST_OUTPUT_FILE = "/tmp/mcp_regression_test_output.txt"
    JAR_PATH = "/Users/souravchakraborty/beam/flink-1.17.0/examples/batch/WordCount.jar"
    
    # Class variables to share state between tests
    server_process = None
    test_job_id = None
    
    @classmethod
    def setUpClass(cls):
        """Set up the test suite by starting the server and creating test files."""
        # Create test input file
        with open(cls.TEST_INPUT_FILE, 'w') as f:
            f.write("This is a regression test for the Apache Beam MCP Server APIs")
        
        # Start server in background if not already running
        if not cls._is_server_running():
            logger.info("Starting MCP server...")
            cls._start_server()
            # Give the server time to start
            time.sleep(5)
    
    @classmethod
    def tearDownClass(cls):
        """Clean up after tests by stopping the server and removing test files."""
        # Stop server if we started it
        if cls.server_process:
            logger.info("Stopping MCP server...")
            cls._stop_server()
        
        # Clean up test files
        try:
            os.remove(cls.TEST_INPUT_FILE)
            os.remove(cls.TEST_OUTPUT_FILE)
        except FileNotFoundError:
            pass
    
    @classmethod
    def _is_server_running(cls) -> bool:
        """Check if the MCP server is already running."""
        try:
            response = requests.get(f"{cls.SERVER_URL}/api/v1/health/health", timeout=2)
            return response.status_code == 200
        except requests.exceptions.RequestException:
            return False
    
    @classmethod
    def _start_server(cls):
        """Start the MCP server as a subprocess."""
        cmd = [
            "python", "main.py",
            "--debug",
            "--port", "8083"
        ]
        env = os.environ.copy()
        env["CONFIG_PATH"] = cls.CONFIG_PATH
        env["PYTHONUNBUFFERED"] = "1"
        
        cls.server_process = subprocess.Popen(
            cmd,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True
        )
    
    @classmethod
    def _stop_server(cls):
        """Stop the MCP server subprocess."""
        if cls.server_process:
            cls.server_process.send_signal(signal.SIGTERM)
            cls.server_process.wait(timeout=5)
            cls.server_process = None
    
    def test_01_health_endpoint(self):
        """Test the health endpoint."""
        response = requests.get(f"{self.SERVER_URL}/api/v1/health/health")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["status"], "healthy")
        self.assertIn("timestamp", data)
        self.assertIn("version", data)
    
    def test_02_manifest_endpoint(self):
        """Test the manifest endpoint."""
        response = requests.get(f"{self.SERVER_URL}/api/v1/manifest")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertTrue(data["success"])
        manifest = data["data"]
        
        # Verify key manifest fields
        self.assertEqual(manifest["name"], "beam-mcp")
        self.assertIn("runner_types", manifest)
        self.assertIn("job_types", manifest)
        self.assertIn("endpoints", manifest)
        
        # Verify Flink is a supported runner type
        self.assertIn("flink", manifest["runner_types"])
    
    def test_03_list_runners(self):
        """Test listing available runners."""
        response = requests.get(f"{self.SERVER_URL}/api/v1/runners")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertTrue(data["success"])
        runners_data = data["data"]
        
        # Verify runners data
        self.assertIn("runners", runners_data)
        self.assertIn("default_runner", runners_data)
        self.assertIn("total_count", runners_data)
        
        # Verify Flink is the default runner
        self.assertEqual(runners_data["default_runner"], "flink")
        
        # Verify runner types
        runner_types = [runner["runner_type"] for runner in runners_data["runners"]]
        self.assertIn("flink", runner_types)
    
    def test_04_get_runner_details(self):
        """Test getting details of a specific runner."""
        response = requests.get(f"{self.SERVER_URL}/api/v1/runners/flink")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertTrue(data["success"])
        runner_data = data["data"]
        
        # Verify runner details
        self.assertEqual(runner_data["runner_type"], "flink")
        self.assertEqual(runner_data["name"], "Apache Flink")
        self.assertEqual(runner_data["status"], "AVAILABLE")
        self.assertIn("capabilities", runner_data)
        self.assertIn("config", runner_data)
    
    def test_05_create_job(self):
        """Test creating a new job."""
        job_params = {
            "job_name": f"regression-test-{uuid.uuid4().hex[:8]}",
            "runner_type": "flink",
            "job_type": "BATCH",
            "code_path": "examples/pipelines/wordcount.py",
            "pipeline_options": {
                "jar_path": self.JAR_PATH,
                "entry_class": "org.apache.flink.examples.java.wordcount.WordCount",
                "parallelism": 1,
                "program_args": f"--input {self.TEST_INPUT_FILE} --output {self.TEST_OUTPUT_FILE}"
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
        self.assertEqual(job_data["runner"], "flink")
        self.assertEqual(job_data["job_type"], "BATCH")
        self.assertIn("create_time", job_data)
        
        # Wait for job to complete (max 10 seconds)
        max_checks = 10
        for i in range(max_checks):
            status_response = requests.get(f"{self.SERVER_URL}/api/v1/jobs/{self.test_job_id}/status")
            status_data = status_response.json()
            
            if status_data["data"]["status"] in ["SUCCEEDED", "FAILED", "CANCELLED"]:
                break
                
            time.sleep(1)
        
        # Verify job completed successfully
        self.assertEqual(status_data["data"]["status"], "SUCCEEDED")
        
        # Verify output file was created
        self.assertTrue(os.path.exists(self.TEST_OUTPUT_FILE))
    
    def test_06_get_job_details(self):
        """Test getting job details."""
        # Skip if job creation failed
        if not self.__class__.test_job_id:
            self.skipTest("Job creation failed, skipping job details test")
        
        response = requests.get(f"{self.SERVER_URL}/api/v1/jobs/{self.test_job_id}")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertTrue(data["success"])
        job_data = data["data"]
        
        # Verify job data
        self.assertEqual(job_data["job_id"], self.test_job_id)
        self.assertEqual(job_data["runner"], "flink")
        self.assertEqual(job_data["status"], "SUCCEEDED")
        self.assertIn("create_time", job_data)
        self.assertIn("end_time", job_data)
    
    def test_07_get_job_status(self):
        """Test getting job status."""
        # Skip if job creation failed
        if not self.__class__.test_job_id:
            self.skipTest("Job creation failed, skipping job status test")
        
        response = requests.get(f"{self.SERVER_URL}/api/v1/jobs/{self.test_job_id}/status")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertTrue(data["success"])
        status_data = data["data"]
        
        # Verify status data
        self.assertEqual(status_data["status"], "SUCCEEDED")
        self.assertEqual(status_data["current_state"], "FINISHED")
    
    def test_08_get_job_metrics(self):
        """Test getting job metrics."""
        # Skip if job creation failed
        if not self.__class__.test_job_id:
            self.skipTest("Job creation failed, skipping job metrics test")
        
        response = requests.get(f"{self.SERVER_URL}/api/v1/jobs/{self.test_job_id}/metrics")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertTrue(data["success"])
        metrics_data = data["data"]
        
        # Verify metrics data
        self.assertEqual(metrics_data["job_id"], self.test_job_id)
        self.assertIn("timestamp", metrics_data)
        self.assertIn("metrics", metrics_data)
    
    def test_09_create_and_cancel_job(self):
        """Test creating and then cancelling a job."""
        job_params = {
            "job_name": f"regression-test-cancel-{uuid.uuid4().hex[:8]}",
            "runner_type": "flink",
            "job_type": "BATCH",
            "code_path": "examples/pipelines/wordcount.py",
            "pipeline_options": {
                "jar_path": self.JAR_PATH,
                "entry_class": "org.apache.flink.examples.java.wordcount.WordCount",
                "parallelism": 1,
                "program_args": f"--input {self.TEST_INPUT_FILE} --output {self.TEST_OUTPUT_FILE}_2"
            }
        }
        
        response = requests.post(
            f"{self.SERVER_URL}/api/v1/jobs",
            json=job_params
        )
        self.assertEqual(response.status_code, 200)
        data = response.json()
        job_id = data["data"]["job_id"]
        
        # Try to cancel the job
        cancel_response = requests.delete(f"{self.SERVER_URL}/api/v1/jobs/{job_id}")
        self.assertEqual(cancel_response.status_code, 200)
        cancel_data = cancel_response.json()
        self.assertTrue(cancel_data["success"])
        
        # Check final status
        time.sleep(1)  # Give the server time to process the cancellation
        status_response = requests.get(f"{self.SERVER_URL}/api/v1/jobs/{job_id}/status")
        status_data = status_response.json()
        
        # It might be SUCCEEDED if it completed too quickly or CANCELLED if we caught it in time
        self.assertIn(status_data["data"]["status"], ["SUCCEEDED", "CANCELLED"])

if __name__ == "__main__":
    unittest.main() 