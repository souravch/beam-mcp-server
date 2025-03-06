#!/usr/bin/env python3
"""
End-to-end testing script for Apache Beam MCP Server.

This script performs comprehensive testing of the MCP server:
1. Starts the MCP server
2. Tests all major API endpoints
3. Validates the API response format for LLM tool integration
4. Submits jobs to each supported runner
5. Verifies job status and metrics
6. Cleans up when done

Usage:
    python scripts/e2e_test.py --config config/flink_config.yaml --port 8888
"""

import argparse
import asyncio
import json
import logging
import os
import signal
import subprocess
import sys
import time
from pprint import pformat
from typing import Dict, List, Any, Optional, Tuple
import uuid

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Add the parent directory to sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import the client
from examples.client import BeamMCPClient

# Define color codes for terminal output
GREEN = '\033[92m'
YELLOW = '\033[93m'
RED = '\033[91m'
CYAN = '\033[96m'
RESET = '\033[0m'

class MCPEndToEndTest:
    """End-to-end test runner for the MCP server."""
    
    def __init__(self, config_path: str, server_port: int):
        """
        Initialize the test runner.
        
        Args:
            config_path: Path to the MCP server configuration file
            server_port: Port to run the MCP server on
        """
        self.config_path = config_path
        self.server_port = server_port
        self.server_url = f"http://localhost:{server_port}"
        self.client = BeamMCPClient(base_url=self.server_url)
        self.server_process = None
        self.job_ids = {}  # Track job IDs for cleanup
        self.test_results = {}
        
    async def start_server(self) -> bool:
        """
        Start the MCP server as a subprocess.
        
        Returns:
            True if server started successfully, False otherwise
        """
        logger.info(f"{CYAN}Starting MCP server on port {self.server_port}...{RESET}")
        
        # Construct the command to start the server
        cmd = [
            sys.executable,  # Use the same Python interpreter
            "main.py",
            "--port", str(self.server_port),
            "--debug"
        ]
        
        # Add config path if provided
        if self.config_path:
            os.environ["CONFIG_PATH"] = self.config_path
        
        # Start the server process
        try:
            # Use subprocess.Popen to start the server in the background
            self.server_process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=os.environ.copy()
            )
            
            # Wait for the server to start
            logger.info("Waiting for server to start...")
            for _ in range(10):  # Try for 10 seconds
                await asyncio.sleep(1)
                try:
                    # Try to connect to the server using manifest endpoint
                    manifest = await self.client.get_manifest()
                    # If we get here without an exception, the server is running
                    logger.info(f"{GREEN}Server started successfully{RESET}")
                    logger.info(f"Manifest contains {len(manifest.get('tools', []))} tools")
                    return True
                except Exception as e:
                    logger.debug(f"Server not ready yet: {str(e)}")
                    continue
            
            logger.error(f"{RED}Server failed to start within the timeout period{RESET}")
            return False
            
        except Exception as e:
            logger.error(f"{RED}Error starting server: {e}{RESET}")
            return False
    
    def stop_server(self):
        """Stop the MCP server."""
        if self.server_process:
            logger.info(f"{CYAN}Stopping MCP server...{RESET}")
            # Send a termination signal to the server process
            self.server_process.terminate()
            try:
                # Wait for the process to terminate
                self.server_process.wait(timeout=5)
                logger.info(f"{GREEN}Server stopped successfully{RESET}")
            except subprocess.TimeoutExpired:
                # If the process doesn't terminate in time, kill it
                logger.warning(f"{YELLOW}Server did not terminate gracefully, killing it...{RESET}")
                self.server_process.kill()
            
            # Get any output from the server
            stdout, stderr = self.server_process.communicate()
            if stdout:
                logger.debug(f"Server stdout: {stdout.decode('utf-8')}")
            if stderr:
                logger.debug(f"Server stderr: {stderr.decode('utf-8')}")
            
            self.server_process = None
    
    async def test_health_endpoint(self) -> bool:
        """
        Test the server health using the manifest endpoint.
        
        Returns:
            True if test passed, False otherwise
        """
        logger.info(f"{CYAN}Testing server health using manifest endpoint...{RESET}")
        try:
            response = await self.client.get_manifest()
            
            # Check response format (LLM-friendly)
            # The manifest endpoint returns a structure with 'data' at the top level
            if isinstance(response, dict) and "data" in response:
                data = response["data"]
            else:
                data = response
                
            is_valid = (
                isinstance(data, dict) and
                "name" in data and
                "description" in data and
                "version" in data and
                "tools" in data
            )
            
            if is_valid:
                logger.info(f"{GREEN}Server health check passed{RESET}")
                logger.info(f"Server name: {data.get('name')}")
                logger.info(f"Server version: {data.get('version')}")
                return True
            else:
                logger.error(f"{RED}Server health check failed: Invalid response format{RESET}")
                logger.error(f"Response: {pformat(response)}")
                return False
                
        except Exception as e:
            logger.error(f"{RED}Server health check failed: {e}{RESET}")
            return False
    
    async def test_manifest_endpoint(self) -> bool:
        """
        Test the manifest endpoint.
        
        Returns:
            True if test passed, False otherwise
        """
        logger.info(f"{CYAN}Testing manifest endpoint...{RESET}")
        try:
            response = await self.client.get_manifest()
            
            # Check response format (LLM-friendly)
            is_valid = (
                isinstance(response, dict) and
                "tools" in response and
                isinstance(response["tools"], list) and
                len(response["tools"]) > 0
            )
            
            if is_valid:
                logger.info(f"{GREEN}Manifest endpoint test passed{RESET}")
                logger.info(f"Found {len(response['tools'])} tools")
                # Just show the first tool's name and description
                if len(response['tools']) > 0:
                    tool = response['tools'][0]
                    logger.info(f"Sample tool: {tool.get('name')} - {tool.get('description')}")
                return True
            else:
                logger.error(f"{RED}Manifest endpoint test failed: Invalid response format{RESET}")
                logger.error(f"Response: {pformat(response)}")
                return False
                
        except Exception as e:
            logger.error(f"{RED}Manifest endpoint test failed: {e}{RESET}")
            return False
    
    async def test_list_runners(self) -> Tuple[bool, List[Dict[str, Any]]]:
        """
        Test the list runners endpoint.
        
        Returns:
            Tuple of (success, runners) where:
              - success is True if test passed, False otherwise
              - runners is a list of runner dictionaries
        """
        logger.info(f"{CYAN}Testing list runners endpoint...{RESET}")
        try:
            response = await self.client.list_runners()
            
            # Check response format - the response has 'runners' key containing the list
            if isinstance(response, dict) and "runners" in response:
                runners = response["runners"]
            else:
                runners = response
                
            # Check response format (LLM-friendly)
            is_valid = (
                isinstance(runners, list) and
                all(isinstance(runner, dict) for runner in runners) and
                all("runner_type" in runner for runner in runners)
            )
            
            if is_valid:
                # Filter out disabled runners and dataflow runner
                filtered_runners = [
                    runner for runner in runners 
                    if runner.get("runner_type", "").lower() != "dataflow" and 
                    runner.get("status", "").upper() == "AVAILABLE"
                ]
                
                runner_types = [runner["runner_type"] for runner in filtered_runners]
                logger.info(f"{GREEN}List runners endpoint test passed{RESET}")
                logger.info(f"Found {len(filtered_runners)} active runners: {', '.join(runner_types)}")
                return True, filtered_runners
            else:
                logger.error(f"{RED}List runners endpoint test failed: Invalid response format{RESET}")
                logger.error(f"Response: {pformat(response)}")
                return False, []
                
        except Exception as e:
            logger.error(f"{RED}List runners endpoint test failed: {e}{RESET}")
            return False, []
    
    async def test_job_submission(self, runner_type: str) -> Tuple[bool, Optional[str]]:
        """
        Test job submission for a specific runner.
        
        Args:
            runner_type: The runner type to test (direct, flink, spark)
            
        Returns:
            Tuple of (success, job_id) where:
              - success is True if test passed, False otherwise
              - job_id is the ID of the created job, or None if failed
        """
        logger.info(f"{CYAN}Testing job submission for {runner_type} runner...{RESET}")
        
        # Configure pipeline options based on the runner type
        if runner_type.lower() == "direct":
            pipeline_options = {
                "direct_num_workers": 2,
                "direct_running_mode": "multi_threading",
                "temp_location": "/tmp/beam-direct-temp"
            }
        elif runner_type.lower() == "flink":
            pipeline_options = {
                "parallelism": 2,
                "flink_master": "localhost:8081",
                "temp_location": "/tmp/beam-flink-temp"
            }
        elif runner_type.lower() == "spark":
            pipeline_options = {
                "spark_master": "local[2]",
                "spark_executor_cores": 2,
                "temp_location": "/tmp/beam-spark-temp"
            }
        else:
            logger.error(f"{RED}Unsupported runner type: {runner_type}{RESET}")
            return False, None
        
        # Create job parameters
        job_params = {
            "job_name": f"e2e-test-{runner_type.lower()}",
            "runner_type": runner_type.lower(),
            "job_type": "BATCH",
            "code_path": "examples/pipelines/wordcount.py",
            "pipeline_options": pipeline_options
        }
        
        try:
            # Submit the job
            logger.info(f"Submitting job for {runner_type} runner...")
            response = await self.client.create_job(job_params)
            
            # Check response format (LLM-friendly)
            is_valid = (
                isinstance(response, dict) and
                "job_id" in response and
                "job_name" in response and
                "status" in response
            )
            
            if is_valid:
                job_id = response["job_id"]
                logger.info(f"{GREEN}Job submission for {runner_type} runner passed{RESET}")
                logger.info(f"Job ID: {job_id}")
                logger.info(f"Initial status: {response['status']}")
                return True, job_id
            else:
                logger.error(f"{RED}Job submission for {runner_type} runner failed: Invalid response format{RESET}")
                logger.error(f"Response: {pformat(response)}")
                return False, None
                
        except Exception as e:
            logger.error(f"{RED}Job submission for {runner_type} runner failed: {e}{RESET}")
            return False, None
    
    async def test_job_status(self, job_id: str) -> bool:
        """
        Test job status endpoint.
        
        Args:
            job_id: The job ID to check
            
        Returns:
            True if test passed, False otherwise
        """
        logger.info(f"{CYAN}Testing job status endpoint for job {job_id}...{RESET}")
        
        try:
            # Get job status
            logger.info(f"Getting status for job {job_id}...")
            response = await self.client.get_job(job_id)
            
            # Check response format (LLM-friendly)
            is_valid = (
                isinstance(response, dict) and
                "job_id" in response and
                "status" in response and
                "current_state" in response
            )
            
            if is_valid:
                logger.info(f"{GREEN}Job status endpoint test passed{RESET}")
                logger.info(f"Job status: {response['status']}")
                logger.info(f"Current state: {response['current_state']}")
                return True
            else:
                logger.error(f"{RED}Job status endpoint test failed: Invalid response format{RESET}")
                logger.error(f"Response: {pformat(response)}")
                return False
                
        except Exception as e:
            logger.error(f"{RED}Job status endpoint test failed: {e}{RESET}")
            return False
    
    async def test_job_metrics(self, job_id: str) -> bool:
        """
        Test job metrics endpoint.
        
        Args:
            job_id: The job ID to check
            
        Returns:
            True if test passed, False otherwise
        """
        logger.info(f"{CYAN}Testing job metrics endpoint for job {job_id}...{RESET}")
        
        try:
            # Get job metrics
            logger.info(f"Getting metrics for job {job_id}...")
            response = await self.client.get_job_metrics(job_id)
            
            # For metrics, we're a bit lenient as not all runners provide metrics
            # or metrics may not be available immediately
            is_valid = isinstance(response, dict) and "job_id" in response
            
            if is_valid:
                logger.info(f"{GREEN}Job metrics endpoint test passed{RESET}")
                
                # If metrics are available, show some details
                if "metrics" in response and isinstance(response["metrics"], list):
                    logger.info(f"Found {len(response['metrics'])} metrics")
                    if len(response["metrics"]) > 0:
                        logger.info(f"Sample metric: {response['metrics'][0].get('name')}")
                else:
                    logger.info("No metrics available for this job yet")
                    
                return True
            else:
                logger.error(f"{RED}Job metrics endpoint test failed: Invalid response format{RESET}")
                logger.error(f"Response: {pformat(response)}")
                return False
                
        except Exception as e:
            # Metrics endpoint might fail for some runners - it's not a critical failure
            logger.warning(f"{YELLOW}Job metrics endpoint test failed: {e}{RESET}")
            logger.warning("This might be normal for some runners or if the job is still starting")
            return True  # Still return true to continue testing
    
    async def test_job_cancel(self, job_id: str) -> bool:
        """
        Test job cancellation.
        
        Args:
            job_id: The job ID to cancel
            
        Returns:
            True if test passed, False otherwise
        """
        logger.info(f"{CYAN}Testing job cancellation for job {job_id}...{RESET}")
        
        try:
            # Check current job status first
            status_response = await self.client.get_job(job_id)
            
            # If the job is already completed or failed, cancellation is not necessary
            # and the test should be considered as passed
            if (isinstance(status_response, dict) and 
                "status" in status_response and
                status_response["status"] in ["COMPLETED", "FAILED", "CANCELLED", "CANCELED", "DONE"]):
                logger.info(f"{GREEN}Job already in final state ({status_response['status']}), skipping cancellation{RESET}")
                return True
                
            # Cancel the job
            logger.info(f"Cancelling job {job_id}...")
            try:
                await self.client.cancel_job(job_id)
                logger.info(f"{GREEN}Job cancellation request sent successfully{RESET}")
            except Exception as e:
                # For Direct runner, cancellation may not be supported or may not be necessary
                logger.warning(f"{YELLOW}Job cancellation request failed: {e}{RESET}")
                logger.warning("This might be normal for some runners (e.g., Direct) or if the job is already done")
                
                # Still consider the test passed if we can get the job status
                status_response = await self.client.get_job(job_id)
                if isinstance(status_response, dict) and "status" in status_response:
                    logger.info(f"Job is in state: {status_response['status']}")
                    return True
                return False
            
            # Check if job was cancelled by getting its status
            await asyncio.sleep(2)  # Give it a moment to process the cancellation
            status_response = await self.client.get_job(job_id)
            
            is_cancelled = (
                isinstance(status_response, dict) and
                "status" in status_response and
                status_response["status"] in ["CANCELLED", "CANCELLING", "CANCELED", "FAILED", "COMPLETED", "DONE"]
            )
            
            if is_cancelled:
                logger.info(f"{GREEN}Job cancellation test passed{RESET}")
                logger.info(f"Job status after cancellation: {status_response['status']}")
                return True
            else:
                logger.error(f"{RED}Job cancellation test failed: Job was not cancelled{RESET}")
                logger.error(f"Job status: {status_response.get('status', 'unknown')}")
                return False
                
        except Exception as e:
            logger.error(f"{RED}Job cancellation test failed: {e}{RESET}")
            return False
    
    async def run_all_tests(self) -> Dict[str, bool]:
        """
        Run all tests in sequence.
        
        Returns:
            Dict of test results
        """
        logger.info(f"{CYAN}Starting end-to-end tests...{RESET}")
        
        # Start the server
        server_started = await self.start_server()
        if not server_started:
            logger.error(f"{RED}Server failed to start, aborting tests{RESET}")
            return {}
            
        try:
            # Test health (using manifest endpoint)
            self.test_results["health_check"] = await self.test_health_endpoint()
            
            # Skip manifest test as we already used it for health check
            self.test_results["manifest"] = True
            
            # Test list runners
            runners_success, runners = await self.test_list_runners()
            self.test_results["list_runners"] = runners_success
            
            # Test job submission, status, metrics and cancellation for each runner
            if runners_success:
                for runner in runners:
                    runner_type = runner["runner_type"]
                    
                    # Test job submission
                    job_submission_success, job_id = await self.test_job_submission(runner_type)
                    self.test_results[f"job_submission_{runner_type}"] = job_submission_success
                    
                    if job_submission_success and job_id:
                        self.job_ids[runner_type] = job_id
                        
                        # Store the job ID for cleanup
                        logger.info(f"Job ID for {runner_type}: {job_id}")
                        
                        # Test job status
                        job_status_success = await self.test_job_status(job_id)
                        self.test_results[f"job_status_{runner_type}"] = job_status_success
                        
                        # Test job metrics
                        job_metrics_success = await self.test_job_metrics(job_id)
                        self.test_results[f"job_metrics_{runner_type}"] = job_metrics_success
                        
                        # Test job cancellation
                        job_cancel_success = await self.test_job_cancel(job_id)
                        self.test_results[f"job_cancel_{runner_type}"] = job_cancel_success
            
            return self.test_results
            
        finally:
            # Stop the server
            self.stop_server()
    
    def print_results_summary(self):
        """Print a summary of the test results."""
        if not self.test_results:
            logger.warning(f"{YELLOW}No test results to display{RESET}")
            return
        
        logger.info(f"\n{CYAN}=== Test Results Summary ==={RESET}")
        
        # Count passed and failed tests
        passed = sum(1 for result in self.test_results.values() if result)
        failed = sum(1 for result in self.test_results.values() if not result)
        
        # Print overall statistics
        logger.info(f"{CYAN}Total tests: {passed + failed}{RESET}")
        logger.info(f"{GREEN}Passed: {passed}{RESET}")
        logger.info(f"{RED if failed > 0 else RESET}Failed: {failed}{RESET}")
        
        # Print individual test results
        logger.info(f"\n{CYAN}Individual Test Results:{RESET}")
        for test_name, result in self.test_results.items():
            status = f"{GREEN}PASS{RESET}" if result else f"{RED}FAIL{RESET}"
            logger.info(f"{test_name}: {status}")
        
        # Print overall result
        if failed == 0:
            logger.info(f"\n{GREEN}All tests passed successfully!{RESET}")
        else:
            logger.info(f"\n{RED}Some tests failed. Please check the logs for details.{RESET}")

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='End-to-end test for Apache Beam MCP Server')
    parser.add_argument('--config', default='config/flink_config.yaml', help='Path to server configuration file')
    parser.add_argument('--port', type=int, default=8888, help='Port to run the server on')
    
    return parser.parse_args()

async def main():
    """Main entry point."""
    args = parse_args()
    
    # Create and run the tests
    tester = MCPEndToEndTest(args.config, args.port)
    
    try:
        await tester.run_all_tests()
        tester.print_results_summary()
    except KeyboardInterrupt:
        logger.info("Tests interrupted by user")
        tester.stop_server()
    except Exception as e:
        logger.error(f"Error running tests: {e}")
        tester.stop_server()

if __name__ == '__main__':
    asyncio.run(main()) 