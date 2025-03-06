#!/usr/bin/env python3
"""
Test script for validating the runner configurations and MCP server integration.

This script tests the MCP server with different runners (Direct, Flink, Spark)
to ensure that job submissions work correctly.
"""

import argparse
import asyncio
import logging
import os
import sys
import time
import subprocess
import json
import requests
from typing import Dict, Any, List, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Add the parent directory to sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from examples.client import BeamMCPClient

class RunnerTest:
    """Test runner for validating MCP server and different Apache Beam runners."""
    
    def __init__(self, server_url: str, config_path: Optional[str] = None):
        """
        Initialize the runner test.
        
        Args:
            server_url: URL of the MCP server
            config_path: Path to the runner configuration file
        """
        self.server_url = server_url
        self.config_path = config_path
        self.client = BeamMCPClient(base_url=server_url)
    
    async def test_list_runners(self) -> List[Dict[str, Any]]:
        """
        Test listing available runners.
        
        Returns:
            List of available runners
        """
        logger.info("Testing list_runners endpoint")
        try:
            runners = await self.client.list_runners()
            logger.info(f"Found {len(runners)} available runners")
            for runner in runners:
                logger.info(f"Runner: {runner['name']} ({runner['runner_type']})")
            return runners
        except Exception as e:
            logger.error(f"Error listing runners: {e}")
            return []
    
    async def test_direct_runner(self) -> Optional[str]:
        """
        Test job submission with Direct runner.
        
        Returns:
            Job ID if successful, None otherwise
        """
        logger.info("Testing Direct runner")
        try:
            job_params = {
                "job_name": "direct-wordcount-test",
                "runner_type": "direct",
                "job_type": "BATCH",
                "code_path": "examples/pipelines/wordcount.py",
                "pipeline_options": {
                    "direct_num_workers": 2,
                    "direct_running_mode": "multi_threading",
                    "temp_location": "/tmp/beam-direct-temp"
                }
            }
            
            job = await self.client.create_job(job_params)
            job_id = job.get("job_id")
            
            if job_id:
                logger.info(f"Direct runner job created: {job_id}")
                await self._monitor_job(job_id)
                return job_id
            
            logger.error("Failed to get job ID for Direct runner job")
            return None
            
        except Exception as e:
            logger.error(f"Error testing Direct runner: {e}")
            return None
    
    async def test_flink_runner(self) -> Optional[str]:
        """
        Test job submission with Flink runner.
        
        Returns:
            Job ID if successful, None otherwise
        """
        logger.info("Testing Flink runner")
        try:
            job_params = {
                "job_name": "flink-wordcount-test",
                "runner_type": "flink",
                "job_type": "BATCH",
                "code_path": "examples/pipelines/wordcount.py",
                "pipeline_options": {
                    "parallelism": 2,
                    "flink_master": "localhost:8081",
                    "temp_location": "/tmp/beam-flink-temp"
                }
            }
            
            job = await self.client.create_job(job_params)
            job_id = job.get("job_id")
            
            if job_id:
                logger.info(f"Flink runner job created: {job_id}")
                await self._monitor_job(job_id)
                return job_id
            
            logger.error("Failed to get job ID for Flink runner job")
            return None
            
        except Exception as e:
            logger.error(f"Error testing Flink runner: {e}")
            return None
    
    async def test_spark_runner(self) -> Optional[str]:
        """
        Test job submission with Spark runner.
        
        Returns:
            Job ID if successful, None otherwise
        """
        logger.info("Testing Spark runner")
        try:
            job_params = {
                "job_name": "spark-wordcount-test",
                "runner_type": "spark",
                "job_type": "BATCH",
                "code_path": "examples/pipelines/wordcount.py",
                "pipeline_options": {
                    "spark_master": "local[2]",
                    "spark_executor_cores": 2,
                    "temp_location": "/tmp/beam-spark-temp"
                }
            }
            
            job = await self.client.create_job(job_params)
            job_id = job.get("job_id")
            
            if job_id:
                logger.info(f"Spark runner job created: {job_id}")
                await self._monitor_job(job_id)
                return job_id
            
            logger.error("Failed to get job ID for Spark runner job")
            return None
            
        except Exception as e:
            logger.error(f"Error testing Spark runner: {e}")
            return None
    
    async def _monitor_job(self, job_id: str, max_attempts: int = 10, delay: int = 2) -> None:
        """
        Monitor a job until completion or timeout.
        
        Args:
            job_id: Job ID to monitor
            max_attempts: Maximum number of attempts to check job status
            delay: Delay between attempts in seconds
        """
        for attempt in range(max_attempts):
            try:
                job = await self.client.get_job(job_id)
                status = job.get("current_state", "UNKNOWN")
                logger.info(f"Job {job_id} status: {status}")
                
                if status in ["DONE", "CANCELLED", "FAILED", "COMPLETED"]:
                    break
                
                await asyncio.sleep(delay)
            except Exception as e:
                logger.error(f"Error monitoring job {job_id}: {e}")
                await asyncio.sleep(delay)
    
    async def run_tests(self) -> Dict[str, bool]:
        """
        Run tests for the MCP server for Direct, Flink and Spark runners.
            
        Returns:
            Dictionary with test results
        """
        results = {}
        
        # Test listing runners
        runners = await self.test_list_runners()
        results["list_runners"] = len(runners) > 0
        
        # Test Direct runner
        direct_job_id = await self.test_direct_runner()
        results["direct_runner"] = direct_job_id is not None
        
        # Test Flink runner
        flink_job_id = await self.test_flink_runner()
        results["flink_runner"] = flink_job_id is not None
        
        # Test Spark runner
        spark_job_id = await self.test_spark_runner()
        results["spark_runner"] = spark_job_id is not None
        
        # Print summary
        logger.info("=== Test Results ===")
        for test, result in results.items():
            logger.info(f"{test}: {'PASS' if result else 'FAIL'}")
        
        return results

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Test MCP runners')
    parser.add_argument('--server', default='http://localhost:8888', help='MCP server URL')
    parser.add_argument('--runner', default='direct', choices=['direct', 'flink', 'spark', 'dataflow'], 
                      help='Runner to test')
    parser.add_argument('--config', help='Path to runner configuration file')
    
    args = parser.parse_args()
    return args

async def main():
    """Main entry point."""
    args = parse_args()
    
    # Create test runner
    tester = RunnerTest(server_url=args.server, config_path=args.config)
    
    # Run tests for Direct, Flink and Spark runners
    await tester.run_tests()

if __name__ == '__main__':
    asyncio.run(main())