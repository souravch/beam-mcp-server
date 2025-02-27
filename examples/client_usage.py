#!/usr/bin/env python3
"""
Example client usage for the Apache Beam MCP Server.

This script demonstrates how to use the client to interact with the MCP server
for all supported runners (Direct, Flink, Spark).
"""

import asyncio
import logging
import argparse
import sys
import os
from typing import Dict, Any, Optional, List

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

async def list_runners(client: BeamMCPClient) -> List[Dict[str, Any]]:
    """
    List available runners.
    
    Args:
        client: The MCP client
        
    Returns:
        List of runners
    """
    logger.info("Listing available runners")
    runners = await client.list_runners()
    
    logger.info(f"Found {len(runners)} runners:")
    for runner in runners:
        logger.info(f"  - {runner['name']} ({runner['runner_type']})")
    
    return runners

async def submit_job(client: BeamMCPClient, runner_type: str) -> Optional[Dict[str, Any]]:
    """
    Submit a job to the specified runner.
    
    Args:
        client: The MCP client
        runner_type: The runner type to use
        
    Returns:
        Job information if successful, None otherwise
    """
    logger.info(f"Submitting job to {runner_type} runner")
    
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
        logger.error(f"Unsupported runner type: {runner_type}")
        return None
    
    # Create job parameters
    job_params = {
        "job_name": f"{runner_type.lower()}-wordcount-example",
        "runner_type": runner_type.lower(),
        "job_type": "BATCH",
        "code_path": "examples/pipelines/wordcount.py",
        "pipeline_options": pipeline_options
    }
    
    try:
        # Submit the job
        job = await client.create_job(job_params)
        job_id = job.get("job_id")
        
        if job_id:
            logger.info(f"Job submitted successfully with ID: {job_id}")
            return job
        else:
            logger.error("Failed to get job ID")
            return None
    except Exception as e:
        logger.error(f"Error submitting job: {e}")
        return None

async def monitor_job(client: BeamMCPClient, job_id: str, timeout_seconds: int = 60) -> Dict[str, Any]:
    """
    Monitor a job until it completes or times out.
    
    Args:
        client: The MCP client
        job_id: The job ID to monitor
        timeout_seconds: Maximum time to wait in seconds
        
    Returns:
        Final job status
    """
    logger.info(f"Monitoring job {job_id}")
    
    # Start monitoring
    start_time = asyncio.get_event_loop().time()
    
    while True:
        # Check if we've exceeded the timeout
        elapsed_time = asyncio.get_event_loop().time() - start_time
        if elapsed_time > timeout_seconds:
            logger.warning(f"Monitoring timed out after {timeout_seconds} seconds")
            break
        
        # Get job status
        try:
            job = await client.get_job(job_id)
            status = job.get("current_state", "UNKNOWN")
            logger.info(f"Job {job_id} status: {status}")
            
            # Check if the job is done
            if status in ["DONE", "CANCELLED", "FAILED", "COMPLETED"]:
                logger.info(f"Job {job_id} finished with status: {status}")
                break
            
            # Wait before checking again
            await asyncio.sleep(2)
        except Exception as e:
            logger.error(f"Error monitoring job: {e}")
            break
    
    # Get final job details
    try:
        return await client.get_job(job_id)
    except Exception as e:
        logger.error(f"Error getting final job details: {e}")
        return {"error": str(e)}

async def cancel_job(client: BeamMCPClient, job_id: str) -> bool:
    """
    Cancel a job.
    
    Args:
        client: The MCP client
        job_id: The job ID to cancel
        
    Returns:
        True if successful, False otherwise
    """
    logger.info(f"Cancelling job {job_id}")
    
    try:
        await client.cancel_job(job_id)
        logger.info(f"Job {job_id} cancelled successfully")
        return True
    except Exception as e:
        logger.error(f"Error cancelling job: {e}")
        return False

async def get_job_metrics(client: BeamMCPClient, job_id: str) -> Dict[str, Any]:
    """
    Get job metrics.
    
    Args:
        client: The MCP client
        job_id: The job ID
        
    Returns:
        Job metrics
    """
    logger.info(f"Getting metrics for job {job_id}")
    
    try:
        metrics = await client.get_job_metrics(job_id)
        logger.info(f"Retrieved metrics for job {job_id}")
        return metrics
    except Exception as e:
        logger.error(f"Error getting job metrics: {e}")
        return {"error": str(e)}

async def main(args):
    """
    Main entry point.
    
    Args:
        args: Command line arguments
    """
    # Create client
    client = BeamMCPClient(base_url=args.server)
    
    # List runners
    runners = await list_runners(client)
    
    # Submit job to specified runner
    job = await submit_job(client, args.runner)
    
    if job and "job_id" in job:
        job_id = job["job_id"]
        
        # Monitor job
        final_job = await monitor_job(client, job_id, args.timeout)
        
        # Get job metrics
        if not args.skip_metrics:
            metrics = await get_job_metrics(client, job_id)
            
            # Display metrics summary
            if "metrics" in metrics and isinstance(metrics["metrics"], list):
                logger.info(f"Job metrics summary ({len(metrics['metrics'])} metrics):")
                for metric in metrics["metrics"][:5]:  # Show first 5 metrics
                    logger.info(f"  - {metric['name']}: {metric.get('values', [{}])[0].get('value', 'N/A')}")
                
                if len(metrics["metrics"]) > 5:
                    logger.info(f"  ... and {len(metrics['metrics']) - 5} more metrics")
        
        # Cancel job if requested
        if args.cancel:
            await cancel_job(client, job_id)

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Apache Beam MCP Client Example')
    parser.add_argument('--server', default='http://localhost:8082', help='MCP server URL')
    parser.add_argument('--runner', default='direct', choices=['direct', 'flink', 'spark'], 
                        help='Runner to use')
    parser.add_argument('--timeout', type=int, default=60, help='Monitoring timeout in seconds')
    parser.add_argument('--cancel', action='store_true', help='Cancel job after monitoring')
    parser.add_argument('--skip-metrics', action='store_true', help='Skip retrieving metrics')
    
    return parser.parse_args()

if __name__ == '__main__':
    args = parse_args()
    asyncio.run(main(args)) 