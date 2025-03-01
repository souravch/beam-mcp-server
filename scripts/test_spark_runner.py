#!/usr/bin/env python
"""
Test script for the Apache Spark runner in the MCP server.

This script submits a wordcount job to the MCP server using the Spark runner.
"""

import argparse
import json
import logging
import time
import requests
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    """Run the Spark runner test."""
    parser = argparse.ArgumentParser(description='Test Apache Spark runner using MCP Server')
    parser.add_argument(
        '--server-url',
        default='http://localhost:8083',
        help='URL of the MCP server'
    )
    parser.add_argument(
        '--input-file',
        default='/tmp/spark_input.txt',
        help='Input file path'
    )
    parser.add_argument(
        '--output-file',
        default='/tmp/spark_output.txt',
        help='Output file path'
    )
    parser.add_argument(
        '--job-name',
        default='spark-wordcount-test',
        help='Name of the job'
    )
    
    args = parser.parse_args()
    
    # Ensure input file exists
    if not os.path.exists(args.input_file):
        logger.error(f"Input file '{args.input_file}' does not exist")
        exit(1)
    
    # Check if MCP server is running
    try:
        response = requests.get(f"{args.server_url}/health")
        if response.status_code != 200:
            logger.error(f"MCP server at {args.server_url} is not available. Status code: {response.status_code}")
            exit(1)
        logger.info(f"MCP server at {args.server_url} is available")
        
        # Get runner info to verify Spark runner is available
        runners_response = requests.get(f"{args.server_url}/api/v1/runners")
        if runners_response.status_code != 200:
            logger.error(f"Failed to get runner list. Status code: {runners_response.status_code}")
            exit(1)
        
        runners_data = runners_response.json()
        logger.info(f"Available runners: {[r['name'] for r in runners_data['runners']]}")
        
        # Find the Spark runner
        spark_runner = None
        for runner in runners_data['runners']:
            if runner['runner_type'] == 'spark':
                spark_runner = runner
                break
        
        if not spark_runner:
            logger.error("Spark runner is not available in the server")
            exit(1)
        
        logger.info(f"Spark runner is available: {spark_runner['name']}")
        
        # Submit a wordcount job using the Spark runner
        job_params = {
            "job_name": args.job_name,
            "runner_type": "spark",
            "job_type": "BATCH",
            "code_path": "examples/pipelines/spark_wordcount.py",
            "pipeline_options": {
                "input": args.input_file,
                "output": args.output_file,
                "spark_master": "local[*]",
                "save_main_session": True
            }
        }
        
        logger.info(f"Submitting job with parameters: {job_params}")
        job_response = requests.post(
            f"{args.server_url}/api/v1/jobs",
            json=job_params
        )
        
        if job_response.status_code != 200:
            logger.error(f"Failed to submit job. Status code: {job_response.status_code}")
            logger.error(f"Response: {job_response.text}")
            exit(1)
        
        job_data = job_response.json()
        job_id = job_data.get('job_id')
        logger.info(f"Job submitted successfully. Job ID: {job_id}")
        
        # Wait for job to complete
        max_wait_time = 300  # 5 minutes
        wait_interval = 5  # 5 seconds
        waited_time = 0
        
        while waited_time < max_wait_time:
            time.sleep(wait_interval)
            waited_time += wait_interval
            
            status_response = requests.get(f"{args.server_url}/api/v1/jobs/{job_id}")
            if status_response.status_code != 200:
                logger.warning(f"Failed to get job status. Status code: {status_response.status_code}")
                continue
            
            status_data = status_response.json()
            job_status = status_data.get('status')
            logger.info(f"Job status: {job_status}")
            
            if job_status in ["COMPLETED", "FAILED", "CANCELLED"]:
                break
        
        if job_status == "COMPLETED":
            logger.info("Job completed successfully")
            
            # Check output file
            if os.path.exists(args.output_file):
                logger.info(f"Output file '{args.output_file}' created successfully")
                with open(args.output_file, 'r') as f:
                    logger.info(f"Content of output file: {f.read()}")
            else:
                logger.warning(f"Output file '{args.output_file}' was not created")
        else:
            logger.error(f"Job did not complete successfully. Final status: {job_status}")
            
            # Get job metrics
            try:
                metrics_response = requests.get(f"{args.server_url}/api/v1/jobs/{job_id}/metrics")
                if metrics_response.status_code == 200:
                    metrics_data = metrics_response.json()
                    logger.info(f"Job metrics: {json.dumps(metrics_data, indent=2)}")
            except Exception as e:
                logger.warning(f"Failed to get job metrics: {str(e)}")
            
            exit(1)
        
    except Exception as e:
        logger.error(f"Error testing Spark runner: {str(e)}")
        exit(1)

if __name__ == "__main__":
    main() 