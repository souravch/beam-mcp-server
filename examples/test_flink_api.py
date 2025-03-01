#!/usr/bin/env python
"""
HTTP client to test the Flink job API.

This script demonstrates how to submit a Flink job through the API,
monitor its status, and cancel it.
"""

import requests
import json
import time
import os
import sys
import logging
from datetime import datetime
import argparse

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_input_file(path):
    """Create a sample input file for the WordCount job."""
    with open(path, 'w') as f:
        f.write("""
        This is a test file for our Flink WordCount job.
        It contains some sample text to count words.
        The API should be able to process this file and submit it to Flink.
        """)
    return path

def submit_job(api_base_url, job_name, jar_path, input_file, output_file):
    """Submit a job to the API."""
    url = f"{api_base_url}/api/v1/jobs"
    
    payload = {
        "job_name": job_name,
        "job_type": "BATCH",
        "runner_type": "flink",  # Change runner to runner_type to match the API
        "code_path": "examples/pipelines/wordcount.py",  # Added code_path to satisfy validation
        "pipeline_options": {
            "jar_path": jar_path,
            "entry_class": "org.apache.flink.examples.java.wordcount.WordCount",
            "parallelism": 2,
            "program_args": f"--input {input_file} --output {output_file}"
        }
    }
    
    logger.info(f"Submitting job to {url}...")
    logger.info(f"Payload: {json.dumps(payload, indent=2)}")
    response = requests.post(url, json=payload)
    
    if response.status_code == 200:
        result = response.json()
        logger.info(f"Job submitted successfully: {json.dumps(result, indent=2)}")
        return result['data']['job_id']
    else:
        logger.error(f"Failed to submit job: {response.status_code} - {response.text}")
        return None

def get_job_status(api_base_url, job_id):
    """Get the status of a job."""
    url = f"{api_base_url}/api/v1/jobs/{job_id}"
    
    logger.info(f"Getting job status from {url}...")
    response = requests.get(url)
    
    if response.status_code == 200:
        result = response.json()
        status = result['data']['status']
        logger.info(f"Job status: {status}")
        return status
    else:
        logger.error(f"Failed to get job status: {response.status_code} - {response.text}")
        return None

def cancel_job(api_base_url, job_id):
    """Cancel a job."""
    url = f"{api_base_url}/api/v1/jobs/{job_id}"
    
    logger.info(f"Cancelling job at {url}...")
    response = requests.delete(url)
    
    if response.status_code == 200:
        result = response.json()
        logger.info(f"Job cancelled: {json.dumps(result, indent=2)}")
        return True
    else:
        logger.error(f"Failed to cancel job: {response.status_code} - {response.text}")
        return False

def main():
    """Run the test."""
    parser = argparse.ArgumentParser(description='Test the Flink job API')
    parser.add_argument('--api-url', default='http://localhost:8083', help='Base URL for the API')
    parser.add_argument('--jar-path', default='/Users/souravchakraborty/beam/flink-1.17.0/examples/batch/WordCount.jar', 
                      help='Path to the Flink WordCount JAR')
    parser.add_argument('--input-file', default='/tmp/api_test_input.txt', help='Path to the input file')
    parser.add_argument('--output-file', default='/tmp/api_test_output.txt', help='Path to the output file')
    parser.add_argument('--job-name', default='api-test-wordcount', help='Name for the job')
    
    args = parser.parse_args()
    
    # Create input file
    create_input_file(args.input_file)
    
    # Submit job
    job_id = submit_job(args.api_url, args.job_name, args.jar_path, args.input_file, args.output_file)
    
    if not job_id:
        logger.error("Failed to get job ID, exiting")
        return
    
    # Monitor job status
    max_checks = 10
    for i in range(max_checks):
        status = get_job_status(args.api_url, job_id)
        if not status:
            break
            
        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            logger.info(f"Job reached terminal state: {status}")
            break
            
        time.sleep(5)
    
    # Ask user if they want to cancel
    if status not in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
        user_input = input("Cancel the job? (y/n): ")
        if user_input.lower() == 'y':
            cancelled = cancel_job(args.api_url, job_id)
            
            if cancelled:
                # Check final status
                time.sleep(5)
                final_status = get_job_status(args.api_url, job_id)
                logger.info(f"Final job status: {final_status}")
    
    logger.info("Test completed!")

if __name__ == "__main__":
    main() 