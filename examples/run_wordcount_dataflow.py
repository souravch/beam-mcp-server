#!/usr/bin/env python3
"""
Run wordcount example on Dataflow runner.

This script demonstrates how to run the wordcount pipeline on the Google Cloud Dataflow runner.
"""

import logging
import os
import sys
import argparse
from typing import Dict, Any, List, Optional
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, GoogleCloudOptions, WorkerOptions, SetupOptions

# Add the parent directory to the Python path so we can import the pipeline
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from examples.pipelines.wordcount import run as run_wordcount

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Run WordCount on Dataflow runner')
    parser.add_argument('--input', help='Input text file (local or GCS path)')
    parser.add_argument('--output', help='Output path prefix (GCS path)', default='gs://your-bucket/output/dataflow_wordcount')
    parser.add_argument('--project', required=True, help='Google Cloud project ID')
    parser.add_argument('--region', default='us-central1', help='Google Cloud region')
    parser.add_argument('--job_name', default='beam-wordcount-dataflow', help='Dataflow job name')
    parser.add_argument('--staging_location', help='GCS staging location', default='gs://your-bucket/staging')
    parser.add_argument('--temp_location', help='GCS temp location', default='gs://your-bucket/temp')
    parser.add_argument('--max_workers', type=int, default=4, help='Maximum number of workers')
    parser.add_argument('--machine_type', default='n1-standard-2', help='Worker machine type')
    parser.add_argument('--disk_size_gb', type=int, default=30, help='Worker disk size in GB')
    parser.add_argument('--streaming', action='store_true', help='Run in streaming mode')
    
    args = parser.parse_args()
    return args

def create_pipeline_options(args) -> PipelineOptions:
    """Create the pipeline options for Dataflow runner."""
    options_dict = {
        'runner': 'DataflowRunner',
        'project': args.project,
        'region': args.region,
        'job_name': args.job_name,
        'staging_location': args.staging_location,
        'temp_location': args.temp_location,
        'worker_machine_type': args.machine_type,
        'max_workers': args.max_workers,
        'disk_size_gb': args.disk_size_gb,
        'autoscaling_algorithm': 'THROUGHPUT_BASED',
        'setup_file': './setup.py',  # Optional: if you have a setup.py
        'save_main_session': True,
        'experiments': [
            'use_runner_v2',  # Recommended for Beam 2.31.0+
        ],
    }
    
    # Create and configure pipeline options
    options = PipelineOptions.from_dictionary(options_dict)
    
    # Configure standard options
    standard_options = options.view_as(StandardOptions)
    if args.streaming:
        standard_options.streaming = True
    
    # Configure Google Cloud options
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = args.project
    google_cloud_options.region = args.region
    google_cloud_options.job_name = args.job_name
    google_cloud_options.staging_location = args.staging_location
    google_cloud_options.temp_location = args.temp_location
    
    # Configure worker options
    worker_options = options.view_as(WorkerOptions)
    worker_options.machine_type = args.machine_type
    worker_options.max_workers = args.max_workers
    worker_options.disk_size_gb = args.disk_size_gb
    
    # Configure setup options
    setup_options = options.view_as(SetupOptions)
    setup_options.save_main_session = True
    
    return options

def main():
    """Main entry point."""
    args = parse_args()
    
    logger.info("Creating Dataflow runner pipeline options")
    options = create_pipeline_options(args)
    
    logger.info(f"Running WordCount pipeline on Dataflow in project {args.project}, region {args.region}")
    run_wordcount(options, args.input, args.output)
    
    logger.info(f"Pipeline submitted to Dataflow. Output will be written to: {args.output}*")
    logger.info(f"Monitor the job progress in the Google Cloud Console Dataflow page: https://console.cloud.google.com/dataflow/jobs?project={args.project}")

if __name__ == '__main__':
    main() 