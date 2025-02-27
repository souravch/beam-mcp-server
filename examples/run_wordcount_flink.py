#!/usr/bin/env python3
"""
Run wordcount example on Flink runner.

This script demonstrates how to run the wordcount pipeline on the Apache Flink runner.
"""

import logging
import os
import sys
import argparse
from typing import Dict, Any, List, Optional
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, FlinkRunnerOptions

# Add the parent directory to the Python path so we can import the pipeline
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from examples.pipelines.wordcount import run as run_wordcount

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Run WordCount on Flink runner')
    parser.add_argument('--input', help='Input text file')
    parser.add_argument('--output', help='Output path prefix', default='flink_wordcount_output')
    parser.add_argument('--flink_master', default='http://localhost:8081', 
                      help='Flink JobManager URL (e.g., http://localhost:8081)')
    parser.add_argument('--parallelism', type=int, default=4, help='Default parallelism')
    parser.add_argument('--streaming', action='store_true', help='Run in streaming mode')
    
    args = parser.parse_args()
    return args

def create_pipeline_options(args) -> PipelineOptions:
    """Create the pipeline options for Flink runner."""
    options_dict = {
        'runner': 'FlinkRunner',
        'flink_master': args.flink_master,
        'flink_version': '1.17',  # Adjust based on your Flink version
        'flink_submit_uber_jar': True,
        'parallelism': args.parallelism,
        'temp_location': '/tmp/beam-flink-temp',
        'save_main_session': True,
        'sdk_worker_parallelism': args.parallelism,
        'environment_type': 'LOOPBACK',  # Use EXTERNAL for production clusters
    }
    
    # Create and configure pipeline options
    options = PipelineOptions.from_dictionary(options_dict)
    
    # Configure standard options
    standard_options = options.view_as(StandardOptions)
    if args.streaming:
        standard_options.streaming = True
    
    # Configure Flink runner specific options
    flink_options = options.view_as(FlinkRunnerOptions)
    flink_options.flink_master = args.flink_master
    
    return options

def main():
    """Main entry point."""
    args = parse_args()
    
    logger.info("Creating Flink runner pipeline options")
    options = create_pipeline_options(args)
    
    logger.info(f"Running WordCount pipeline on Flink runner with master {args.flink_master}")
    run_wordcount(options, args.input, args.output)
    
    logger.info(f"Pipeline submitted to Flink. Output will be written to: {args.output}*")
    logger.info(f"View the Flink dashboard at {args.flink_master}")

if __name__ == '__main__':
    main() 