#!/usr/bin/env python3
"""
Run wordcount example on Spark runner.

This script demonstrates how to run the wordcount pipeline on the Apache Spark runner.
"""

import logging
import os
import sys
import argparse
from typing import Dict, Any, List, Optional
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

# Add the parent directory to the Python path so we can import the pipeline
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from examples.pipelines.wordcount import run as run_wordcount

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Run WordCount on Spark runner')
    parser.add_argument('--input', help='Input text file')
    parser.add_argument('--output', help='Output path prefix', default='spark_wordcount_output')
    parser.add_argument('--spark_master', default='local[*]', 
                      help='Spark master URL (e.g., local[*], spark://host:port)')
    parser.add_argument('--num_executors', type=int, default=2, help='Number of executors')
    parser.add_argument('--executor_cores', type=int, default=2, help='Number of cores per executor')
    parser.add_argument('--executor_memory', default='1g', help='Executor memory')
    parser.add_argument('--driver_memory', default='1g', help='Driver memory')
    parser.add_argument('--streaming', action='store_true', help='Run in streaming mode')
    
    args = parser.parse_args()
    return args

def create_pipeline_options(args) -> PipelineOptions:
    """Create the pipeline options for Spark runner."""
    options_dict = {
        'runner': 'SparkRunner',
        'spark_master': args.spark_master,
        'spark_job_name': 'beam-wordcount-spark',
        'spark_executor_instances': args.num_executors,
        'spark_executor_cores': args.executor_cores,
        'spark_executor_memory': args.executor_memory,
        'spark_driver_memory': args.driver_memory,
        'temp_location': '/tmp/beam-spark-temp',
        'save_main_session': True,
        # If using an external cluster, you may need these options:
        # 'spark_app_name': 'beam-wordcount-spark',
        # 'spark_submit_packages': 'org.apache.beam:beam-runners-spark-3:2.50.0',
        # 'spark_conf': {"spark.yarn.submit.waitAppCompletion": "false"},
    }
    
    # Create and configure pipeline options
    options = PipelineOptions.from_dictionary(options_dict)
    
    # Configure standard options
    standard_options = options.view_as(StandardOptions)
    if args.streaming:
        standard_options.streaming = True
    
    return options

def main():
    """Main entry point."""
    args = parse_args()
    
    logger.info("Creating Spark runner pipeline options")
    options = create_pipeline_options(args)
    
    logger.info(f"Running WordCount pipeline on Spark runner with master {args.spark_master}")
    run_wordcount(options, args.input, args.output)
    
    logger.info(f"Pipeline submitted to Spark. Output will be written to: {args.output}*")
    
    # If using local mode, provide information on where to find results
    if args.spark_master.startswith('local'):
        logger.info("Since you're using local mode, you can find the output files in the current directory")
    
    # If using Spark UI, provide information on how to access it
    if not args.spark_master.startswith('local'):
        logger.info("You can monitor the Spark job on the Spark UI at http://localhost:4040")

if __name__ == '__main__':
    main() 