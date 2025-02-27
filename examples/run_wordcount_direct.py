#!/usr/bin/env python3
"""
Run wordcount example on Direct runner.

This script demonstrates how to run the wordcount pipeline on the Direct runner.
"""

import logging
import os
import sys
import argparse
from typing import Dict, Any, List, Optional
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, DirectOptions

# Add the parent directory to the Python path so we can import the pipeline
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from examples.pipelines.wordcount import run as run_wordcount

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Run WordCount on Direct runner')
    parser.add_argument('--input', help='Input text file')
    parser.add_argument('--output', help='Output path prefix', default='direct_wordcount_output')
    parser.add_argument('--num_workers', type=int, default=4, help='Number of direct workers')
    parser.add_argument('--streaming', action='store_true', help='Run in streaming mode')
    
    args = parser.parse_args()
    return args

def create_pipeline_options(args) -> PipelineOptions:
    """Create the pipeline options for Direct runner."""
    options_dict = {
        'runner': 'DirectRunner',
        'direct_num_workers': args.num_workers,
        'direct_running_mode': 'multi_threading',
        'temp_location': '/tmp/beam-direct-temp',
        'save_main_session': True
    }
    
    # Create and configure pipeline options
    options = PipelineOptions.from_dictionary(options_dict)
    
    # Configure standard options
    standard_options = options.view_as(StandardOptions)
    if args.streaming:
        standard_options.streaming = True
    
    # Configure Direct runner specific options
    direct_options = options.view_as(DirectOptions)
    direct_options.direct_num_workers = args.num_workers
    
    return options

def main():
    """Main entry point."""
    args = parse_args()
    
    logger.info("Creating Direct runner pipeline options")
    options = create_pipeline_options(args)
    
    logger.info("Running WordCount pipeline on Direct runner")
    run_wordcount(options, args.input, args.output)
    
    logger.info(f"Output written to: {args.output}*")

if __name__ == '__main__':
    main() 