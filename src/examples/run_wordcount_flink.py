#!/usr/bin/env python
"""
Script to run the WordCount example directly on a Flink cluster.
"""
import argparse
import logging
import os
import sys
from pathlib import Path

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from src.examples.wordcount import create_pipeline

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


def run_wordcount_on_flink(input_file, output_path, flink_master="http://localhost:8081", parallelism=1):
    """
    Run the WordCount example directly on a Flink cluster.
    
    Args:
        input_file: Path to the input text file
        output_path: Path where output files will be written
        flink_master: URL of the Flink JobManager (default: http://localhost:8081)
        parallelism: Parallelism for the Flink job (default: 1)
    """
    logging.info(f"Running WordCount on Flink at {flink_master}")
    logging.info(f"Reading from: {input_file}")
    logging.info(f"Writing to: {output_path}")
    logging.info(f"Using installed Flink 1.15.0")
    logging.info(f"Running without uber jar")
    
    # Check if input file exists
    if not os.path.exists(input_file):
        logging.error(f"Input file does not exist: {input_file}")
        sys.exit(1)
    
    # Create output directory if it doesn't exist
    output_dir = os.path.dirname(output_path)
    os.makedirs(output_dir, exist_ok=True)
    
    # Create a dictionary of options
    options_dict = {
        'runner': 'FlinkRunner',
        'flink_master': flink_master,
        'parallelism': parallelism,
        'flink_version': '1.15',  # Specify the version to match your local Flink
        'flink_submit_uber_jar': False,  # Run without the uber jar
        'input_file': input_file,
        'output_path': output_path,
        'streaming': False,
        'sdk_worker_parallelism': parallelism,
        'environment_type': 'PROCESS'  # Use PROCESS environment for direct execution
    }
    
    # Convert to args list for pipeline
    pipeline_args = []
    for k, v in options_dict.items():
        if isinstance(v, bool):
            if v:
                pipeline_args.append(f'--{k}')
        else:
            pipeline_args.append(f'--{k}={v}')
    
    logging.info(f"Pipeline arguments: {pipeline_args}")
    
    try:
        # Create the pipeline options
        options = PipelineOptions(pipeline_args)
        
        # Pass the options to create_pipeline
        pipeline = create_pipeline(options)
        result = pipeline.run()
        logging.info("Pipeline submitted!")
        logging.info("Waiting for pipeline to finish...")
        result.wait_until_finish()
        logging.info("Pipeline completed!")
    except Exception as e:
        logging.error(f"Error running the pipeline: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run WordCount example on Flink")
    parser.add_argument("--input_file", required=True, help="Path to the input text file")
    parser.add_argument("--output_path", required=True, help="Path where output files will be written")
    parser.add_argument("--flink_master", default="http://localhost:8081", help="URL of the Flink JobManager")
    parser.add_argument("--parallelism", type=int, default=1, help="Parallelism for the Flink job")
    
    args = parser.parse_args()
    
    run_wordcount_on_flink(
        input_file=args.input_file,
        output_path=args.output_path,
        flink_master=args.flink_master,
        parallelism=args.parallelism
    ) 