#!/usr/bin/env python
"""
Script to run the WordCount example on a Spark cluster.

This script runs the WordCount example using the Spark runner
with support for both local and cluster Spark deployments.
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

# Define a custom PipelineOptions class to handle our specific arguments
class WordCountSparkOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        # Add custom arguments that will be properly recognized
        parser.add_argument('--input_file', required=True, help='Path to the input text file')
        parser.add_argument('--output_path', required=True, help='Path where output files will be written')
        parser.add_argument('--spark_master', default='local[*]', help='Spark master URL')


def run_wordcount_spark(input_file, output_path, spark_master="local[*]"):
    """
    Run the WordCount example with the Spark runner.
    
    Args:
        input_file: Path to the input text file
        output_path: Path where output files will be written
        spark_master: Spark master URL (default: local[*] for local mode)
    """
    logging.info(f"Running WordCount with Spark runner")
    logging.info(f"Spark master: {spark_master}")
    logging.info(f"Reading from: {input_file}")
    logging.info(f"Writing to: {output_path}")
    
    # Check if input file exists
    if not os.path.exists(input_file):
        logging.error(f"Input file does not exist: {input_file}")
        sys.exit(1)
    
    # Create output directory if it doesn't exist
    output_dir = os.path.dirname(output_path)
    os.makedirs(output_dir, exist_ok=True)
    
    # Create pipeline options
    pipeline_args = [
        "--runner=SparkRunner",
        f"--spark_master_url={spark_master}",
        "--spark_submit_uber_jar",  # Use uber jar for easier dependency management
        "--save_main_session=True",  # This helps with pickle serialization
        f"--input_file={input_file}",
        f"--output_path={output_path}"
    ]
    
    logging.info(f"Pipeline arguments: {pipeline_args}")
    
    # Use our custom options class to parse arguments
    options = WordCountSparkOptions(pipeline_args)
    
    try:
        # Create and run the pipeline
        pipeline = create_pipeline(options)
        result = pipeline.run()
        logging.info("Pipeline submitted!")
        logging.info("Waiting for pipeline to finish...")
        result.wait_until_finish()
        logging.info("Pipeline completed!")
        
        # Show the output file path
        output_files = list(Path(output_dir).glob(f"{os.path.basename(output_path)}-*-of-*"))
        if output_files:
            logging.info(f"Output written to: {', '.join(str(f) for f in output_files)}")
        else:
            logging.info(f"Check output directory: {output_dir}")
    except Exception as e:
        logging.error(f"Error running the pipeline: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run WordCount example with Spark runner")
    parser.add_argument("--input_file", required=True, help="Path to the input text file")
    parser.add_argument("--output_path", required=True, help="Path where output files will be written")
    parser.add_argument("--spark_master", default="local[*]", help="Spark master URL (default: local[*])")
    
    args = parser.parse_args()
    
    run_wordcount_spark(
        input_file=args.input_file,
        output_path=args.output_path,
        spark_master=args.spark_master
    ) 