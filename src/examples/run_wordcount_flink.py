#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
WordCount example for Apache Beam on Flink.
This example reads a text file, counts the words, and writes the results to an output file.
It attempts to use Flink as the runner, but falls back to DirectRunner if Flink is not available.
"""

import argparse
import json
import logging
import os
import re
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import List, Optional, Dict, Any, Union
from urllib.parse import urlparse
import glob

import apache_beam as beam
from apache_beam.io import ReadFromText, WriteToText
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, SetupOptions, FlinkRunnerOptions
from apache_beam.runners.direct.direct_runner import DirectRunner

# Use direct import of wordcount to avoid module issues
from src.examples.wordcount import create_pipeline

# Timeout for Flink operations (in seconds)
FLINK_TIMEOUT = 20

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Run WordCount example on Flink')
    parser.add_argument('--input_file', required=True, help='Input file path')
    parser.add_argument('--output_path', required=True, help='Output path')
    parser.add_argument('--flink_master', default='http://localhost:8081', help='Flink master URL')
    parser.add_argument('--parallelism', type=int, default=1, help='Parallelism')
    parser.add_argument('--force_direct', action='store_true', help='Force DirectRunner instead of Flink')
    
    return parser.parse_known_args()


def check_flink_version(flink_master):
    """Check Flink cluster version."""
    try:
        import requests
        response = requests.get(f"{flink_master}/config", timeout=5)
        if response.status_code == 200:
            config = response.json()
            return config.get('flink-version', 'unknown')
        return 'unknown'
    except Exception as e:
        logger.warning(f"Failed to check Flink version: {e}")
        return 'unknown'


def check_flink_connectivity(flink_master):
    """Check if we can connect to the Flink cluster."""
    try:
        import requests
        response = requests.get(f"{flink_master}/overview", timeout=5)
        if response.status_code == 200:
            return True
        logger.warning(f"Flink connectivity check failed with status code: {response.status_code}")
        return False
    except Exception as e:
        logger.warning(f"Failed to connect to Flink cluster: {e}")
        return False


def run_pipeline(args):
    """Run the WordCount pipeline with Apache Beam and Flink Runner."""
    try:
        input_file = args.input_file
        output_path = args.output_path
        flink_master = args.flink_master
        parallelism = args.parallelism
        force_direct = args.force_direct
    except AttributeError as e:
        logger.error(f"Error extracting parameters: {e}")
        return False
    
    logger.info(f"Attempting to run WordCount on Flink at {flink_master}")
    logger.info(f"Reading from: {input_file}")
    logger.info(f"Writing to: {output_path}")
    
    # Check if Flink is running
    if not check_flink_connectivity(flink_master):
        logger.warning(f"Flink master at {flink_master} is not reachable. Falling back to DirectRunner.")
        return use_direct_runner(args)
    
    if force_direct:
        logger.info("DirectRunner forced via arguments.")
        return use_direct_runner(args)
    
    logger.info("Attempting to use Flink runner...")
    
    # Get Flink version for logging
    flink_version = check_flink_version(flink_master)
    logger.info(f"Using Flink {flink_version}")
    
    # Build the pipeline options as a list of command-line arguments
    pipeline_args = [
        f"--runner=FlinkRunner",
        f"--flink_master={flink_master}",
        f"--parallelism={parallelism}",
        f"--input_file={input_file}",
        f"--output_path={output_path}",
        # Use DOCKER environment type instead of EXTERNAL
        f"--environment_type=LOOPBACK",
        f"--save_main_session"
    ]
    
    logger.info(f"Using pipeline options: {pipeline_args}")
    logger.info("Creating and running pipeline with Flink runner")
    
    try:
        # Create the pipeline directly with Apache Beam
        options = PipelineOptions(pipeline_args)
        with beam.Pipeline(options=options) as p:
            (p 
             | 'ReadFromText' >> beam.io.ReadFromText(input_file)
             | 'SplitWords' >> beam.FlatMap(lambda x: re.findall(r'[A-Za-z\']+', x))
             | 'PairWithOne' >> beam.Map(lambda x: (x, 1))
             | 'GroupAndSum' >> beam.CombinePerKey(sum)
             | 'FormatOutput' >> beam.Map(lambda word_count: f"{word_count[0]}: {word_count[1]}")
             | 'WriteToText' >> beam.io.WriteToText(output_path)
            )
        
        # Check if output files were created
        output_files = glob.glob(f"{output_path}*")
        if output_files:
            logger.info(f"Flink job completed successfully. Found {len(output_files)} output file(s):")
            for file in output_files[:5]:  # Show first 5 output files
                logger.info(f"  - {file}")
                # Show a sample of the output
                try:
                    with open(file, 'r') as f:
                        sample = [next(f).strip() for _ in range(5) if f.readable()]
                        logger.info(f"  Sample output: {', '.join(sample)}")
                except (StopIteration, IOError) as e:
                    logger.warning(f"Could not read sample from {file}: {e}")
            return True
        else:
            logger.warning(f"No output files found at {output_path}*")
            return False
        
    except Exception as e:
        logger.error(f"Error running Flink pipeline: {str(e)}")
        logger.warning("Falling back to DirectRunner")
        return use_direct_runner(args)


def use_direct_runner(args):
    """Run with DirectRunner."""
    logger.info("Using DirectRunner instead of Flink...")
    
    try:
        input_file = args.input_file
        output_path = args.output_path
        parallelism = args.parallelism
    except AttributeError as e:
        logger.error(f"Error extracting parameters for DirectRunner: {e}")
        return False
    
    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Build Direct Runner pipeline args
    direct_pipeline_args = [
        '--runner=DirectRunner',
        f'--parallelism={parallelism}',
        '--save_main_session',
        f'--input_file={input_file}',
        f'--output_path={output_path}'
    ]
    logger.info(f"Direct runner pipeline arguments: {direct_pipeline_args}")
    
    # Run with Direct Runner
    logger.info("Using DirectRunner for WordCount")
    logger.info(f"Input file: {input_file}")
    logger.info(f"Output path: {output_path}")
    
    try:
        # Create the pipeline directly with Apache Beam
        options = PipelineOptions(direct_pipeline_args)
        with beam.Pipeline(options=options) as p:
            (p 
             | 'ReadFromText' >> beam.io.ReadFromText(input_file)
             | 'SplitWords' >> beam.FlatMap(lambda x: re.findall(r'[A-Za-z\']+', x))
             | 'PairWithOne' >> beam.Map(lambda x: (x, 1))
             | 'GroupAndSum' >> beam.CombinePerKey(sum)
             | 'FormatOutput' >> beam.Map(lambda word_count: f"{word_count[0]}: {word_count[1]}")
             | 'WriteToText' >> beam.io.WriteToText(output_path)
            )
        
        # Check if output files were created
        output_files = glob.glob(f"{output_path}*")
        if output_files:
            logger.info(f"DirectRunner pipeline completed successfully")
            logger.info(f"Found {len(output_files)} output files")
            for file in output_files[:5]:
                logger.info(f"Output file: {file}")
                # Show a sample of the output
                try:
                    with open(file, 'r') as f:
                        sample = [next(f).strip() for _ in range(5) if f.readable()]
                        logger.info(f"Sample output: {', '.join(sample)}")
                except (StopIteration, IOError) as e:
                    logger.warning(f"Could not read sample from {file}: {e}")
            return True
        else:
            logger.warning(f"No output files found after DirectRunner job")
            return False
    
    except Exception as e:
        logger.error(f"Error running DirectRunner pipeline: {str(e)}")
        return False


def extract_flink_job_id(output):
    """Extract the Flink job ID from command output."""
    match = re.search(r"Started Flink job as ([a-f0-9]+)", output)
    if match:
        return match.group(1)
    return None


def check_job_status(flink_master, job_id):
    """Check the status of a Flink job."""
    try:
        import requests
        response = requests.get(f"{flink_master}/jobs/{job_id}", timeout=5)
        if response.status_code == 200:
            job_info = response.json()
            logger.info(f"Job status: {job_info.get('state', 'UNKNOWN')}")
            return job_info
        logger.warning(f"Failed to get job status: HTTP {response.status_code}")
    except Exception as e:
        logger.warning(f"Error checking job status: {e}")
    return None


def create_temp_process_script(input_file, output_path):
    """
    Create a temporary script that can be used for process_command
    """
    with tempfile.NamedTemporaryFile(suffix='.py', mode='w', delete=False) as f:
        # Write a more complete and standalone script that can run in a separate process
        f.write(f"""#!/usr/bin/env python3
import sys
import re
import logging
import os
import apache_beam as beam
from apache_beam.io import ReadFromText, WriteToText
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions

# Set up basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Hardcoded paths to avoid passing arguments
INPUT_FILE = "{input_file}"
OUTPUT_PATH = "{output_path}"

def run():
    logger.info(f"Process script starting: reading from {{INPUT_FILE}}, writing to {{OUTPUT_PATH}}")
    
    # Create output directory if it doesn't exist
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    
    # Set up pipeline options
    options = PipelineOptions()
    options.view_as(SetupOptions).save_main_session = True
    
    # Create and run pipeline
    with beam.Pipeline(options=options) as p:
        (p 
         | "ReadInputFile" >> ReadFromText(INPUT_FILE)
         | "SplitWords" >> beam.FlatMap(lambda line: re.findall(r'\\w+', line.lower()))
         | "PairWithOne" >> beam.Map(lambda word: (word, 1))
         | "GroupAndSum" >> beam.CombinePerKey(sum)
         | "FormatOutput" >> beam.Map(lambda word_count: f'{{word_count[0]}}: {{word_count[1]}}')
         | "WriteOutput" >> WriteToText(OUTPUT_PATH)
        )
    
    logger.info("Pipeline execution completed")
    
    # Verify output exists
    output_dir = os.path.dirname(OUTPUT_PATH)
    output_base = os.path.basename(OUTPUT_PATH)
    output_files = [f for f in os.listdir(output_dir) if f.startswith(output_base)]
    
    if output_files:
        logger.info(f"Successfully wrote {{len(output_files)}} output files")
        return True
    else:
        logger.warning("No output files found!")
        return False

if __name__ == "__main__":
    logger.info("Process script called. Starting execution.")
    success = run()
    logger.info(f"Process completed with success={{success}}")
    sys.exit(0 if success else 1)
""")
    
    # Make the script executable
    os.chmod(f.name, 0o755)
    logger.info(f"Created temporary process script at {f.name}")
    return f.name


if __name__ == "__main__":
    args, remaining_args = parse_arguments()
    # Add remaining_args as an attribute to args object for consistency
    args.remaining_args = remaining_args
    run_pipeline(args) 