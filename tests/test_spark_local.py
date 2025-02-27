"""
Test for executing WordCount using PySpark directly.

This test runs the WordCount job using PySpark directly or falls back to the Beam DirectRunner.
It will write the output to a location that can be manually checked.
"""

import os
import pytest
import logging
import uuid
import time
import subprocess
from pathlib import Path

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Get the absolute path to the example files
EXAMPLES_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src', 'examples'))
INPUT_FILE = os.path.join(EXAMPLES_DIR, 'sample_text.txt')
OUTPUT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'output'))

# Create output directory if it doesn't exist
os.makedirs(OUTPUT_DIR, exist_ok=True)

@pytest.mark.integration
def test_spark_wordcount_direct():
    """Test running a WordCount job with PySpark directly."""
    # Check if PySpark is available
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        pytest.skip("PySpark is not installed")
    
    # Generate a unique output path
    output_path = os.path.join(OUTPUT_DIR, f"test-pyspark-{uuid.uuid4().hex[:8]}")
    
    # Command to run the PySpark script
    cmd = [
        "python", 
        os.path.join(EXAMPLES_DIR, "run_wordcount_spark_direct.py"),
        "--input_file", INPUT_FILE,
        "--output_path", output_path
    ]
    
    # Run the command
    logger.info(f"Running: {' '.join(cmd)}")
    process = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=dict(os.environ, PYTHONPATH=".")
    )
    
    # Log the output
    logger.info(f"Exit code: {process.returncode}")
    logger.info(f"Output:")
    for line in process.stdout.split('\n'):
        if line.strip():
            logger.info(line)
    
    # Check if the process was successful
    assert process.returncode == 0, f"Process failed with exit code {process.returncode}:\n{process.stderr}"
    
    # Check for output files
    output_files = list(Path(os.path.dirname(output_path)).glob(f"{os.path.basename(output_path)}/part-*"))
    assert len(output_files) > 0, "No output files found"
    
    # Print the path to the output files for manual verification
    logger.info("Job completed successfully. Output files:")
    for file in output_files:
        logger.info(f"  - {file}")
        
        # Read and print a sample of the output for verification
        with open(file, 'r') as f:
            sample_lines = []
            for i in range(10):
                try:
                    line = next(f)
                    sample_lines.append(line)
                except StopIteration:
                    break
                    
            logger.info("Sample output (first 10 lines or less):")
            for line in sample_lines:
                logger.info(f"  {line.strip()}")
    
    logger.info(f"You can manually check the full output at: {output_path}")

@pytest.mark.integration
def test_direct_with_spark_fallback():
    """Test running a WordCount job using DirectRunner with Spark fallback."""
    # Generate a unique output path
    output_path = os.path.join(OUTPUT_DIR, f"test-spark-fallback-{uuid.uuid4().hex[:8]}")
    
    # Command to run the Spark script with DirectRunner fallback
    cmd = [
        "python", 
        os.path.join(EXAMPLES_DIR, "run_wordcount_spark.py"),
        "--input_file", INPUT_FILE,
        "--output_path", output_path
    ]
    
    # Run the command
    logger.info(f"Running: {' '.join(cmd)}")
    process = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=dict(os.environ, PYTHONPATH=".")
    )
    
    # Log the output
    logger.info(f"Exit code: {process.returncode}")
    logger.info(f"Output:")
    for line in process.stdout.split('\n'):
        if line.strip():
            logger.info(line)
    
    # Check if the process was successful
    assert process.returncode == 0, f"Process failed with exit code {process.returncode}:\n{process.stderr}"
    
    # Check for output files
    output_files = list(Path(os.path.dirname(output_path)).glob(f"{os.path.basename(output_path)}-*-of-*"))
    assert len(output_files) > 0, "No output files found"
    
    # Print the path to the output files for manual verification
    logger.info("Job completed successfully. Output files:")
    for file in output_files:
        logger.info(f"  - {file}")
        
        # Read and print a sample of the output for verification
        with open(file, 'r') as f:
            sample_lines = []
            for i in range(10):
                try:
                    line = next(f)
                    sample_lines.append(line)
                except StopIteration:
                    break
                    
            logger.info("Sample output (first 10 lines or less):")
            for line in sample_lines:
                logger.info(f"  {line.strip()}")
    
    logger.info(f"You can manually check the full output at: {output_path}")

if __name__ == "__main__":
    # This allows running the tests directly
    test_spark_wordcount_direct()
    test_direct_with_spark_fallback() 