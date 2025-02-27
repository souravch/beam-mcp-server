#!/usr/bin/env python
"""
Script to run the WordCount example using PySpark directly.

This approach uses PySpark directly rather than Apache Beam's SparkRunner,
which is useful for development and testing without the complexity of the Beam Spark runner.
"""
import argparse
import logging
import os
import sys
import socket
import time
import json
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

try:
    from pyspark.sql import SparkSession
    import pyspark.sql.functions as F
except ImportError:
    logging.error("PySpark is not installed. Please install it with: pip install pyspark==3.3.0")
    sys.exit(1)

def check_spark_connectivity(spark_master):
    """Check if we can connect to the Spark master."""
    logging.info(f"Checking connectivity to Spark master: {spark_master}")
    
    # For local mode, no need to check connectivity
    if spark_master.startswith("local"):
        logging.info("Using local Spark mode, no connectivity check needed")
        return True
    
    # Parse the Spark master URL
    if spark_master.startswith("spark://"):
        # Format: spark://host:port
        parts = spark_master.replace("spark://", "").split(":")
        if len(parts) != 2:
            logging.error(f"Invalid Spark master URL format: {spark_master}")
            logging.error("Expected format: spark://host:port")
            return False
        
        host, port_str = parts
        try:
            port = int(port_str)
        except ValueError:
            logging.error(f"Invalid port in Spark master URL: {port_str}")
            return False
        
        # Try to connect to the Spark master UI (usually on port 8080 or port+1)
        try:
            # Try the Web UI port first
            ui_port = 8080  # Default Spark UI port
            url = f"http://{host}:{ui_port}"
            logging.info(f"Attempting to connect to Spark UI at {url}")
            
            req = Request(url)
            response = urlopen(req, timeout=5)
            if response.getcode() == 200:
                logging.info(f"Successfully connected to Spark UI at {url}")
                return True
            else:
                logging.error(f"Failed to connect to Spark UI: HTTP {response.getcode()}")
        except (URLError, HTTPError, socket.error) as e:
            logging.warning(f"Could not connect to Spark UI at {url}: {e}")
            
            # Try direct socket connection to the master port
            try:
                logging.info(f"Attempting direct socket connection to Spark master at {host}:{port}")
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(5)
                s.connect((host, port))
                s.close()
                logging.info(f"Successfully connected to Spark master at {host}:{port}")
                return True
            except socket.error as e:
                logging.error(f"Failed to connect to Spark master: {e}")
                return False
    
    # Default to True for other modes
    return True

def run_wordcount_pyspark(input_file, output_path, spark_master="local[*]"):
    """
    Run the WordCount example using PySpark directly.
    
    Args:
        input_file: Path to the input text file
        output_path: Path where output files will be written
        spark_master: Spark master URL (default: local[*])
    """
    logging.info(f"Running WordCount with PySpark")
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
    
    # Check Spark connectivity before attempting to create a session
    if not check_spark_connectivity(spark_master):
        logging.warning("Could not connect to Spark master, but will attempt to create SparkSession anyway.")
    
    # Create a Spark session with the specified master
    try:
        logging.info(f"Creating SparkSession with master: {spark_master}")
        builder = SparkSession.builder \
            .appName("PySpark WordCount") \
            .master(spark_master) \
            .config("spark.executor.memory", "1g") \
            .config("spark.driver.memory", "1g") \
            .config("spark.driver.bindAddress", "0.0.0.0") \
            .config("spark.network.timeout", "600s") \
            .config("spark.executor.heartbeatInterval", "120s") \
            .config("spark.network.maxFrameSizeBytes", "200000000") \
            .config("spark.maxRemoteBlockSizeFetchToMem", "200000000") \
            .config("spark.driver.maxResultSize", "1g")
            
        # Add configuration to handle the "Too large frame" error
        if not spark_master.startswith("local"):
            builder = builder.config("spark.rpc.message.maxSize", "256") \
                .config("spark.kryoserializer.buffer.max", "256m") \
                .config("spark.driver.port", "0") \
                .config("spark.blockManager.port", "0") \
                .config("spark.port.maxRetries", "100") \
                .config("spark.shuffle.io.maxRetries", "10") \
                .config("spark.shuffle.io.retryWait", "60s")
        
        spark = builder.getOrCreate()
        
        # Log Spark configuration for debugging
        spark_version = spark.version
        logging.info(f"Connected to Spark version: {spark_version}")
        logging.info(f"Spark UI available at: {spark.sparkContext.uiWebUrl}")
        
        # Additional debug info
        logging.info(f"Application ID: {spark.sparkContext.applicationId}")
        logging.info(f"Default parallelism: {spark.sparkContext.defaultParallelism}")
        
        # Read the input file
        logging.info(f"Reading text file: {input_file}")
        lines = spark.read.text(input_file)
        logging.info(f"Read {lines.count()} lines from input file")
        
        # Split lines into words
        logging.info("Splitting lines into words")
        words = lines.select(
            F.explode(
                F.split(F.col("value"), "\\s+")
            ).alias("word")
        )
        
        # Filter out empty words and clean
        logging.info("Filtering and cleaning words")
        clean_words = words.filter(F.length("word") > 0) \
            .select(F.regexp_replace(F.col("word"), "[^a-zA-Z0-9']", "").alias("word")) \
            .filter(F.length("word") > 0)
        
        # Count words
        logging.info("Counting words")
        word_counts = clean_words.groupBy("word").count()
        
        # Convert to a format similar to Beam output
        logging.info("Formatting results")
        formatted_counts = word_counts.select(
            F.concat_ws(": ", F.col("word"), F.col("count").cast("string")).alias("result")
        )
        
        # Save the output
        logging.info(f"Writing results to: {output_path}")
        formatted_counts.write.mode("overwrite").text(output_path)
        
        logging.info("WordCount completed successfully!")
        
        # Show the output location
        logging.info(f"Results written to: {output_path}")
        
        # Show some sample results
        logging.info("Sample results:")
        # Collect top words and their counts
        top_words = word_counts.orderBy(F.desc("count")).limit(10).collect()
        for row in top_words:
            logging.info(f"  {row['word']}: {row['count']}")
        
        # If we reach here, the job was successful
        return True
        
    except Exception as e:
        logging.error(f"Error running the PySpark WordCount: {str(e)}")
        logging.error("Stack trace:", exc_info=True)
        
        # Fallback to local mode if we were trying to use a cluster
        if spark_master != "local[*]" and not spark_master.startswith("local["):
            logging.warning("Attempting to fall back to local Spark mode...")
            try:
                logging.info("*** FALLING BACK TO LOCAL SPARK MODE ***")
                return run_wordcount_pyspark(input_file, output_path, "local[*]")
            except Exception as e:
                logging.error(f"Fallback to local mode also failed: {str(e)}")
                return False
        
        return False
    finally:
        # Make sure to stop the Spark session if it exists
        try:
            if 'spark' in locals():
                logging.info("Stopping SparkSession")
                spark.stop()
        except Exception as e:
            logging.error(f"Error stopping SparkSession: {str(e)}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run WordCount example with PySpark")
    parser.add_argument("--input_file", required=True, help="Path to the input text file")
    parser.add_argument("--output_path", required=True, help="Path where output files will be written")
    parser.add_argument("--spark_master", default="local[*]", 
                        help="Spark master URL (default: local[*], use spark://host:port for cluster)")
    
    args = parser.parse_args()
    
    # Run the WordCount job
    success = run_wordcount_pyspark(
        input_file=args.input_file,
        output_path=args.output_path,
        spark_master=args.spark_master
    )
    
    # Exit with appropriate status code
    sys.exit(0 if success else 1) 