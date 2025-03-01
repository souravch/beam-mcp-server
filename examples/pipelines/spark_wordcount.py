#!/usr/bin/env python
"""
Apache Beam wordcount example for Spark runner.

This is a simple wordcount example designed to run on Apache Spark.
"""

import logging
import argparse
import re
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.options.pipeline_options import SparkRunnerOptions
from apache_beam.io import ReadFromText, WriteToText

def run(argv=None, save_main_session=True):
    """Main entry point; defines and runs the wordcount pipeline."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        default='/tmp/input.txt',
        help='Input file path')
    parser.add_argument(
        '--output',
        dest='output',
        required=True,
        help='Output file path')
    
    known_args, pipeline_args = parser.parse_known_args(argv)
    
    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    
    # Force the pipeline to use the SparkRunner
    pipeline_options.view_as(StandardOptions).runner = 'SparkRunner'
    
    # Create the pipeline
    with beam.Pipeline(options=pipeline_options) as p:
        # Read the text file into a PCollection.
        lines = p | 'Read' >> ReadFromText(known_args.input)
        
        # Count the occurrences of each word.
        counts = (
            lines
            | 'Split' >> beam.FlatMap(lambda x: re.findall(r'[A-Za-z\']+', x))
            | 'PairWithOne' >> beam.Map(lambda x: (x.lower(), 1))
            | 'GroupAndSum' >> beam.CombinePerKey(sum))
            
        # Format the results into a PCollection of strings.
        def format_result(word_count):
            (word, count) = word_count
            return '%s: %s' % (word, count)
            
        formatted_counts = counts | 'Format' >> beam.Map(format_result)
        
        # Write the output using a "Write" transform.
        formatted_counts | 'Write' >> WriteToText(known_args.output)
            
        # Actually run the pipeline (all operations above are deferred until run() is called).
        
if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()

def create_pipeline(pipeline_options):
    """Create and return the pipeline for external usage.
    
    This function is used by the MCP server to create the pipeline.
    """
    return beam.Pipeline(options=pipeline_options) 