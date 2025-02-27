#!/usr/bin/env python
"""
Apache Beam WordCount example for Flink runner.
This example reads a text file, counts the occurrences of each word, and writes results to output files.
"""

import argparse
import logging
import re
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


class WordExtractingDoFn(beam.DoFn):
    """Parse each line of input text into words."""
    def process(self, element):
        """Returns an iterator over the words of this element.
        
        The element is a line of text. If the line is blank, no words are yielded.
        """
        return re.findall(r'[\w\']+', element, re.UNICODE)


def create_pipeline(pipeline_options):
    """Create and return the WordCount pipeline."""
    # Try to extract input and output paths from options
    input_file = None
    output_path = None
    
    # First, check if the options object has the attributes directly
    if hasattr(pipeline_options, 'input_file') and pipeline_options.input_file:
        input_file = pipeline_options.input_file
    
    if hasattr(pipeline_options, 'output_path') and pipeline_options.output_path:
        output_path = pipeline_options.output_path
    
    # If not found as direct attributes, try the options dictionary
    if input_file is None or output_path is None:
        options_dict = pipeline_options.get_all_options()
        if input_file is None:
            input_file = options_dict.get('input_file')
        if output_path is None:
            output_path = options_dict.get('output_path')
    
    # If still not found, try custom _beam_options container
    if (input_file is None or output_path is None) and hasattr(pipeline_options, '_beam_options'):
        beam_options = pipeline_options._beam_options
        if input_file is None:
            input_file = beam_options.get('input_file')
        if output_path is None:
            output_path = beam_options.get('output_path')
    
    # If still not found, try command line args as last resort
    if input_file is None or output_path is None:
        import argparse
        parser = argparse.ArgumentParser()
        parser.add_argument('--input_file', required=True)
        parser.add_argument('--output_path', required=True)
        
        if hasattr(pipeline_options, '_flags'):
            all_args = pipeline_options._flags
            try:
                known_args, _ = parser.parse_known_args(all_args)
                if input_file is None:
                    input_file = known_args.input_file
                if output_path is None:
                    output_path = known_args.output_path
            except Exception as e:
                logging.warning(f"Failed to parse arguments: {str(e)}")
    
    # Make sure we found the paths
    if input_file is None:
        raise ValueError("Input file path not provided in options")
    if output_path is None:
        raise ValueError("Output path not provided in options")
    
    logging.info(f"Using input file: {input_file}")
    logging.info(f"Using output path: {output_path}")
    
    # Create the pipeline
    p = beam.Pipeline(options=pipeline_options)
    
    lines = p | 'ReadInputFile' >> beam.io.ReadFromText(input_file)
    
    counts = (
        lines
        | 'ExtractWords' >> beam.ParDo(WordExtractingDoFn())
        | 'PairWithOne' >> beam.Map(lambda x: (x, 1))
        | 'GroupAndSum' >> beam.CombinePerKey(sum)
    )
    
    # Format the counts into a PCollection of strings
    formatted_counts = counts | 'Format' >> beam.Map(
        lambda word_count: f'{word_count[0]}: {word_count[1]}')
    
    # Write the formatted counts to the output file
    formatted_counts | 'WriteOutput' >> beam.io.WriteToText(
        output_path, file_name_suffix='.txt')
    
    return p


if __name__ == '__main__':
    # Set up command line options
    parser = argparse.ArgumentParser(description='Run a WordCount pipeline.')
    parser.add_argument('--input_file', required=True, help='Input file to process.')
    parser.add_argument('--output_path', required=True, help='Output path for results.')
    
    # Parse arguments from the command line
    known_args, pipeline_args = parser.parse_known_args()
    
    # Define the pipeline options
    options = PipelineOptions(pipeline_args)
    
    # Add the known arguments to the pipeline options
    options.input_file = known_args.input_file
    options.output_path = known_args.output_path
    
    # Set up logging
    logging.getLogger().setLevel(logging.INFO)
    
    # Create and run the pipeline
    pipeline = create_pipeline(options)
    pipeline.run().wait_until_finish() 