"""
Example word count pipeline for testing multiple runners.

This pipeline can be run on Direct, Flink, Spark, and Dataflow runners.
"""

import logging
import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from typing import List, Tuple, Dict, Any, Optional

logger = logging.getLogger(__name__)

class WordExtractingDoFn(beam.DoFn):
    """Parse each line of input text into words."""
    
    def process(self, element: str) -> List[str]:
        """
        Returns an iterator over the words of this element.
        
        Args:
            element: The element being processed
            
        Returns:
            An iterator over the words in this line.
        """
        import re
        # Use regular expression to split words and remove punctuation
        words = re.findall(r'[A-Za-z\']+', element)
        return [word.lower() for word in words if word]

def get_sample_text() -> List[str]:
    """
    Returns sample text for the wordcount pipeline when no input is provided.
    
    Returns:
        List[str]: Sample text lines
    """
    return [
        'To be, or not to be: that is the question:',
        'Whether tis nobler in the mind to suffer',
        'The slings and arrows of outrageous fortune,',
        'Or to take arms against a sea of troubles,',
        'And by opposing end them. To die: to sleep;',
        'No more; and by a sleep to say we end',
        'The heart-ache and the thousand natural shocks',
        'That flesh is heir to, tis a consummation',
        'Devoutly to be wishd. To die, to sleep;'
    ]

def create_pipeline(pipeline_options: PipelineOptions, input_text: Optional[str] = None, output_path: Optional[str] = None) -> beam.Pipeline:
    """
    Creates a word count pipeline.
    
    Args:
        pipeline_options: The pipeline options
        input_text: Path to input text file (optional)
        output_path: Path to output directory (optional)
        
    Returns:
        The pipeline object
    """
    # Ensure output path has a default value
    output_path = output_path or 'wordcount_output'
    
    # Create the pipeline
    pipeline = beam.Pipeline(options=pipeline_options)
    
    # Get the input data
    if input_text:
        logger.info(f"Reading text from: {input_text}")
        lines = pipeline | 'Read' >> beam.io.ReadFromText(input_text)
    else:
        logger.info("Using sample text")
        lines = pipeline | 'Create' >> beam.Create(get_sample_text())
    
    # Process the data
    counts = (
        lines
        | 'Split' >> beam.ParDo(WordExtractingDoFn())
        | 'PairWithOne' >> beam.Map(lambda x: (x, 1))
        | 'GroupAndSum' >> beam.CombinePerKey(sum)
    )
    
    # Format and write results
    output = counts | 'Format' >> beam.Map(lambda kv: f'{kv[0]}: {kv[1]}')
    
    # Write results to a text file
    output | 'Write' >> beam.io.WriteToText(output_path)
    
    return pipeline

def run(pipeline_options: PipelineOptions, input_text: Optional[str] = None, output_path: Optional[str] = None) -> None:
    """
    Run the pipeline.
    
    Args:
        pipeline_options: The pipeline options
        input_text: Path to input text file (optional)
        output_path: Path to output directory (optional)
    """
    # Configure logging
    logging.basicConfig(level=logging.INFO)
    
    # Get pipeline runner from options
    runner = pipeline_options.get_all_options().get('runner', 'DirectRunner')
    logger.info(f"Running pipeline with {runner}")
    
    # Create and run the pipeline
    pipeline = create_pipeline(pipeline_options, input_text, output_path)
    pipeline_result = pipeline.run()
    
    # For DirectRunner, wait until completion
    if runner == 'DirectRunner':
        logger.info("Waiting for pipeline to finish...")
        pipeline_result.wait_until_finish()
        logger.info("Pipeline finished successfully")
    
    # For other runners, log the job ID
    else:
        try:
            job_id = pipeline_result.job_id()
            logger.info(f"Job submitted with ID: {job_id}")
        except:
            logger.info("Job submitted successfully")

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Word Count Pipeline')
    parser.add_argument('--input', help='Input text file')
    parser.add_argument('--output', help='Output path')
    parser.add_argument('--streaming', action='store_true', help='Run in streaming mode')
    
    # Parse known args and pass the rest to PipelineOptions
    known_args, pipeline_args = parser.parse_known_args()
    
    # Create pipeline options from remaining args
    options = PipelineOptions(pipeline_args)
    
    # Set streaming option if requested
    if known_args.streaming:
        options.view_as(StandardOptions).streaming = True
    
    return known_args, options

if __name__ == '__main__':
    known_args, pipeline_options = parse_args()
    run(pipeline_options, known_args.input, known_args.output) 