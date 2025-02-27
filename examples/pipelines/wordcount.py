"""
Example word count pipeline for testing the Direct runner.
"""

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from typing import List, Tuple

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
        return element.split()

def create_pipeline(pipeline_options: PipelineOptions) -> beam.Pipeline:
    """
    Creates a word count pipeline.
    
    Args:
        pipeline_options: The pipeline options
        
    Returns:
        The pipeline object
    """
    pipeline = beam.Pipeline(options=pipeline_options)
    
    lines = pipeline | 'Read' >> beam.Create([
        'To be, or not to be: that is the question:',
        'Whether tis nobler in the mind to suffer',
        'The slings and arrows of outrageous fortune,',
        'Or to take arms against a sea of troubles,'
    ])
    
    counts = (
        lines
        | 'Split' >> beam.ParDo(WordExtractingDoFn())
        | 'PairWithOne' >> beam.Map(lambda x: (x, 1))
        | 'GroupAndSum' >> beam.CombinePerKey(sum)
    )
    
    # Write results to a text file
    counts | 'Format' >> beam.Map(lambda kv: f'{kv[0]}: {kv[1]}') | 'Write' >> beam.io.WriteToText('wordcount_output')
    
    return pipeline

def run(pipeline_options: PipelineOptions) -> None:
    """
    Run the pipeline.
    
    Args:
        pipeline_options: The pipeline options
    """
    pipeline = create_pipeline(pipeline_options)
    pipeline.run().wait_until_finish() 