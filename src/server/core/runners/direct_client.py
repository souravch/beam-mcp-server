"""
Direct runner client for Apache Beam MCP server.

This module provides a client implementation for the Direct runner,
which is used for local development and testing.
"""

import logging
import uuid
from typing import Dict, Any, List, Optional
from datetime import datetime
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

from ...models import (
    JobParameters, JobInfo, JobState, JobType,
    SavepointParameters, SavepointInfo,
    JobMetrics, LogEntry
)
from .base_client import BaseRunnerClient

logger = logging.getLogger(__name__)

class DirectClient(BaseRunnerClient):
    """Client for Apache Beam Direct runner."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the Direct runner client.
        
        Args:
            config (Dict[str, Any]): Configuration for the Direct runner
        """
        super().__init__()
        self.config = config
        self.jobs = {}  # In-memory job storage
        self.pipeline_results = {}  # Store pipeline results
        
    async def create_job(self, params: JobParameters) -> JobInfo:
        """
        Create and run a job using the Direct runner.
        
        Args:
            params (JobParameters): Job parameters
            
        Returns:
            JobInfo: Information about the created job
        """
        job_id = str(uuid.uuid4())
        
        try:
            # Prepare pipeline options
            options = {
                'runner': 'DirectRunner',
                'direct_num_workers': self.config.get('options', {}).get('direct_num_workers', 1),
                'direct_running_mode': self.config.get('options', {}).get('direct_running_mode', 'in_memory'),
                **params.pipeline_options
            }
            
            pipeline_options = PipelineOptions.from_dictionary(options)
            if params.job_type == JobType.STREAMING:
                pipeline_options.view_as(StandardOptions).streaming = True
            
            # Create and run pipeline
            if params.template_path:
                # Load from template
                pipeline = beam.Pipeline.from_template(
                    params.template_path,
                    parameters=params.template_parameters or {},
                    options=pipeline_options
                )
            else:
                # Load from code path
                pipeline = self._load_pipeline_from_code(
                    params.code_path,
                    pipeline_options
                )
            
            # Run pipeline in background
            result = pipeline.run()
            self.pipeline_results[job_id] = result
            
            # Create job info
            job_info = JobInfo(
                job_id=job_id,
                name=params.job_name,
                state=JobState.RUNNING,
                runner_type='direct',
                creation_time=datetime.utcnow(),
                pipeline_options=options,
                template_path=params.template_path,
                code_path=params.code_path
            )
            
            self.jobs[job_id] = job_info
            return job_info
            
        except Exception as e:
            logger.error(f"Failed to create Direct runner job: {str(e)}")
            raise
    
    async def get_job(self, job_id: str) -> Optional[JobInfo]:
        """
        Get information about a job.
        
        Args:
            job_id (str): Job ID
            
        Returns:
            Optional[JobInfo]: Job information if found
        """
        job = self.jobs.get(job_id)
        if not job:
            return None
            
        # Update job state from pipeline result
        result = self.pipeline_results.get(job_id)
        if result:
            try:
                state = result.state
                if state in [beam.runners.runner.PipelineState.DONE]:
                    job.state = JobState.SUCCEEDED
                elif state in [beam.runners.runner.PipelineState.FAILED]:
                    job.state = JobState.FAILED
                elif state in [beam.runners.runner.PipelineState.CANCELLED]:
                    job.state = JobState.CANCELLED
                elif state in [beam.runners.runner.PipelineState.RUNNING]:
                    job.state = JobState.RUNNING
            except:
                pass
                
        return job
    
    async def list_jobs(self) -> List[JobInfo]:
        """
        List all jobs.
        
        Returns:
            List[JobInfo]: List of jobs
        """
        return list(self.jobs.values())
    
    async def cancel_job(self, job_id: str) -> bool:
        """
        Cancel a running job.
        
        Args:
            job_id (str): Job ID
            
        Returns:
            bool: True if cancelled successfully
        """
        job = self.jobs.get(job_id)
        if not job:
            return False
            
        result = self.pipeline_results.get(job_id)
        if result:
            try:
                result.cancel()
                job.state = JobState.CANCELLED
                return True
            except:
                return False
        return False
    
    async def get_metrics(self, job_id: str) -> Optional[JobMetrics]:
        """
        Get metrics for a job.
        
        Args:
            job_id (str): Job ID
            
        Returns:
            Optional[JobMetrics]: Job metrics if available
        """
        result = self.pipeline_results.get(job_id)
        if not result:
            return None
            
        try:
            metrics = result.metrics()
            return JobMetrics(
                job_id=job_id,
                metrics={
                    'counters': metrics.query(beam.metrics.MetricsFilter().with_name('.*'))['counters'],
                    'distributions': metrics.query(beam.metrics.MetricsFilter().with_name('.*'))['distributions'],
                    'gauges': metrics.query(beam.metrics.MetricsFilter().with_name('.*'))['gauges']
                },
                timestamp=datetime.utcnow()
            )
        except:
            return None
    
    async def get_logs(self, job_id: str, start_time: Optional[datetime] = None) -> List[LogEntry]:
        """
        Get logs for a job.
        
        Args:
            job_id (str): Job ID
            start_time (datetime, optional): Start time for logs
            
        Returns:
            List[LogEntry]: List of log entries
        """
        # Direct runner doesn't support log retrieval
        # In a real implementation, we could capture logs during pipeline execution
        return []
    
    def _load_pipeline_from_code(self, code_path: str, options: PipelineOptions):
        """
        Load a pipeline from Python code.
        
        Args:
            code_path (str): Path to pipeline code
            options (PipelineOptions): Pipeline options
            
        Returns:
            Pipeline: Beam pipeline
        """
        try:
            # Import pipeline module
            import importlib.util
            spec = importlib.util.spec_from_file_location("pipeline", code_path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            
            # Create pipeline
            if hasattr(module, 'create_pipeline'):
                return module.create_pipeline(options)
            elif hasattr(module, 'run'):
                return module.run(options)
            else:
                raise ValueError(f"No pipeline creation function found in {code_path}")
        except Exception as e:
            logger.error(f"Failed to load pipeline from code: {str(e)}")
            raise 