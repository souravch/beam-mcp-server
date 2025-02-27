"""
Direct runner client for Apache Beam MCP server.

This module provides a client implementation for the Direct runner.
"""

import logging
import uuid
from typing import Dict, Any, List, Optional
from datetime import datetime
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.runners.runner import PipelineState
import importlib.util

from ...models.job import JobParameters, Job, JobStatus, JobType, JobState
from ...models.savepoint import SavepointRequest, Savepoint
from ...models.metrics import JobMetrics
from ...models.runner import Runner, RunnerType, RunnerStatus, RunnerCapability
from .base_client import BaseRunnerClient

logger = logging.getLogger(__name__)

class DirectClient(BaseRunnerClient):
    """Client implementation for Direct runner."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the Direct runner client.
        
        Args:
            config (Dict[str, Any]): Runner configuration
        """
        super().__init__(config)
        self._validate_config()
        self._jobs: Dict[str, Dict[str, Any]] = {}
        
    async def create_job(self, params: JobParameters) -> Job:
        """
        Create a new Direct runner job.
        
        Args:
            params (JobParameters): Job parameters
            
        Returns:
            Job: Created job
        """
        job_id = str(uuid.uuid4())
        
        # Set up pipeline options
        pipeline_options = {
            'runner': 'DirectRunner',
            'direct_num_workers': self.config.get('direct_num_workers', 1),
            'direct_running_mode': self.config.get('direct_running_mode', 'in_memory'),
            'save_main_session': True,
            **params.pipeline_options
        }
        
        try:
            # Import and run the pipeline
            import sys
            module_name = f"pipeline_module_{job_id}"
            spec = importlib.util.spec_from_file_location(module_name, params.code_path)
            if not spec or not spec.loader:
                raise ImportError(f"Could not load pipeline from {params.code_path}")
            
            pipeline_module = importlib.util.module_from_spec(spec)
            sys.modules[module_name] = pipeline_module
            spec.loader.exec_module(pipeline_module)
            
            # Create and run pipeline
            pipeline = pipeline_module.create_pipeline(PipelineOptions(pipeline_options))
            
            # Store job info
            result = pipeline.run()
            
            # Initially set to RUNNING
            now = datetime.utcnow().isoformat()
            self._jobs[job_id] = {
                'pipeline': pipeline,
                'start_time': now,
                'status': JobStatus.RUNNING,
                'result': result,
                'job_name': params.job_name,
                'pipeline_options': pipeline_options,
                'job_type': params.job_type,
                'last_check': now
            }
            
            return Job(
                mcp_resource_id=job_id,
                job_id=job_id,
                job_name=params.job_name,
                project=params.pipeline_options.get('project', 'local'),
                region=params.pipeline_options.get('region', 'local'),
                status=JobStatus.RUNNING,
                create_time=now,
                update_time=now,
                runner=RunnerType.DIRECT,
                job_type=params.job_type,
                pipeline_options=pipeline_options,
                current_state=JobState.RUNNING
            )
            
        except Exception as e:
            logger.error(f"Failed to create Direct runner job: {str(e)}")
            now = datetime.utcnow().isoformat()
            return Job(
                mcp_resource_id=job_id,
                job_id=job_id,
                job_name=params.job_name,
                project=params.pipeline_options.get('project', 'local'),
                region=params.pipeline_options.get('region', 'local'),
                status=JobStatus.FAILED,
                create_time=now,
                update_time=now,
                runner=RunnerType.DIRECT,
                job_type=params.job_type,
                pipeline_options=pipeline_options,
                current_state=JobState.FAILED,
                error_message=str(e)
            )
    
    async def get_job(self, job_id: str) -> Optional[Job]:
        """Get job details."""
        if job_id not in self._jobs:
            return None
        
        job_info = self._jobs[job_id]
        result = job_info.get('result')
        
        # Check pipeline state
        if result:
            state = result.state
            now = datetime.utcnow().isoformat()
            
            # Update last check time
            job_info['last_check'] = now
            
            # Check if the pipeline has completed
            if state in [PipelineState.DONE, PipelineState.STOPPED]:
                if job_info.get('status') != JobStatus.SUCCEEDED:
                    job_info['status'] = JobStatus.SUCCEEDED
                    job_info['update_time'] = now
                    job_info['end_time'] = now
                
                return Job(
                    mcp_resource_id=job_id,
                    job_id=job_id,
                    job_name=job_info['job_name'],
                    status=JobStatus.SUCCEEDED,
                    create_time=job_info['start_time'],
                    update_time=now,
                    end_time=now,
                    runner=RunnerType.DIRECT,
                    job_type=job_info['job_type'],
                    pipeline_options=job_info['pipeline_options'],
                    current_state=JobState.SUCCEEDED
                )
            elif state == PipelineState.FAILED:
                if job_info.get('status') != JobStatus.FAILED:
                    job_info['status'] = JobStatus.FAILED
                    job_info['update_time'] = now
                    job_info['end_time'] = now
                
                return Job(
                    mcp_resource_id=job_id,
                    job_id=job_id,
                    job_name=job_info['job_name'],
                    status=JobStatus.FAILED,
                    create_time=job_info['start_time'],
                    update_time=now,
                    end_time=now,
                    runner=RunnerType.DIRECT,
                    job_type=job_info['job_type'],
                    pipeline_options=job_info['pipeline_options'],
                    current_state=JobState.FAILED
                )
            elif state == PipelineState.CANCELLED:
                if job_info.get('status') != JobStatus.CANCELLED:
                    job_info['status'] = JobStatus.CANCELLED
                    job_info['update_time'] = now
                    job_info['end_time'] = now
                
                return Job(
                    mcp_resource_id=job_id,
                    job_id=job_id,
                    job_name=job_info['job_name'],
                    status=JobStatus.CANCELLED,
                    create_time=job_info['start_time'],
                    update_time=now,
                    end_time=now,
                    runner=RunnerType.DIRECT,
                    job_type=job_info['job_type'],
                    pipeline_options=job_info['pipeline_options'],
                    current_state=JobState.CANCELLED
                )
            
            # Check if the pipeline has completed by looking at the result state
            try:
                result.wait_until_finish(duration=0)  # Check current state without waiting
                if result.state in [PipelineState.DONE, PipelineState.STOPPED]:
                    if job_info.get('status') != JobStatus.SUCCEEDED:
                        job_info['status'] = JobStatus.SUCCEEDED
                        job_info['update_time'] = now
                        job_info['end_time'] = now
                    
                    return Job(
                        mcp_resource_id=job_id,
                        job_id=job_id,
                        job_name=job_info['job_name'],
                        status=JobStatus.SUCCEEDED,
                        create_time=job_info['start_time'],
                        update_time=now,
                        end_time=now,
                        runner=RunnerType.DIRECT,
                        job_type=job_info['job_type'],
                        pipeline_options=job_info['pipeline_options'],
                        current_state=JobState.SUCCEEDED
                    )
            except Exception as e:
                logger.debug(f"Error checking pipeline state: {str(e)}")
        
        # Job is still running
        return Job(
            mcp_resource_id=job_id,
            job_id=job_id,
            job_name=job_info['job_name'],
            status=JobStatus.RUNNING,
            create_time=job_info['start_time'],
            update_time=job_info.get('update_time', job_info['start_time']),
            runner=RunnerType.DIRECT,
            job_type=job_info['job_type'],
            pipeline_options=job_info['pipeline_options'],
            current_state=JobState.RUNNING
        )
    
    async def list_jobs(self) -> List[Job]:
        """
        List all Direct runner jobs.
        
        Returns:
            List[Job]: List of jobs
        """
        jobs = []
        for job_id, job_info in self._jobs.items():
            jobs.append(
                Job(
                    job_id=job_id,
                    status=job_info['status'],
                    create_time=job_info['start_time'],
                    runner=RunnerType.DIRECT,
                    current_state=JobState.SUCCEEDED if job_info['status'] == JobStatus.SUCCEEDED else JobState.FAILED
                )
            )
        return jobs
    
    async def cancel_job(self, job_id: str) -> bool:
        """
        Cancel a running Direct runner job.
        
        Args:
            job_id (str): Job ID
            
        Returns:
            bool: True if cancelled successfully
        """
        if job_id not in self._jobs:
            return False
            
        job_info = self._jobs[job_id]
        
        try:
            if 'result' in job_info:
                job_info['result'].cancel()
                job_info['status'] = JobStatus.CANCELLED
                return True
        except Exception as e:
            logger.error(f"Failed to cancel Direct runner job: {str(e)}")
            
        return False
    
    async def get_metrics(self, job_id: str) -> Optional[JobMetrics]:
        """
        Get Direct runner job metrics.
        
        Args:
            job_id (str): Job ID
            
        Returns:
            Optional[JobMetrics]: Job metrics if available
        """
        if job_id not in self._jobs:
            return None
            
        job_info = self._jobs[job_id]
        result = job_info.get('result')
        
        if not result:
            return None
            
        try:
            metrics = JobMetrics(
                job_id=job_id,
                metrics=[],
                timestamp=datetime.utcnow().isoformat()
            )
            
            # Get metrics from result
            metric_results = result.metrics().query()
            for metric in metric_results['counters']:
                metrics.add_metric(
                    name=metric.key.metric.name,
                    value=metric.committed,
                    labels={
                        'namespace': metric.key.metric.namespace,
                        'step': metric.key.step
                    }
                )
            
            return metrics
            
        except Exception as e:
            logger.error(f"Failed to get Direct runner metrics: {str(e)}")
            
        return None
    
    async def create_savepoint(self, job_id: str, params: SavepointRequest) -> Savepoint:
        """
        Create a savepoint for a Direct runner job.
        
        Args:
            job_id (str): Job ID
            params (SavepointRequest): Savepoint parameters
            
        Returns:
            Savepoint: Created savepoint
            
        Raises:
            NotImplementedError: Direct runner does not support savepoints
        """
        raise NotImplementedError("Savepoints are not supported by the Direct runner")
    
    async def get_runner_info(self) -> Runner:
        """
        Get information about the Direct runner.
        
        Returns:
            Runner: Runner information
        """
        return Runner(
            mcp_resource_id="direct-runner",
            name="Direct Runner",
            runner_type=RunnerType.DIRECT,
            status=RunnerStatus.AVAILABLE,
            description="Local Direct runner for Apache Beam",
            capabilities=[
                RunnerCapability.BATCH,
                RunnerCapability.METRICS,
                RunnerCapability.LOGGING
            ],
            config=self.config,
            version=beam.__version__,
            mcp_provider="apache"
        )
    
    def _validate_config(self) -> None:
        """
        Validate Direct runner configuration.
        
        Raises:
            ValueError: If configuration is invalid
        """
        super()._validate_config()
        # No additional validation needed for Direct runner 