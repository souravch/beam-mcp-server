"""
Apache Spark runner client for Apache Beam MCP server.

This module provides a client implementation for the Apache Spark runner.
"""

import logging
import uuid
import os
import importlib.util
from typing import Dict, Any, List, Optional
from datetime import datetime
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.runners.spark.spark_runner import SparkRunner
from apache_beam.runners.portability.fn_api_runner import FnApiRunner
import py4j
import requests

from ...models.job import JobParameters, Job, JobStatus, JobType
from ...models.savepoint import SavepointRequest, Savepoint, SavepointStatus
from ...models.metrics import JobMetrics, Metric, MetricType, MetricValue
from ...models.runner import Runner, RunnerType, RunnerStatus, RunnerCapability
from .base_client import BaseRunnerClient

logger = logging.getLogger(__name__)

class SparkClient(BaseRunnerClient):
    """Client implementation for Apache Spark runner."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the Spark runner client.
        
        Args:
            config (Dict[str, Any]): Runner configuration
        """
        super().__init__(config)
        self._validate_config()
        self.spark_master = config.get('spark_master', 'local[*]')
        self.spark_home = config.get('spark_home')
        self.app_name = config.get('app_name', 'beam-mcp-spark')
        self.rest_api_port = config.get('rest_api_port', 4040)
        self._jobs: Dict[str, Dict[str, Any]] = {}
        
    async def create_job(self, params: JobParameters) -> Job:
        """
        Create a new Spark job.
        
        Args:
            params (JobParameters): Job parameters
            
        Returns:
            Job: Created job
        """
        job_id = str(uuid.uuid4())
        
        # Set up Spark-specific pipeline options
        pipeline_options = {
            'runner': 'SparkRunner',
            'spark_master': self.spark_master,
            'spark_app_name': f"{self.app_name}-{job_id}",
            'spark_home': self.spark_home,
            'save_main_session': True,
            'setup_file': './setup.py',  # If exists
            **params.pipeline_options
        }
        
        # Load pipeline module safely
        try:
            spec = importlib.util.spec_from_file_location(
                "pipeline_module",
                params.pipeline_path
            )
            if not spec or not spec.loader:
                raise ImportError(f"Could not load pipeline from {params.pipeline_path}")
            
            pipeline_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(pipeline_module)
            
            # Create pipeline using Beam's Spark runner
            pipeline_options = PipelineOptions(pipeline_options)
            pipeline = pipeline_module.create_pipeline(pipeline_options)
            
            # Store job info
            self._jobs[job_id] = {
                'pipeline': pipeline,
                'start_time': datetime.utcnow().isoformat(),
                'status': JobStatus.RUNNING,
                'app_id': None  # Will be set when available
            }
            
            # Run pipeline asynchronously
            runner = SparkRunner.from_options(pipeline_options)
            result = runner.run_pipeline(pipeline)
            
            # Create job object
            job = Job(
                job_id=job_id,
                job_name=params.job_name,
                status=JobStatus.RUNNING,
                create_time=self._jobs[job_id]['start_time'],
                runner=RunnerType.SPARK,
                job_type=params.job_type,
                pipeline_options=pipeline_options.get_all_options(),
                current_state="RUNNING"
            )
            
            return job
            
        except Exception as e:
            logger.error(f"Failed to create Spark job: {str(e)}")
            return Job(
                job_id=job_id,
                job_name=params.job_name,
                status=JobStatus.FAILED,
                create_time=datetime.utcnow().isoformat(),
                runner=RunnerType.SPARK,
                job_type=params.job_type,
                pipeline_options=pipeline_options,
                current_state="FAILED",
                error_message=str(e)
            )
    
    async def get_job(self, job_id: str) -> Optional[Job]:
        """
        Get Spark job details.
        
        Args:
            job_id (str): Job ID
            
        Returns:
            Optional[Job]: Job details if found
        """
        if job_id not in self._jobs:
            return None
            
        job_info = self._jobs[job_id]
        
        # Try to get status from Spark REST API if available
        if job_info['app_id']:
            try:
                response = requests.get(
                    f"http://localhost:{self.rest_api_port}/api/v1/applications/{job_info['app_id']}"
                )
                if response.status_code == 200:
                    app_data = response.json()
                    job_info['status'] = self._convert_spark_state(app_data['status'])
            except Exception as e:
                logger.warning(f"Failed to get Spark job status: {str(e)}")
        
        return Job(
            job_id=job_id,
            status=job_info['status'],
            create_time=job_info['start_time'],
            runner=RunnerType.SPARK,
            current_state=job_info['status'].value
        )
    
    async def list_jobs(self) -> List[Job]:
        """
        List all Spark jobs.
        
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
                    runner=RunnerType.SPARK,
                    current_state=job_info['status'].value
                )
            )
        return jobs
    
    async def cancel_job(self, job_id: str) -> bool:
        """
        Cancel a running Spark job.
        
        Args:
            job_id (str): Job ID
            
        Returns:
            bool: True if cancelled successfully
        """
        if job_id not in self._jobs:
            return False
            
        job_info = self._jobs[job_id]
        
        try:
            if job_info['app_id']:
                # Try to cancel via REST API
                response = requests.post(
                    f"http://localhost:{self.rest_api_port}/api/v1/applications/{job_info['app_id']}/kill"
                )
                if response.status_code == 200:
                    job_info['status'] = JobStatus.CANCELLED
                    return True
            
            # Fallback: try to cancel via pipeline runner
            if 'pipeline' in job_info:
                job_info['pipeline'].cancel()
                job_info['status'] = JobStatus.CANCELLED
                return True
                
        except Exception as e:
            logger.error(f"Failed to cancel Spark job: {str(e)}")
            
        return False
    
    async def get_metrics(self, job_id: str) -> Optional[JobMetrics]:
        """
        Get Spark job metrics.
        
        Args:
            job_id (str): Job ID
            
        Returns:
            Optional[JobMetrics]: Job metrics if available
        """
        if job_id not in self._jobs or not self._jobs[job_id]['app_id']:
            return None
            
        try:
            # Get metrics from Spark REST API
            response = requests.get(
                f"http://localhost:{self.rest_api_port}/api/v1/applications/{self._jobs[job_id]['app_id']}/stages"
            )
            if response.status_code == 200:
                stages_data = response.json()
                metrics = JobMetrics(
                    job_id=job_id,
                    metrics=[],
                    timestamp=datetime.utcnow().isoformat()
                )
                
                # Convert Spark metrics to Beam metrics
                for stage in stages_data:
                    for metric_name, value in stage.get('metrics', {}).items():
                        metrics.add_metric(
                            name=metric_name,
                            value=value,
                            labels={'stage_id': str(stage['stageId'])}
                        )
                
                return metrics
                
        except Exception as e:
            logger.error(f"Failed to get Spark metrics: {str(e)}")
            
        return None
    
    async def create_savepoint(self, job_id: str, params: SavepointRequest) -> Savepoint:
        """
        Create a savepoint for a Spark job.
        
        Args:
            job_id (str): Job ID
            params (SavepointRequest): Savepoint parameters
            
        Returns:
            Savepoint: Created savepoint
            
        Raises:
            NotImplementedError: Spark does not support savepoints
        """
        raise NotImplementedError("Savepoints are not supported by the Spark runner")
    
    async def get_runner_info(self) -> Runner:
        """
        Get information about the Spark runner.
        
        Returns:
            Runner: Runner information
        """
        return Runner(
            name="Apache Spark",
            runner_type=RunnerType.SPARK,
            status=RunnerStatus.AVAILABLE if self._check_spark_available() else RunnerStatus.UNAVAILABLE,
            description="Apache Spark runner for Apache Beam",
            capabilities=[
                RunnerCapability.BATCH,
                RunnerCapability.METRICS,
                RunnerCapability.LOGGING
            ],
            config={
                'spark_master': self.spark_master,
                'spark_home': self.spark_home,
                'app_name': self.app_name
            },
            version=self._get_spark_version(),
            mcp_provider="apache"
        )
    
    def _validate_config(self) -> None:
        """
        Validate Spark runner configuration.
        
        Raises:
            ValueError: If configuration is invalid
        """
        super()._validate_config()
        
        # Check Spark home if not using local mode
        if self.spark_master != 'local[*]' and 'spark_home' not in self.config:
            raise ValueError("spark_home must be specified when not using local mode")
            
        # Validate Spark master URL format
        if not any(self.spark_master.startswith(prefix) for prefix in ['local', 'spark://', 'yarn', 'mesos://']):
            raise ValueError(f"Invalid Spark master URL format: {self.spark_master}")
    
    def _check_spark_available(self) -> bool:
        """Check if Spark is available."""
        try:
            # Try to import Spark dependencies
            import pyspark
            
            # Check Spark home if specified
            if self.spark_home:
                if not os.path.exists(self.spark_home):
                    return False
                    
            # Try to connect to master if not local
            if self.spark_master != 'local[*]':
                response = requests.get(f"http://{self.spark_master.replace('spark://', '')}")
                return response.status_code == 200
                
            return True
            
        except (ImportError, Exception):
            return False
    
    def _get_spark_version(self) -> str:
        """Get Spark version."""
        try:
            import pyspark
            return pyspark.__version__
        except ImportError:
            return "unknown"
            
    def _convert_spark_state(self, state: str) -> JobStatus:
        """Convert Spark application state to JobStatus."""
        state_map = {
            'SUBMITTED': JobStatus.PENDING,
            'RUNNING': JobStatus.RUNNING,
            'FINISHED': JobStatus.COMPLETED,
            'FAILED': JobStatus.FAILED,
            'KILLED': JobStatus.CANCELLED,
        }
        return state_map.get(state, JobStatus.UNKNOWN) 