"""
Apache Spark runner client for Apache Beam MCP server.

This module provides a client implementation for the Apache Spark runner.
"""

import logging
import uuid
import os
import asyncio
import importlib.util
import subprocess
import json
import tempfile
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SparkRunnerOptions
import requests
import time
import sys

from ...models.job import JobParameters, Job, JobStatus, JobState, JobType
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
        
        # Extract configuration values
        self.spark_master = config.get('spark_master', 'local[*]')
        self.spark_home = config.get('spark_home') or os.environ.get('SPARK_HOME')
        self.app_name = config.get('app_name', 'beam-mcp-spark')
        self.rest_api_port = config.get('rest_api_port', 4040)
        self.pipeline_options = config.get('pipeline_options', {})
        
        # Internal state tracking
        self._jobs = {}
        self._session = None
        self._monitoring_tasks = {}
        
        # Check spark availability at init time
        self._is_available = self._check_spark_available()
        if self._is_available:
            logger.info(f"Successfully initialized Spark client with master: {self.spark_master}")
        else:
            logger.warning(f"Spark is not available. Please check if Spark is installed and running.")
        
        self._validate_config()
    
    async def create_job(self, params: JobParameters) -> Job:
        """
        Create a new Spark job.
        
        Args:
            params (JobParameters): Job parameters
            
        Returns:
            Job: Created job
        """
        job_id = f"spark-{uuid.uuid4()}"
        job_name = params.job_name
        create_time = datetime.now(timezone.utc).isoformat()
        
        logger.info(f"Creating Spark job: {job_name} (ID: {job_id})")
        
        # Create a new job object in PENDING state
        job = Job(
            mcp_resource_id=job_id,
            job_id=job_id,
            job_name=job_name,
            project=params.pipeline_options.get('project', 'default-project'),
            region=params.pipeline_options.get('region', 'us-central1'),
            status=JobStatus.PENDING,
            current_state=JobState.PENDING,
            create_time=create_time,
            runner=RunnerType.SPARK,
            job_type=params.job_type,
            pipeline_options=params.pipeline_options
        )
        
        # Store job information
        self._jobs[job_id] = {
            'job': job,
            'spark_app_id': None,
            'start_time': create_time,
            'update_time': create_time,
            'status': JobStatus.PENDING,
            'error_message': None
        }
        
        # Make sure we have the code path
        code_path = params.code_path
        if not code_path or not os.path.exists(code_path):
            job.status = JobStatus.FAILED
            job.current_state = JobState.FAILED
            job.error_message = f"Pipeline code not found at: {code_path}"
            self._jobs[job_id]['status'] = JobStatus.FAILED
            self._jobs[job_id]['error_message'] = job.error_message
            return job
        
        try:
            # Check if Spark is available
            if not self._is_available and not self._check_spark_available():
                job.status = JobStatus.FAILED
                job.current_state = JobState.FAILED
                job.error_message = "Spark is not available. Please check if Spark is installed and running."
                self._jobs[job_id]['status'] = JobStatus.FAILED
                self._jobs[job_id]['error_message'] = job.error_message
                return job
            
            # Create the pipeline options
            runner_options = {
                'runner': 'SparkRunner',
                'spark_master': self.spark_master,
                'spark_app_name': f"{job_name}-{job_id}",
                'save_main_session': True
            }
            
            # Add Spark home if available
            if self.spark_home:
                runner_options['spark_home'] = self.spark_home
            
            # Merge with default pipeline options and user-provided options
            pipeline_options = {
                **self.pipeline_options,
                **runner_options,
                **params.pipeline_options
            }
            
            # Launch job in a background task to avoid blocking
            asyncio.create_task(
                self._run_job_async(job_id, code_path, pipeline_options, params)
            )
            
            # Update job status
            job.status = JobStatus.STARTING
            job.current_state = JobState.STARTING
            self._jobs[job_id]['status'] = JobStatus.STARTING
            
            logger.info(f"Successfully initiated Spark job: {job_id}")
            return job
            
        except Exception as e:
            logger.error(f"Failed to create Spark job: {str(e)}", exc_info=True)
            job.status = JobStatus.FAILED
            job.current_state = JobState.FAILED
            job.error_message = f"Error: {str(e)}"
            self._jobs[job_id]['status'] = JobStatus.FAILED
            self._jobs[job_id]['error_message'] = job.error_message
            return job
    
    async def _run_job_async(self, job_id: str, code_path: str, pipeline_options: Dict[str, Any], params: JobParameters):
        """
        Run a Spark job asynchronously.
        
        Args:
            job_id (str): Job ID
            code_path (str): Path to the pipeline code
            pipeline_options (Dict[str, Any]): Pipeline options
            params (JobParameters): Original job parameters
        """
        try:
            logger.info(f"Starting job execution for {job_id}")
            
            # Update job status
            self._update_job_status(job_id, JobStatus.RUNNING, "Job is running")
            
            # Two options for running the job:
            # 1. Use Beam's SparkRunner (Python API)
            # 2. Use spark-submit directly (more reliable for complex jobs)
            
            # For this implementation, we'll use option 1 for simplicity
            await self._run_with_beam_api(job_id, code_path, pipeline_options)
            
            # Start monitoring the job
            asyncio.create_task(self._monitor_job(job_id))
            
        except Exception as e:
            logger.error(f"Error executing Spark job {job_id}: {str(e)}", exc_info=True)
            self._update_job_status(job_id, JobStatus.FAILED, f"Error: {str(e)}")
    
    async def _run_with_beam_api(self, job_id: str, code_path: str, pipeline_options: Dict[str, Any]):
        """
        Run a Beam pipeline using the Python API directly.
        
        This method imports the pipeline module and creates a pipeline using the options.
        
        Args:
            job_id: Job ID
            code_path: Path to the pipeline code
            pipeline_options: Pipeline options
        """
        try:
            # Special handling for direct PySpark implementation
            if "run_wordcount_spark_direct.py" in code_path:
                logger.info(f"Running direct PySpark implementation: {code_path}")
                
                # Prepare command for direct PySpark execution
                cmd = [sys.executable, code_path]
                
                # Only pass the expected arguments for direct PySpark implementation
                # Restricting to these specific arguments that are known to work
                expected_args = ['input_file', 'output_path', 'spark_master']
                
                # Add only the expected arguments
                for key in expected_args:
                    if key in pipeline_options and pipeline_options[key] is not None:
                        cmd.append(f'--{key}={pipeline_options[key]}')
                
                logger.info(f"Running direct PySpark command: {' '.join(cmd)}")
                
                # Run the pipeline in a subprocess
                process = await asyncio.create_subprocess_exec(
                    *cmd, 
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                
                self._jobs[job_id]['process'] = process
                
                # Start a task to log process output
                asyncio.create_task(self._log_process_output(job_id, process))
                
                return
            
            # Remove runner from pipeline options if it's set to SparkRunner
            use_direct_runner = False
            if pipeline_options.get('runner') == 'SparkRunner':
                # Check if SparkRunner is available
                try:
                    # Just check if we can import it
                    from apache_beam.runners.spark.spark_runner import SparkRunner  # noqa
                except ImportError:
                    # SparkRunner not available, fall back to DirectRunner
                    logger.warning("SparkRunner not available, falling back to DirectRunner")
                    pipeline_options['runner'] = 'DirectRunner'
                    use_direct_runner = True
            
            # Import the pipeline module
            sys.path.insert(0, os.path.dirname(os.path.abspath(code_path)))
            module_name = os.path.basename(code_path).replace('.py', '')
            pipeline_module = importlib.import_module(module_name)
            
            # Convert pipeline_options dict to command-line style arguments
            beam_args = []
            for key, value in pipeline_options.items():
                if value is None:
                    continue
                
                # Handle boolean flags properly
                if isinstance(value, bool):
                    # Only add the flag if True
                    if value:
                        beam_args.append(f'--{key}')
                else:
                    # For non-boolean values, add as key=value
                    beam_args.append(f'--{key}={value}')
            
            # If original code expected SparkRunner but we're using DirectRunner
            if use_direct_runner and 'runner' not in pipeline_options:
                beam_args.append('--runner=DirectRunner')
            
            # Log the created pipeline options for debugging
            logger.info(f"Creating pipeline with options: {beam_args}")
            
            # Create pipeline options
            options = PipelineOptions(beam_args)
            
            # Check if create_pipeline function exists
            if hasattr(pipeline_module, 'create_pipeline'):
                logger.info(f"Using create_pipeline() function from {code_path}")
                # Call create_pipeline
                pipeline = pipeline_module.create_pipeline(options)
                # Run pipeline
                result = pipeline.run()
                
                # Store the pipeline result for monitoring
                self._jobs[job_id]['result'] = result
            else:
                # Fall back to subprocess if create_pipeline doesn't exist
                logger.warning(f"No create_pipeline function found in {code_path}, falling back to subprocess")
                
                # Prepare command
                cmd = [sys.executable, code_path]
                for arg in beam_args:
                    cmd.append(arg)
                
                logger.info(f"Running command: {' '.join(cmd)}")
                
                # Run the pipeline in a subprocess
                process = await asyncio.create_subprocess_exec(
                    *cmd, 
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                
                self._jobs[job_id]['process'] = process
                
                # Start a task to log process output
                asyncio.create_task(self._log_process_output(job_id, process))
            
        except Exception as e:
            logger.error(f"Error running pipeline: {str(e)}", exc_info=True)
            self._update_job_status(job_id, JobStatus.FAILED, f"Error running pipeline: {str(e)}")
            raise
    
    async def _log_process_output(self, job_id: str, process):
        """Log the output from a subprocess."""
        try:
            stdout, stderr = await process.communicate()
            
            if stdout:
                logger.info(f"Job {job_id} stdout: {stdout.decode()}")
            
            if stderr:
                stderr_text = stderr.decode()
                logger.error(f"Job {job_id} stderr: {stderr_text}")
                
                # Update job status if there's an error
                if process.returncode != 0:
                    self._update_job_status(job_id, JobStatus.FAILED, f"Process error: {stderr_text}")
                    return
            
            # If process completed successfully
            if process.returncode == 0:
                self._update_job_status(job_id, JobStatus.COMPLETED, "Job completed successfully")
            else:
                self._update_job_status(job_id, JobStatus.FAILED, f"Process exited with code {process.returncode}")
                
        except Exception as e:
            logger.error(f"Error logging process output for job {job_id}: {str(e)}")
            self._update_job_status(job_id, JobStatus.FAILED, f"Error monitoring job: {str(e)}")
    
    async def _monitor_job(self, job_id: str):
        """
        Monitor a running Spark job.
        
        Args:
            job_id (str): Job ID
        """
        if job_id not in self._jobs:
            return
        
        logger.info(f"Starting monitoring for job {job_id}")
        
        # Try to detect the Spark application ID by checking Spark's web UI
        await self._detect_spark_app_id(job_id)
        
        check_interval = 5  # seconds
        max_check_time = 60 * 30  # 30 minutes
        current_check_time = 0
        
        while current_check_time < max_check_time:
            if job_id not in self._jobs:
                logger.info(f"Job {job_id} no longer exists, stopping monitoring")
                return
            
            job_info = self._jobs[job_id]
            
            # If job already completed/failed/cancelled, stop monitoring
            if job_info['status'] in [JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED]:
                logger.info(f"Job {job_id} already in terminal state: {job_info['status']}, stopping monitoring")
                return
            
            # 1. If we have a Spark app ID, check via Spark REST API
            if job_info.get('spark_app_id'):
                await self._check_via_spark_api(job_id)
            
            # 2. If we have a subprocess, check its status
            elif 'process' in job_info:
                await self._check_process_status(job_id)
            
            # 3. Otherwise, try to detect the Spark app ID again
            else:
                await self._detect_spark_app_id(job_id)
            
            # Wait before next check
            await asyncio.sleep(check_interval)
            current_check_time += check_interval
        
        # If we get here, we've been monitoring for too long without resolution
        logger.warning(f"Monitoring for job {job_id} timed out after {max_check_time} seconds")
        self._update_job_status(
            job_id, 
            JobStatus.FAILED, 
            "Job monitoring timed out without detecting completion"
        )
    
    async def _detect_spark_app_id(self, job_id: str):
        """
        Try to detect the Spark application ID for a job.
        
        Args:
            job_id (str): Job ID
        """
        if job_id not in self._jobs:
            return
        
        # If we already have an app ID, nothing to do
        if self._jobs[job_id].get('spark_app_id'):
            return
        
        try:
            # Try to get app ID from Spark Web UI
            response = requests.get(f"http://localhost:{self.rest_api_port}/api/v1/applications")
            if response.status_code == 200:
                apps = response.json()
                
                # Find the app with our job name in it
                job_name = self._jobs[job_id]['job'].job_name
                
                for app in apps:
                    if job_name in app.get('name', ''):
                        self._jobs[job_id]['spark_app_id'] = app['id']
                        logger.info(f"Detected Spark application ID for job {job_id}: {app['id']}")
                        return
        except Exception as e:
            logger.warning(f"Failed to detect Spark application ID: {str(e)}")
    
    async def _check_via_spark_api(self, job_id: str):
        """
        Check job status via Spark REST API.
        
        Args:
            job_id (str): Job ID
        """
        if job_id not in self._jobs or not self._jobs[job_id].get('spark_app_id'):
            return
        
        try:
            app_id = self._jobs[job_id]['spark_app_id']
            response = requests.get(f"http://localhost:{self.rest_api_port}/api/v1/applications/{app_id}")
            
            if response.status_code == 200:
                app_data = response.json()
                spark_state = app_data.get('status')
                
                logger.info(f"Spark application {app_id} state: {spark_state}")
                
                # Update job status based on Spark state
                if spark_state == 'FINISHED':
                    self._update_job_status(job_id, JobStatus.COMPLETED, "Job completed successfully")
                elif spark_state == 'FAILED':
                    self._update_job_status(job_id, JobStatus.FAILED, "Job failed in Spark")
                elif spark_state == 'KILLED':
                    self._update_job_status(job_id, JobStatus.CANCELLED, "Job was killed in Spark")
        except Exception as e:
            logger.warning(f"Failed to check job status via Spark API: {str(e)}")
    
    async def _check_process_status(self, job_id: str):
        """
        Check status of a job running via subprocess.
        
        Args:
            job_id (str): Job ID
        """
        if job_id not in self._jobs or 'process' not in self._jobs[job_id]:
            return
        
        proc = self._jobs[job_id]['process']
        
        # For asyncio subprocess, we need to check returncode directly
        # instead of using poll() which is only for non-asyncio processes
        returncode = proc.returncode  # Access the returncode attribute directly
        
        if returncode is not None:
            # Process has finished
            if returncode == 0:
                self._update_job_status(job_id, JobStatus.COMPLETED, "Job completed successfully")
            else:
                # Since we're using asyncio subprocesses, we should have already logged the stderr
                # in _log_process_output. Just update the status here.
                self._update_job_status(job_id, JobStatus.FAILED, f"Job failed with exit code {returncode}")
    
    def _update_job_status(self, job_id: str, status: JobStatus, message: str = None):
        """
        Update job status.
        
        Args:
            job_id (str): Job ID
            status (JobStatus): New job status
            message (str, optional): Status message
        """
        if job_id not in self._jobs:
            return
        
        logger.info(f"Updating job {job_id} status to {status}: {message}")
        
        job_info = self._jobs[job_id]
        job_info['status'] = status
        job_info['update_time'] = datetime.now(timezone.utc).isoformat()
        
        if message:
            job_info['error_message'] = message
        
        if 'job' in job_info:
            job_info['job'].status = status
            job_info['job'].current_state = JobState.from_status(status)
            job_info['job'].update_time = job_info['update_time']
            
            if message:
                job_info['job'].error_message = message
    
    async def get_job(self, job_id: str) -> Optional[Job]:
        """
        Get Spark job details.
        
        Args:
            job_id (str): Job ID
            
        Returns:
            Optional[Job]: Job details if found
        """
        if job_id not in self._jobs:
            logger.warning(f"Job not found: {job_id}")
            return None
            
        job_info = self._jobs[job_id]
        
        # If we have a stored job object, return it
        if 'job' in job_info:
            job = job_info['job']
            
            # Ensure status is up to date
            job.status = job_info['status']
            job.current_state = JobState.from_status(job_info['status'])
            job.update_time = job_info['update_time']
            
            if job_info.get('error_message'):
                job.error_message = job_info['error_message']
                
            return job
        
        # Otherwise, create a new job object
        return Job(
            job_id=job_id,
            job_name=job_id,  # Use job_id as name if we don't have it
            status=job_info['status'],
            current_state=JobState.from_status(job_info['status']),
            create_time=job_info['start_time'],
            update_time=job_info['update_time'],
            runner=RunnerType.SPARK,
            error_message=job_info.get('error_message')
        )
    
    async def list_jobs(self) -> List[Job]:
        """
        List all Spark jobs.
        
        Returns:
            List[Job]: List of jobs
        """
        jobs = []
        
        for job_id, job_info in self._jobs.items():
            # If we have a stored job object, use it
            if 'job' in job_info:
                job = job_info['job']
                job.status = job_info['status']
                job.current_state = JobState.from_status(job_info['status'])
                jobs.append(job)
            else:
                # Otherwise, create a new job object
                job = Job(
                    job_id=job_id,
                    job_name=job_id,  # Use job_id as name if we don't have it
                    status=job_info['status'],
                    current_state=JobState.from_status(job_info['status']),
                    create_time=job_info['start_time'],
                    update_time=job_info['update_time'],
                    runner=RunnerType.SPARK,
                    error_message=job_info.get('error_message')
                )
                jobs.append(job)
        
        return jobs
    
    async def list_job_ids(self) -> List[str]:
        """
        List all Spark job IDs.
        
        Returns:
            List[str]: List of job IDs
        """
        return list(self._jobs.keys())
    
    async def cancel_job(self, job_id: str) -> bool:
        """
        Cancel a running Spark job.
        
        Args:
            job_id (str): Job ID
            
        Returns:
            bool: True if cancelled successfully
        """
        if job_id not in self._jobs:
            logger.warning(f"Cannot cancel job {job_id}: job not found")
            return False
            
        job_info = self._jobs[job_id]
        
        # If job is already in a terminal state, just return success
        if job_info.get('status') in [JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED]:
            logger.info(f"Job {job_id} is already in terminal state: {job_info.get('status')}")
            return True
        
        try:
            # Method 1: If we have a Spark app ID, cancel via REST API
            if job_info.get('spark_app_id'):
                app_id = job_info['spark_app_id']
                logger.info(f"Cancelling Spark job {job_id} with app ID: {app_id}")
                
                response = requests.post(
                    f"http://localhost:{self.rest_api_port}/api/v1/applications/{app_id}/kill"
                )
                
                if response.status_code == 200:
                    self._update_job_status(job_id, JobStatus.CANCELLED, "Job cancelled by user")
                    logger.info(f"Successfully cancelled Spark job {job_id} via REST API")
                    return True
            
            # Method 2: If we have a subprocess, terminate it
            if 'process' in job_info:
                proc = job_info['process']
                logger.info(f"Terminating Spark job process for {job_id}")
                
                # Check if process is still running before attempting to terminate
                if proc.returncode is None:
                    try:
                        proc.terminate()
                        self._update_job_status(job_id, JobStatus.CANCELLED, "Job cancelled by user")
                        logger.info(f"Successfully terminated Spark job process for {job_id}")
                        return True
                    except ProcessLookupError:
                        # Process already terminated
                        logger.info(f"Process for job {job_id} already terminated")
                        self._update_job_status(job_id, JobStatus.CANCELLED, "Job process already terminated")
                        return True
                else:
                    logger.info(f"Process for job {job_id} already completed with code {proc.returncode}")
                    # Update status if it wasn't already a terminal state
                    if job_info.get('status') not in [JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED]:
                        if proc.returncode == 0:
                            self._update_job_status(job_id, JobStatus.COMPLETED, "Job completed successfully")
                        else:
                            self._update_job_status(job_id, JobStatus.FAILED, f"Job failed with exit code {proc.returncode}")
                    return True
            
            # Method 3: If we have a 'result' object from run_pipeline, try to cancel it
            if 'result' in job_info:
                result = job_info['result']
                if hasattr(result, 'cancel'):
                    logger.info(f"Cancelling Spark job {job_id} via pipeline result")
                    try:
                        result.cancel()
                        self._update_job_status(job_id, JobStatus.CANCELLED, "Job cancelled by user")
                        logger.info(f"Successfully cancelled Spark job {job_id} via pipeline result")
                        return True
                    except Exception as e:
                        logger.warning(f"Error cancelling job {job_id} via pipeline result: {str(e)}")
            
            # If we got here, we don't have a good way to cancel the job
            logger.warning(f"Could not find a way to cancel Spark job {job_id}")
            return False
                
        except Exception as e:
            logger.error(f"Failed to cancel Spark job {job_id}: {str(e)}", exc_info=True)
            
        return False
    
    async def get_metrics(self, job_id: str) -> Optional[JobMetrics]:
        """
        Get Spark job metrics.
        
        Args:
            job_id (str): Job ID
            
        Returns:
            Optional[JobMetrics]: Job metrics if available
        """
        if job_id not in self._jobs:
            return None
        
        job_info = self._jobs[job_id]
        app_id = job_info.get('spark_app_id')
        
        if not app_id:
            # Try to detect the Spark app ID
            await self._detect_spark_app_id(job_id)
            app_id = job_info.get('spark_app_id')
            
            if not app_id:
                logger.warning(f"Cannot get metrics for job {job_id}: Spark application ID not found")
                return None
            
        try:
            # Get metrics from Spark REST API
            response = requests.get(f"http://localhost:{self.rest_api_port}/api/v1/applications/{app_id}/stages")
            
            if response.status_code == 200:
                stages_data = response.json()
                metrics = JobMetrics(
                    job_id=job_id,
                    metrics=[],
                    timestamp=datetime.now(timezone.utc).isoformat()
                )
                
                # Convert Spark metrics to Beam metrics
                for stage in stages_data:
                    for metric_name, value in stage.get('metrics', {}).items():
                        metric = Metric(
                            name=metric_name,
                            value=MetricValue(value=value, type=MetricType.GAUGE),
                            labels={'stage_id': str(stage.get('stageId', 'unknown'))}
                        )
                        metrics.metrics.append(metric)
                
                return metrics
                
        except Exception as e:
            logger.error(f"Failed to get Spark metrics: {str(e)}", exc_info=True)
            
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
        # Refresh availability status
        self._is_available = self._check_spark_available()
        
        return Runner(
            mcp_resource_id="spark",
            name="Apache Spark",
            runner_type=RunnerType.SPARK,
            status=RunnerStatus.AVAILABLE if self._is_available else RunnerStatus.UNAVAILABLE,
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
        
        # Validate Spark master URL format
        if not any(self.spark_master.startswith(prefix) for prefix in ['local', 'spark://', 'yarn', 'mesos://']):
            raise ValueError(f"Invalid Spark master URL format: {self.spark_master}")
    
    def _check_spark_available(self) -> bool:
        """
        Check if Spark is available.
        
        Returns:
            bool: True if Spark is available, False otherwise
        """
        try:
            # First, check if PySpark is installed
            try:
                import pyspark
                logger.info(f"Found PySpark version: {pyspark.__version__}")
            except ImportError:
                logger.warning("PySpark is not installed")
                return False
            
            # Check Spark home if specified
            if self.spark_home:
                if not os.path.exists(self.spark_home):
                    logger.warning(f"Spark home directory not found: {self.spark_home}")
                    return False
                    
            # For local mode, we're good if we have PySpark installed
            if self.spark_master.startswith('local'):
                return True
                
            # For remote Spark master, check if we can connect to it
            if self.spark_master.startswith('spark://'):
                host_port = self.spark_master.replace('spark://', '')
                try:
                    response = requests.get(f"http://{host_port}")
                    if response.status_code == 200:
                        logger.info(f"Successfully connected to Spark master: {self.spark_master}")
                        return True
                    else:
                        logger.warning(f"Could not connect to Spark master {self.spark_master}: HTTP {response.status_code}")
                        return False
                except Exception as e:
                    logger.warning(f"Could not connect to Spark master {self.spark_master}: {str(e)}")
                    return False
            
            # For other modes like YARN, we need to check differently
            # This is more complex and depends on the environment
            
            return True
            
        except Exception as e:
            logger.warning(f"Error checking Spark availability: {str(e)}")
            return False
    
    def _get_spark_version(self) -> str:
        """
        Get Spark version.
        
        Returns:
            str: Spark version
        """
        try:
            # Try to get version from PySpark
            import pyspark
            return pyspark.__version__
        except ImportError:
            pass
            
        # Try to get version from spark-submit
        try:
            if self.spark_home:
                spark_submit = os.path.join(self.spark_home, 'bin', 'spark-submit')
            else:
                spark_submit = 'spark-submit'
                
            output = subprocess.check_output([spark_submit, '--version'], universal_newlines=True)
            for line in output.split('\n'):
                if 'version' in line:
                    return line.strip().split(' ')[-1]
        except Exception:
            pass
            
        return "unknown"
    
    async def close(self):
        """
        Close the client and clean up resources.
        """
        logger.info("Closing Spark client")
        
        # Cancel any running jobs
        for job_id in list(self._jobs.keys()):
            job_info = self._jobs[job_id]
            if job_info['status'] not in [JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED]:
                try:
                    logger.info(f"Cancelling job {job_id} during cleanup")
                    await self.cancel_job(job_id)
                except Exception as e:
                    logger.warning(f"Error cancelling job {job_id} during cleanup: {str(e)}")
        
        # Clear monitoring tasks
        self._monitoring_tasks.clear()
        
        logger.info("Spark client closed successfully") 