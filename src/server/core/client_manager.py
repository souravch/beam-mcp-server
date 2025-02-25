"""
Client manager for Apache Beam MCP server.

This module provides a manager for multiple runner clients,
supporting Dataflow, Spark, Flink, and Direct runners.
"""

import logging
from typing import Dict, Any, Optional, List
from datetime import datetime

from ..models import (
    RunnerType, JobState, JobType, JobParameters, JobInfo,
    JobList, JobUpdateParameters, SavepointParameters, SavepointInfo,
    SavepointList, JobMetrics, LogEntry, LogList,
    RunnerInfo, RunnerList
)
from .client_factory import ClientFactory

logger = logging.getLogger(__name__)

class BeamClientManager:
    """Manager for Apache Beam runner clients."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the client manager
        
        Args:
            config (Dict[str, Any]): Server configuration
        """
        self.config = config
        self.clients = {}
        self.init_clients()
        
        # Mock data for demonstration
        self.jobs = {}
        self.savepoints = {}
    
    def init_clients(self):
        """Initialize runner clients based on configuration."""
        for runner_name, runner_config in self.config['runners'].items():
            if runner_config.get('enabled', False):
                logger.info(f"Initializing client for runner: {runner_name}")
                try:
                    # Use client factory to create client
                    runner_type = RunnerType(runner_name)
                    client = ClientFactory.create_client(runner_type, runner_config)
                    self.clients[runner_name] = client
                except Exception as e:
                    logger.error(f"Failed to initialize client for runner {runner_name}: {str(e)}")
                    # Fall back to using this client manager as the client
                    self.clients[runner_name] = None
    
    def get_client(self, runner_type: RunnerType):
        """
        Get client for the specified runner type
        
        Args:
            runner_type (RunnerType): Type of runner
        
        Returns:
            Any: Runner client
            
        Raises:
            ValueError: If the runner is not enabled or not supported
        """
        runner_type_str = runner_type.value
        
        if runner_type_str not in self.config['runners']:
            raise ValueError(f"Runner type not supported: {runner_type_str}")
        
        if not self.config['runners'][runner_type_str].get('enabled', False):
            raise ValueError(f"Runner type not enabled: {runner_type_str}")
        
        # If we have a client, use it; otherwise, fall back to this client manager
        client = self.clients.get(runner_type_str)
        if client:
            return client
        return self
    
    async def create_job(self, params: JobParameters) -> JobInfo:
        """
        Create a new job
        
        Args:
            params (JobParameters): Job parameters
            
        Returns:
            JobInfo: Created job information
        """
        # Generate a unique job ID
        job_id = f"{datetime.now().strftime('%Y-%m-%d')}_{len(self.jobs) + 1}"
        now = datetime.now().isoformat() + "Z"
        
        job = JobInfo(
            job_id=job_id,
            job_name=params.job_name,
            runner_type=params.runner_type,
            create_time=now,
            update_time=now,
            current_state=JobState.RUNNING,
            job_type=params.job_type,
            pipeline_options=params.pipeline_options,
            metrics={
                "cpu_utilization": 0.0,
                "memory_usage_gb": 0.0,
                "elements_processed": 0
            },
            template_path=params.template_path,
            code_path=params.code_path
        )
        
        self.jobs[job_id] = job
        logger.info(f"Created job: {job_id} ({params.job_name})")
        
        return job
    
    async def list_jobs(
        self, 
        runner_type: Optional[RunnerType] = None, 
        job_type: Optional[JobType] = None, 
        page_size: int = 10, 
        page_token: Optional[str] = None
    ) -> JobList:
        """
        List jobs with optional filtering
        
        Args:
            runner_type (RunnerType, optional): Filter by runner type
            job_type (JobType, optional): Filter by job type
            page_size (int): Number of jobs to return
            page_token (str, optional): Pagination token
            
        Returns:
            JobList: List of jobs
        """
        filtered_jobs = list(self.jobs.values())
        
        if runner_type:
            filtered_jobs = [job for job in filtered_jobs if job.runner_type == runner_type]
        
        if job_type:
            filtered_jobs = [job for job in filtered_jobs if job.job_type == job_type]
        
        # Sort by creation time, newest first
        filtered_jobs.sort(key=lambda job: job.create_time, reverse=True)
        
        # Apply pagination (in a real implementation, this would be more sophisticated)
        start_idx = 0
        if page_token:
            # In a real implementation, decode the page token to get the start index
            try:
                start_idx = int(page_token)
            except ValueError:
                start_idx = 0
        
        end_idx = min(start_idx + page_size, len(filtered_jobs))
        next_token = str(end_idx) if end_idx < len(filtered_jobs) else None
        
        return JobList(
            jobs=filtered_jobs[start_idx:end_idx], 
            total_count=len(filtered_jobs), 
            next_page_token=next_token
        )
    
    async def get_job(self, job_id: str) -> JobInfo:
        """
        Get job information
        
        Args:
            job_id (str): Job ID
            
        Returns:
            JobInfo: Job information
            
        Raises:
            ValueError: If the job does not exist
        """
        if job_id not in self.jobs:
            raise ValueError(f"Job not found: {job_id}")
        
        return self.jobs[job_id]
    
    async def update_job(self, job_id: str, params: JobUpdateParameters) -> JobInfo:
        """
        Update job configuration
        
        Args:
            job_id (str): Job ID
            params (JobUpdateParameters): Update parameters
            
        Returns:
            JobInfo: Updated job information
            
        Raises:
            ValueError: If the job does not exist
        """
        if job_id not in self.jobs:
            raise ValueError(f"Job not found: {job_id}")
        
        job = self.jobs[job_id]
        
        # Update pipeline options with scaling parameters
        if params.scaling:
            for key, value in params.scaling.items():
                job.pipeline_options[key] = value
        
        # Update other options
        if params.update_options:
            for key, value in params.update_options.items():
                job.pipeline_options[key] = value
        
        # Update timestamp
        job.update_time = datetime.now().isoformat() + "Z"
        
        logger.info(f"Updated job: {job_id}")
        return job
    
    async def cancel_job(self, job_id: str) -> JobInfo:
        """
        Cancel a job
        
        Args:
            job_id (str): Job ID
            
        Returns:
            JobInfo: Updated job information
            
        Raises:
            ValueError: If the job does not exist
        """
        if job_id not in self.jobs:
            raise ValueError(f"Job not found: {job_id}")
        
        job = self.jobs[job_id]
        job.current_state = JobState.CANCELLED
        job.update_time = datetime.now().isoformat() + "Z"
        
        logger.info(f"Cancelled job: {job_id}")
        return job
    
    async def create_savepoint(self, job_id: str, params: SavepointParameters) -> SavepointInfo:
        """
        Create a savepoint for a job
        
        Args:
            job_id (str): Job ID
            params (SavepointParameters): Savepoint parameters
            
        Returns:
            SavepointInfo: Created savepoint information
            
        Raises:
            ValueError: If the job does not exist or does not support savepoints
        """
        if job_id not in self.jobs:
            raise ValueError(f"Job not found: {job_id}")
        
        job = self.jobs[job_id]
        
        # Only streaming jobs can have savepoints
        if job.job_type != JobType.STREAMING:
            raise ValueError("Savepoints are only supported for streaming jobs")
        
        savepoint_id = f"sp-{datetime.now().strftime('%Y-%m-%d-%H%M%S')}"
        savepoint_location = params.savepoint_dir or f"gs://default-bucket/savepoints/{job_id}/{savepoint_id}"
        
        savepoint = SavepointInfo(
            savepoint_id=savepoint_id,
            job_id=job_id,
            create_time=datetime.now().isoformat() + "Z",
            state="COMPLETED",
            location=savepoint_location
        )
        
        self.savepoints[savepoint_id] = savepoint
        logger.info(f"Created savepoint: {savepoint_id} for job: {job_id}")
        
        return savepoint
    
    async def list_savepoints(self, job_id: str) -> SavepointList:
        """
        List savepoints for a job
        
        Args:
            job_id (str): Job ID
            
        Returns:
            SavepointList: List of savepoints
            
        Raises:
            ValueError: If the job does not exist
        """
        if job_id not in self.jobs:
            raise ValueError(f"Job not found: {job_id}")
        
        filtered_savepoints = [sp for sp in self.savepoints.values() if sp.job_id == job_id]
        return SavepointList(savepoints=filtered_savepoints, total_count=len(filtered_savepoints))
    
    async def restore_from_savepoint(self, job_id: str, savepoint_id: str) -> JobInfo:
        """
        Restore a job from a savepoint
        
        Args:
            job_id (str): Job ID
            savepoint_id (str): Savepoint ID
            
        Returns:
            JobInfo: Updated job information
            
        Raises:
            ValueError: If the job or savepoint does not exist
        """
        if job_id not in self.jobs:
            raise ValueError(f"Job not found: {job_id}")
        
        if savepoint_id not in self.savepoints:
            raise ValueError(f"Savepoint not found: {savepoint_id}")
        
        job = self.jobs[job_id]
        savepoint = self.savepoints[savepoint_id]
        
        # Check if the savepoint belongs to this job
        if savepoint.job_id != job_id:
            raise ValueError(f"Savepoint {savepoint_id} does not belong to job {job_id}")
        
        # Update job state and timestamp
        job.current_state = JobState.RUNNING
        job.update_time = datetime.now().isoformat() + "Z"
        
        logger.info(f"Restored job: {job_id} from savepoint: {savepoint_id}")
        return job
    
    async def get_job_metrics(self, job_id: str) -> JobMetrics:
        """
        Get metrics for a job
        
        Args:
            job_id (str): Job ID
            
        Returns:
            JobMetrics: Job metrics
            
        Raises:
            ValueError: If the job does not exist
        """
        if job_id not in self.jobs:
            raise ValueError(f"Job not found: {job_id}")
        
        job = self.jobs[job_id]
        
        # In a real implementation, these would be actual metrics from the runner
        metrics = {
            "cpu_utilization": 65.2,
            "memory_usage_gb": 12.8,
            "elements_processed": 15000000,
            "processing_rate": 3500,
            "worker_count": 10,
            "elapsed_time_ms": 1800000
        }
        
        # Add streaming-specific metrics if applicable
        if job.job_type == JobType.STREAMING:
            metrics.update({
                "system_lag_ms": 120,
                "watermark_lag_ms": 95,
                "events_per_second": 5000
            })
        
        return JobMetrics(
            job_id=job_id,
            timestamp=datetime.now().isoformat() + "Z",
            metrics=metrics
        )
    
    async def get_job_logs(
        self, 
        job_id: str, 
        max_results: int = 10, 
        page_token: Optional[str] = None
    ) -> LogList:
        """
        Get logs for a job
        
        Args:
            job_id (str): Job ID
            max_results (int): Maximum number of log entries to return
            page_token (str, optional): Pagination token
            
        Returns:
            LogList: List of log entries
            
        Raises:
            ValueError: If the job does not exist
        """
        if job_id not in self.jobs:
            raise ValueError(f"Job not found: {job_id}")
        
        # Mock log entries (in a real implementation, these would come from the runner)
        logs = [
            LogEntry(
                timestamp=datetime.now().isoformat() + "Z",
                severity="INFO",
                message="Pipeline started successfully",
                job_id=job_id,
                worker_id=None,
                step_name=None
            ),
            LogEntry(
                timestamp=datetime.now().isoformat() + "Z",
                severity="INFO",
                message="Processing elements at 3500 elements/second",
                job_id=job_id,
                worker_id="worker-1",
                step_name="ProcessElements"
            )
        ]
        
        return LogList(logs=logs[:max_results], next_page_token=None)
    
    async def list_runners(self) -> RunnerList:
        """
        List available runners
        
        Returns:
            RunnerList: List of available runners
        """
        # Available runners based on configuration
        runners = []
        
        # Dataflow runner
        if self.config['runners'].get('dataflow', {}).get('enabled', False):
            runners.append(RunnerInfo(
                runner_type=RunnerType.DATAFLOW,
                name="Google Cloud Dataflow",
                description="Fully managed runner for Apache Beam on Google Cloud",
                supported_job_types=[JobType.BATCH, JobType.STREAMING],
                supported_features=["autoscaling", "monitoring", "savepoints", "flexible_resource_scheduling"],
                configuration_examples={
                    "basic": {
                        "project": "my-gcp-project",
                        "region": "us-central1",
                        "tempLocation": "gs://my-bucket/temp"
                    },
                    "advanced": {
                        "project": "my-gcp-project",
                        "region": "us-central1",
                        "tempLocation": "gs://my-bucket/temp",
                        "maxWorkers": 10,
                        "machineType": "n2-standard-4"
                    }
                }
            ))
        
        # Spark runner
        if self.config['runners'].get('spark', {}).get('enabled', False):
            runners.append(RunnerInfo(
                runner_type=RunnerType.SPARK,
                name="Apache Spark",
                description="Distributed processing engine for batch and streaming workloads",
                supported_job_types=[JobType.BATCH, JobType.STREAMING],
                supported_features=["dynamic_allocation", "monitoring", "checkpointing", "ml_integration"],
                configuration_examples={
                    "basic": {
                        "master": "yarn",
                        "deploy-mode": "cluster",
                        "executor-memory": "4g"
                    },
                    "advanced": {
                        "master": "yarn",
                        "deploy-mode": "cluster",
                        "driver-memory": "4g",
                        "executor-memory": "4g",
                        "executor-cores": "2",
                        "num-executors": "10"
                    }
                }
            ))
        
        # Flink runner
        if self.config['runners'].get('flink', {}).get('enabled', False):
            runners.append(RunnerInfo(
                runner_type=RunnerType.FLINK,
                name="Apache Flink",
                description="Stream processing framework for stateful computations over data streams",
                supported_job_types=[JobType.BATCH, JobType.STREAMING],
                supported_features=["exactly_once_semantics", "savepoints", "advanced_windowing", "stateful_processing"],
                configuration_examples={
                    "basic": {
                        "jobmanager.memory.process.size": "4096m",
                        "taskmanager.memory.process.size": "4096m"
                    },
                    "advanced": {
                        "jobmanager.memory.process.size": "4096m",
                        "taskmanager.memory.process.size": "4096m",
                        "taskmanager.numberOfTaskSlots": "4",
                        "parallelism.default": "8"
                    }
                }
            ))
        
        # Direct runner
        if self.config['runners'].get('direct', {}).get('enabled', False):
            runners.append(RunnerInfo(
                runner_type=RunnerType.DIRECT,
                name="Direct Runner",
                description="Local runner for development and testing",
                supported_job_types=[JobType.BATCH, JobType.STREAMING],
                supported_features=["local_execution", "debugging", "fast_iteration"],
                configuration_examples={
                    "basic": {},
                    "advanced": {
                        "direct_num_workers": 4,
                        "direct_running_mode": "multi_threading"
                    }
                }
            ))
        
        return RunnerList(runners=runners) 