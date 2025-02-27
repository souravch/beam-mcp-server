"""
Google Cloud Dataflow client implementation.

This module provides a concrete client implementation for interacting with
Google Cloud Dataflow.
"""

import logging
import json
from typing import Dict, Any, Optional, List
from datetime import datetime
import os
import time

try:
    from google.cloud import dataflow_v1beta3
    from google.api_core.client_options import ClientOptions
    from google.cloud.dataflow_v1beta3.types import Job as DataflowJob
    from google.cloud.dataflow_v1beta3.types import Environment, FlexTemplateRuntimeEnvironment
except ImportError:
    logging.warning("Google Cloud Dataflow libraries not installed. Install with 'pip install google-cloud-dataflow'")

from ..models import (
    RunnerType, JobState, JobType, JobParameters, JobInfo,
    JobList, JobUpdateParameters, SavepointParameters, SavepointInfo,
    SavepointList, JobMetrics, LogEntry, LogList
)

logger = logging.getLogger(__name__)

# Mapping of Dataflow job states to our JobState enum
DATAFLOW_JOB_STATE_MAP = {
    "JOB_STATE_PENDING": JobState.PENDING,
    "JOB_STATE_RUNNING": JobState.RUNNING,
    "JOB_STATE_DONE": JobState.SUCCEEDED,
    "JOB_STATE_FAILED": JobState.FAILED,
    "JOB_STATE_CANCELLED": JobState.CANCELLED,
    "JOB_STATE_UPDATED": JobState.RUNNING,
    "JOB_STATE_DRAINING": JobState.DRAINING,
    "JOB_STATE_DRAINED": JobState.SUCCEEDED,
    "JOB_STATE_UNKNOWN": JobState.UNKNOWN,
}

class DataflowClient:
    """Google Cloud Dataflow client."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the Dataflow client.
        
        Args:
            config (Dict[str, Any]): Dataflow runner configuration
        """
        self.config = config
        self.default_project = config.get('default_project', os.environ.get('GCP_PROJECT_ID', ''))
        self.default_region = config.get('default_region', os.environ.get('GCP_REGION', 'us-central1'))
        self.default_temp_location = config.get('default_temp_location', '')
        
        # Initialize Dataflow client
        try:
            client_options = ClientOptions(
                api_endpoint=f"{self.default_region}-dataflow.googleapis.com:443"
            )
            self.jobs_client = dataflow_v1beta3.JobsV1Beta3Client(client_options=client_options)
            self.templates_client = dataflow_v1beta3.TemplatesServiceClient(client_options=client_options)
            self.metrics_client = dataflow_v1beta3.MetricsV1Beta3Client(client_options=client_options)
            logger.info(f"Initialized Dataflow client for region {self.default_region}")
        except NameError:
            logger.error("Failed to initialize Dataflow client. Google Cloud libraries not installed.")
            self.jobs_client = None
            self.templates_client = None
            self.metrics_client = None
    
    async def create_job(self, params: JobParameters) -> JobInfo:
        """
        Create a new Dataflow job.
        
        Args:
            params (JobParameters): Job parameters
            
        Returns:
            JobInfo: Created job information
            
        Raises:
            ValueError: If the client is not initialized or parameters are invalid
        """
        if not self.jobs_client or not self.templates_client:
            raise ValueError("Dataflow client not initialized")
        
        # Get job parameters
        project_id = params.pipeline_options.get('project', self.default_project)
        region = params.pipeline_options.get('region', self.default_region)
        
        if not project_id:
            raise ValueError("Project ID is required")
        
        # Create job
        dataflow_job = None
        now = datetime.now().isoformat() + "Z"
        
        try:
            if params.template_path:
                # Create job from template
                template_path = params.template_path
                template_parameters = params.template_parameters or {}
                
                if template_path.startswith("gs://"):
                    # Classic template
                    parent = f"projects/{project_id}/locations/{region}"
                    
                    request = dataflow_v1beta3.LaunchTemplateRequest(
                        project_id=project_id,
                        location=region,
                        gcs_path=template_path,
                        parameters=template_parameters,
                        job_name=params.job_name,
                        environment=self._create_environment(params)
                    )
                    
                    response = self.templates_client.launch_template(request=request)
                    dataflow_job = response.job
                else:
                    # Flex template
                    parent = f"projects/{project_id}/locations/{region}"
                    
                    request = dataflow_v1beta3.LaunchFlexTemplateRequest(
                        project_id=project_id,
                        location=region,
                        launch_parameter=dataflow_v1beta3.LaunchFlexTemplateParameter(
                            job_name=params.job_name,
                            parameters=template_parameters,
                            container_spec_gcs_path=template_path,
                            environment=FlexTemplateRuntimeEnvironment(
                                **params.pipeline_options
                            )
                        )
                    )
                    
                    response = self.templates_client.launch_flex_template(request=request)
                    dataflow_job = response.job
            elif params.code_path:
                # This would be a more complex implementation using the Dataflow SDK
                # For simplicity, we're not implementing this here
                raise ValueError("Creating jobs from code_path is not implemented")
            else:
                raise ValueError("Either template_path or code_path must be provided")
            
            # Map Dataflow job to our JobInfo model
            job_info = self._dataflow_job_to_job_info(dataflow_job, params)
            
            logger.info(f"Created Dataflow job: {job_info.job_id}")
            return job_info
        
        except Exception as e:
            logger.error(f"Error creating Dataflow job: {str(e)}")
            raise ValueError(f"Failed to create Dataflow job: {str(e)}")
    
    async def list_jobs(
        self, 
        runner_type: Optional[RunnerType] = None, 
        job_type: Optional[JobType] = None, 
        page_size: int = 10, 
        page_token: Optional[str] = None
    ) -> JobList:
        """
        List Dataflow jobs.
        
        Args:
            runner_type (RunnerType, optional): Always Dataflow for this client
            job_type (JobType, optional): Filter by job type
            page_size (int): Number of jobs to return
            page_token (str, optional): Pagination token
            
        Returns:
            JobList: List of jobs
        """
        if not self.jobs_client:
            raise ValueError("Dataflow client not initialized")
        
        project_id = self.default_project
        region = self.default_region
        
        if not project_id:
            raise ValueError("Project ID is required")
        
        try:
            parent = f"projects/{project_id}/locations/{region}"
            
            # Set up filter
            filter_str = ""
            if job_type == JobType.BATCH:
                filter_str = "type=JOB_TYPE_BATCH"
            elif job_type == JobType.STREAMING:
                filter_str = "type=JOB_TYPE_STREAMING"
            
            # List jobs
            request = dataflow_v1beta3.ListJobsRequest(
                project_id=project_id,
                location=region,
                filter=filter_str,
                page_size=page_size,
                page_token=page_token
            )
            
            response = self.jobs_client.list_jobs(request=request)
            
            # Map Dataflow jobs to our JobInfo model
            jobs = []
            for job in response.jobs:
                try:
                    job_info = self._dataflow_job_to_job_info(job)
                    jobs.append(job_info)
                except Exception as e:
                    logger.error(f"Error mapping Dataflow job: {str(e)}")
            
            return JobList(
                jobs=jobs, 
                total_count=len(jobs), 
                next_page_token=response.next_page_token
            )
        
        except Exception as e:
            logger.error(f"Error listing Dataflow jobs: {str(e)}")
            raise ValueError(f"Failed to list Dataflow jobs: {str(e)}")
    
    async def get_job(self, job_id: str) -> JobInfo:
        """
        Get a Dataflow job.
        
        Args:
            job_id (str): Job ID
            
        Returns:
            JobInfo: Job information
        """
        if not self.jobs_client:
            raise ValueError("Dataflow client not initialized")
        
        project_id = self.default_project
        region = self.default_region
        
        if not project_id:
            raise ValueError("Project ID is required")
        
        try:
            # Get job
            request = dataflow_v1beta3.GetJobRequest(
                project_id=project_id,
                location=region,
                job_id=job_id
            )
            
            job = self.jobs_client.get_job(request=request)
            
            # Map Dataflow job to our JobInfo model
            job_info = self._dataflow_job_to_job_info(job)
            
            return job_info
        
        except Exception as e:
            logger.error(f"Error getting Dataflow job: {str(e)}")
            raise ValueError(f"Failed to get Dataflow job: {str(e)}")
    
    async def update_job(self, job_id: str, params: JobUpdateParameters) -> JobInfo:
        """
        Update a Dataflow job.
        
        Args:
            job_id (str): Job ID
            params (JobUpdateParameters): Update parameters
            
        Returns:
            JobInfo: Updated job information
        """
        if not self.jobs_client:
            raise ValueError("Dataflow client not initialized")
        
        project_id = self.default_project
        region = self.default_region
        
        if not project_id:
            raise ValueError("Project ID is required")
        
        try:
            # Get the current job
            job_info = await self.get_job(job_id)
            
            # Update job
            updated_params = {}
            
            # Add scaling parameters
            if params.scaling:
                updated_params.update(params.scaling)
            
            # Add other update options
            if params.update_options:
                updated_params.update(params.update_options)
            
            if not updated_params:
                # Nothing to update
                return job_info
            
            # Create update mask
            update_mask = {
                "paths": list(updated_params.keys())
            }
            
            # Update job
            request = dataflow_v1beta3.UpdateJobRequest(
                project_id=project_id,
                location=region,
                job_id=job_id,
                job=DataflowJob(
                    id=job_id,
                    name=job_info.job_name,
                    type=DataflowJob.Type.JOB_TYPE_BATCH if job_info.job_type == JobType.BATCH else DataflowJob.Type.JOB_TYPE_STREAMING,
                    environment=Environment(
                        **updated_params
                    )
                ),
                update_mask=update_mask
            )
            
            updated_job = self.jobs_client.update_job(request=request)
            
            # Map Dataflow job to our JobInfo model
            updated_job_info = self._dataflow_job_to_job_info(updated_job)
            
            return updated_job_info
        
        except Exception as e:
            logger.error(f"Error updating Dataflow job: {str(e)}")
            raise ValueError(f"Failed to update Dataflow job: {str(e)}")
    
    async def cancel_job(self, job_id: str) -> JobInfo:
        """
        Cancel a Dataflow job.
        
        Args:
            job_id (str): Job ID
            
        Returns:
            JobInfo: Updated job information
        """
        if not self.jobs_client:
            raise ValueError("Dataflow client not initialized")
        
        project_id = self.default_project
        region = self.default_region
        
        if not project_id:
            raise ValueError("Project ID is required")
        
        try:
            # Cancel job
            request = dataflow_v1beta3.UpdateJobRequest(
                project_id=project_id,
                location=region,
                job_id=job_id,
                job=DataflowJob(
                    id=job_id,
                    requested_state=DataflowJob.State.JOB_STATE_CANCELLED
                )
            )
            
            updated_job = self.jobs_client.update_job(request=request)
            
            # Map Dataflow job to our JobInfo model
            updated_job_info = self._dataflow_job_to_job_info(updated_job)
            
            return updated_job_info
        
        except Exception as e:
            logger.error(f"Error cancelling Dataflow job: {str(e)}")
            raise ValueError(f"Failed to cancel Dataflow job: {str(e)}")
    
    async def create_savepoint(self, job_id: str, params: SavepointParameters) -> SavepointInfo:
        """
        Create a savepoint for a Dataflow job.
        
        Args:
            job_id (str): Job ID
            params (SavepointParameters): Savepoint parameters
            
        Returns:
            SavepointInfo: Created savepoint information
            
        Raises:
            ValueError: Dataflow doesn't support savepoints in the same way as Flink
        """
        # Dataflow doesn't support savepoints in the same way as Flink
        # For streaming jobs, we can use snapshots, but it's a bit different
        # For simplicity, we're just creating a mock implementation here
        job_info = await self.get_job(job_id)
        
        if job_info.job_type != JobType.STREAMING:
            raise ValueError("Savepoints are only supported for streaming jobs")
        
        # Create a mock savepoint
        savepoint_id = f"sp-{datetime.now().strftime('%Y-%m-%d-%H%M%S')}"
        savepoint_location = params.savepoint_dir or f"gs://mock-bucket/snapshots/{job_id}/{savepoint_id}"
        
        # In a real implementation, we would trigger a snapshot
        # This is just a mock implementation
        time.sleep(1)  # Simulate work
        
        return SavepointInfo(
            savepoint_id=savepoint_id,
            job_id=job_id,
            create_time=datetime.now().isoformat() + "Z",
            state="COMPLETED",
            location=savepoint_location
        )
    
    async def list_savepoints(self, job_id: str) -> SavepointList:
        """
        List savepoints for a Dataflow job.
        
        Args:
            job_id (str): Job ID
            
        Returns:
            SavepointList: List of savepoints
        """
        # Mock implementation
        # In a real implementation, we would list snapshots
        return SavepointList(savepoints=[], total_count=0)
    
    async def restore_from_savepoint(self, job_id: str, savepoint_id: str) -> JobInfo:
        """
        Restore a Dataflow job from a savepoint.
        
        Args:
            job_id (str): Job ID
            savepoint_id (str): Savepoint ID
            
        Returns:
            JobInfo: Updated job information
        """
        # Mock implementation
        # In a real implementation, we would restore from a snapshot
        job_info = await self.get_job(job_id)
        return job_info
    
    async def get_job_metrics(self, job_id: str) -> JobMetrics:
        """
        Get metrics for a Dataflow job.
        
        Args:
            job_id (str): Job ID
            
        Returns:
            JobMetrics: Job metrics
        """
        if not self.metrics_client:
            raise ValueError("Dataflow client not initialized")
        
        project_id = self.default_project
        region = self.default_region
        
        if not project_id:
            raise ValueError("Project ID is required")
        
        try:
            # Get job metrics
            request = dataflow_v1beta3.GetJobMetricsRequest(
                project_id=project_id,
                location=region,
                job_id=job_id
            )
            
            response = self.metrics_client.get_job_metrics(request=request)
            
            # Map metrics
            metrics_dict = {}
            
            # Add metrics
            for metric in response.metrics:
                key = metric.name.name
                value = metric.scalar
                metrics_dict[key] = value
            
            return JobMetrics(
                job_id=job_id,
                timestamp=datetime.now().isoformat() + "Z",
                metrics=metrics_dict
            )
        
        except Exception as e:
            logger.error(f"Error getting Dataflow job metrics: {str(e)}")
            raise ValueError(f"Failed to get Dataflow job metrics: {str(e)}")
    
    async def get_job_logs(
        self, 
        job_id: str, 
        max_results: int = 10, 
        page_token: Optional[str] = None
    ) -> LogList:
        """
        Get logs for a Dataflow job.
        
        Args:
            job_id (str): Job ID
            max_results (int): Maximum number of log entries
            page_token (str, optional): Pagination token
            
        Returns:
            LogList: List of log entries
        """
        # In a real implementation, we would use the Cloud Logging API
        # This is just a mock implementation
        
        # Mock log entries
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
    
    def _dataflow_job_to_job_info(self, job, params: Optional[JobParameters] = None) -> JobInfo:
        """
        Convert a Dataflow job to our JobInfo model.
        
        Args:
            job: Dataflow job
            params (JobParameters, optional): Original job parameters
            
        Returns:
            JobInfo: Job information
        """
        # Map job type
        job_type = JobType.BATCH
        if hasattr(job, 'type') and job.type:
            # Use the job type from the job if available
            if job.type == "JOB_TYPE_STREAMING":
                job_type = JobType.STREAMING
        elif params and params.job_type:
            # Fall back to the job type from the parameters
            job_type = params.job_type
        
        # Map job state
        current_state = JobState.UNKNOWN
        if hasattr(job, 'current_state') and job.current_state:
            current_state = DATAFLOW_JOB_STATE_MAP.get(job.current_state, JobState.UNKNOWN)
        
        # Extract create and update times
        create_time = job.create_time.isoformat() + "Z" if hasattr(job, 'create_time') else datetime.now().isoformat() + "Z"
        update_time = job.current_state_time.isoformat() + "Z" if hasattr(job, 'current_state_time') else datetime.now().isoformat() + "Z"
        
        # Get template path and code path from params or job
        template_path = None
        code_path = None
        
        if params:
            template_path = params.template_path
            code_path = params.code_path
        
        # Convert pipeline options
        pipeline_options = {}
        if hasattr(job, 'environment') and job.environment:
            if hasattr(job.environment, 'sdk_pipeline_options') and job.environment.sdk_pipeline_options:
                pipeline_options = json.loads(job.environment.sdk_pipeline_options.additional_properties)
            
            # Add other environment properties
            for key, value in job.environment._pb.items():
                if key not in ['sdk_pipeline_options'] and value:
                    pipeline_options[key] = value
        
        # Create job info
        job_info = JobInfo(
            job_id=job.id,
            job_name=job.name,
            runner_type=RunnerType.DATAFLOW,
            create_time=create_time,
            update_time=update_time,
            current_state=current_state,
            job_type=job_type,
            pipeline_options=pipeline_options,
            template_path=template_path,
            code_path=code_path,
            runner_job_id=job.id,
            runner_job_url=f"https://console.cloud.google.com/dataflow/jobs/{self.default_region}/{job.id}"
        )
        
        return job_info
    
    def _create_environment(self, params: JobParameters) -> Environment:
        """
        Create a Dataflow job environment.
        
        Args:
            params (JobParameters): Job parameters
            
        Returns:
            Environment: Dataflow job environment
        """
        # Create environment
        environment_options = params.pipeline_options.copy()
        
        # Set default options if not provided
        if 'tempLocation' not in environment_options and self.default_temp_location:
            environment_options['tempLocation'] = self.default_temp_location
        
        return Environment(**environment_options) 