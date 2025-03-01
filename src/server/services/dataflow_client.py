import asyncio
from typing import List, Dict, Optional
from datetime import datetime, UTC
from google.cloud import dataflow_v1beta3
from google.cloud.dataflow_v1beta3 import Job as DataflowJob
from google.cloud.dataflow_v1beta3 import JobState
import logging
from fastapi import HTTPException

from ..models.job import Job, JobParameters, JobStatus, JobType
from ..models.savepoint import Savepoint, SavepointRequest, SavepointStatus
from ..models.runner import Runner, RunnerType, RunnerStatus, RunnerCapability
from ..models.metrics import JobMetrics, Metric, MetricValue, MetricType
from ..config import Settings

logger = logging.getLogger(__name__)

class DataflowClient:
    """Client for interacting with Google Cloud Dataflow."""
    
    def __init__(self, settings: Settings):
        """Initialize the client."""
        self.settings = settings
        self.client = dataflow_v1beta3.JobsV1Beta3Client()
        self._local_jobs = {}  # Track local job statuses
        
    async def create_job(self, params: JobParameters) -> Job:
        """Create a new Dataflow job."""
        try:
            # For Direct runner jobs (local jobs)
            if params.runner_type == RunnerType.DIRECT:
                job_id = f"local-{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}"
                job = Job(
                    mcp_resource_id=job_id,
                    job_id=job_id,
                    job_name=params.job_name,
                    project="local",
                    region="local",
                    status=JobStatus.RUNNING,
                    create_time=datetime.utcnow().isoformat(),
                    update_time=datetime.utcnow().isoformat(),
                    runner=params.runner_type.value,
                    job_type=params.job_type,
                    pipeline_options=params.pipeline_options,
                    current_state="RUNNING"
                )
                self._local_jobs[job_id] = job
                
                # Import and run the pipeline
                import importlib.util
                import sys
                
                # Import the pipeline module
                spec = importlib.util.spec_from_file_location("pipeline", params.code_path)
                module = importlib.util.module_from_spec(spec)
                sys.modules["pipeline"] = module
                spec.loader.exec_module(module)
                
                # Create pipeline options
                from apache_beam.options.pipeline_options import PipelineOptions
                pipeline_options = PipelineOptions(flags=[], **params.pipeline_options)
                
                # Run the pipeline
                module.run(pipeline_options)
                
                return job

            # For Dataflow runner, get project and region from pipeline options
            project = params.pipeline_options.get("project")
            region = params.pipeline_options.get("region")
            
            # Use settings values as fallbacks if not provided in pipeline options
            if not project:
                project = self.settings.gcp_project_id
                if not project or project == "your-gcp-project-id":
                    raise ValueError("Project ID is required in pipeline_options for Dataflow runner")
                # Add to pipeline options
                params.pipeline_options["project"] = project
                
            if not region:
                region = self.settings.gcp_region
                # Add to pipeline options
                params.pipeline_options["region"] = region

            # Convert parameters to Dataflow job
            job = DataflowJob()
            job.name = params.job_name
            job.project_id = project
            job.location = region
            job.type_ = DataflowJob.Type.JOB_TYPE_BATCH if params.job_type == JobType.BATCH else DataflowJob.Type.JOB_TYPE_STREAMING
            
            # Add pipeline options
            pipeline_options = params.pipeline_options.copy()
            
            if params.template_parameters:
                pipeline_options.update(params.template_parameters)
            
            job.pipeline_description.user_pipeline_options.update(pipeline_options)
            
            # Create job
            response = await asyncio.to_thread(
                self.client.create_job,
                project_id=project,
                location=region,
                job=job
            )
            
            # Convert response to Job model
            return Job(
                mcp_resource_id=response.id,
                job_id=response.id,
                job_name=response.name,
                project=response.project_id,
                region=response.location,
                status=JobStatus.PENDING,
                create_time=response.create_time.isoformat(),
                update_time=datetime.utcnow().isoformat(),
                runner=params.runner_type.value,
                job_type=params.job_type,
                pipeline_options=pipeline_options,
                current_state=response.current_state.name
            )
        except Exception as e:
            logger.error("Error creating Dataflow job: %s", str(e))
            raise Exception(f"Failed to create job: {str(e)}")
    
    async def list_jobs(self) -> List[Job]:
        """List all jobs."""
        try:
            response = await asyncio.to_thread(
                self.client.list_jobs,
                project_id=self.settings.gcp_project_id,
                location=self.settings.gcp_region
            )
            
            jobs = []
            for job in response:
                jobs.append(Job(
                    job_id=job.id,
                    job_name=job.name,
                    project=job.project_id,
                    region=job.location,
                    status=self._convert_job_state(job.current_state),
                    create_time=job.create_time.isoformat(),
                    start_time=job.start_time.isoformat() if job.start_time else None,
                    end_time=job.current_state_time.isoformat() if job.current_state in [JobState.JOB_STATE_DONE, JobState.JOB_STATE_FAILED, JobState.JOB_STATE_CANCELLED] else None,
                    runner="dataflow",
                    job_type=JobType.STREAMING if job.type_ == DataflowJob.Type.JOB_TYPE_STREAMING else JobType.BATCH,
                    pipeline_options=job.pipeline_description.user_pipeline_options,
                    current_state=job.current_state.name,
                    error_message=job.error_message if job.error_message else None
                ))
            
            return jobs
            
        except Exception as e:
            raise Exception(f"Failed to list jobs: {str(e)}")
    
    async def get_job(self, job_id: str) -> Job:
        """Get job details."""
        try:
            if job_id.startswith('local-'):
                if job_id not in self._local_jobs:
                    raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
                job = self._local_jobs[job_id]
                
                # For local jobs, check if the process has completed
                if job.status == JobStatus.RUNNING:
                    # For Direct runner jobs, we assume they complete quickly
                    # In a production environment, you would want to actually check the process status
                    job.status = JobStatus.COMPLETED
                    job.end_time = datetime.now(UTC).isoformat()
                    self._local_jobs[job_id] = job
                
                return job
            else:
                # For Dataflow jobs, we need to handle the case where the job doesn't exist
                try:
                    # Try to get the job from Dataflow
                    job = await asyncio.to_thread(
                        self.client.get_job,
                        project_id=self.settings.gcp_project_id,
                        location=self.settings.gcp_region,
                        job_id=job_id
                    )
                    return self._convert_dataflow_job_to_job(job)
                except Exception:
                    # Any error from the Dataflow client means the job doesn't exist
                    raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
    
    async def cancel_job(self, job_id: str) -> None:
        """Cancel a running job."""
        try:
            # For Direct runner jobs (local jobs)
            if job_id.startswith('local-'):
                job = self._local_jobs.get(job_id)
                if not job:
                    raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
                job.status = JobStatus.CANCELLED
                job.current_state = "CANCELLED"
                return

            # For Dataflow jobs
            await asyncio.to_thread(
                self.client.update_job,
                project_id=self.settings.gcp_project_id,
                location=self.settings.gcp_region,
                job_id=job_id,
                job=DataflowJob(
                    requested_state=JobState.JOB_STATE_CANCELLED
                )
            )
        except Exception as e:
            logger.error(f"Failed to cancel job {job_id}: {str(e)}")
            raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
    
    async def drain_job(self, job_id: str) -> None:
        """Drain a running job."""
        try:
            await asyncio.to_thread(
                self.client.update_job,
                project_id=self.settings.gcp_project_id,
                location=self.settings.gcp_region,
                job_id=job_id,
                job=DataflowJob(
                    requested_state=JobState.JOB_STATE_DRAINING
                )
            )
        except Exception as e:
            raise Exception(f"Failed to drain job {job_id}: {str(e)}")
    
    async def create_savepoint(self, params: SavepointRequest) -> Savepoint:
        """Create a savepoint for a job."""
        try:
            # Get job to verify it exists and is running
            job = await self.get_job(params.job_id)
            if job.status not in [JobStatus.RUNNING]:
                raise Exception("Job must be running to create a savepoint")
            
            # Create savepoint
            savepoint_id = f"sp-{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}"
            
            # Update job with savepoint request
            await asyncio.to_thread(
                self.client.update_job,
                project_id=self.settings.gcp_project_id,
                location=self.settings.gcp_region,
                job_id=params.job_id,
                job=DataflowJob(
                    requested_state=JobState.JOB_STATE_DRAINING if params.drain else JobState.JOB_STATE_RUNNING,
                    pipeline_description={
                        "transform_name_mapping": {
                            "savepoint": {
                                "user_name": savepoint_id,
                                "system_name": params.savepoint_path
                            }
                        }
                    }
                )
            )
            
            return Savepoint(
                savepoint_id=savepoint_id,
                job_id=params.job_id,
                status=SavepointStatus.PENDING,
                create_time=datetime.utcnow().isoformat(),
                savepoint_path=params.savepoint_path
            )
            
        except Exception as e:
            raise Exception(f"Failed to create savepoint: {str(e)}")
    
    async def list_runners(self) -> List[Runner]:
        """List available runners."""
        try:
            logger.debug("Listing available runners")
            
            # Create a list for runners
            runners = []
            
            # Check if Direct runner is enabled in settings
            if getattr(self.settings, 'enable_direct_runner', True):
                logger.debug("Adding Direct runner to available runners")
                runners.append(Runner(
                    mcp_resource_id="direct",
                    name="Direct Runner",
                    runner_type=RunnerType.DIRECT,
                    status=RunnerStatus.AVAILABLE,
                    description="Apache Beam Direct runner for local development and testing",
                    capabilities=["batch", "monitoring"],
                    config={
                        "direct_num_workers": 4,
                        "direct_running_mode": "multi_threading",
                        "temp_location": "/tmp/beam-test",
                        "save_main_session": True
                    },
                    version="2.0.0",
                    mcp_provider="apache"
                ))
            
            # Only add Dataflow runner if relevant settings are configured properly
            if (hasattr(self.settings, 'gcp_project_id') and 
                self.settings.gcp_project_id and 
                self.settings.gcp_project_id != "your-gcp-project-id" and
                getattr(self.settings, 'enable_dataflow_runner', True)):
                
                logger.debug("Adding Dataflow runner to available runners")
                runners.append(Runner(
                    mcp_resource_id="dataflow",
                    name="Google Cloud Dataflow",
                    runner_type=RunnerType.DATAFLOW,
                    status=RunnerStatus.AVAILABLE,
                    description="Fully managed runner for Apache Beam on Google Cloud",
                    capabilities=["batch", "streaming", "autoscaling", "monitoring"],
                    config={
                        "project": self.settings.gcp_project_id,
                        "region": self.settings.gcp_region,
                        "temp_location": "gs://my-bucket/temp"
                    },
                    version="2.0.0",
                    default_region=self.settings.gcp_region,
                    supported_regions=["us-central1", "us-east1", "us-west1", "europe-west1"],
                    mcp_provider="google"
                ))
            
            logger.debug(f"Found {len(runners)} runners in DataflowClient.list_runners")
            return runners
        except Exception as e:
            logger.error(f"Error listing runners: {str(e)}", exc_info=True)
            raise Exception(f"Failed to list runners: {str(e)}")
    
    def _job_status_to_numeric(self, status: JobStatus) -> int:
        """Convert job status to numeric value."""
        status_map = {
            JobStatus.PENDING: 0,
            JobStatus.RUNNING: 1,
            JobStatus.COMPLETED: 2,
            JobStatus.FAILED: 3,
            JobStatus.CANCELLED: 4,
            JobStatus.DRAINING: 5,
            JobStatus.DRAINED: 6
        }
        return status_map.get(status, -1)

    async def get_metrics(self, job_id: str) -> JobMetrics:
        """Get metrics for a job."""
        try:
            if job_id.startswith("local-"):
                if job_id not in self._local_jobs:
                    raise ValueError(f"Job {job_id} not found")
                
                job = self._local_jobs[job_id]
                timestamp = datetime.now(UTC).isoformat()
                # For Direct runner jobs, we return basic metrics
                return JobMetrics(
                    job_id=job_id,
                    mcp_resource_id=f"{job_id}-metrics-{timestamp}",
                    metrics=[
                        Metric(
                            name="job_status",
                            type=MetricType.GAUGE,
                            description="Current job status",
                            mcp_resource_id=f"{job_id}-metric-status",
                            values=[
                                MetricValue(
                                    value=self._job_status_to_numeric(job.status),
                                    timestamp=timestamp,
                                    mcp_resource_id=f"{job_id}-metric-status-value",
                                    mcp_resource_type="metric_value"
                                )
                            ]
                        )
                    ],
                    timestamp=timestamp
                )
            else:
                # For Dataflow jobs
                response = await asyncio.to_thread(
                    self.client.get_job_metrics,
                    project_id=self.settings.gcp_project_id,
                    location=self.settings.gcp_region,
                    job_id=job_id
                )
                
                metrics = []
                timestamp = datetime.now(UTC).isoformat()
                for metric in response.metrics:
                    values = []
                    for point in metric.time_series:
                        values.append(MetricValue(
                            value=point.value.value,
                            timestamp=point.time_stamp.isoformat(),
                            labels=point.labels,
                            mcp_resource_id=f"{job_id}-metric-{metric.name.value}-value-{point.time_stamp.isoformat()}",
                            mcp_resource_type="metric_value"
                        ))
                    
                    metrics.append(Metric(
                        name=metric.name.value,
                        type=self._convert_metric_type(metric.type_),
                        description=metric.description,
                        values=values,
                        unit=metric.unit,
                        namespace=metric.namespace,
                        mcp_resource_id=f"{job_id}-metric-{metric.name.value}"
                    ))
                
                return JobMetrics(
                    job_id=job_id,
                    metrics=metrics,
                    timestamp=timestamp,
                    mcp_resource_id=f"{job_id}-metrics-{timestamp}"
                )
                
        except ValueError as e:
            logger.error(f"Error getting job metrics: {str(e)}")
            raise HTTPException(status_code=404, detail=str(e))
        except Exception as e:
            logger.error(f"Error getting job metrics: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))
    
    def _convert_job_state(self, state: JobState) -> JobStatus:
        """Convert Dataflow job state to MCP job status."""
        state_map = {
            JobState.JOB_STATE_PENDING: JobStatus.PENDING,
            JobState.JOB_STATE_RUNNING: JobStatus.RUNNING,
            JobState.JOB_STATE_DONE: JobStatus.COMPLETED,
            JobState.JOB_STATE_FAILED: JobStatus.FAILED,
            JobState.JOB_STATE_CANCELLED: JobStatus.CANCELLED,
            JobState.JOB_STATE_DRAINING: JobStatus.DRAINING,
            JobState.JOB_STATE_DRAINED: JobStatus.DRAINED,
            JobState.JOB_STATE_UPDATED: JobStatus.COMPLETED,
            JobState.JOB_STATE_UNKNOWN: JobStatus.FAILED
        }
        return state_map.get(state, JobStatus.FAILED)
    
    def _convert_metric_type(self, metric_type: str) -> MetricType:
        """Convert Dataflow metric type to MCP metric type."""
        type_map = {
            "counter": MetricType.COUNTER,
            "gauge": MetricType.GAUGE,
            "distribution": MetricType.DISTRIBUTION,
            "histogram": MetricType.HISTOGRAM
        }
        return type_map.get(metric_type.lower(), MetricType.COUNTER) 