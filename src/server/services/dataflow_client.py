import asyncio
from typing import List, Dict, Optional
from datetime import datetime
from google.cloud import dataflow_v1beta3
from google.cloud.dataflow_v1beta3 import Job as DataflowJob
from google.cloud.dataflow_v1beta3 import JobState

from ..models.job import Job, JobParameters, JobStatus, JobType
from ..models.savepoint import Savepoint, SavepointRequest, SavepointStatus
from ..models.runner import Runner, RunnerType, RunnerStatus
from ..models.metrics import JobMetrics, Metric, MetricValue, MetricType
from ..config import Settings

class DataflowClient:
    """Client for interacting with Google Cloud Dataflow."""
    
    def __init__(self, settings: Settings):
        """Initialize the client."""
        self.settings = settings
        self.client = dataflow_v1beta3.JobsV1Beta3Client()
        
    async def create_job(self, params: JobParameters) -> Job:
        """Create a new Dataflow job."""
        try:
            # Convert parameters to Dataflow job
            job = DataflowJob()
            job.name = params.job_name
            job.project_id = params.project
            job.location = params.region
            job.type_ = DataflowJob.Type.JOB_TYPE_BATCH if params.job_type == JobType.BATCH else DataflowJob.Type.JOB_TYPE_STREAMING
            
            # Add pipeline options
            pipeline_options = {
                "project": params.project,
                "region": params.region,
                "tempLocation": params.temp_location,
                "runner": params.runner,
                **(params.pipeline_options or {})
            }
            
            if params.template_parameters:
                pipeline_options.update(params.template_parameters)
            
            job.pipeline_description.user_pipeline_options.update(pipeline_options)
            
            # Create job
            response = await asyncio.to_thread(
                self.client.create_job,
                project_id=params.project,
                location=params.region,
                job=job
            )
            
            # Convert response to Job model
            return Job(
                job_id=response.id,
                job_name=response.name,
                project=response.project_id,
                region=response.location,
                status=JobStatus.PENDING,
                create_time=response.create_time.isoformat(),
                runner=params.runner,
                job_type=params.job_type,
                pipeline_options=pipeline_options,
                current_state=response.current_state.name
            )
            
        except Exception as e:
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
        """Get details of a specific job."""
        try:
            response = await asyncio.to_thread(
                self.client.get_job,
                project_id=self.settings.gcp_project_id,
                location=self.settings.gcp_region,
                job_id=job_id
            )
            
            return Job(
                job_id=response.id,
                job_name=response.name,
                project=response.project_id,
                region=response.location,
                status=self._convert_job_state(response.current_state),
                create_time=response.create_time.isoformat(),
                start_time=response.start_time.isoformat() if response.start_time else None,
                end_time=response.current_state_time.isoformat() if response.current_state in [JobState.JOB_STATE_DONE, JobState.JOB_STATE_FAILED, JobState.JOB_STATE_CANCELLED] else None,
                runner="dataflow",
                job_type=JobType.STREAMING if response.type_ == DataflowJob.Type.JOB_TYPE_STREAMING else JobType.BATCH,
                pipeline_options=response.pipeline_description.user_pipeline_options,
                current_state=response.current_state.name,
                error_message=response.error_message if response.error_message else None
            )
            
        except Exception as e:
            raise Exception(f"Failed to get job {job_id}: {str(e)}")
    
    async def cancel_job(self, job_id: str) -> None:
        """Cancel a running job."""
        try:
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
            raise Exception(f"Failed to cancel job {job_id}: {str(e)}")
    
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
        # For now, just return Dataflow runner
        return [
            Runner(
                name="dataflow",
                runner_type=RunnerType.DATAFLOW,
                status=RunnerStatus.AVAILABLE,
                description="Google Cloud Dataflow runner",
                capabilities=["batch", "streaming", "autoscaling", "monitoring"],
                config={
                    "project": self.settings.gcp_project_id,
                    "region": self.settings.gcp_region
                },
                version="2.0.0",
                default_region=self.settings.gcp_region,
                supported_regions=["us-central1", "us-east1", "us-west1", "europe-west1"]
            )
        ]
    
    async def get_metrics(self, job_id: str) -> JobMetrics:
        """Get metrics for a job."""
        try:
            response = await asyncio.to_thread(
                self.client.get_job_metrics,
                project_id=self.settings.gcp_project_id,
                location=self.settings.gcp_region,
                job_id=job_id
            )
            
            metrics = []
            for metric in response.metrics:
                values = []
                for point in metric.time_series:
                    values.append(MetricValue(
                        value=point.value.value,
                        timestamp=point.time_stamp.isoformat(),
                        labels=point.labels
                    ))
                
                metrics.append(Metric(
                    name=metric.name.value,
                    type=self._convert_metric_type(metric.type_),
                    description=metric.description,
                    values=values,
                    unit=metric.unit,
                    namespace=metric.namespace
                ))
            
            return JobMetrics(
                job_id=job_id,
                metrics=metrics,
                timestamp=datetime.utcnow().isoformat()
            )
            
        except Exception as e:
            raise Exception(f"Failed to get metrics for job {job_id}: {str(e)}")
    
    def _convert_job_state(self, state: JobState) -> JobStatus:
        """Convert Dataflow job state to JobStatus."""
        state_map = {
            JobState.JOB_STATE_PENDING: JobStatus.PENDING,
            JobState.JOB_STATE_RUNNING: JobStatus.RUNNING,
            JobState.JOB_STATE_DONE: JobStatus.COMPLETED,
            JobState.JOB_STATE_FAILED: JobStatus.FAILED,
            JobState.JOB_STATE_CANCELLED: JobStatus.CANCELLED,
            JobState.JOB_STATE_DRAINING: JobStatus.DRAINING,
            JobState.JOB_STATE_DRAINED: JobStatus.DRAINED,
        }
        return state_map.get(state, JobStatus.FAILED)
    
    def _convert_metric_type(self, metric_type: str) -> MetricType:
        """Convert Dataflow metric type to MetricType."""
        type_map = {
            "counter": MetricType.COUNTER,
            "gauge": MetricType.GAUGE,
            "distribution": MetricType.DISTRIBUTION,
            "histogram": MetricType.HISTOGRAM
        }
        return type_map.get(metric_type.lower(), MetricType.COUNTER) 