"""
Job models for the Dataflow MCP Server.

This module defines the data models for job execution and status tracking.
"""

from enum import Enum
from typing import Dict, List, Optional, Any, Union, Literal
from datetime import datetime
from pydantic import BaseModel, Field, field_validator, model_validator, ConfigDict
#from mcp.core import MCPModel
from .base import BaseMCPModel
from .base import BaseMCPModel
from .jobs import JobParameters

class JobStatus(str, Enum):
    """Status of a job."""
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    DRAINING = "DRAINING"
    DRAINED = "DRAINED"

class JobType(str, Enum):
    """Type of job execution."""
    BATCH = "BATCH"
    STREAMING = "STREAMING"

class MetricValue(BaseModel):
    """Value of a metric."""
    value: Union[int, float, str, bool]
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    labels: Optional[Dict[str, str]] = None

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "value": 42,
                "timestamp": "2023-02-25T12:00:00",
                "labels": {"source": "pipeline_step_1"}
            }
        }
    )

class JobMetrics(BaseModel):
    """Metrics for a job."""
    metrics: Dict[str, List[MetricValue]] = Field(default_factory=dict)
    
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "metrics": {
                    "records_processed": [
                        {"value": 100, "timestamp": "2023-02-25T12:00:00", "labels": {"step": "read"}}
                    ]
                }
            }
        }
    )
    
    def add_metric(self, name: str, value: Any, labels: Optional[Dict[str, str]] = None):
        """Add a metric value."""
        if name not in self.metrics:
            self.metrics[name] = []
        
        self.metrics[name].append(
            MetricValue(
                value=value,
                timestamp=datetime.utcnow(),
                labels=labels
            )
        )

class JobParameters(BaseMCPModel):
    """Parameters for creating a job."""
    mcp_resource_type: Literal["job_parameters"] = Field(default="job_parameters")
    
    job_name: str = Field(..., description="Name of the job")
    pipeline_path: str = Field(..., description="Path to the pipeline code or template")
    runner: str = Field(default="dataflow", description="Runner to use (dataflow, spark, flink)")
    project: str = Field(..., description="Google Cloud project ID")
    region: str = Field(default="us-central1", description="Google Cloud region")
    temp_location: str = Field(..., description="GCS location for temporary files")
    job_type: JobType = Field(default=JobType.BATCH, description="Type of job (batch or streaming)")
    pipeline_options: Optional[Dict] = Field(default=None, description="Additional pipeline options")
    template_parameters: Optional[Dict] = Field(default=None, description="Template parameters if using a template")

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "mcp_resource_type": "job_parameters",
                "job_name": "wordcount",
                "pipeline_path": "gs://dataflow-templates/wordcount",
                "runner": "dataflow",
                "project": "my-gcp-project",
                "region": "us-central1",
                "temp_location": "gs://my-bucket/temp",
                "job_type": "BATCH",
                "pipeline_options": {"maxWorkers": 5},
                "template_parameters": {"inputFile": "gs://my-bucket/input.txt"}
            }
        }
    )

class JobCreateRequest(BaseModel):
    """Request model for creating a new job."""
    job_type: JobType = Field(..., description="Type of job to create")
    parameters: JobParameters = Field(..., description="Parameters for job execution")
    description: Optional[str] = Field(None, description="Human-readable description of the job")
    labels: Optional[Dict[str, str]] = Field(None, description="Labels for the job")

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "job_type": "BATCH",
                "parameters": {
                    "job_name": "wordcount",
                    "pipeline_path": "gs://dataflow-templates/wordcount",
                    "runner": "dataflow",
                    "project": "my-gcp-project",
                    "region": "us-central1",
                    "temp_location": "gs://my-bucket/temp"
                },
                "description": "Word count job example",
                "labels": {"env": "dev"}
            }
        }
    )

class JobDetails(BaseModel):
    """Details of a job execution."""
    id: str = Field(..., description="Unique identifier for the job")
    pipeline_id: str = Field(..., description="ID of the pipeline")
    pipeline_version: str = Field(..., description="Version of the pipeline")
    job_type: JobType = Field(..., description="Type of job")
    status: JobStatus = Field(..., description="Current status of the job")
    parameters: JobParameters = Field(..., description="Parameters used for this job")
    description: Optional[str] = Field(None, description="Human-readable description")
    labels: Optional[Dict[str, str]] = Field(None, description="Labels for the job")
    
    # Execution details
    runner_job_id: Optional[str] = Field(None, description="ID of the job in the runner system")
    runner_job_url: Optional[str] = Field(None, description="URL to the job in the runner system")
    start_time: Optional[datetime] = Field(None, description="Time when the job started")
    end_time: Optional[datetime] = Field(None, description="Time when the job ended")
    execution_duration_seconds: Optional[int] = Field(None, description="Duration of job execution in seconds")
    
    # Metrics and results
    metrics: Optional[JobMetrics] = Field(None, description="Metrics for this job")
    input_records: Optional[int] = Field(None, description="Number of input records processed")
    output_records: Optional[Dict[str, int]] = Field(None, description="Number of output records by sink")
    error_count: Optional[int] = Field(None, description="Number of errors during execution")
    
    # Metadata
    created_at: datetime = Field(..., description="Time when the job was created")
    updated_at: datetime = Field(..., description="Time when the job was last updated")
    created_by: Optional[str] = Field(None, description="Creator identifier")

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "id": "job-123",
                "pipeline_id": "pipeline-abc",
                "pipeline_version": "1.0.0",
                "job_type": "BATCH",
                "status": "RUNNING",
                "parameters": {
                    "job_name": "wordcount",
                    "pipeline_path": "gs://dataflow-templates/wordcount",
                    "runner": "dataflow",
                    "project": "my-gcp-project",
                    "region": "us-central1",
                    "temp_location": "gs://my-bucket/temp"
                },
                "description": "Word count job",
                "labels": {"env": "dev"},
                "runner_job_id": "dataflow-job-456",
                "runner_job_url": "https://console.cloud.google.com/dataflow/jobs/us-central1/dataflow-job-456",
                "start_time": "2023-02-25T12:00:00",
                "created_at": "2023-02-25T11:50:00",
                "updated_at": "2023-02-25T12:05:00"
            }
        }
    )
    
    @field_validator('execution_duration_seconds', mode='before')
    @classmethod
    def calculate_duration(cls, v, info):
        """Calculate the execution duration if start and end times are available."""
        values = info.data
        start_time = values.get('start_time')
        end_time = values.get('end_time')
        
        if start_time and end_time:
            return int((end_time - start_time).total_seconds())
        return v

class JobUpdateRequest(BaseModel):
    """Request model for updating an existing job."""
    status: Optional[JobStatus] = None
    runner_job_id: Optional[str] = None
    runner_job_url: Optional[str] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    metrics: Optional[JobMetrics] = None
    input_records: Optional[int] = None
    output_records: Optional[Dict[str, int]] = None
    error_count: Optional[int] = None
    labels: Optional[Dict[str, str]] = None

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "status": "COMPLETED",
                "end_time": "2023-02-25T13:00:00",
                "metrics": {
                    "metrics": {
                        "records_processed": [
                            {"value": 1000, "timestamp": "2023-02-25T13:00:00"}
                        ]
                    }
                },
                "input_records": 1000,
                "output_records": {"main_output": 1000},
                "error_count": 0
            }
        }
    )

class JobListResponse(BaseModel):
    """Response model for listing jobs."""
    total: int
    jobs: List[JobDetails]

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "total": 1,
                "jobs": [
                    {
                        "id": "job-123",
                        "pipeline_id": "pipeline-abc",
                        "pipeline_version": "1.0.0",
                        "job_type": "BATCH",
                        "status": "RUNNING",
                        "parameters": {
                            "job_name": "wordcount",
                            "pipeline_path": "gs://dataflow-templates/wordcount",
                            "runner": "dataflow",
                            "project": "my-gcp-project",
                            "region": "us-central1",
                            "temp_location": "gs://my-bucket/temp"
                        },
                        "created_at": "2023-02-25T11:50:00",
                        "updated_at": "2023-02-25T12:05:00"
                    }
                ]
            }
        }
    )

class JobDetailResponse(BaseModel):
    """Response model for getting job details."""
    job: JobDetails

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "job": {
                    "id": "job-123",
                    "pipeline_id": "pipeline-abc",
                    "pipeline_version": "1.0.0",
                    "job_type": "BATCH",
                    "status": "RUNNING",
                    "parameters": {
                        "job_name": "wordcount",
                        "pipeline_path": "gs://dataflow-templates/wordcount",
                        "runner": "dataflow",
                        "project": "my-gcp-project",
                        "region": "us-central1",
                        "temp_location": "gs://my-bucket/temp"
                    },
                    "created_at": "2023-02-25T11:50:00",
                    "updated_at": "2023-02-25T12:05:00"
                }
            }
        }
    )

class JobLogEntry(BaseModel):
    """Log entry for a job."""
    timestamp: datetime = Field(..., description="Timestamp of the log entry")
    level: str = Field(..., description="Log level")
    message: str = Field(..., description="Log message")
    job_id: str = Field(..., description="ID of the job")
    worker_id: Optional[str] = Field(None, description="ID of the worker")
    step: Optional[str] = Field(None, description="Step in the pipeline")
    labels: Optional[Dict[str, str]] = Field(None, description="Additional labels")

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "timestamp": "2023-02-25T12:05:00",
                "level": "INFO",
                "message": "Processing input file",
                "job_id": "job-123",
                "worker_id": "worker-1",
                "step": "read-input"
            }
        }
    )

class JobLogsResponse(BaseModel):
    """Response model for getting job logs."""
    job_id: str
    logs: List[JobLogEntry]

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "job_id": "job-123",
                "logs": [
                    {
                        "timestamp": "2023-02-25T12:05:00",
                        "level": "INFO",
                        "message": "Processing input file",
                        "job_id": "job-123",
                        "worker_id": "worker-1",
                        "step": "read-input"
                    }
                ]
            }
        }
    )
    
class JobCancelRequest(BaseModel):
    """Request model for cancelling a job."""
    drain: bool = Field(False, description="Whether to drain the job instead of cancelling immediately")

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "drain": True
            }
        }
    )

class Job(BaseMCPModel):
    """Represents a Dataflow job."""
    mcp_resource_type: Literal["job"] = Field(default="job")
    
    # Job-specific fields
    job_id: str = Field(..., description="Unique identifier for the job")
    job_name: str = Field(..., description="Name of the job")
    project: str = Field(..., description="Google Cloud project ID")
    region: str = Field(..., description="Google Cloud region")
    status: JobStatus = Field(..., description="Current status of the job")
    create_time: str = Field(..., description="Job creation timestamp")
    start_time: Optional[str] = Field(None, description="Job start timestamp")
    end_time: Optional[str] = Field(None, description="Job end timestamp")
    runner: str = Field(..., description="Runner used for the job")
    job_type: JobType = Field(..., description="Type of job (batch or streaming)")
    pipeline_options: Dict = Field(default_factory=dict, description="Pipeline options used")
    current_state: str = Field(..., description="Current state of the job")
    error_message: Optional[str] = Field(None, description="Error message if job failed")
    
    # MCP-specific fields
    mcp_owner: Optional[str] = Field(None, description="Owner of the job")
    mcp_priority: int = Field(default=0, description="Job priority")
    mcp_dependencies: List[str] = Field(default_factory=list, description="Job dependencies")
    mcp_retry_count: int = Field(default=0, description="Number of retries")
    mcp_max_retries: int = Field(default=3, description="Maximum number of retries")
    mcp_timeout_seconds: Optional[int] = Field(None, description="Job timeout in seconds")
    mcp_tags: List[str] = Field(default_factory=list, description="Job tags")

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "mcp_resource_type": "job",
                "job_id": "dataflow-job-456",
                "job_name": "wordcount",
                "project": "my-gcp-project",
                "region": "us-central1",
                "status": "RUNNING",
                "create_time": "2023-02-25T11:50:00",
                "start_time": "2023-02-25T12:00:00",
                "runner": "dataflow",
                "job_type": "BATCH",
                "current_state": "RUNNING",
                "mcp_tags": ["production", "wordcount"]
            }
        }
    )
    
    @field_validator('mcp_resource_id', mode='before')
    @classmethod
    def set_resource_id(cls, v, info):
        """Set resource ID from job_id if not provided."""
        if not v and 'job_id' in info.data:
            return info.data['job_id']
        return v

class JobList(BaseMCPModel):
    """List of jobs with pagination."""
    mcp_resource_type: Literal["job_list"] = Field(default="job_list")
    
    jobs: List[Job] = Field(..., description="List of jobs")
    total: int = Field(..., description="Total number of jobs")
    page: int = Field(default=1, description="Current page number")
    page_size: int = Field(default=10, description="Number of jobs per page")
    
    # MCP-specific fields
    mcp_continuation_token: Optional[str] = Field(None, description="Token for getting next page")
    mcp_filter_criteria: Optional[Dict] = Field(None, description="Applied filters")
    mcp_sort_criteria: Optional[Dict] = Field(None, description="Applied sorting")

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "mcp_resource_type": "job_list",
                "jobs": [
                    {
                        "mcp_resource_type": "job",
                        "job_id": "dataflow-job-456",
                        "job_name": "wordcount",
                        "project": "my-gcp-project",
                        "region": "us-central1",
                        "status": "RUNNING",
                        "create_time": "2023-02-25T11:50:00",
                        "start_time": "2023-02-25T12:00:00",
                        "runner": "dataflow",
                        "job_type": "BATCH",
                        "current_state": "RUNNING"
                    }
                ],
                "total": 1,
                "page": 1,
                "page_size": 10
            }
        }
    ) 