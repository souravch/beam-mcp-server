"""
Job models for the Apache Beam MCP server.

This module defines data models for job management operations.
"""

from typing import Dict, List, Optional, Any
from datetime import datetime
from pydantic import BaseModel, Field, validator, root_validator

from .common import RunnerType, JobState, JobType

class JobParameters(BaseModel):
    """Parameters for creating a pipeline job."""
    job_name: str = Field(
        ..., 
        description="Unique name for the job",
        example="daily-user-activity-analysis"
    )
    runner_type: RunnerType = Field(
        RunnerType.DATAFLOW, 
        description="Type of runner to use (dataflow, spark, flink, direct)"
    )
    pipeline_options: Dict[str, Any] = Field(
        ..., 
        description="Runner-specific pipeline options",
        example={
            "project": "my-gcp-project",
            "region": "us-central1",
            "tempLocation": "gs://my-bucket/temp",
            "serviceAccount": "sa@project.iam.gserviceaccount.com"
        }
    )
    template_path: Optional[str] = Field(
        None, 
        description="Path to pipeline template",
        example="gs://dataflow-templates/latest/Word_Count"
    )
    template_parameters: Optional[Dict[str, Any]] = Field(
        None, 
        description="Parameters for the pipeline template",
        example={"inputFile": "gs://dataflow-samples/shakespeare/kinglear.txt", "output": "gs://my-bucket/output/"}
    )
    code_path: Optional[str] = Field(
        None, 
        description="Path to pipeline code (JAR for Spark/Flink, Python file for Dataflow)",
        example="gs://my-bucket/pipelines/wordcount.py"
    )
    job_type: JobType = Field(
        JobType.BATCH, 
        description="Job type (BATCH or STREAMING)"
    )
    
    @root_validator
    def validate_job_source(cls, values):
        """Validate that either template_path or code_path is provided."""
        if not values.get('template_path') and not values.get('code_path'):
            raise ValueError("Either template_path or code_path must be provided")
        return values

class JobInfo(BaseModel):
    """Job details."""
    job_id: str = Field(..., description="Unique job ID")
    job_name: str = Field(..., description="Job name")
    runner_type: RunnerType = Field(..., description="Runner type")
    create_time: str = Field(..., description="Job creation time (ISO format)")
    update_time: str = Field(..., description="Last job update time (ISO format)")
    current_state: JobState = Field(..., description="Current job state")
    job_type: JobType = Field(..., description="Job type (BATCH or STREAMING)")
    pipeline_options: Dict[str, Any] = Field(..., description="Pipeline options used")
    metrics: Optional[Dict[str, Any]] = Field(None, description="Latest job metrics")
    template_path: Optional[str] = Field(None, description="Template path if used")
    code_path: Optional[str] = Field(None, description="Code path if used")
    runner_job_id: Optional[str] = Field(None, description="ID in the runner system")
    runner_job_url: Optional[str] = Field(None, description="URL to job in runner system")

class JobList(BaseModel):
    """List of jobs."""
    jobs: List[JobInfo] = Field(..., description="List of jobs")
    total_count: int = Field(..., description="Total number of jobs")
    next_page_token: Optional[str] = Field(None, description="Token for the next page")

class JobUpdateParameters(BaseModel):
    """Parameters for updating a job."""
    scaling: Optional[Dict[str, Any]] = Field(
        None, 
        description="Scaling parameters",
        example={"maxWorkers": 20, "minWorkers": 5}
    )
    update_options: Optional[Dict[str, Any]] = Field(
        None, 
        description="Runner-specific update options",
        example={"machineType": "n2-standard-4"}
    )

class SavepointParameters(BaseModel):
    """Parameters for creating a savepoint."""
    drain: bool = Field(
        False, 
        description="Whether to drain the pipeline during savepointing",
        example=False
    )
    async_mode: bool = Field(
        True, 
        description="Whether to create the savepoint asynchronously",
        example=True
    )
    savepoint_dir: Optional[str] = Field(
        None, 
        description="Directory to store the savepoint",
        example="gs://my-bucket/savepoints/"
    )

class SavepointInfo(BaseModel):
    """Savepoint details."""
    savepoint_id: str = Field(..., description="Unique savepoint ID")
    job_id: str = Field(..., description="Job ID")
    create_time: str = Field(..., description="Creation time (ISO format)")
    state: str = Field(..., description="Savepoint state")
    location: str = Field(..., description="Storage location of the savepoint")

class SavepointList(BaseModel):
    """List of savepoints."""
    savepoints: List[SavepointInfo] = Field(..., description="List of savepoints")
    total_count: int = Field(..., description="Total number of savepoints")

class JobMetrics(BaseModel):
    """Detailed job metrics."""
    job_id: str = Field(..., description="Job ID")
    timestamp: str = Field(..., description="Timestamp of metrics collection (ISO format)")
    metrics: Dict[str, Any] = Field(..., description="Job metrics")

class LogEntry(BaseModel):
    """Log entry."""
    timestamp: str = Field(..., description="Log timestamp (ISO format)")
    severity: str = Field(..., description="Log severity")
    message: str = Field(..., description="Log message")
    job_id: str = Field(..., description="Job ID")
    worker_id: Optional[str] = Field(None, description="Worker ID")
    step_name: Optional[str] = Field(None, description="Processing step name")

class LogList(BaseModel):
    """List of log entries."""
    logs: List[LogEntry] = Field(..., description="List of log entries")
    next_page_token: Optional[str] = Field(None, description="Token for the next page of logs") 