"""
Runner models for the Apache Beam MCP server.

This module defines data models for runner management operations.
"""

from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field

from .common import RunnerType, JobType

class RunnerInfo(BaseModel):
    """Runner information."""
    runner_type: RunnerType = Field(..., description="Runner type")
    name: str = Field(..., description="Runner name")
    description: str = Field(..., description="Runner description")
    supported_job_types: List[JobType] = Field(..., description="Supported job types")
    supported_features: List[str] = Field(..., description="List of supported features")
    configuration_examples: Dict[str, Any] = Field(..., description="Example configurations")
    
    class Config:
        schema_extra = {
            "example": {
                "runner_type": "dataflow",
                "name": "Google Cloud Dataflow",
                "description": "Fully managed runner for Apache Beam on Google Cloud",
                "supported_job_types": ["BATCH", "STREAMING"],
                "supported_features": ["autoscaling", "monitoring", "savepoints", "flexible_resource_scheduling"],
                "configuration_examples": {
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
                        "machineType": "n2-standard-4",
                        "serviceAccount": "sa@project.iam.gserviceaccount.com",
                        "network": "default",
                        "subnetwork": "regions/us-central1/subnetworks/default"
                    }
                }
            }
        }

class RunnerList(BaseModel):
    """List of available runners."""
    runners: List[RunnerInfo] = Field(..., description="List of available runners")

class RunnerScalingParameters(BaseModel):
    """Parameters for scaling a runner."""
    min_workers: Optional[int] = Field(None, description="Minimum number of workers")
    max_workers: Optional[int] = Field(None, description="Maximum number of workers")
    scaling_algorithm: Optional[str] = Field(None, description="Scaling algorithm to use")
    target_cpu_utilization: Optional[float] = Field(None, description="Target CPU utilization for autoscaling")
    target_throughput_per_worker: Optional[float] = Field(None, description="Target throughput per worker") 