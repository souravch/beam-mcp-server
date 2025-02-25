"""
Common models for the Apache Beam MCP server.

This module defines shared data models used across the application.
"""

from enum import Enum
from typing import Dict, List, Optional, Any, Union
from datetime import datetime
from pydantic import BaseModel, Field

class RunnerType(str, Enum):
    """Type of runner for executing Apache Beam pipelines."""
    DATAFLOW = "dataflow"
    SPARK = "spark"
    FLINK = "flink"
    DIRECT = "direct"

class JobState(str, Enum):
    """Status of a pipeline job."""
    PENDING = "PENDING"
    STARTING = "STARTING"
    RUNNING = "RUNNING"
    CANCELLED = "CANCELLED"
    DRAINING = "DRAINING"
    FAILED = "FAILED"
    SUCCEEDED = "SUCCEEDED"
    UNKNOWN = "UNKNOWN"

class JobType(str, Enum):
    """Type of job execution."""
    BATCH = "BATCH"
    STREAMING = "STREAMING"

class HealthResponse(BaseModel):
    """Health check response."""
    status: str = Field(..., description="Health status")
    timestamp: str = Field(..., description="Current timestamp")
    version: str = Field(..., description="Server version")
    service: str = Field(..., description="Service name")
    environment: str = Field(..., description="Environment (dev/prod)")
    error: Optional[str] = Field(None, description="Error message if unhealthy")

class ErrorResponse(BaseModel):
    """Error response."""
    error: str = Field(..., description="Error message")
    timestamp: str = Field(default_factory=lambda: datetime.utcnow().isoformat(), description="Error timestamp")

class MetricValue(BaseModel):
    """Value of a metric."""
    value: Union[int, float, str, bool] = Field(..., description="Metric value")
    timestamp: str = Field(default_factory=lambda: datetime.utcnow().isoformat(), description="Timestamp when the metric was collected")
    labels: Optional[Dict[str, str]] = Field(None, description="Additional labels for the metric")

class LLMToolResponse(BaseModel):
    """Standard response format for LLM tools."""
    success: bool = Field(..., description="Whether the operation was successful")
    data: Any = Field(..., description="Response data")
    message: str = Field(..., description="Human-readable message")
    error: Optional[str] = Field(None, description="Error message if any")
    
    class Config:
        schema_extra = {
            "example": {
                "success": True,
                "data": {"job_id": "2023-08-15_12345"},
                "message": "Job created successfully",
                "error": None
            }
        } 