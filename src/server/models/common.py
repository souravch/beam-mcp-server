"""
Common models for the Apache Beam MCP server.

This module defines shared data models used across the application.
"""

from enum import Enum
from typing import Dict, List, Optional, Any, Union, TypeVar, Generic
from datetime import datetime
from pydantic import BaseModel, Field
from mcp.types import Request, Result, RequestParams

# Define a type variable for generic typing
T = TypeVar('T')

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

class LLMToolResponse(BaseModel, Generic[T]):
    """Standard response format for LLM tools."""
    success: bool = Field(..., description="Whether the operation was successful")
    data: T = Field(..., description="Response data")
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

class OperationType(str, Enum):
    """Type of operation on a resource."""
    CREATE = "create"
    READ = "read"
    UPDATE = "update"
    DELETE = "delete"
    LIST = "list"

class MCPRequestParams(RequestParams):
    """Parameters for an MCP request."""
    parameters: Dict[str, Any] = Field(default_factory=dict, description="Request parameters")

class MCPRequest(Request[MCPRequestParams, str]):
    """Request model for MCP operations."""
    parameters: Dict[str, Any] = Field(default_factory=dict, description="Request parameters")

class MCPResponse(Result):
    """Response model for MCP operations."""
    data: Optional[Any] = Field(default=None, description="Response data")
    error: Optional[str] = Field(default=None, description="Error message if any")
    context: Optional[Dict[str, Any]] = Field(default=None, description="Response context")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Response metadata") 