"""
Models for MCP execution context management.

This module defines models for execution contexts that can be used to run Beam pipelines,
including environment configurations, runtime parameters, and execution options.
"""

from enum import Enum
from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field
from datetime import datetime

from .base import BaseMCPModel

class ContextType(str, Enum):
    """Type of execution context."""
    DATAFLOW = "dataflow"
    SPARK = "spark"
    FLINK = "flink"
    DIRECT = "direct"
    CUSTOM = "custom"

class ContextStatus(str, Enum):
    """Status of execution context."""
    ACTIVE = "active"
    INACTIVE = "inactive"
    PROVISIONING = "provisioning"
    ERROR = "error"

class ContextDefinition(BaseModel):
    """Definition for creating a new execution context."""
    name: str = Field(..., description="Context name")
    description: str = Field(..., description="Context description")
    context_type: ContextType = Field(..., description="Context type")
    parameters: Dict[str, Any] = Field(default_factory=dict, description="Context parameters")
    resources: Dict[str, str] = Field(default_factory=dict, description="Resource requirements")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Context metadata")
    
    class Config:
        """Pydantic model configuration."""
        schema_extra = {
            "example": {
                "name": "Dataflow Production",
                "description": "Production execution context for Dataflow jobs",
                "context_type": "dataflow",
                "parameters": {
                    "region": "us-central1",
                    "project": "my-beam-project",
                    "temp_location": "gs://my-bucket/temp",
                    "machine_type": "n1-standard-2"
                },
                "resources": {
                    "cpu": "2",
                    "memory": "4GB",
                    "disk": "50GB"
                },
                "metadata": {
                    "environment": "production",
                    "team": "data-engineering"
                }
            }
        }

class Context(BaseMCPModel):
    """Execution context model extending BaseMCPModel."""
    name: str = Field(..., description="Context name")
    description: str = Field(..., description="Context description")
    context_type: ContextType = Field(..., description="Context type")
    parameters: Dict[str, Any] = Field(default_factory=dict, description="Context parameters")
    resources: Dict[str, str] = Field(default_factory=dict, description="Resource requirements")
    status: ContextStatus = Field(default=ContextStatus.ACTIVE, description="Context status")
    last_used: Optional[datetime] = Field(None, description="Time this context was last used")
    job_count: int = Field(default=0, description="Number of jobs executed in this context")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Context metadata")
    
    # MCP required fields (overrides)
    mcp_resource_type: str = Field(default="context", description="Type of MCP resource")
    
    class Config:
        """Pydantic model configuration."""
        schema_extra = {
            "example": {
                "id": "dataflow-prod-123",
                "name": "Dataflow Production",
                "description": "Production execution context for Dataflow jobs",
                "context_type": "dataflow",
                "parameters": {
                    "region": "us-central1",
                    "project": "my-beam-project",
                    "temp_location": "gs://my-bucket/temp",
                    "machine_type": "n1-standard-2"
                },
                "resources": {
                    "cpu": "2",
                    "memory": "4GB",
                    "disk": "50GB"
                },
                "status": "active",
                "last_used": "2023-08-15T10:30:00Z",
                "job_count": 15,
                "metadata": {
                    "environment": "production",
                    "team": "data-engineering",
                    "created_at": "2023-01-15T08:00:00Z"
                },
                "mcp_resource_type": "context"
            }
        } 