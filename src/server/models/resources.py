"""
Models for MCP resource management.

This module defines models for resources that can be used in data pipelines,
such as datasets, files, ML models, and other assets.
"""

from enum import Enum
from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field
from datetime import datetime

from .base import BaseMCPModel

class ResourceType(str, Enum):
    """Type of resource."""
    DATASET = "dataset"
    FILE = "file"
    MODEL = "model"
    CONFIG = "config"
    IMAGE = "image"
    SCHEMA = "schema"
    CUSTOM = "custom"

class ResourceStatus(str, Enum):
    """Status of resource."""
    AVAILABLE = "available"
    UNAVAILABLE = "unavailable"
    PROCESSING = "processing"
    ERROR = "error"
    DELETED = "deleted"

class ResourceDefinition(BaseModel):
    """Definition for creating a new resource."""
    name: str = Field(..., description="Resource name")
    description: str = Field(..., description="Resource description")
    resource_type: ResourceType = Field(..., description="Resource type")
    location: str = Field(..., description="Resource location or URI")
    format: Optional[str] = Field(None, description="Resource format (e.g., CSV, JSON, PyTorch)")
    schema: Optional[Dict[str, Any]] = Field(None, description="Resource schema (if applicable)")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Resource metadata")
    
    class Config:
        """Pydantic model configuration."""
        schema_extra = {
            "example": {
                "name": "Example Dataset",
                "description": "An example dataset for documentation",
                "resource_type": "dataset",
                "location": "gs://beam-examples/datasets/example.csv",
                "format": "csv",
                "schema": {
                    "fields": [
                        {"name": "id", "type": "integer", "description": "Unique identifier"},
                        {"name": "name", "type": "string", "description": "Name field"},
                        {"name": "value", "type": "float", "description": "Value field"}
                    ]
                },
                "metadata": {
                    "created_by": "docs_team",
                    "rows": 1000,
                    "size_bytes": 102400
                }
            }
        }

class ResourceAccessMode(str, Enum):
    """Access mode for resources."""
    READ_ONLY = "read_only"
    READ_WRITE = "read_write"
    WRITE_ONLY = "write_only"

class ResourcePermission(BaseModel):
    """Permission for resource access."""
    user_id: str = Field(..., description="User or system ID")
    access_mode: ResourceAccessMode = Field(
        default=ResourceAccessMode.READ_ONLY,
        description="Access mode"
    )
    expiration: Optional[datetime] = Field(None, description="Permission expiration time")

class Resource(BaseMCPModel):
    """Resource model extending BaseMCPModel."""
    name: str = Field(..., description="Resource name")
    description: str = Field(..., description="Resource description")
    resource_type: ResourceType = Field(..., description="Resource type")
    location: str = Field(..., description="Resource location or URI")
    format: Optional[str] = Field(None, description="Resource format (e.g., CSV, JSON, PyTorch)")
    status: ResourceStatus = Field(default=ResourceStatus.AVAILABLE, description="Resource status")
    size_bytes: Optional[int] = Field(None, description="Resource size in bytes")
    schema: Optional[Dict[str, Any]] = Field(None, description="Resource schema (if applicable)")
    permissions: List[ResourcePermission] = Field(
        default_factory=list,
        description="Resource access permissions"
    )
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Resource metadata")
    
    # MCP required fields (overrides)
    mcp_resource_type: str = Field(default="resource", description="Type of MCP resource")
    
    class Config:
        """Pydantic model configuration."""
        schema_extra = {
            "example": {
                "id": "dataset-123",
                "name": "Example Dataset",
                "description": "An example dataset for documentation",
                "resource_type": "dataset",
                "location": "gs://beam-examples/datasets/example.csv",
                "format": "csv",
                "status": "available",
                "size_bytes": 102400,
                "schema": {
                    "fields": [
                        {"name": "id", "type": "integer", "description": "Unique identifier"},
                        {"name": "name", "type": "string", "description": "Name field"},
                        {"name": "value", "type": "float", "description": "Value field"}
                    ]
                },
                "permissions": [
                    {
                        "user_id": "user1",
                        "access_mode": "read_only",
                        "expiration": "2023-12-31T23:59:59Z"
                    }
                ],
                "metadata": {
                    "created_by": "docs_team",
                    "created_at": "2023-01-15T10:30:00Z",
                    "rows": 1000
                },
                "mcp_resource_type": "resource"
            }
        } 