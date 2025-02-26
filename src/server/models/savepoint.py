"""
Savepoint models for the Dataflow MCP Server.
"""

from typing import Optional, Dict, List, Literal
from enum import Enum
from pydantic import Field, validator

from .base import BaseMCPModel

class SavepointStatus(str, Enum):
    """Status of a savepoint."""
    PENDING = "PENDING"
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"

class SavepointRequest(BaseMCPModel):
    """Request to create a savepoint."""
    mcp_resource_type: Literal["savepoint_request"] = Field(default="savepoint_request")
    
    job_id: str = Field(..., description="ID of the job to create savepoint for")
    savepoint_path: str = Field(..., description="GCS path to store the savepoint")
    drain: bool = Field(default=False, description="Whether to drain the pipeline after savepoint")
    async_: bool = Field(default=True, description="Whether to create the savepoint asynchronously")
    
    # MCP-specific fields
    mcp_timeout_seconds: Optional[int] = Field(default=3600, description="Timeout for savepoint creation")
    mcp_retry_strategy: Dict = Field(
        default_factory=lambda: {
            "max_retries": 3,
            "initial_delay_seconds": 5,
            "max_delay_seconds": 60
        },
        description="Retry strategy for savepoint creation"
    )

class Savepoint(BaseMCPModel):
    """Represents a job savepoint."""
    mcp_resource_type: Literal["savepoint"] = Field(default="savepoint")
    
    # Savepoint-specific fields
    savepoint_id: str = Field(..., description="Unique identifier for the savepoint")
    job_id: str = Field(..., description="ID of the job this savepoint belongs to")
    status: SavepointStatus = Field(..., description="Current status of the savepoint")
    create_time: str = Field(..., description="Savepoint creation timestamp")
    complete_time: Optional[str] = Field(None, description="Savepoint completion timestamp")
    savepoint_path: str = Field(..., description="GCS path where savepoint is stored")
    error_message: Optional[str] = Field(None, description="Error message if savepoint failed")
    
    # MCP-specific fields
    mcp_owner: Optional[str] = Field(None, description="Owner of the savepoint")
    mcp_expiration_time: Optional[str] = Field(None, description="When the savepoint expires")
    mcp_size_bytes: Optional[int] = Field(None, description="Size of the savepoint in bytes")
    mcp_metadata: Dict = Field(default_factory=dict, description="Additional metadata")
    mcp_parent_job: str = Field(..., description="ID of the parent job")
    
    @validator('mcp_resource_id', pre=True, always=True)
    def set_resource_id(cls, v, values):
        """Set resource ID from savepoint_id if not provided."""
        if not v and 'savepoint_id' in values:
            return values['savepoint_id']
        return v

class SavepointList(BaseMCPModel):
    """List of savepoints with pagination."""
    mcp_resource_type: Literal["savepoint_list"] = Field(default="savepoint_list")
    
    savepoints: List[Savepoint] = Field(..., description="List of savepoints")
    total: int = Field(..., description="Total number of savepoints")
    page: int = Field(default=1, description="Current page number")
    page_size: int = Field(default=10, description="Number of savepoints per page")
    
    # MCP-specific fields
    mcp_continuation_token: Optional[str] = Field(None, description="Token for getting next page")
    mcp_filter_criteria: Optional[Dict] = Field(None, description="Applied filters")
    mcp_sort_criteria: Optional[Dict] = Field(None, description="Applied sorting") 