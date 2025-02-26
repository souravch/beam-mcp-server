"""
Base models for the MCP server.
"""
from typing import Dict, Optional
from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, Field

class BaseMCPModel(BaseModel):
    """Base model with common MCP fields."""
    
    # MCP protocol fields
    mcp_version: str = Field(default="1.0", description="MCP protocol version")
    mcp_resource_type: str = Field(..., description="Type of MCP resource")
    mcp_resource_id: str = Field(..., description="Unique identifier for the resource")
    mcp_created_at: datetime = Field(default_factory=datetime.utcnow, description="Resource creation timestamp")
    mcp_updated_at: datetime = Field(default_factory=datetime.utcnow, description="Resource last update timestamp")
    mcp_created_by: Optional[str] = Field(None, description="User who created the resource")
    mcp_updated_by: Optional[str] = Field(None, description="User who last updated the resource")
    mcp_labels: Dict[str, str] = Field(default_factory=dict, description="Resource labels")
    mcp_annotations: Dict[str, str] = Field(default_factory=dict, description="Resource annotations")
    mcp_state: str = Field(default="ACTIVE", description="Resource state")
    mcp_generation: int = Field(default=1, description="Resource generation/version")
    
    class Config:
        """Pydantic model configuration."""
        arbitrary_types_allowed = True
        json_encoders = {
            datetime: lambda v: v.isoformat(),
            UUID: str,
        }