"""
Models for MCP tool management.

This module defines models for tools that can be used in data pipelines,
including AI models, transformation functions, and data processors.
"""

from enum import Enum
from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field
from datetime import datetime

from .base import BaseMCPModel

class ToolType(str, Enum):
    """Type of tool."""
    TRANSFORMATION = "transformation"
    MODEL = "model"
    PROCESSOR = "processor"
    VALIDATOR = "validator"
    CONNECTOR = "connector"
    CUSTOM = "custom"

class ToolStatus(str, Enum):
    """Status of tool."""
    ACTIVE = "active"
    INACTIVE = "inactive"
    DEPRECATED = "deprecated"
    DEVELOPMENT = "development"

class ToolDefinition(BaseModel):
    """Definition for creating a new tool."""
    name: str = Field(..., description="Tool name")
    description: str = Field(..., description="Tool description")
    tool_type: ToolType = Field(..., description="Tool type")
    version: str = Field(..., description="Tool version")
    parameters: Dict[str, Any] = Field(default_factory=dict, description="Tool parameters schema")
    capabilities: List[str] = Field(default_factory=list, description="Tool capabilities")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Tool metadata")
    
    class Config:
        """Pydantic model configuration."""
        schema_extra = {
            "example": {
                "name": "text-classifier",
                "description": "Classifies text into categories",
                "tool_type": "model",
                "version": "1.0.0",
                "parameters": {
                    "input_text": {
                        "type": "string",
                        "description": "Text to classify",
                        "required": True
                    },
                    "max_categories": {
                        "type": "integer",
                        "description": "Maximum number of categories to return",
                        "default": 3
                    }
                },
                "capabilities": ["classification", "nlp"],
                "metadata": {
                    "model_size": "small",
                    "languages": ["en", "es", "fr"]
                }
            }
        }

class Tool(BaseMCPModel):
    """Tool model extending BaseMCPModel."""
    name: str = Field(..., description="Tool name")
    description: str = Field(..., description="Tool description")
    tool_type: ToolType = Field(..., description="Tool type")
    version: str = Field(..., description="Tool version")
    status: ToolStatus = Field(default=ToolStatus.ACTIVE, description="Tool status")
    parameters: Dict[str, Any] = Field(default_factory=dict, description="Tool parameters schema")
    capabilities: List[str] = Field(default_factory=list, description="Tool capabilities")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Tool metadata")
    
    # MCP required fields (overrides)
    mcp_resource_type: str = Field(default="tool", description="Type of MCP resource")
    
    class Config:
        """Pydantic model configuration."""
        schema_extra = {
            "example": {
                "mcp_resource_id": "text-classifier-123",
                "mcp_resource_type": "tool",
                "mcp_version": "1.0",
                "mcp_created_at": "2023-08-15T12:00:00Z",
                "mcp_updated_at": "2023-08-15T12:00:00Z",
                "mcp_state": "ACTIVE",
                "name": "text-classifier",
                "description": "Classifies text into categories",
                "tool_type": "model",
                "version": "1.0.0",
                "status": "active",
                "parameters": {
                    "input_text": {
                        "type": "string",
                        "description": "Text to classify",
                        "required": True
                    },
                    "max_categories": {
                        "type": "integer",
                        "description": "Maximum number of categories to return",
                        "default": 3
                    }
                },
                "capabilities": ["classification", "nlp"],
                "metadata": {
                    "model_size": "small",
                    "languages": ["en", "es", "fr"]
                }
            }
        } 