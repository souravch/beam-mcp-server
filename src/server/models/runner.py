"""
Runner models for the Dataflow MCP Server.
"""

from typing import Dict, List, Optional, Literal
from enum import Enum
from pydantic import Field, field_validator

from .base import BaseMCPModel

class RunnerType(str, Enum):
    """Type of runner for executing Apache Beam pipelines."""
    DATAFLOW = "dataflow"
    SPARK = "spark"
    FLINK = "flink"
    DIRECT = "direct"

class RunnerStatus(str, Enum):
    """Status of a runner."""
    AVAILABLE = "AVAILABLE"
    UNAVAILABLE = "UNAVAILABLE"
    MAINTENANCE = "MAINTENANCE"

class RunnerCapability(str, Enum):
    """Capabilities supported by a runner."""
    BATCH = "batch"
    STREAMING = "streaming"
    AUTOSCALING = "autoscaling"
    MONITORING = "monitoring"
    SAVEPOINTS = "savepoints"
    CHECKPOINTING = "checkpointing"
    METRICS = "metrics"
    LOGGING = "logging"

class Runner(BaseMCPModel):
    """Represents a pipeline runner."""
    mcp_resource_type: Literal["runner"] = Field(default="runner")
    
    # Runner-specific fields
    name: str = Field(..., description="Name of the runner")
    runner_type: RunnerType = Field(..., description="Type of runner")
    status: RunnerStatus = Field(..., description="Current status of the runner")
    description: str = Field(..., description="Description of the runner")
    capabilities: List[str] = Field(default_factory=list, description="List of runner capabilities")
    config: Dict = Field(default_factory=dict, description="Runner-specific configuration")
    version: str = Field(..., description="Runner version")
    default_region: Optional[str] = Field(None, description="Default region for the runner")
    supported_regions: List[str] = Field(default_factory=list, description="List of supported regions")
    
    # MCP-specific fields
    mcp_provider: str = Field(default="apache", description="Provider of the runner (e.g., Google, Apache)")
    mcp_cost_tier: str = Field(default="standard", description="Cost tier of the runner")
    mcp_min_workers: int = Field(default=1, description="Minimum number of workers")
    mcp_max_workers: int = Field(default=100, description="Maximum number of workers")
    mcp_auto_scaling: bool = Field(default=True, description="Whether autoscaling is enabled")
    mcp_supported_sdk_versions: List[str] = Field(default_factory=list, description="Supported SDK versions")
    mcp_maintenance_window: Optional[Dict] = Field(None, description="Scheduled maintenance window")
    mcp_quotas: Dict = Field(default_factory=dict, description="Runner quotas and limits")
    
    @field_validator('mcp_resource_id', mode='before')
    def set_resource_id(cls, v, values):
        """Set resource ID from name if not provided."""
        if not v and 'name' in values:
            return values['name']
        return v

class RunnerList(BaseMCPModel):
    """List of available runners."""
    mcp_resource_type: Literal["runner_list"] = Field(default="runner_list")
    
    runners: List[Runner] = Field(..., description="List of runners")
    default_runner: str = Field(..., description="Name of the default runner")
    
    # MCP-specific fields
    mcp_filter_criteria: Optional[Dict] = Field(None, description="Applied filters")
    mcp_sort_criteria: Optional[Dict] = Field(None, description="Applied sorting")
    mcp_total_runners: int = Field(..., description="Total number of runners available") 