"""
Metrics models for the Dataflow MCP Server.
"""

from typing import Dict, Any, List, Optional, Literal
from enum import Enum
from pydantic import Field, validator

from .base import BaseMCPModel

class MetricType(str, Enum):
    """Type of metric."""
    COUNTER = "COUNTER"
    GAUGE = "GAUGE"
    DISTRIBUTION = "DISTRIBUTION"
    HISTOGRAM = "HISTOGRAM"

class MetricValue(BaseMCPModel):
    """Value of a metric."""
    mcp_resource_type: Literal["metric_value"] = Field(default="metric_value")
    
    value: float = Field(..., description="Current value of the metric")
    timestamp: str = Field(..., description="Timestamp when the value was recorded")
    labels: Dict[str, str] = Field(default_factory=dict, description="Metric labels")
    
    # MCP-specific fields
    mcp_window_start: Optional[str] = Field(None, description="Start of the measurement window")
    mcp_window_end: Optional[str] = Field(None, description="End of the measurement window")
    mcp_confidence: Optional[float] = Field(None, description="Confidence level of the measurement")
    mcp_source: str = Field(default="dataflow", description="Source of the measurement")

class Metric(BaseMCPModel):
    """Represents a job metric."""
    mcp_resource_type: Literal["metric"] = Field(default="metric")
    
    name: str = Field(..., description="Name of the metric")
    type: MetricType = Field(..., description="Type of metric")
    description: str = Field(..., description="Description of the metric")
    values: List[MetricValue] = Field(..., description="List of metric values")
    unit: Optional[str] = Field(None, description="Unit of measurement")
    namespace: str = Field(default="beam", description="Metric namespace")
    
    # MCP-specific fields
    mcp_aggregation: str = Field(default="none", description="Aggregation type")
    mcp_retention_days: int = Field(default=30, description="Days to retain metric data")
    mcp_sampling_rate: Optional[float] = Field(None, description="Sampling rate if applicable")
    mcp_alert_thresholds: Optional[Dict] = Field(None, description="Alert thresholds")
    mcp_metadata: Dict = Field(default_factory=dict, description="Additional metadata")

class JobMetrics(BaseMCPModel):
    """Collection of job metrics."""
    mcp_resource_type: Literal["job_metrics"] = Field(default="job_metrics")
    
    job_id: str = Field(..., description="ID of the job")
    metrics: List[Metric] = Field(..., description="List of job metrics")
    timestamp: str = Field(..., description="Timestamp when metrics were collected")
    window_start: Optional[str] = Field(None, description="Start of the metrics window")
    window_end: Optional[str] = Field(None, description="End of the metrics window")
    
    # MCP-specific fields
    mcp_collection_interval: int = Field(default=60, description="Collection interval in seconds")
    mcp_aggregation_window: str = Field(default="1m", description="Aggregation window")
    mcp_data_quality: Dict = Field(
        default_factory=lambda: {
            "completeness": 1.0,
            "accuracy": 1.0,
            "freshness_seconds": 0
        },
        description="Quality metrics for the data"
    )
    mcp_next_collection: Optional[str] = Field(None, description="Next scheduled collection time")
    
    @validator('mcp_resource_id', pre=True, always=True)
    def set_resource_id(cls, v, values):
        """Set resource ID from job_id and timestamp if not provided."""
        if not v and 'job_id' in values and 'timestamp' in values:
            return f"{values['job_id']}-metrics-{values['timestamp']}"
        return v