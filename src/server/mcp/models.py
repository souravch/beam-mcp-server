from enum import Enum
from typing import Dict, List, Optional, Any, Set
from pydantic import BaseModel, Field, validator
from datetime import datetime, UTC

from .version import VersionCompatibility


class ConnectionLifecycleState(str, Enum):
    """Represents the possible states of an MCP connection."""
    UNINITIALIZED = "uninitialized"
    INITIALIZING = "initializing"
    ACTIVE = "active"
    TERMINATING = "terminating"
    TERMINATED = "terminated"


class ClientInfo(BaseModel):
    """Information about an MCP client."""
    name: str
    version: str
    additional_info: Dict[str, Any] = Field(default_factory=dict)


class FeatureCapabilityLevel(str, Enum):
    """Level of capability support for a feature."""
    REQUIRED = "required"     # Feature must be supported
    PREFERRED = "preferred"   # Feature is preferred but not required
    OPTIONAL = "optional"     # Feature is optional
    EXPERIMENTAL = "experimental"  # Feature is experimental


class FeatureCapabilityConfig(BaseModel):
    """Configuration for a feature capability."""
    version_compatibility: VersionCompatibility = VersionCompatibility.COMPATIBLE
    required_properties: Set[str] = Field(default_factory=set)
    level: FeatureCapabilityLevel = FeatureCapabilityLevel.OPTIONAL


class FeatureCapability(BaseModel):
    """Represents a specific feature capability."""
    supported: bool = True
    version: str
    properties: Dict[str, Any] = Field(default_factory=dict)
    config: Optional[FeatureCapabilityConfig] = None
    
    def __init__(self, **data):
        # Ensure config is created if not provided
        if "config" not in data:
            data["config"] = FeatureCapabilityConfig()
        super().__init__(**data)


class ServerCapabilities(BaseModel):
    """Server capabilities as communicated during initialization."""
    protocol_version: str = "1.0"
    server: Dict[str, str]
    capabilities: Dict[str, FeatureCapability]


class ConnectionState(BaseModel):
    """Represents the state of an MCP connection."""
    id: str
    client_info: ClientInfo
    client_capabilities: Dict[str, Any]
    state: ConnectionLifecycleState
    created_at: datetime
    last_activity: datetime
    user_id: Optional[str] = None
    session_data: Dict[str, Any] = Field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary suitable for serialization."""
        data = self.dict(exclude={"created_at", "last_activity"})
        data["created_at"] = self.created_at.isoformat()
        data["last_activity"] = self.last_activity.isoformat()
        return data
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ConnectionState":
        """Create from dictionary, handling special types."""
        if isinstance(data.get("created_at"), str):
            data["created_at"] = datetime.fromisoformat(data["created_at"])
        if isinstance(data.get("last_activity"), str):
            data["last_activity"] = datetime.fromisoformat(data["last_activity"])
        return cls(**data)


class InitializeRequest(BaseModel):
    """MCP initialize request parameters."""
    protocol_version: str
    client: ClientInfo
    capabilities: Dict[str, Any]


class InitializeResponse(BaseModel):
    """MCP initialize response."""
    protocol_version: str
    server: Dict[str, str]
    capabilities: Dict[str, Any]


class ShutdownRequest(BaseModel):
    """MCP shutdown request parameters."""
    pass  # No parameters needed


class ShutdownResponse(BaseModel):
    """MCP shutdown response."""
    pass  # Empty response 