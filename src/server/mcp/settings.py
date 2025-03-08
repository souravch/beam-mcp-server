from pydantic import BaseSettings, Field, validator
from typing import Optional


class MCPSettings(BaseSettings):
    """Settings for MCP protocol support."""
    
    # Enable MCP protocol support
    enabled: bool = Field(True, env="MCP_ENABLED")
    
    # Redis URL for connection state storage
    redis_url: str = Field("redis://localhost:6379/0", env="MCP_REDIS_URL")
    
    # Server information
    server_name: str = Field("beam-mcp-server", env="MCP_SERVER_NAME")
    server_version: str = Field("1.0.0", env="MCP_SERVER_VERSION")
    
    # Router prefix
    prefix: str = Field("/api/v1/mcp", env="MCP_PREFIX")
    
    # Connection expiration (seconds)
    connection_expiry: int = Field(1800, env="MCP_CONNECTION_EXPIRY")
    
    class Config:
        env_prefix = ""
        case_sensitive = False


def get_mcp_settings() -> MCPSettings:
    """Get MCP settings from environment variables."""
    return MCPSettings() 