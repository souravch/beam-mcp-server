"""
Configuration module for the Apache Beam MCP server.

This module handles loading and validating configuration from YAML files
and provides MCP protocol standard configuration options.
"""

import os
import yaml
import logging
from typing import Dict, Any, Optional, List
from pydantic import Field
from pydantic_settings import BaseSettings
from functools import lru_cache

logger = logging.getLogger(__name__)

class Settings(BaseSettings):
    # Service configuration
    service_name: str = Field(default="beam-mcp", description="Server name")
    service_type: str = Field(default="beam", description="Server type")
    service_version: str = Field(default="1.0.0", description="Server version")
    
    # Runner configuration
    default_runner: str = Field(default="direct", description="Default runner to use for jobs")
    
    # Google Cloud configuration
    gcp_project_id: str = Field(default="servys", description="Google Cloud project ID")
    gcp_region: str = Field(default="us-central1", description="Google Cloud region")
    
    # Optional Spark configuration
    spark_master: Optional[str] = Field(default="local[*]", description="Spark master URL")
    
    # Optional Flink configuration
    flink_jobmanager: Optional[str] = Field(default="localhost:8081", description="Flink JobManager address")
    
    # API configuration
    api_prefix: str = Field(default="/api/v1", description="API prefix for all endpoints")
    cors_origins: list = Field(default=["*"], description="CORS allowed origins")
    base_url: Optional[str] = Field(default="http://localhost:8888", description="Base URL for API endpoints")
    
    # MCP protocol standard configuration
    mcp_version: str = Field(default="1.0", description="MCP protocol version")
    mcp_server_name: str = Field(default="beam-mcp-server", description="MCP server name")
    mcp_implementation_name: str = Field(default="FastMCP", description="MCP implementation name")
    mcp_implementation_version: str = Field(default="1.0.0", description="MCP implementation version")
    mcp_provider: str = Field(default="apache", description="MCP provider name")
    mcp_lifespan: bool = Field(default=True, description="Enable MCP server lifespan management")
    mcp_warn_on_duplicate_tools: bool = Field(default=True, description="Warn on duplicate tool registrations")
    mcp_warn_on_duplicate_resources: bool = Field(default=True, description="Warn on duplicate resource registrations")
    mcp_warn_on_duplicate_prompts: bool = Field(default=True, description="Warn on duplicate prompt registrations")
    mcp_tool_schema_validation: bool = Field(default=True, description="Validate tool schemas")
    mcp_resource_schema_validation: bool = Field(default=True, description="Validate resource schemas")
    mcp_create_default_tools: bool = Field(default=True, description="Create default MCP tools")
    mcp_context_headers: List[str] = Field(
        default=["MCP-Session-ID", "MCP-Trace-ID", "MCP-Transaction-ID", "MCP-User-ID"],
        description="MCP context headers"
    )
    mcp_streaming_support: bool = Field(default=True, description="Enable MCP streaming support")
    mcp_log_level: str = Field(default="INFO", description="MCP server log level")
    
    # Additional MCP server capabilities
    enable_resources: bool = Field(default=True, description="Enable MCP resources")
    enable_tools: bool = Field(default=True, description="Enable MCP tools")
    enable_notifications: bool = Field(default=True, description="Enable MCP notifications")
    enable_subscriptions: bool = Field(default=True, description="Enable MCP subscriptions")
    enable_prompts: bool = Field(default=True, description="Enable MCP prompts")
    enable_completions: bool = Field(default=True, description="Enable MCP completions")
    
    # Raw loaded configuration
    raw_config: Dict[str, Any] = Field(default_factory=dict, exclude=True)
    
    class Config:
        env_prefix = "BEAM_MCP_"
        case_sensitive = False
        env_file = ".env"
        
    def dict(self, *args, **kwargs):
        """
        Convert settings to a dictionary, preserving the nested structure.
        
        Returns:
            Dict[str, Any]: Settings as a dictionary
        """
        # Get base dictionary from parent
        base_dict = super().dict(*args, **kwargs)
        
        # If we have a raw config with nested structure, use it
        if self.raw_config:
            # Make a copy to avoid modifying the original
            result = self.raw_config.copy()
            # Update with flattened settings
            for key, value in base_dict.items():
                if not key.startswith('_'):
                    result[key] = value
            return result
        
        # Otherwise just return the flat dictionary
        return base_dict
    
    def get_mcp_settings(self) -> Dict[str, Any]:
        """
        Get MCP-specific settings as a dictionary.
        
        Returns:
            Dict[str, Any]: MCP settings dictionary suitable for FastMCP initialization
        """
        return {
            "name": self.mcp_server_name,
            "instructions": f"Apache Beam MCP Server for managing data pipelines across different runners",
            "lifespan": self.mcp_lifespan,
            "warn_on_duplicate_tools": self.mcp_warn_on_duplicate_tools,
            "warn_on_duplicate_resources": self.mcp_warn_on_duplicate_resources,
            "warn_on_duplicate_prompts": self.mcp_warn_on_duplicate_prompts,
            "log_level": self.mcp_log_level
        }

@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()

def load_config(config_path: str) -> Dict[str, Any]:
    """
    Load configuration from a YAML file
    
    Args:
        config_path (str): Path to the configuration file
        
    Returns:
        Dict[str, Any]: Configuration dictionary
        
    Raises:
        FileNotFoundError: If the configuration file does not exist
        ValueError: If the configuration is invalid
    """
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    validate_config(config)
    
    # Store the raw config in settings
    settings = Settings()
    settings.raw_config = config
    
    return config

def validate_config(config: Dict[str, Any]) -> None:
    """
    Validate configuration
    
    Args:
        config (Dict[str, Any]): Configuration dictionary
        
    Raises:
        ValueError: If the configuration is invalid
    """
    # Required top-level sections
    required_sections = ['service', 'runners', 'interfaces']
    for section in required_sections:
        if section not in config:
            raise ValueError(f"Missing required configuration section: {section}")
    
    # Validate service section
    if 'name' not in config['service']:
        raise ValueError("Missing required field: service.name")
    if 'type' not in config['service']:
        raise ValueError("Missing required field: service.type")
    
    # Validate MCP section if present
    if 'mcp' in config:
        if 'version' in config['mcp'] and config['mcp']['version'] != '1.0':
            logger.warning(f"Unsupported MCP version: {config['mcp']['version']}, using 1.0")
            config['mcp']['version'] = '1.0'
    else:
        # Add default MCP section
        config['mcp'] = {'version': '1.0'}
    
    # Validate runners section (at least one runner must be enabled)
    if not any(runner.get('enabled', False) for runner in config['runners'].values()):
        raise ValueError("At least one runner must be enabled")
    
    # Check for default_runner
    if 'default_runner' not in config:
        if 'dataflow' in config['runners'] and config['runners']['dataflow'].get('enabled', False):
            config['default_runner'] = 'dataflow'
        else:
            # Set the first enabled runner as default
            for runner_name, runner_config in config['runners'].items():
                if runner_config.get('enabled', False):
                    config['default_runner'] = runner_name
                    break
    
    # Validate interfaces section
    required_interfaces = ['jobs', 'metrics', 'logs', 'savepoints']
    for interface in required_interfaces:
        if interface not in config['interfaces']:
            config['interfaces'][interface] = {'enabled': True}
            logger.warning(f"Missing interface configuration for {interface}, enabling by default") 