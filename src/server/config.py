"""
Configuration module for the Apache Beam MCP server.

This module handles loading and validating configuration from YAML files.
"""

import os
import yaml
import logging
from typing import Dict, Any, Optional
from pydantic import Field
from pydantic_settings import BaseSettings
from functools import lru_cache

logger = logging.getLogger(__name__)

class Settings(BaseSettings):
    # Service configuration
    service_name: str = "beam-mcp"
    service_type: str = "beam"
    service_version: str = "1.0.0"
    
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
    
    # MCP configuration
    mcp_version: str = Field(default="1.0", description="MCP protocol version")
    
    class Config:
        env_prefix = "BEAM_MCP_"
        case_sensitive = False
        env_file = ".env"

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