"""
Manifest API Router for the Apache Beam MCP Server.

This module provides endpoints for tool discovery and MCP protocol metadata.
"""

import logging
import os
from datetime import datetime
from fastapi import APIRouter, Request, Depends

from ..models import LLMToolResponse, MCPContext
from ..models.common import RunnerType, JobType

# Import authentication dependencies if available
try:
    from ..auth import require_read, require_write
except ImportError:
    # Create dummy auth functions for backward compatibility
    from typing import Callable
    
    # Create a dummy UserSession for type hints
    class UserSession:
        user_id: str = "anonymous"
        roles: list = ["admin"]
    
    # Create a dummy dependency that just returns a default user
    async def get_dummy_user():
        return UserSession()
    
    # Create dummy auth dependencies
    def require_read(func: Callable = None):
        return get_dummy_user
        
    def require_write(func: Callable = None):
        return get_dummy_user

logger = logging.getLogger(__name__)

# Create router
router = APIRouter()

@router.get("/manifest", response_model=LLMToolResponse, summary="Get tool manifest", tags=["Manifest"])
async def get_manifest(request: Request, user = Depends(require_read)):
    """
    Get the tool manifest describing this MCP server's capabilities.
    
    Returns a complete description of all operations, parameters, and capabilities
    in a format designed for discovery by LLMs and agents.
    
    This endpoint is used by LLMs and agent frameworks to discover what operations
    this MCP server supports and how to use them.
    """
    try:
        # Create a simplified manifest without complex objects
        manifest = {
            "schema_version": "1.0",
            "version": "1.0.0",
            "tool_type": "mcp_server",
            "name": "beam-mcp",
            "display_name": "Apache Beam MCP Server",
            "description": "Model Context Protocol server for managing Apache Beam data pipelines",
            "capabilities": [
                "job_management",
                "pipeline_monitoring",
                "savepoint_management",
                "runner_management"
            ],
            "runner_types": [rt.value for rt in RunnerType],
            "job_types": [jt.value for jt in JobType],
            "authentication": {
                "type": request.app.state.config.get("security", {}).get("auth_type", "none")
            },
            "endpoints": {
                "list_runners": "/api/v1/runners",
                "create_job": "/api/v1/jobs",
                "list_jobs": "/api/v1/jobs",
                "get_job": "/api/v1/jobs/{job_id}",
                "cancel_job": "/api/v1/jobs/{job_id}",
                "get_job_metrics": "/api/v1/jobs/{job_id}/metrics",
                "health": "/api/v1/health/health"
            },
            "configurations": {
                "supported_runners": {
                    "spark": {
                        "display_name": "Apache Spark",
                        "description": "Apache Spark runner for batch processing",
                        "versions_supported": ["3.3.0"],
                        "deployment_modes": ["local", "cluster"]
                    },
                    "flink": {
                        "display_name": "Apache Flink",
                        "description": "Apache Flink runner for streaming and batch processing",
                        "versions_supported": ["1.17.0"],
                        "deployment_modes": ["local", "session"]
                    },
                    "direct": {
                        "display_name": "Direct Runner",
                        "description": "Apache Beam DirectRunner for local execution",
                        "versions_supported": ["2.50.0"],
                        "deployment_modes": ["local"]
                    }
                },
                "server_info": {
                    "version": "1.0.0",
                    "beam_sdk_version": "2.50.0"
                }
            }
        }
        
        return LLMToolResponse(
            success=True,
            data=manifest,
            message="Tool manifest retrieved successfully",
            error=None
        )
    except Exception as e:
        logger.error(f"Error in get_manifest: {e}")
        return LLMToolResponse(
            success=False,
            data=None,
            message=f"An error occurred: {str(e)}",
            error=str(e)
        )

@router.get("/context", response_model=LLMToolResponse, summary="Get current context", tags=["Manifest"])
async def get_context(request: Request, mcp_context: MCPContext = Depends(), user = Depends(require_read)):
    """
    Get the current MCP context.
    
    Returns information about the current context, including session ID,
    trace ID, and any context parameters.
    
    This endpoint is used by LLMs and agent frameworks to manage stateful
    operations and track context across requests.
    """
    return LLMToolResponse(
        success=True,
        data=mcp_context.dict(),
        message="Context retrieved successfully",
        error=None
    ) 