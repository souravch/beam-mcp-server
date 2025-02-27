"""
Health Check API Router for the Apache Beam MCP Server.

This module provides health check endpoints to verify server status according to
the Model Context Protocol (MCP) standard.
"""

import logging
import os
from datetime import datetime
from fastapi import APIRouter, Request, Depends
from typing import Dict, Any, Optional

from ..models import HealthResponse, LLMToolResponse, MCPContext

logger = logging.getLogger(__name__)

# Create router
router = APIRouter()

# Custom dependency to make MCPContext optional
async def get_optional_mcp_context(request: Request) -> Optional[MCPContext]:
    """
    Optional MCP context for health endpoints.
    Returns None if not provided, avoiding validation errors.
    """
    # Check for context in headers
    session_id = request.headers.get("MCP-Session-ID")
    trace_id = request.headers.get("MCP-Trace-ID")
    transaction_id = request.headers.get("MCP-Transaction-ID")
    user_id = request.headers.get("MCP-User-ID")
    
    # If no context headers provided, return None
    if not any([session_id, trace_id, transaction_id, user_id]):
        return None
    
    # Otherwise create and return a context
    context = MCPContext(
        session_id=session_id,
        trace_id=trace_id,
        transaction_id=transaction_id,
        user_id=user_id
    )
    
    return context

@router.get("/health", response_model=HealthResponse, tags=["Health"], summary="Check server health")
async def health_check():
    """
    Health check endpoint to verify server status.
    
    This endpoint returns the current health status of the server,
    including version information and environment.
    
    Returns:
        HealthResponse: Health check response with status and metadata
    """
    try:
        return {
            "status": "healthy",
            "timestamp": datetime.utcnow().isoformat(),
            "version": "1.0.0",
            "service": "beam-mcp",
            "environment": os.environ.get("ENVIRONMENT", "development")
        }
    except Exception as e:
        logger.error(f"Error in health check: {e}")
        return {
            "status": "unhealthy",
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat(),
            "version": "1.0.0",
            "service": "beam-mcp",
            "environment": os.environ.get("ENVIRONMENT", "development")
        }

@router.get("/health/llm", response_model=LLMToolResponse, tags=["Health"], summary="LLM-friendly health check")
async def llm_health_check(request: Request, context: Optional[MCPContext] = Depends(get_optional_mcp_context)):
    """
    LLM-friendly health check endpoint following the MCP protocol.
    
    This endpoint is designed for LLMs to check server health with a standardized
    response format (success, data, message, error) and maintains MCP context.
    
    Args:
        request (Request): FastAPI request object
        context (Optional[MCPContext]): MCP context for context propagation
    
    Returns:
        LLMToolResponse: Standardized health check response for LLM consumption
    """
    try:
        # Access MCP server from app state if available
        mcp_server = getattr(request.app.state, "mcp_server", None)
        
        # Check MCP server status
        server_status = "healthy"
        mcp_status = "operational"
        
        if mcp_server is not None:
            # Add MCP server-specific checks
            tool_manager = getattr(mcp_server, "_tool_manager", {})
            resource_manager = getattr(mcp_server, "_resource_manager", {})
            
            # Access internal tool and resource collections directly
            tool_count = len(tool_manager._tools) if hasattr(tool_manager, '_tools') else 0
            resource_count = len(resource_manager._resources) if hasattr(resource_manager, '_resources') else 0
            
            mcp_status_data = {
                "tool_count": tool_count,
                "resource_count": resource_count
            }
        else:
            mcp_status_data = {"message": "MCP server not available"}
            server_status = "degraded"
            mcp_status = "limited"
        
        health_data = {
            "status": server_status,
            "timestamp": datetime.utcnow().isoformat(),
            "version": "1.0.0",
            "service": "beam-mcp",
            "environment": os.environ.get("ENVIRONMENT", "development"),
            "mcp": {
                "status": mcp_status,
                "version": "1.0",
                "context": context.model_dump(exclude={"parameters"}) if context else None,
                "details": mcp_status_data
            }
        }
        
        return LLMToolResponse(
            success=True,
            data=health_data,
            message=f"Apache Beam MCP Server is {server_status} and MCP is {mcp_status}",
            error=None
        )
    except Exception as e:
        logger.error(f"Error in LLM health check: {e}")
        return LLMToolResponse(
            success=False,
            data=None,
            message="Apache Beam MCP Server is experiencing issues",
            error=str(e)
        )

@router.get("/health/mcp", response_model=LLMToolResponse, tags=["Health"], summary="MCP-specific health check")
async def mcp_health_check(request: Request, context: Optional[MCPContext] = Depends(get_optional_mcp_context)):
    """
    MCP-specific health check endpoint that follows the MCP protocol.
    
    This endpoint provides detailed information about the MCP server status,
    including tools, resources, and context handling.
    
    Args:
        request (Request): FastAPI request object
        context (Optional[MCPContext]): MCP context for context propagation
    
    Returns:
        LLMToolResponse: Detailed MCP health check response
    """
    try:
        # Access MCP server from app state
        mcp_server = getattr(request.app.state, "mcp_server", None)
        
        if mcp_server is None:
            return LLMToolResponse(
                success=False,
                data=None,
                message="MCP server is not available",
                error="MCP server not found in application state"
            )
        
        # Get MCP component status
        tool_manager = getattr(mcp_server, "_tool_manager", None)
        resource_manager = getattr(mcp_server, "_resource_manager", None)
        
        # Access internal tool and resource collections directly
        tools = list(tool_manager._tools.values()) if tool_manager and hasattr(tool_manager, '_tools') else []
        resources = list(resource_manager._resources.values()) if resource_manager and hasattr(resource_manager, '_resources') else []
        
        # Gather MCP-specific health information
        mcp_health = {
            "version": "1.0",
            "server_name": mcp_server.name,
            "tools": {
                "count": len(tools),
                "names": [tool.name for tool in tools]
            },
            "resources": {
                "count": len(resources),
                "names": [resource.name for resource in resources]
            },
            "context": {
                "session_id": context.session_id if context else None,
                "transaction_id": context.transaction_id if context else None,
                "trace_id": context.trace_id if context else None
            } if context else {"status": "no context provided"}
        }
        
        return LLMToolResponse(
            success=True,
            data=mcp_health,
            message="MCP server is operational",
            error=None
        )
    except Exception as e:
        logger.error(f"Error in MCP health check: {e}")
        return LLMToolResponse(
            success=False,
            data=None,
            message="Error checking MCP server health",
            error=str(e)
        ) 