"""
Dependencies for FastAPI routes.
"""

from fastapi import Depends, Request

from ..core.client_manager import BeamClientManager
from ..core.tool_registry import ToolRegistry
from ..core.resource_registry import ResourceRegistry
from ..core.context_registry import ContextRegistry

def get_client_manager(request: Request) -> BeamClientManager:
    """Get the client manager from request state."""
    return request.app.state.client_manager

def get_tool_registry(request: Request) -> ToolRegistry:
    """Get the tool registry from request state."""
    return request.app.state.tool_registry

def get_resource_registry(request: Request) -> ResourceRegistry:
    """Get the resource registry from request state."""
    return request.app.state.resource_registry

def get_context_registry(request: Request) -> ContextRegistry:
    """Get the context registry from request state."""
    return request.app.state.context_registry 