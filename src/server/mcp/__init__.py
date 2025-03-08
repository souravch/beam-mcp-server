import logging
from typing import Dict, Any, Optional
from fastapi import FastAPI

from .models import ConnectionState, FeatureCapabilityLevel, FeatureCapabilityConfig
from .connection import ConnectionManager
from .capabilities import CapabilityRegistry
from .transport import HTTPTransport
from .fastapi import MCPFastAPI
from .version import VersionCompatibility


logger = logging.getLogger(__name__)


def setup_mcp(
    app: FastAPI,
    redis_url: str,
    server_name: str = "beam-mcp-server",
    server_version: str = "1.0.0",
    prefix: str = "/api/v1/mcp"
) -> Dict[str, Any]:
    """
    Set up MCP protocol support in a FastAPI application.
    
    Args:
        app: FastAPI application
        redis_url: Redis URL for connection state
        server_name: Server name (for capabilities)
        server_version: Server version (for capabilities)
        prefix: Router prefix
        
    Returns:
        Dictionary containing the created MCP components
    """
    # Create capability registry
    registry = CapabilityRegistry(
        server_name=server_name,
        server_version=server_version,
        protocol_version="1.0"
    )
    
    # Register core MCP capabilities
    registry.register_feature(
        "core.jsonrpc",
        properties={
            "version": "2.0",
            "methods": ["initialize", "shutdown", "jsonrpc"]
        },
        level=FeatureCapabilityLevel.REQUIRED,
        version_compatibility=VersionCompatibility.EXACT
    )
    
    registry.register_feature(
        "core.transport.http",
        properties={
            "version": "1.0",
            "supportedMethods": ["POST"]
        },
        level=FeatureCapabilityLevel.REQUIRED,
        version_compatibility=VersionCompatibility.COMPATIBLE
    )
    
    registry.register_feature(
        "core.transport.sse",
        properties={
            "version": "1.0"
        },
        level=FeatureCapabilityLevel.REQUIRED,
        version_compatibility=VersionCompatibility.COMPATIBLE
    )
    
    # Register resource capabilities
    registry.register_feature(
        "resource.types",
        properties={
            "supportedTypes": ["file", "dataset", "model", "schema"],
            "maxSize": 104857600  # 100 MB
        },
        level=FeatureCapabilityLevel.OPTIONAL,
        version_compatibility=VersionCompatibility.COMPATIBLE
    )
    
    # Register tool capabilities
    registry.register_feature(
        "tool.runner_management",
        properties={
            "supportedRunners": ["direct", "flink", "dataflow", "spark"]
        },
        level=FeatureCapabilityLevel.OPTIONAL,
        version_compatibility=VersionCompatibility.COMPATIBLE
    )
    
    registry.register_feature(
        "tool.job_management",
        properties={
            "supportedActions": ["create", "list", "cancel", "pause", "resume"]
        },
        level=FeatureCapabilityLevel.OPTIONAL,
        version_compatibility=VersionCompatibility.COMPATIBLE
    )
    
    registry.register_feature(
        "tool.pipeline_monitoring",
        properties={
            "metrics": True,
            "logs": True,
            "events": True
        },
        level=FeatureCapabilityLevel.OPTIONAL,
        version_compatibility=VersionCompatibility.COMPATIBLE
    )
    
    # Register Beam-specific capabilities
    registry.register_feature(
        "beam.runners",
        properties={
            "supported": ["direct", "flink", "dataflow", "spark"],
            "default": "direct"
        },
        level=FeatureCapabilityLevel.OPTIONAL,
        version_compatibility=VersionCompatibility.COMPATIBLE
    )
    
    registry.register_feature(
        "beam.job_types",
        properties={
            "supported": ["BATCH", "STREAMING"]
        },
        level=FeatureCapabilityLevel.OPTIONAL,
        version_compatibility=VersionCompatibility.COMPATIBLE
    )
    
    registry.register_feature(
        "beam.sdk",
        properties={
            "language": "python",
            "version": "2.50.0"
        },
        level=FeatureCapabilityLevel.OPTIONAL,
        version_compatibility=VersionCompatibility.COMPATIBLE
    )
    
    # Create connection manager
    connection_manager = ConnectionManager(redis_url)
    
    # Create transport
    transport = HTTPTransport()
    
    # Connect transport with connection manager
    connection_manager.set_transport(transport)
    
    # Register message handlers
    from .messages import JsonRpcRequest, JsonRpcResponse
    
    # Define initialize handler
    async def handle_initialize(request: JsonRpcRequest, connection: ConnectionState) -> Dict[str, Any]:
        """Handle initialize request."""
        # This is already handled by the router
        return {"status": "ok"}
    
    # Define shutdown handler
    async def handle_shutdown(request: JsonRpcRequest, connection: ConnectionState) -> Dict[str, Any]:
        """Handle shutdown request."""
        # This is already handled by the router
        return {"status": "ok"}
    
    # Define other handlers as needed...
    # These handlers will be called for JSON-RPC requests that don't match 
    # the built-in lifecycle methods
    
    # Register handlers with transport
    transport.register_request_handlers({
        "initialize": handle_initialize,
        "shutdown": handle_shutdown,
        # Add other handlers here
    })
    
    # Create FastAPI integration
    fastapi_integration = MCPFastAPI(
        connection_manager=connection_manager,
        capability_registry=registry,
        transport=transport,
        prefix=prefix
    )
    
    # Return components
    return {
        "capability_registry": registry,
        "connection_manager": connection_manager,
        "transport": transport,
        "fastapi": fastapi_integration
    }


def get_mcp_connection_dependency():
    """
    Get FastAPI dependency for MCP connection validation.
    
    Returns:
        Callable that validates the connection and returns the connection state
    """
    # This is a shortcut to the dependency from MCPFastAPI
    from fastapi import Request, Depends
    
    async def get_mcp_components(request: Request):
        """Get MCP components from FastAPI app state."""
        return {
            "connection_manager": request.app.state.mcp_connection_manager,
            "capability_registry": request.app.state.mcp_capability_registry,
            "transport": request.app.state.mcp_transport,
            "mcp_fastapi": request.app.state.mcp_fastapi
        }
    
    async def get_connection(
        components=Depends(get_mcp_components),
        connection=Depends(lambda req: req.app.state.mcp_fastapi.get_dependency())
    ) -> ConnectionState:
        """Get validated MCP connection."""
        return connection
    
    return Depends(get_connection)


def is_feature_enabled(feature_name: str, request: Optional[Any] = None, connection: Optional[ConnectionState] = None):
    """
    Check if a feature is enabled for a connection.
    
    Args:
        feature_name: Feature name
        request: Optional FastAPI request object
        connection: Optional connection state
        
    Returns:
        True if feature is enabled, False otherwise
    """
    # Get FastAPI app from request or connection
    if request is not None:
        app = request.app
    elif connection is not None and hasattr(connection, "app"):
        app = connection.app
    else:
        return False
    
    # Get capability registry
    if not hasattr(app.state, "mcp_capability_registry"):
        return False
    
    capability_registry = app.state.mcp_capability_registry
    
    # Get connection capabilities
    if connection is not None:
        connection_capabilities = {
            "capabilities": connection.client_capabilities
        }
    elif request is not None and hasattr(app.state, "mcp_connection_manager"):
        # Try to get connection from request
        connection_manager = app.state.mcp_connection_manager
        connection_id = connection_manager.extract_connection_id(request)
        if not connection_id:
            return False
        
        connection = connection_manager.get_connection(connection_id)
        if not connection:
            return False
        
        connection_capabilities = {
            "capabilities": connection.client_capabilities
        }
    else:
        return False
    
    # Check if feature is enabled
    return capability_registry.is_feature_enabled(feature_name, connection_capabilities) 