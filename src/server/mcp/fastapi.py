from typing import Dict, Any, Optional, List, Callable, Awaitable
from fastapi import APIRouter, Request, Response, Depends, HTTPException, status
from sse_starlette.sse import EventSourceResponse
import logging

from .models import (
    InitializeRequest, 
    InitializeResponse,
    ShutdownRequest, 
    ShutdownResponse,
    ConnectionLifecycleState
)
from .connection import ConnectionManager
from .capabilities import CapabilityRegistry
from .transport import HTTPTransport
from .errors import (
    MCPError, 
    protocol_version_mismatch, 
    incompatible_capabilities,
    connection_expired
)


logger = logging.getLogger(__name__)


class MCPFastAPI:
    """
    FastAPI integration for MCP protocol.
    Provides router with endpoints for MCP protocol messages.
    """
    
    def __init__(
        self,
        connection_manager: ConnectionManager,
        capability_registry: CapabilityRegistry,
        transport: HTTPTransport,
        prefix: str = "/api/v1/mcp"
    ):
        """
        Initialize the FastAPI integration.
        
        Args:
            connection_manager: Connection manager instance
            capability_registry: Capability registry instance
            transport: HTTP transport instance
            prefix: Router prefix
        """
        self.connection_manager = connection_manager
        self.capability_registry = capability_registry
        self.transport = transport
        self.prefix = prefix
        
        # Register request handlers
        self._register_request_handlers()
        
        # Create router
        self.router = self._create_router()
    
    def _register_request_handlers(self) -> None:
        """Register MCP request handlers with the transport."""
        # Basic request handlers can be registered here
        # More complex handlers would be handled by the actual MCP implementation
        pass
    
    def _feature_enabled_endpoint(
        self, 
        endpoint: Callable[..., Awaitable[Any]], 
        feature_name: str
    ) -> Callable[..., Awaitable[Any]]:
        """
        Wrap an endpoint with feature check.
        
        Args:
            endpoint: Original endpoint function
            feature_name: Feature name to check
            
        Returns:
            Wrapped endpoint function
        """
        async def wrapped_endpoint(*args, **kwargs):
            # Get request from args or kwargs
            request = None
            for arg in args:
                if isinstance(arg, Request):
                    request = arg
                    break
            
            if not request and "request" in kwargs:
                request = kwargs["request"]
            
            if not request:
                # If request is not found, assume feature is enabled
                return await endpoint(*args, **kwargs)
            
            # Check if feature is enabled
            if not self.capability_registry.is_feature_enabled(
                feature_name, 
                self._get_connection_capabilities(request)
            ):
                # Feature is not enabled, return 501 Not Implemented
                raise HTTPException(
                    status_code=status.HTTP_501_NOT_IMPLEMENTED,
                    detail=f"Feature not enabled: {feature_name}"
                )
            
            # Feature is enabled, call original endpoint
            return await endpoint(*args, **kwargs)
        
        # Copy metadata from original function
        wrapped_endpoint.__name__ = endpoint.__name__
        wrapped_endpoint.__doc__ = endpoint.__doc__
        
        return wrapped_endpoint
    
    def _get_connection_capabilities(self, request: Request) -> Dict[str, Any]:
        """Get connection capabilities from request."""
        # Try to get connection ID from request
        connection_id = self.connection_manager.extract_connection_id(request)
        if not connection_id:
            return {"capabilities": {}}
        
        # Try to get connection
        connection = self.connection_manager.get_connection(connection_id)
        if not connection:
            return {"capabilities": {}}
        
        # Return connection capabilities
        return {"capabilities": connection.client_capabilities}
    
    def _create_router(self) -> APIRouter:
        """Create FastAPI router with MCP endpoints."""
        router = APIRouter(prefix=self.prefix, tags=["mcp"])
        
        @router.post("/initialize")
        async def initialize(request: Request, init_request: InitializeRequest, response: Response):
            """Handle MCP initialize request."""
            try:
                # Validate protocol version
                server_version = self.capability_registry.get_capabilities().protocol_version
                if init_request.protocol_version != server_version:
                    raise protocol_version_mismatch(
                        client_version=init_request.protocol_version,
                        server_version=server_version
                    )
                
                # Check capability compatibility
                compatible, reasons = self.capability_registry.is_compatible_with({
                    "protocol_version": init_request.protocol_version,
                    "capabilities": init_request.capabilities
                })
                
                if not compatible:
                    raise incompatible_capabilities(reasons)
                
                # Create new connection
                connection = self.connection_manager.create_connection(init_request)
                
                # Set connection ID in response headers and cookie
                response.headers["X-MCP-Connection-ID"] = connection.id
                self.connection_manager.set_connection_cookie(response, connection.id)
                
                # Return initialize response
                server_capabilities = self.capability_registry.get_capabilities()
                return InitializeResponse(
                    protocol_version=server_capabilities.protocol_version,
                    server=server_capabilities.server,
                    capabilities=server_capabilities.capabilities
                )
            
            except MCPError as e:
                # Handle MCP errors
                raise e.to_http_exception()
        
        @router.post("/initialized")
        async def initialized(request: Request):
            """Handle MCP initialized notification."""
            # Get connection ID
            connection_id = self.connection_manager.extract_connection_id(request)
            if not connection_id:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Missing connection ID"
                )
            
            try:
                # Mark connection as active
                self.connection_manager.mark_connection_active(connection_id)
                
                # Return empty response (no content)
                return Response(status_code=status.HTTP_204_NO_CONTENT)
            
            except MCPError as e:
                # Handle MCP errors
                raise e.to_http_exception()
        
        @router.post("/shutdown")
        async def shutdown(request: Request, shutdown_request: ShutdownRequest):
            """Handle MCP shutdown request."""
            # Get connection ID
            connection_id = self.connection_manager.extract_connection_id(request)
            if not connection_id:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Missing connection ID"
                )
            
            try:
                # Mark connection as terminating
                self.connection_manager.mark_connection_terminating(connection_id)
                
                # Return shutdown response
                return ShutdownResponse()
            
            except MCPError as e:
                # Handle MCP errors
                raise e.to_http_exception()
        
        @router.post("/exit")
        async def exit(request: Request):
            """Handle MCP exit notification."""
            # Get connection ID
            connection_id = self.connection_manager.extract_connection_id(request)
            if not connection_id:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Missing connection ID"
                )
            
            try:
                # Mark connection as terminated
                self.connection_manager.mark_connection_terminated(connection_id)
                
                # Return empty response (no content)
                return Response(status_code=status.HTTP_204_NO_CONTENT)
            
            except MCPError as e:
                # Handle MCP errors
                raise e.to_http_exception()
        
        @router.post("/jsonrpc")
        async def jsonrpc(request: Request):
            """Handle general JSON-RPC requests."""
            # Get connection ID
            connection_id = self.connection_manager.extract_connection_id(request)
            if not connection_id:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Missing connection ID"
                )
            
            # Get connection
            connection = self.connection_manager.get_connection(connection_id)
            if not connection:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Invalid or expired connection"
                )
            
            # Check connection state
            if connection.state != ConnectionLifecycleState.ACTIVE:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Connection is not active (current state: {connection.state})"
                )
            
            # Handle JSON-RPC request
            response = await self.transport.handle_jsonrpc_request(request, connection)
            
            # Update connection last activity
            self.connection_manager.touch_connection(connection_id)
            
            # Return JSON-RPC response
            return response
        
        @router.get("/events")
        async def events(request: Request):
            """Handle SSE connection for notifications."""
            # Get connection ID
            connection_id = self.connection_manager.extract_connection_id(request)
            if not connection_id:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Missing connection ID"
                )
            
            # Get connection
            connection = self.connection_manager.get_connection(connection_id)
            if not connection:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Invalid or expired connection"
                )
            
            # Handle SSE connection
            return await self.transport.handle_sse_connection(connection_id)
        
        return router
    
    def register_feature_endpoint(
        self,
        router: APIRouter,
        path: str,
        endpoint: Callable[..., Awaitable[Any]],
        feature_name: str,
        methods: List[str] = ["GET"],
        **kwargs
    ):
        """
        Register an endpoint that requires a specific feature.
        
        Args:
            router: FastAPI router to register endpoint with
            path: Endpoint path
            endpoint: Endpoint function
            feature_name: Feature name required for endpoint
            methods: HTTP methods for endpoint
            **kwargs: Additional arguments for router.add_api_route
        """
        # Wrap endpoint with feature check
        wrapped_endpoint = self._feature_enabled_endpoint(endpoint, feature_name)
        
        # Register endpoint with router
        router.add_api_route(
            path=path,
            endpoint=wrapped_endpoint,
            methods=methods,
            **kwargs
        )
    
    def get_dependency(self):
        """
        Get FastAPI dependency for connection validation.
        
        Returns:
            Callable that validates the connection and returns the connection state
        """
        
        async def get_connection(request: Request) -> Optional[Any]:
            """
            Validate connection and return connection state.
            
            Args:
                request: FastAPI request object
                
            Returns:
                Connection state if valid, None otherwise
                
            Raises:
                HTTPException: If connection is invalid or expired
            """
            # Get connection ID
            connection_id = self.connection_manager.extract_connection_id(request)
            if not connection_id:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Missing connection ID"
                )
            
            # Get connection
            connection = self.connection_manager.get_connection(connection_id)
            if not connection:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Invalid or expired connection"
                )
            
            # Check connection state
            if connection.state != ConnectionLifecycleState.ACTIVE:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Connection is not active (current state: {connection.state})"
                )
            
            # Update connection last activity
            self.connection_manager.touch_connection(connection_id)
            
            return connection
        
        return Depends(get_connection) 