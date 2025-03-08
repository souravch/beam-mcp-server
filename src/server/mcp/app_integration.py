from fastapi import FastAPI
import logging
from typing import Dict, Any, Optional, List, Callable, Awaitable, TypeVar, Generic, Union

from .settings import get_mcp_settings
from . import setup_mcp
from .models import ConnectionState
from .fastapi import MCPFastAPI
from . import get_mcp_connection_dependency, is_feature_enabled


logger = logging.getLogger(__name__)

T = TypeVar('T')


class MCPFeatureRouter(Generic[T]):
    """
    Router for MCP feature-specific endpoints.
    
    This router wraps an existing FastAPI router and provides methods for
    registering endpoints that require specific MCP features to be enabled.
    """
    
    def __init__(
        self,
        router: APIRouter,
        mcp_fastapi: MCPFastAPI,
        feature_name: str
    ):
        """
        Initialize the MCP feature router.
        
        Args:
            router: Existing FastAPI router
            mcp_fastapi: MCP FastAPI integration
            feature_name: Feature name required for all endpoints
        """
        self.router = router
        self.mcp_fastapi = mcp_fastapi
        self.feature_name = feature_name
    
    def add_api_route(
        self,
        path: str,
        endpoint: Callable[..., Awaitable[Any]],
        methods: List[str] = ["GET"],
        **kwargs
    ):
        """
        Add an API route that requires the feature to be enabled.
        
        Args:
            path: Endpoint path
            endpoint: Endpoint function
            methods: HTTP methods
            **kwargs: Additional arguments for router.add_api_route
        """
        self.mcp_fastapi.register_feature_endpoint(
            router=self.router,
            path=path,
            endpoint=endpoint,
            feature_name=self.feature_name,
            methods=methods,
            **kwargs
        )
    
    def include_router(self, router: APIRouter, **kwargs):
        """
        Include another router.
        
        Args:
            router: Router to include
            **kwargs: Additional arguments for router.include_router
        """
        self.router.include_router(router, **kwargs)
    
    def get_router(self) -> APIRouter:
        """
        Get the underlying router.
        
        Returns:
            Underlying FastAPI router
        """
        return self.router


def create_mcp_feature_router(
    app: FastAPI,
    feature_name: str,
    prefix: str = "",
    tags: Optional[List[str]] = None,
    mcp_prefix: str = "/api/v1/mcp"
) -> MCPFeatureRouter:
    """
    Create a router for endpoints that require a specific MCP feature.
    
    Args:
        app: FastAPI application
        feature_name: Feature name required for all endpoints
        prefix: Router prefix
        tags: Router tags
        mcp_prefix: MCP router prefix
        
    Returns:
        MCPFeatureRouter instance
    """
    # Create router
    router = APIRouter(prefix=prefix, tags=tags)
    
    # Get MCP FastAPI integration
    mcp_integration = app.state.mcp_fastapi
    
    # Create feature router
    feature_router = MCPFeatureRouter(
        router=router,
        mcp_fastapi=mcp_integration,
        feature_name=feature_name
    )
    
    return feature_router


def get_feature_dependency(feature_name: str):
    """
    Create a dependency that checks if a feature is enabled.
    
    Args:
        feature_name: Feature name to check
        
    Returns:
        FastAPI dependency that checks if the feature is enabled
    """
    
    async def check_feature(connection: ConnectionState = get_mcp_connection_dependency()):
        """
        Check if the feature is enabled.
        
        Args:
            connection: Connection state from MCP dependency
            
        Returns:
            Connection state if feature is enabled
            
        Raises:
            HTTPException: If feature is not enabled
        """
        if not is_feature_enabled(feature_name, {"capabilities": connection.client_capabilities}):
            raise HTTPException(
                status_code=status.HTTP_501_NOT_IMPLEMENTED,
                detail=f"Feature not enabled: {feature_name}"
            )
        
        return connection
    
    return Depends(check_feature)


def integrate_mcp_with_app(app: FastAPI) -> Dict[str, Any]:
    """
    Integrate MCP protocol with a FastAPI application.
    
    This function initializes MCP protocol support for a FastAPI application
    using the settings from environment variables. It sets up the router and
    all required dependencies.
    
    Args:
        app: FastAPI application instance
        
    Returns:
        Dictionary with MCP integration components
    """
    # Get settings
    settings = get_mcp_settings()
    
    # Skip if MCP is disabled
    if not settings.enabled:
        logger.info("MCP protocol support is disabled")
        return {}
    
    try:
        # Setup MCP
        components = setup_mcp(
            app=app,
            redis_url=settings.redis_url,
            server_name=settings.server_name,
            server_version=settings.server_version,
            prefix=settings.prefix
        )
        
        # Set connection expiry
        components["connection_manager"].CONNECTION_EXPIRY_SECONDS = settings.connection_expiry
        
        # Include MCP router
        app.include_router(components["fastapi"].router)
        
        # Store MCP components in app state for later use
        app.state.mcp_fastapi = components["fastapi"]
        app.state.mcp_connection_manager = components["connection_manager"]
        app.state.mcp_capability_registry = components["capability_registry"]
        app.state.mcp_transport = components["transport"]
        
        logger.info(f"MCP protocol support integrated with app (server: {settings.server_name} v{settings.server_version})")
        
        # Return components
        return components
    
    except Exception as e:
        # Log error and return empty dict to allow application startup
        # even if MCP integration fails
        logger.error(f"Failed to initialize MCP: {e}")
        logger.exception(e)
        return {}


def is_mcp_available(app: FastAPI) -> bool:
    """
    Check if MCP is available in the application.
    
    Args:
        app: FastAPI application instance
        
    Returns:
        True if MCP is available, False otherwise
    """
    return hasattr(app.state, "mcp_fastapi") and app.state.mcp_fastapi is not None 