"""
Apache Beam MCP Server - Main Application

This module provides a Model Context Protocol (MCP) server
for managing Apache Beam pipelines across different runners (Dataflow, Spark, Flink).
The server implements the MCP 1.0 protocol for standardized LLM/AI tool integration.
"""

import os
import logging
from typing import Optional, Dict, Any
from fastapi import FastAPI, Request, Depends, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.utils import get_openapi
from dotenv import load_dotenv

from .config import Settings, load_config
from .core.client_manager import BeamClientManager
from .models.common import ErrorResponse, LLMToolResponse
from .models.context import MCPContext
from .mcp_server import BeamMCPServer
from .api.manifest_router import router as manifest_router
from .api.jobs_router import router as jobs_router
from .api.runners_router import router as runners_router
from .api.metrics_router import router as metrics_router
from .api.savepoints_router import router as savepoints_router
from .api.health_router import router as health_router
from .api.tools_router import router as tools_router
from .api.resources_router import router as resources_router
from .api.contexts_router import router as contexts_router
from .core.tool_registry import ToolRegistry
from .core.resource_registry import ResourceRegistry
from .core.context_registry import ContextRegistry

# Import authentication components - if auth module is not found, disable auth
try:
    from .auth import get_auth_config, require_read, require_write
    from .auth.router import router as auth_router
    AUTH_MODULE_AVAILABLE = True
except ImportError:
    AUTH_MODULE_AVAILABLE = False
    # Create dummy auth functions for backward compatibility
    def require_read(func):
        return func
    def require_write(func):
        return func
    def get_auth_config():
        return type('AuthConfig', (), {'enabled': False})

# Import MCP integration
try:
    from .mcp.app_integration import integrate_mcp_with_app
    MCP_MODULE_AVAILABLE = True
except ImportError:
    MCP_MODULE_AVAILABLE = False

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# MCP Context dependency
async def get_mcp_context(request: Request) -> MCPContext:
    """
    Extract MCP context from request headers or create a new one.
    
    This function follows the MCP protocol standard for context extraction.
    """
    # Check for context in headers
    session_id = request.headers.get("MCP-Session-ID")
    trace_id = request.headers.get("MCP-Trace-ID")
    transaction_id = request.headers.get("MCP-Transaction-ID")
    user_id = request.headers.get("MCP-User-ID")
    
    # Create context
    context = MCPContext(
        session_id=session_id,
        trace_id=trace_id,
        transaction_id=transaction_id,
        user_id=user_id
    )
    
    # Add to request state for use in endpoints
    request.state.mcp_context = context
    
    return context

def custom_openapi(app: FastAPI):
    """
    Custom OpenAPI schema generator that includes MCP-specific information.
    
    This enhances the standard OpenAPI schema with MCP protocol metadata
    for better integration with MCP clients.
    """
    if app.openapi_schema:
        return app.openapi_schema
    
    openapi_schema = get_openapi(
        title=app.title,
        version=app.version,
        description=app.description,
        routes=app.routes,
    )
    
    # Add MCP protocol version
    openapi_schema["info"]["x-mcp-version"] = "1.0"
    openapi_schema["info"]["x-mcp-server"] = "beam-mcp-server"
    
    # Add MCP-specific endpoint documentation
    if "paths" not in openapi_schema:
        openapi_schema["paths"] = {}
    
    # Document MCP context headers
    openapi_schema["components"] = openapi_schema.get("components", {})
    openapi_schema["components"]["x-mcp-headers"] = {
        "MCP-Session-ID": {
            "description": "Session identifier for MCP context",
            "required": False,
            "schema": {"type": "string", "format": "uuid"}
        },
        "MCP-Trace-ID": {
            "description": "Trace identifier for distributed tracing",
            "required": False,
            "schema": {"type": "string"}
        },
        "MCP-Transaction-ID": {
            "description": "Transaction identifier for multi-step operations",
            "required": False,
            "schema": {"type": "string", "format": "uuid"}
        },
        "MCP-User-ID": {
            "description": "User identifier",
            "required": False,
            "schema": {"type": "string"}
        }
    }
    
    app.openapi_schema = openapi_schema
    return app.openapi_schema

def create_app(config: Optional[Dict[str, Any]] = None) -> FastAPI:
    """
    Create the Beam MCP Server application.
    
    This function initializes a FastAPI application with MCP protocol support
    for managing Apache Beam pipelines across different runners.
    
    Args:
        config (Optional[Dict[str, Any]]): Configuration dictionary. If not provided,
            will load from environment variables.
    
    Returns:
        FastAPI: FastAPI application with MCP protocol support
    """
    # Create settings
    if config is not None:
        # Use provided config
        yaml_config = config
    else:
        # Check if config path is provided via environment variable
        config_path = os.environ.get('CONFIG_PATH')
        if config_path and os.path.exists(config_path):
            # Load config from YAML file
            yaml_config = load_config(config_path)
        else:
            # Fall back to default settings
            settings = Settings()
            yaml_config = settings.dict()
    
    # Extract settings from yaml_config for the Settings object
    # Create a flattened version for Settings initialization
    flat_config = {}
    
    # Map service section
    if 'service' in yaml_config:
        service = yaml_config['service']
        flat_config['service_name'] = service.get('name', 'beam-mcp')
        flat_config['service_type'] = service.get('type', 'beam')
        flat_config['service_version'] = service.get('version', '1.0.0')
    
    # Map default runner
    flat_config['default_runner'] = yaml_config.get('default_runner', 'direct')
    
    # Map GCP settings
    if 'gcp' in yaml_config:
        gcp = yaml_config['gcp']
        flat_config['gcp_project_id'] = gcp.get('project_id', 'servys')
        flat_config['gcp_region'] = gcp.get('region', 'us-central1')
    
    # Map MCP settings
    if 'mcp' in yaml_config:
        mcp = yaml_config['mcp']
        flat_config['mcp_version'] = mcp.get('version', '1.0')
        flat_config['mcp_server_name'] = mcp.get('server_name', 'beam-mcp-server')
        flat_config['mcp_provider'] = mcp.get('provider', 'apache')
        flat_config['mcp_streaming_support'] = mcp.get('streaming_support', True)
        flat_config['mcp_log_level'] = mcp.get('log_level', 'INFO')
    
    # Map API settings
    if 'api' in yaml_config:
        api = yaml_config['api']
        flat_config['api_prefix'] = api.get('prefix', '/api/v1')
        flat_config['cors_origins'] = api.get('cors_origins', ['*'])
        flat_config['base_url'] = api.get('base_url', 'http://localhost:8888')
    
    # Create settings from flattened config
    settings = Settings(**flat_config)
    
    # Store raw config in settings
    settings.raw_config = yaml_config
    
    # Create the BeamMCPServer
    mcp_server = BeamMCPServer(settings)
    
    # Get FastAPI app from server
    app = mcp_server.fastapi_app
    
    # Update FastAPI app metadata
    app.title = "Apache Beam MCP Server"
    app.description = "Model Context Protocol server for Apache Beam pipelines"
    app.version = "1.0.0"
    
    # Initialize client manager with the full YAML config
    client_manager = BeamClientManager(yaml_config)
    
    # Initialize tool registry
    tool_registry = ToolRegistry()
    
    # Initialize resource registry
    resource_registry = ResourceRegistry()
    
    # Initialize context registry
    context_registry = ContextRegistry()
    
    # Store client manager and config in app state
    app.state.client_manager = client_manager
    app.state.config = yaml_config
    app.state.mcp_server = mcp_server
    app.state.tool_registry = tool_registry
    app.state.resource_registry = resource_registry
    app.state.context_registry = context_registry
    
    # Check if authentication is enabled
    auth_enabled = False
    if AUTH_MODULE_AVAILABLE:
        auth_config = get_auth_config()
        auth_enabled = auth_config.enabled
        logger.info(f"Authentication module found. Auth enabled: {auth_enabled}")
        
        # Add auth router
        app.include_router(auth_router, prefix="/api/v1", tags=["auth"])
    else:
        logger.info("Authentication module not found. Auth disabled.")
    
    # Add MCP protocol support
    if MCP_MODULE_AVAILABLE:
        try:
            mcp_components = integrate_mcp_with_app(app)
            logger.info("MCP protocol support initialized.")
        except Exception as e:
            logger.error(f"Failed to initialize MCP protocol support: {e}")
            logger.exception(e)
    else:
        logger.info("MCP protocol module not found. MCP protocol support disabled.")
    
    # Add routers with or without authentication
    # Health endpoints don't require authentication
    app.include_router(health_router, prefix="/api/v1/health", tags=["health"])
    
    # Manifest endpoints don't require authentication
    app.include_router(manifest_router, prefix="/api/v1", tags=["manifest"])
    
    # Add other routers with appropriate authentication
    # For secured endpoints, apply dependency to router level
    if auth_enabled:
        # Resources API (secured)
        app.include_router(
            resources_router, 
            prefix="/api/v1/resources", 
            tags=["resources"]
        )
        
        # Tools API (secured)
        app.include_router(
            tools_router, 
            prefix="/api/v1/tools", 
            tags=["tools"]
        )
        
        # Contexts API (secured)
        app.include_router(
            contexts_router, 
            prefix="/api/v1/contexts", 
            tags=["contexts"]
        )
        
        # Jobs API (secured)
        app.include_router(
            jobs_router, 
            prefix="/api/v1/jobs", 
            tags=["jobs"]
        )
        
        # Runners API (secured)
        app.include_router(
            runners_router, 
            prefix="/api/v1/runners", 
            tags=["runners"]
        )
        
        # Metrics API (secured)
        app.include_router(
            metrics_router, 
            prefix="/api/v1/metrics", 
            tags=["metrics"]
        )
        
        # Savepoints API (secured)
        app.include_router(
            savepoints_router, 
            prefix="/api/v1/savepoints", 
            tags=["savepoints"]
        )
    else:
        # Add routers without authentication
        app.include_router(resources_router, prefix="/api/v1/resources", tags=["resources"])
        app.include_router(tools_router, prefix="/api/v1/tools", tags=["tools"])
        app.include_router(contexts_router, prefix="/api/v1/contexts", tags=["contexts"])
        app.include_router(jobs_router, prefix="/api/v1/jobs", tags=["jobs"])
        app.include_router(runners_router, prefix="/api/v1/runners", tags=["runners"])
        app.include_router(metrics_router, prefix="/api/v1/metrics", tags=["metrics"])
        app.include_router(savepoints_router, prefix="/api/v1/savepoints", tags=["savepoints"])
    
    # Configure CORS
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    
    # Add MCP-specific exception handler
    @app.exception_handler(Exception)
    async def mcp_exception_handler(request: Request, exc: Exception):
        """
        Exception handler that returns MCP-compatible error responses.
        """
        status_code = 500
        if isinstance(exc, HTTPException):
            status_code = exc.status_code
        
        return JSONResponse(
            status_code=status_code,
            content=LLMToolResponse(
                success=False,
                data=None,
                message=f"An error occurred: {str(exc)}",
                error=str(exc)
            ).model_dump()
        )
    
    # Custom OpenAPI schema with MCP metadata
    app.openapi = lambda: custom_openapi(app)
    
    # Add OpenAPI tags metadata
    app.openapi_tags = [
        {
            "name": "Auth",
            "description": "Authentication and authorization operations",
        },
        {
            "name": "Jobs",
            "description": "Job management operations",
        },
        {
            "name": "Runners",
            "description": "Runner management operations",
        },
        {
            "name": "Metrics",
            "description": "Metrics retrieval operations",
        },
        {
            "name": "Savepoints",
            "description": "Savepoint operations",
        },
        {
            "name": "Contexts",
            "description": "Context management operations",
        }
    ]
    
    # Add direct test endpoint for debugging
    @app.get("/api/v1/debug-test", tags=["Debug"])
    async def direct_test_endpoint():
        """Debug endpoint for direct testing."""
        try:
            # Get the client manager directly from app state
            client_manager = app.state.client_manager
            
            # Print detailed debugging information
            print("===== DIRECT DEBUG TEST ENDPOINT =====")
            print(f"client_manager class: {type(client_manager).__name__}")
            print(f"client_manager ID: {id(client_manager)}")
            print(f"config keys: {list(client_manager.config.keys())}")
            print(f"runners config: {list(client_manager.config['runners'].keys())}")
            for runner_name, runner_config in client_manager.config['runners'].items():
                print(f"Runner {runner_name} enabled: {runner_config.get('enabled', False)}")
            print(f"client keys: {list(client_manager.clients.keys())}")
            
            # Call list_runners directly like in test script
            print("Calling client_manager.list_runners() directly")
            runners_list = await client_manager.list_runners()
            
            # Print results
            runner_count = len(runners_list.runners)
            print(f"Found {runner_count} runners")
            for i, runner in enumerate(runners_list.runners):
                print(f"Runner {i+1}: {runner.name} (type={runner.runner_type})")
            
            return {
                "success": True,
                "runner_count": runner_count,
                "runners": [
                    {
                        "name": runner.name,
                        "type": runner.runner_type.value,
                        "status": runner.status.value 
                    }
                    for runner in runners_list.runners
                ]
            }
        except Exception as e:
            print(f"ERROR in direct test endpoint: {str(e)}")
            import traceback
            traceback_str = traceback.format_exc()
            print(f"Traceback: {traceback_str}")
            return {
                "success": False,
                "error": str(e),
                "traceback": traceback_str
            }
    
    logger.info(f"Initialized Apache Beam MCP Server with default runner: {config.get('default_runner', 'direct') if config else 'direct'}")
    
    @app.on_event("startup")
    async def startup():
        """Initialize the application on startup."""
        try:
            await client_manager.initialize()
        except Exception as e:
            logger.error(f"Failed to initialize client manager: {str(e)}")
            raise HTTPException(status_code=500, detail="Failed to initialize server")

    @app.on_event("shutdown")
    async def shutdown():
        """Cleanup on application shutdown."""
        try:
            await client_manager.cleanup()
        except Exception as e:
            logger.error(f"Error during cleanup: {str(e)}")
    
    return app

if __name__ == '__main__':
    import uvicorn
    import argparse
    import sys
    
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description='Apache Beam MCP Server')
    parser.add_argument('-c', '--config', help='Path to config file')
    parser.add_argument('-p', '--port', type=int, default=8000, help='Port to listen on')
    parser.add_argument('--host', default='0.0.0.0', help='Host to bind to')
    args = parser.parse_args()
    
    # Load config from file if provided
    config = None
    if args.config:
        try:
            config = load_config(args.config)
            logger.info(f"Loaded configuration from {args.config}")
        except Exception as e:
            logger.error(f"Failed to load config from {args.config}: {str(e)}")
            sys.exit(1)
    
    # Create app with config
    app = create_app(config)
    
    # Run with uvicorn
    uvicorn.run(app, host=args.host, port=args.port)