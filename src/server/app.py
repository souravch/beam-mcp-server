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
        flat_config['base_url'] = api.get('base_url', 'http://localhost:8080')
    
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
    
    # Store client manager and config in app state
    app.state.client_manager = client_manager
    app.state.config = yaml_config
    app.state.mcp_server = mcp_server
    
    # Add routers
    app.include_router(manifest_router, prefix="/api/v1", tags=["manifest"])
    app.include_router(jobs_router, prefix="/api/v1/jobs", tags=["jobs"])
    app.include_router(runners_router, prefix="/api/v1/runners", tags=["runners"])
    app.include_router(metrics_router, prefix="/api/v1/metrics", tags=["metrics"])
    app.include_router(savepoints_router, prefix="/api/v1/savepoints", tags=["savepoints"])
    app.include_router(health_router, prefix="/api/v1/health", tags=["health"])
    
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
            "name": "Jobs",
            "description": "Job management operations",
        },
        {
            "name": "Savepoints",
            "description": "Job savepoint operations",
        },
        {
            "name": "Runners",
            "description": "Runner operations",
        },
        {
            "name": "Metrics",
            "description": "Job metrics operations",
        },
        {
            "name": "Health",
            "description": "Health check operations",
        },
        {
            "name": "Manifest",
            "description": "MCP tool and resource discovery",
        }
    ]
    
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
    uvicorn.run(create_app(), host="0.0.0.0", port=8000) 