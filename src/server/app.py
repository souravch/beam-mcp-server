"""
Apache Beam MCP Server - Main Application

This module provides a FastAPI-based Model Context Protocol (MCP) server
for managing Apache Beam pipelines across different runners (Dataflow, Spark, Flink).
"""

import os
import logging
from typing import Optional, Dict, Any
from fastapi import FastAPI, Request, Depends, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.utils import get_openapi
from dotenv import load_dotenv

from .config import Settings
from .core.client_manager import BeamClientManager
from .models.common import ErrorResponse
from .models.context import MCPContext
from .api.manifest_router import router as manifest_router
from .api.jobs_router import router as jobs_router
from .api.runners_router import router as runners_router
from .api.metrics_router import router as metrics_router
from .api.savepoints_router import router as savepoints_router

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

def create_app(config: Optional[Dict[str, Any]] = None) -> FastAPI:
    """
    Create FastAPI application.
    
    Args:
        config (Optional[Dict[str, Any]]): Configuration dictionary. If not provided,
            will load from environment variables.
    
    Returns:
        FastAPI: FastAPI application
    """
    # Create FastAPI app
    app = FastAPI(
        title="Apache Beam MCP Server",
        description="Management and Control Plane server for Apache Beam",
        version="0.1.0"
    )
    
    # Initialize client manager with configuration
    if config is not None:
        client_manager = BeamClientManager(config)
    else:
        settings = Settings()
        client_manager = BeamClientManager(settings.dict())
        config = settings.dict()
    
    # Store client manager and config in app state
    app.state.client_manager = client_manager
    app.state.config = config
    
    # Add routers
    app.include_router(manifest_router, prefix="/api/v1", tags=["manifest"])
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