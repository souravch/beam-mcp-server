"""
Apache Beam MCP Server - Main Application

This module provides a FastAPI-based Model Context Protocol (MCP) server
for managing Apache Beam pipelines across different runners (Dataflow, Spark, Flink).
"""

import os
import logging
from fastapi import FastAPI, Request, Depends
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.utils import get_openapi
from dotenv import load_dotenv

from .config import Settings
from .mcp_server import DataflowMCPServer
from .models.common import ErrorResponse
from .models.context import MCPContext

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

def create_app() -> FastAPI:
    # Load settings
    settings = Settings()
    
    # Create MCP server
    mcp_server = DataflowMCPServer(settings)
    
    # Create FastAPI app using MCP server
    app = mcp_server.fastapi_app
    
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
            "name": "MCP",
            "description": "Model Context Protocol operations",
        },
        {
            "name": "Jobs",
            "description": "Dataflow job management operations",
        },
        {
            "name": "Savepoints",
            "description": "Job savepoint operations",
        },
        {
            "name": "Runners",
            "description": "Dataflow runner operations",
        },
        {
            "name": "Metrics",
            "description": "Job metrics operations",
        }
    ]
    
    logger.info(f"Initialized Apache Beam MCP Server with default runner: {settings.default_runner}")
    
    return app

if __name__ == '__main__':
    import uvicorn
    port = int(os.environ.get('PORT', 8080))
    uvicorn.run(
        "app:create_app", 
        host="0.0.0.0", 
        port=port, 
        factory=True,
        log_level="debug"
    ) 