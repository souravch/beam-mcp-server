"""
Health Check API Router for the Apache Beam MCP Server.

This module provides a health check endpoint to verify server status.
"""

import logging
import os
from datetime import datetime
from fastapi import APIRouter

from ..models import HealthResponse, LLMToolResponse

logger = logging.getLogger(__name__)

# Create router
router = APIRouter()

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
async def llm_health_check():
    """
    LLM-friendly health check endpoint.
    
    This endpoint is designed for LLMs to check server health with a standardized
    response format (success, data, message, error).
    
    Returns:
        LLMToolResponse: Standardized health check response for LLM consumption
    """
    try:
        health_data = {
            "status": "healthy",
            "timestamp": datetime.utcnow().isoformat(),
            "version": "1.0.0",
            "service": "beam-mcp",
            "environment": os.environ.get("ENVIRONMENT", "development")
        }
        
        return LLMToolResponse(
            success=True,
            data=health_data,
            message="Apache Beam MCP Server is healthy and operational",
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