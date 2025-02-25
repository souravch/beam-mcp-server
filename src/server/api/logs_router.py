"""
Logs API Router for the Apache Beam MCP Server.

This module provides endpoints for retrieving job logs.
"""

import logging
from typing import Optional
from fastapi import APIRouter, Depends, Path, Query, Request

from ..models import LogList, LLMToolResponse
from ..core import BeamClientManager

logger = logging.getLogger(__name__)

# Create router
router = APIRouter()

# Helper function to get client manager
async def get_client_manager(request: Request) -> BeamClientManager:
    """Get the client manager from the application state."""
    return request.app.state.client_manager

# Logs Endpoints
@router.get("/jobs/{job_id}/logs", response_model=LLMToolResponse, summary="Get job logs")
async def get_job_logs(
    job_id: str = Path(..., description="Job ID to get logs for"),
    max_results: int = Query(10, description="Maximum number of log entries to return", ge=1, le=1000),
    page_token: Optional[str] = Query(None, description="Token for pagination"),
    client_manager: BeamClientManager = Depends(get_client_manager)
):
    """
    Get logs for a pipeline job.
    
    This endpoint returns log entries for a job, which can be used for monitoring,
    debugging, and troubleshooting. Logs are paginated, so you can retrieve them
    in chunks.
    
    Example usage by LLM:
    - Check for error messages in a failed job
    - Monitor job progress
    - Debug processing issues
    """
    try:
        # Get the job to determine its runner type
        job = await client_manager.get_job(job_id)
        client = client_manager.get_client(job.runner_type)
        
        logs = await client.get_job_logs(job_id, max_results, page_token)
        
        return LLMToolResponse(
            success=True,
            data=logs,
            message=f"Successfully retrieved {len(logs.logs)} log entries for job {job_id}",
            error=None
        )
    except ValueError as e:
        return LLMToolResponse(
            success=False,
            data=None,
            message=f"Failed to get logs for job {job_id}",
            error=str(e)
        )
    except Exception as e:
        logger.error(f"Error getting job logs: {str(e)}")
        return LLMToolResponse(
            success=False,
            data=None,
            message=f"Failed to get logs for job {job_id}",
            error=str(e)
        ) 