"""
Metrics API Router for the Apache Beam MCP Server.

This module provides endpoints for retrieving job metrics.
"""

import logging
from fastapi import APIRouter, Depends, Path, Request

from ..models import JobMetrics, LLMToolResponse
from ..core import BeamClientManager

logger = logging.getLogger(__name__)

# Create router
router = APIRouter()

# Helper function to get client manager
async def get_client_manager(request: Request) -> BeamClientManager:
    """Get the client manager from the application state."""
    return request.app.state.client_manager

# Metrics Endpoints
@router.get("/jobs/{job_id}/metrics", response_model=LLMToolResponse, summary="Get job metrics")
async def get_job_metrics(
    job_id: str = Path(..., description="Job ID to get metrics for"),
    client_manager: BeamClientManager = Depends(get_client_manager)
):
    """
    Get detailed metrics for a pipeline job.
    
    This endpoint returns comprehensive metrics for a job, including resource usage,
    processing rates, and job-specific metrics. The available metrics depend on the
    runner type and job type.
    
    Example usage by LLM:
    - Check performance metrics for a job
    - Monitor resource usage
    - Diagnose processing bottlenecks
    """
    try:
        # Get the job to determine its runner type
        job = await client_manager.get_job(job_id)
        client = client_manager.get_client(job.runner_type)
        
        metrics = await client.get_job_metrics(job_id)
        
        return LLMToolResponse(
            success=True,
            data=metrics,
            message=f"Successfully retrieved metrics for job {job_id}",
            error=None
        )
    except ValueError as e:
        return LLMToolResponse(
            success=False,
            data=None,
            message=f"Failed to get metrics for job {job_id}",
            error=str(e)
        )
    except Exception as e:
        logger.error(f"Error getting job metrics: {str(e)}")
        return LLMToolResponse(
            success=False,
            data=None,
            message=f"Failed to get metrics for job {job_id}",
            error=str(e)
        ) 