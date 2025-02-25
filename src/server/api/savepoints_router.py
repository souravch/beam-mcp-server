"""
Savepoints API Router for the Apache Beam MCP Server.

This module provides endpoints for savepoint management operations.
"""

import logging
from typing import Optional
from fastapi import APIRouter, Depends, Path, Request, BackgroundTasks

from ..models import (
    SavepointParameters, SavepointInfo, SavepointList, JobInfo,
    LLMToolResponse
)
from ..core import BeamClientManager

logger = logging.getLogger(__name__)

# Create router
router = APIRouter()

# Helper function to get client manager
async def get_client_manager(request: Request) -> BeamClientManager:
    """Get the client manager from the application state."""
    return request.app.state.client_manager

# Savepoint Management Endpoints
@router.post("/jobs/{job_id}/savepoints", response_model=LLMToolResponse, summary="Create a savepoint")
async def create_savepoint(
    job_id: str = Path(..., description="Job ID to create savepoint for"),
    savepoint_params: Optional[SavepointParameters] = None,
    background_tasks: BackgroundTasks = None,
    client_manager: BeamClientManager = Depends(get_client_manager)
):
    """
    Create a savepoint for a streaming job.
    
    This endpoint triggers the creation of a savepoint for a streaming job. Savepoints
    capture the exact state of a streaming job, allowing you to restart from that point
    later. This is useful for version upgrades, code changes, or migration.
    
    Note: This is only valid for streaming jobs on runners that support savepoints (Flink, Dataflow).
    
    Example usage by LLM:
    - Create a savepoint before upgrading a pipeline
    - Capture state before maintenance
    - Prepare for job migration
    """
    try:
        if savepoint_params is None:
            savepoint_params = SavepointParameters()
            
        # Get the job to determine its runner type
        job = await client_manager.get_job(job_id)
        client = client_manager.get_client(job.runner_type)
        
        savepoint = await client.create_savepoint(job_id, savepoint_params)
        
        drain_msg = " with drain enabled" if savepoint_params.drain else ""
        return LLMToolResponse(
            success=True,
            data=savepoint,
            message=f"Successfully created savepoint {savepoint.savepoint_id} for job {job_id}{drain_msg}",
            error=None
        )
    except ValueError as e:
        return LLMToolResponse(
            success=False,
            data=None,
            message=f"Failed to create savepoint for job {job_id}",
            error=str(e)
        )
    except Exception as e:
        logger.error(f"Error creating savepoint: {str(e)}")
        return LLMToolResponse(
            success=False,
            data=None,
            message=f"Failed to create savepoint for job {job_id}",
            error=str(e)
        )

@router.get("/jobs/{job_id}/savepoints", response_model=LLMToolResponse, summary="List savepoints")
async def list_savepoints(
    job_id: str = Path(..., description="Job ID to list savepoints for"),
    client_manager: BeamClientManager = Depends(get_client_manager)
):
    """
    List all savepoints for a specific job.
    
    This endpoint returns a list of all savepoints created for a particular job.
    
    Example usage by LLM:
    - Find the most recent savepoint for a job
    - Check if savepoints exist before updating a job
    - List all available restore points
    """
    try:
        # Get the job to determine its runner type
        job = await client_manager.get_job(job_id)
        client = client_manager.get_client(job.runner_type)
        
        savepoints = await client.list_savepoints(job_id)
        
        return LLMToolResponse(
            success=True,
            data=savepoints,
            message=f"Found {len(savepoints.savepoints)} savepoints for job {job_id}",
            error=None
        )
    except ValueError as e:
        return LLMToolResponse(
            success=False,
            data=None,
            message=f"Failed to list savepoints for job {job_id}",
            error=str(e)
        )
    except Exception as e:
        logger.error(f"Error listing savepoints: {str(e)}")
        return LLMToolResponse(
            success=False,
            data=None,
            message=f"Failed to list savepoints for job {job_id}",
            error=str(e)
        )

@router.post("/jobs/{job_id}/restore/{savepoint_id}", response_model=LLMToolResponse, summary="Restore from savepoint")
async def restore_from_savepoint(
    job_id: str = Path(..., description="Job ID to restore"),
    savepoint_id: str = Path(..., description="Savepoint ID to restore from"),
    client_manager: BeamClientManager = Depends(get_client_manager)
):
    """
    Restore a job from a savepoint.
    
    This endpoint restarts a job from a previously created savepoint, preserving its
    processing state. This is useful after job upgrades, code changes, or when recovering
    from failures.
    
    Example usage by LLM:
    - Restore a job after code updates
    - Roll back to a previous state
    - Recover a job after planned maintenance
    """
    try:
        # Get the job to determine its runner type
        job = await client_manager.get_job(job_id)
        client = client_manager.get_client(job.runner_type)
        
        restored_job = await client.restore_from_savepoint(job_id, savepoint_id)
        
        return LLMToolResponse(
            success=True,
            data=restored_job,
            message=f"Successfully restored job {job_id} from savepoint {savepoint_id}",
            error=None
        )
    except ValueError as e:
        return LLMToolResponse(
            success=False,
            data=None,
            message=f"Failed to restore job {job_id} from savepoint {savepoint_id}",
            error=str(e)
        )
    except Exception as e:
        logger.error(f"Error restoring from savepoint: {str(e)}")
        return LLMToolResponse(
            success=False,
            data=None,
            message=f"Failed to restore job {job_id} from savepoint {savepoint_id}",
            error=str(e)
        ) 