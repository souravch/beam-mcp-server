"""
Jobs API Router for the Apache Beam MCP Server.

This module provides endpoints for job management operations.
"""

import logging
from typing import Optional, List, Dict, Any, Union
from fastapi import APIRouter, Depends, Query, Path, BackgroundTasks, Request, HTTPException
import uuid
from datetime import datetime
import sys
from enum import Enum

# Define JobCancelMode here since it's not found in imported modules
class JobCancelMode(str, Enum):
    """Job cancellation mode."""
    DRAIN = "DRAIN"
    CANCEL = "CANCEL"
    FORCE = "FORCE"

# Import from job.py
from src.server.models.job import (
    Job, JobStatus, JobType, JobState, JobList
)
# Import from jobs.py 
from src.server.models.jobs import JobParameters, JobUpdateParameters, JobInfo
# Import from runner.py
from src.server.models.runner import RunnerType
# Import from savepoint.py
from src.server.models.savepoint import SavepointRequest, SavepointStatus, Savepoint
# Import from common.py for LLMToolResponse
from src.server.models.common import LLMToolResponse
# Import BeamClientManager
from src.server.core.client_manager import BeamClientManager
from src.server.constants import API_PREFIX, JSON_HEADERS

# Import authentication dependencies if available
try:
    from ..auth import require_read, require_write
except ImportError:
    # Create dummy auth functions for backward compatibility
    from fastapi import Depends
    from typing import Callable, Any
    
    # Create a dummy UserSession for type hints
    class UserSession:
        user_id: str = "anonymous"
        roles: list = ["admin"]
    
    # Create a dummy dependency that just returns a default user
    async def get_dummy_user():
        return UserSession()
    
    # Create dummy auth dependencies
    def require_read(func: Callable = None):
        return get_dummy_user
        
    def require_write(func: Callable = None):
        return get_dummy_user

logger = logging.getLogger(__name__)

# Create router - Remove the prefix since it's already included in app.py
router = APIRouter(tags=["jobs"])

# Define get_client_manager dependency function
async def get_client_manager(request: Request) -> BeamClientManager:
    """Get the client manager from the application state."""
    return request.app.state.client_manager

# Job Management Endpoints
@router.post("", response_model=LLMToolResponse, summary="Create a new pipeline job")
async def create_job(
    job_params: JobParameters, 
    background_tasks: BackgroundTasks,
    client_manager: BeamClientManager = Depends(get_client_manager),
    user = Depends(require_write)
):
    """
    Create a new pipeline job.
    
    Args:
        job_params: Job parameters
        background_tasks: Background tasks
        client_manager: Client manager
        user: Authentication user
        
    Returns:
        LLMToolResponse with job creation result
    """
    try:
        # Validate input
        if not job_params.runner_type:
            return LLMToolResponse(
                success=False,
                message="Missing runner_type",
                data=None
            )
        
        # Create job
        job = await client_manager.create_job(job_params, background_tasks)
        
        response_data = {
            "job_id": job.job_id,
            "runner_type": job.runner_type,
            "job_type": job.job_type,
            "status": job.status,
            "job_name": job.job_name
        }
        
        return LLMToolResponse(
            success=True,
            message=f"Created job {job.job_id} on {job.runner_type}",
            data=response_data
        )
    except Exception as e:
        logger.error(f"Error creating job: {str(e)}")
        return LLMToolResponse(
            success=False,
            message=f"Failed to create job: {str(e)}",
            data=None
        )

@router.get("", response_model=LLMToolResponse, summary="List all jobs")
async def list_jobs(
    status: Optional[str] = Query(None, description="Filter by job status"),
    runner: Optional[str] = Query(None, description="Filter by runner type"),
    limit: int = Query(100, description="Maximum number of jobs to return"),
    offset: int = Query(0, description="Offset for pagination"),
    client_manager: BeamClientManager = Depends(get_client_manager),
    user = Depends(require_read)
):
    """
    List all pipeline jobs with optional filtering.
    
    Args:
        status: Filter by job status
        runner: Filter by runner type
        limit: Maximum number of jobs to return
        offset: Offset for pagination
        client_manager: Client manager
        user: Authentication user
        
    Returns:
        LLMToolResponse with job list
    """
    try:
        # Debug print parameters
        logger.debug(f"list_jobs called with: status={status}, runner={runner}")
        
        # Initialize client manager - should be done in app startup
        if not client_manager.initialized:
            logger.debug("Initializing client manager from list_jobs")
            try:
                # This shouldn't run, should be initialized at startup
                await client_manager.initialize()
            except Exception as init_err:
                logger.error(f"Error initializing client manager: {str(init_err)}")
                return LLMToolResponse(
                    success=False,
                    message=f"Client manager initialization error: {str(init_err)}",
                    data={"jobs": []}
                )
        
        # List jobs from client manager
        job_list = await client_manager.list_jobs(
            status=status,
            runner=runner,
            limit=limit,
            offset=offset
        )
        
        # Convert job list to response
        jobs_data = []
        for job in job_list.jobs:
            job_dict = {
                "job_id": job.job_id,
                "job_name": job.job_name,
                "runner_type": job.runner_type.value if hasattr(job.runner_type, "value") else job.runner_type,
                "job_type": job.job_type.value if hasattr(job.job_type, "value") else job.job_type,
                "status": job.status.value if hasattr(job.status, "value") else job.status,
                "created_at": job.created_at.isoformat() if job.created_at else None,
                "updated_at": job.updated_at.isoformat() if job.updated_at else None
            }
            if job.metrics and hasattr(job.metrics, "model_dump"):
                job_dict["metrics"] = job.metrics.model_dump()
            jobs_data.append(job_dict)
        
        response_data = {
            "jobs": jobs_data,
            "next_page_token": job_list.next_page_token,
            "total_size": job_list.total_size
        }
        
        # Return response
        return LLMToolResponse(
            success=True,
            message=f"Retrieved {len(jobs_data)} jobs",
            data=response_data
        )
    except Exception as e:
        logger.error(f"Error listing jobs: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        
        return LLMToolResponse(
            success=False,
            message=f"Failed to list jobs: {str(e)}",
            data={"jobs": []}
        )

@router.get("/{job_id}", response_model=LLMToolResponse, summary="Get job details")
async def get_job(
    job_id: str = Path(..., description="Job ID to retrieve"),
    client_manager: BeamClientManager = Depends(get_client_manager),
    user = Depends(require_read)
):
    """
    Get details for a specific job.
    
    Args:
        job_id: Job ID to retrieve
        client_manager: Client manager
        user: Authentication user
        
    Returns:
        LLMToolResponse with job details
    """
    try:
        job = await client_manager.get_job(job_id)
        if not job:
            return LLMToolResponse(
                success=False,
                message=f"Job {job_id} not found",
                data=None
            )
        
        return LLMToolResponse(
            success=True,
            message=f"Retrieved job {job_id}",
            data=job
        )
    except Exception as e:
        logger.error(f"Error getting job {job_id}: {str(e)}")
        return LLMToolResponse(
            success=False,
            message=f"Failed to get job: {str(e)}",
            data=None
        )

@router.put("/{job_id}", response_model=LLMToolResponse, summary="Update job configuration")
async def update_job(
    job_id: str = Path(..., description="Job ID to update"),
    update_params: JobUpdateParameters = None,
    client_manager: BeamClientManager = Depends(get_client_manager),
    user = Depends(require_write)
):
    """
    Update job configuration.
    
    Args:
        job_id: Job ID to update
        update_params: Parameters to update
        client_manager: Client manager
        user: Authentication user
        
    Returns:
        LLMToolResponse with update result
    """
    try:
        updated_job = await client_manager.update_job(job_id, update_params)
        return LLMToolResponse(
            success=True,
            message=f"Job {job_id} updated successfully",
            data=updated_job
        )
    except Exception as e:
        logger.error(f"Error updating job {job_id}: {str(e)}")
        return LLMToolResponse(
            success=False,
            message=f"Failed to update job: {str(e)}",
            data=None
        )

@router.delete("/{job_id}", response_model=LLMToolResponse, summary="Cancel a job")
async def cancel_job(
    job_id: str = Path(..., description="Job ID to cancel"),
    mode: JobCancelMode = Query(JobCancelMode.DRAIN, description="Cancellation mode"),
    client_manager: BeamClientManager = Depends(get_client_manager),
    user = Depends(require_write)
):
    """
    Cancel a job.
    
    Args:
        job_id: Job ID to cancel
        mode: Cancellation mode
        client_manager: Client manager
        user: Authentication user
        
    Returns:
        LLMToolResponse with cancellation result
    """
    try:
        job = await client_manager.cancel_job(job_id)
        return LLMToolResponse(
            success=True,
            message=f"Job {job_id} cancelled successfully",
            data=job
        )
    except Exception as e:
        logger.error(f"Error canceling job {job_id}: {str(e)}")
        return LLMToolResponse(
            success=False,
            message=f"Failed to cancel job: {str(e)}",
            data=None
        )

@router.get("/{job_id}/status", response_model=LLMToolResponse, summary="Get job status")
async def get_job_status(
    job_id: str = Path(..., description="Job ID to get status for"),
    client_manager: BeamClientManager = Depends(get_client_manager)
):
    """
    Get job status.
    
    Args:
        job_id: Job ID to get status for
        client_manager: Client manager
        
    Returns:
        LLMToolResponse with job status
    """
    try:
        job = await client_manager.get_job(job_id)
        if not job:
            return LLMToolResponse(
                success=False,
                message=f"Job {job_id} not found",
                data=None
            )
        
        return LLMToolResponse(
            success=True,
            message=f"Retrieved status for job {job_id}",
            data={"status": job.status}
        )
    except Exception as e:
        logger.error(f"Error getting job status for {job_id}: {str(e)}")
        return LLMToolResponse(
            success=False,
            message=f"Failed to get job status: {str(e)}",
            data=None
        )

@router.post("/{job_id}/savepoints", response_model=LLMToolResponse, summary="Create a savepoint")
async def create_savepoint(
    job_id: str = Path(..., description="Job ID to create savepoint for"),
    savepoint_params: SavepointRequest = None,
    client_manager: BeamClientManager = Depends(get_client_manager),
    user = Depends(require_write)
):
    """
    Create a savepoint for a job.
    
    Args:
        job_id: Job ID to create savepoint for
        savepoint_params: Savepoint parameters
        client_manager: Client manager
        user: Authentication user
        
    Returns:
        LLMToolResponse with savepoint creation result
    """
    if savepoint_params is None:
        savepoint_params = SavepointRequest()
        
    try:
        # Check if job exists
        job = await client_manager.get_job(job_id)
        if not job:
            return LLMToolResponse(
                success=False,
                message=f"Job {job_id} not found",
                data=None
            )
            
        # Check if job supports savepoints
        if job.runner_type not in [RunnerType.FLINK]:
            return LLMToolResponse(
                success=False,
                message=f"Savepoints not supported for runner type {job.runner_type}",
                data=None
            )
        
        # Create savepoint
        savepoint = await client_manager.create_savepoint(
            job_id, 
            savepoint_type=savepoint_params.savepoint_type,
            cancel_job=savepoint_params.cancel_job,
            savepoint_path=savepoint_params.savepoint_path
        )
        
        # Format response data
        response_data = {
            "savepoint_id": savepoint.savepoint_id,
            "job_id": savepoint.job_id,
            "savepoint_type": savepoint.savepoint_type,
            "status": savepoint.status,
            "created_at": savepoint.created_at.isoformat() if savepoint.created_at else None
        }
        
        # Include optional fields if present
        if savepoint.location:
            response_data["location"] = savepoint.location
        if savepoint.error:
            response_data["error"] = savepoint.error
        if savepoint.updated_at:
            response_data["updated_at"] = savepoint.updated_at.isoformat()
        
        return LLMToolResponse(
            success=True,
            message=f"Created savepoint for job {job_id}",
            data=response_data
        )
    except Exception as e:
        # Handle savepoint creation error
        logger.error(f"Error creating savepoint for job {job_id}: {str(e)}")
        
        # Create a default response with error details
        response_data = {
            "savepoint_id": str(uuid.uuid4()),
            "job_id": job_id,
            "savepoint_type": savepoint_params.savepoint_type if savepoint_params else "SAVEPOINT",
            "status": SavepointStatus.FAILED,
            "error": str(e),
            "created_at": datetime.utcnow().isoformat()
        }
        
        return LLMToolResponse(
            success=False,
            message=f"Failed to create savepoint: {str(e)}",
            data=response_data
        )

@router.get("/{job_id}/savepoints/{savepoint_id}", response_model=LLMToolResponse, summary="Get savepoint details")
async def get_savepoint(
    job_id: str = Path(..., description="Job ID"),
    savepoint_id: str = Path(..., description="Savepoint ID"),
    client_manager: BeamClientManager = Depends(get_client_manager),
    user = Depends(require_read)
):
    """
    Get savepoint status.
    
    Args:
        job_id: Job ID
        savepoint_id: Savepoint ID
        client_manager: Client manager
        user: Authentication user
        
    Returns:
        LLMToolResponse with savepoint status
    """
    try:
        # Check if job exists
        job = await client_manager.get_job(job_id)
        if not job:
            return LLMToolResponse(
                success=False,
                message=f"Job {job_id} not found",
                data=None
            )
            
        # Check if job supports savepoints
        if job.runner_type not in [RunnerType.FLINK]:
            return LLMToolResponse(
                success=False,
                message=f"Savepoints not supported for runner type {job.runner_type}",
                data=None
            )
        
        # Get savepoint status
        savepoint = await client_manager.get_savepoint(job_id, savepoint_id)
        if not savepoint:
            return LLMToolResponse(
                success=False,
                message=f"Savepoint {savepoint_id} not found for job {job_id}",
                data=None
            )
        
        # Format response data
        response_data = {
            "savepoint_id": savepoint.savepoint_id,
            "job_id": savepoint.job_id,
            "savepoint_type": savepoint.savepoint_type,
            "status": savepoint.status,
            "created_at": savepoint.created_at.isoformat() if savepoint.created_at else None
        }
        
        # Include optional fields if present
        if savepoint.location:
            response_data["location"] = savepoint.location
        if savepoint.error:
            response_data["error"] = savepoint.error
        if savepoint.updated_at:
            response_data["updated_at"] = savepoint.updated_at.isoformat()
        
        return LLMToolResponse(
            success=True,
            message=f"Retrieved savepoint {savepoint_id} for job {job_id}",
            data=response_data
        )
    except Exception as e:
        # Handle savepoint retrieval error
        logger.error(f"Error getting savepoint {savepoint_id} for job {job_id}: {str(e)}")
        
        # Create a default response with error details
        response_data = {
            "savepoint_id": savepoint_id,
            "job_id": job_id,
            "status": "ERROR",
            "error": str(e)
        }
        
        return LLMToolResponse(
            success=False,
            message=f"Failed to get savepoint: {str(e)}",
            data=response_data
        )

@router.get("/{job_id}/savepoints", response_model=LLMToolResponse, summary="List savepoints")
async def list_savepoints(
    job_id: str = Path(..., description="Job ID to list savepoints for"),
    client_manager: BeamClientManager = Depends(get_client_manager),
    user = Depends(require_read)
):
    """
    List all savepoints for a job.
    
    Args:
        job_id: Job ID to list savepoints for
        client_manager: Client manager
        user: Authentication user
        
    Returns:
        LLMToolResponse with savepoint list
    """
    try:
        # Check if job exists
        job = await client_manager.get_job(job_id)
        if not job:
            return LLMToolResponse(
                success=False,
                message=f"Job {job_id} not found",
                data={"savepoints": []}
            )
            
        # Check if job supports savepoints
        if job.runner_type not in [RunnerType.FLINK]:
            return LLMToolResponse(
                success=False,
                message=f"Savepoints not supported for runner type {job.runner_type}",
                data={"savepoints": []}
            )
        
        # List savepoints
        savepoints = await client_manager.list_savepoints(job_id)
        
        # Format response data
        savepoints_data = []
        for sp in savepoints:
            sp_dict = {
                "savepoint_id": sp.savepoint_id,
                "job_id": sp.job_id,
                "savepoint_type": sp.savepoint_type,
                "status": sp.status,
                "created_at": sp.created_at.isoformat() if sp.created_at else None
            }
            if sp.location:
                sp_dict["location"] = sp.location
            if sp.error:
                sp_dict["error"] = sp.error
            if sp.updated_at:
                sp_dict["updated_at"] = sp.updated_at.isoformat()
            savepoints_data.append(sp_dict)
        
        return LLMToolResponse(
            success=True,
            message=f"Retrieved {len(savepoints_data)} savepoints for job {job_id}",
            data={"savepoints": savepoints_data}
        )
    except Exception as e:
        logger.error(f"Error listing savepoints for job {job_id}: {str(e)}")
        return LLMToolResponse(
            success=False,
            message=f"Failed to list savepoints: {str(e)}",
            data={"savepoints": []}
        )

@router.get("/{job_id}/metrics", response_model=LLMToolResponse, summary="Get job metrics")
async def get_job_metrics(
    job_id: str = Path(..., description="Job ID to get metrics for"),
    client_manager: BeamClientManager = Depends(get_client_manager),
    user = Depends(require_read)
):
    """
    Get metrics for a job.
    
    Args:
        job_id: Job ID to get metrics for
        client_manager: Client manager
        user: Authentication user
        
    Returns:
        LLMToolResponse with job metrics
    """
    try:
        # Check if job exists
        job = await client_manager.get_job(job_id)
        if not job:
            return LLMToolResponse(
                success=False,
                message=f"Job {job_id} not found",
                data=None
            )
        
        # Get metrics - job should have metrics property
        metrics = job.metrics if hasattr(job, "metrics") else {}
        
        # Convert to dict if not already
        if hasattr(metrics, "model_dump"):
            metrics = metrics.model_dump()
        
        return LLMToolResponse(
            success=True,
            message=f"Retrieved metrics for job {job_id}",
            data=metrics or {}
        )
    except Exception as e:
        logger.error(f"Error getting metrics for job {job_id}: {str(e)}")
        return LLMToolResponse(
            success=False,
            message=f"Failed to get job metrics: {str(e)}",
            data={}
        )

@router.post("/{job_id}/restart", response_model=LLMToolResponse, summary="Restart a job")
async def restart_job(
    job_id: str = Path(..., description="Job ID to restart"),
    client_manager: BeamClientManager = Depends(get_client_manager),
    user = Depends(require_write)
):
    """
    Restart a job.
    
    Args:
        job_id: Job ID to restart
        client_manager: Client manager
        user: Authentication user
        
    Returns:
        LLMToolResponse with restart result
    """
    try:
        job = await client_manager.restart_job(job_id)
        return LLMToolResponse(
            success=True,
            message=f"Job {job_id} restarted successfully",
            data=job
        )
    except Exception as e:
        logger.error(f"Error restarting job {job_id}: {str(e)}")
        return LLMToolResponse(
            success=False,
            message=f"Failed to restart job: {str(e)}",
            data=None
        )

@router.get("/{job_id}/logs", response_model=LLMToolResponse, summary="Get job logs")
async def get_job_logs(
    job_id: str = Path(..., description="Job ID to get logs for"),
    client_manager: BeamClientManager = Depends(get_client_manager),
    user = Depends(require_read)
):
    """
    Get job logs.
    
    Args:
        job_id: Job ID to get logs for
        client_manager: Client manager
        user: Authentication user
        
    Returns:
        LLMToolResponse with job logs
    """
    try:
        # Get job logs
        logs = await client_manager.get_job_logs(job_id)
        if not logs:
            return LLMToolResponse(
                success=False,
                message=f"Job {job_id} not found or no logs available",
                data=None
            )
        
        return LLMToolResponse(
            success=True,
            message=f"Retrieved logs for job {job_id}",
            data=logs
        )
    except Exception as e:
        logger.error(f"Error getting logs for job {job_id}: {str(e)}")
        return LLMToolResponse(
            success=False,
            message=f"Failed to get job logs: {str(e)}",
            data=None
        ) 