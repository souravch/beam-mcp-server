"""
Jobs API Router for the Apache Beam MCP Server.

This module provides endpoints for job management operations.
"""

import logging
from typing import Optional, List
from fastapi import APIRouter, Depends, Query, Path, BackgroundTasks, Request, HTTPException

from ..models import (
    RunnerType, JobType, JobState, JobParameters, JobInfo, JobList,
    JobUpdateParameters, LLMToolResponse
)
from ..core import BeamClientManager

logger = logging.getLogger(__name__)

# Create router
router = APIRouter()

# Helper function to get client manager
async def get_client_manager(request: Request) -> BeamClientManager:
    """Get the client manager from the application state."""
    return request.app.state.client_manager

# Job Management Endpoints
@router.post("/jobs", response_model=LLMToolResponse, summary="Create a new pipeline job")
async def create_job(
    job_params: JobParameters, 
    background_tasks: BackgroundTasks,
    client_manager: BeamClientManager = Depends(get_client_manager)
):
    """
    Create and launch a data pipeline job on the specified runner.
    
    This endpoint creates a new pipeline job using the provided parameters. It supports different runners
    including Dataflow, Spark, and Flink. You can specify either a template path or code path to define
    the pipeline logic.
    
    - For template-based jobs, provide the template_path and template_parameters
    - For code-based jobs, provide the code_path
    
    Example usage by LLM:
    - Create a batch job to process daily customer data
    - Launch a streaming job for real-time event processing
    - Set up a Spark job for data transformation
    """
    try:
        # Get the appropriate client based on runner type
        client = client_manager.get_client(job_params.runner_type)
        
        # Create the job
        job = await client.create_job(job_params)
        
        return LLMToolResponse(
            success=True,
            data=job,
            message=f"Successfully created {job_params.job_type.value} job '{job_params.job_name}' on {job_params.runner_type.value} runner with job ID: {job.job_id}",
            error=None
        )
    except Exception as e:
        logger.error(f"Error creating job: {str(e)}")
        return LLMToolResponse(
            success=False,
            data=None,
            message=f"Failed to create job",
            error=str(e)
        )

@router.get("/jobs", response_model=LLMToolResponse, summary="List pipeline jobs")
async def list_jobs(
    runner_type: Optional[RunnerType] = Query(None, description="Filter by runner type"),
    job_type: Optional[JobType] = Query(None, description="Filter by job type"),
    page_size: int = Query(10, description="Number of jobs to return", ge=1, le=100),
    page_token: Optional[str] = Query(None, description="Token for pagination"),
    client_manager: BeamClientManager = Depends(get_client_manager)
):
    """
    List pipeline jobs with optional filtering by runner type and job type.
    
    This endpoint returns a list of jobs that match the specified criteria. You can filter by:
    - Runner type (dataflow, spark, flink, direct)
    - Job type (BATCH, STREAMING)
    
    Example usage by LLM:
    - List all running Dataflow jobs
    - Find streaming jobs on Flink
    - Get the most recent batch processing jobs
    """
    try:
        # Use default client if no runner type is specified
        if runner_type:
            client = client_manager.get_client(runner_type)
        else:
            # Use the first client available, typically the default runner
            default_runner = RunnerType(client_manager.config['default_runner'])
            client = client_manager.get_client(default_runner)
        
        jobs = await client.list_jobs(runner_type, job_type, page_size, page_token)
        
        filter_description = ""
        if runner_type:
            filter_description += f" on {runner_type.value} runner"
        if job_type:
            filter_description += f" of type {job_type.value}"
        
        return LLMToolResponse(
            success=True,
            data=jobs,
            message=f"Found {len(jobs.jobs)} jobs{filter_description} (total: {jobs.total_count})",
            error=None
        )
    except Exception as e:
        logger.error(f"Error listing jobs: {str(e)}")
        return LLMToolResponse(
            success=False,
            data=None,
            message="Failed to list jobs",
            error=str(e)
        )

@router.get("/jobs/{job_id}", response_model=LLMToolResponse, summary="Get job details")
async def get_job(
    job_id: str = Path(..., description="Job ID to retrieve"),
    client_manager: BeamClientManager = Depends(get_client_manager)
):
    """
    Get detailed information about a specific pipeline job.
    
    This endpoint returns comprehensive details about a job, including its current state,
    configuration, and basic metrics.
    
    Example usage by LLM:
    - Check the status of a specific job
    - Get configuration details of a running job
    - Retrieve job information before making changes
    """
    try:
        # In this simplified implementation, all jobs are stored in the manager
        # In a real implementation, we would need to lookup which runner has this job
        job = await client_manager.get_job(job_id)
        
        return LLMToolResponse(
            success=True,
            data=job,
            message=f"{job.job_type.value} job '{job.job_name}' on {job.runner_type.value} is in state {job.current_state.value}",
            error=None
        )
    except ValueError as e:
        return LLMToolResponse(
            success=False,
            data=None,
            message=f"Failed to get job {job_id}",
            error=str(e)
        )
    except Exception as e:
        logger.error(f"Error getting job: {str(e)}")
        return LLMToolResponse(
            success=False,
            data=None,
            message=f"Failed to get job {job_id}",
            error=str(e)
        )

@router.put("/jobs/{job_id}", response_model=LLMToolResponse, summary="Update job configuration")
async def update_job(
    job_id: str = Path(..., description="Job ID to update"),
    update_params: JobUpdateParameters = None,
    client_manager: BeamClientManager = Depends(get_client_manager)
):
    """
    Update a running pipeline job's configuration.
    
    This endpoint allows you to update certain aspects of a running job, such as scaling
    parameters or other runner-specific configurations. The available options depend on
    the runner type and job state.
    
    Example usage by LLM:
    - Scale up a job to handle increased load
    - Change machine type for better performance
    - Adjust parallelism settings
    """
    try:
        if update_params is None:
            update_params = JobUpdateParameters()
            
        # Get the job to determine its runner type
        job = await client_manager.get_job(job_id)
        client = client_manager.get_client(job.runner_type)
        
        updated_job = await client.update_job(job_id, update_params)
        
        update_description = ""
        if update_params and update_params.scaling:
            scaling_str = ", ".join(f"{k}={v}" for k, v in update_params.scaling.items())
            update_description += f" with scaling: {scaling_str}"
        
        return LLMToolResponse(
            success=True,
            data=updated_job,
            message=f"Successfully updated job {job_id}{update_description}",
            error=None
        )
    except ValueError as e:
        return LLMToolResponse(
            success=False,
            data=None,
            message=f"Failed to update job {job_id}",
            error=str(e)
        )
    except Exception as e:
        logger.error(f"Error updating job: {str(e)}")
        return LLMToolResponse(
            success=False,
            data=None,
            message=f"Failed to update job {job_id}",
            error=str(e)
        )

@router.delete("/jobs/{job_id}", response_model=LLMToolResponse, summary="Cancel job")
async def cancel_job(
    job_id: str = Path(..., description="Job ID to cancel"),
    client_manager: BeamClientManager = Depends(get_client_manager)
):
    """
    Cancel a running pipeline job.
    
    This endpoint cancels a job, causing it to stop processing data and clean up resources.
    For streaming jobs, this is an immediate shutdown without savepointing. Use the savepoint
    endpoint first if you want to preserve state.
    
    Example usage by LLM:
    - Cancel a job that's no longer needed
    - Stop a problematic job
    - Terminate a job before making changes
    """
    try:
        # Get the job to determine its runner type
        job = await client_manager.get_job(job_id)
        client = client_manager.get_client(job.runner_type)
        
        cancelled_job = await client.cancel_job(job_id)
        
        return LLMToolResponse(
            success=True,
            data=cancelled_job,
            message=f"Successfully cancelled job {job_id}",
            error=None
        )
    except ValueError as e:
        return LLMToolResponse(
            success=False,
            data=None,
            message=f"Failed to cancel job {job_id}",
            error=str(e)
        )
    except Exception as e:
        logger.error(f"Error cancelling job: {str(e)}")
        return LLMToolResponse(
            success=False,
            data=None,
            message=f"Failed to cancel job {job_id}",
            error=str(e)
        )

@router.get("/jobs/{job_id}/status", response_model=LLMToolResponse, summary="Get job status")
async def get_job_status(
    job_id: str = Path(..., description="Job ID to get status for"),
    client_manager: BeamClientManager = Depends(get_client_manager)
):
    """
    Get the current status of a pipeline job.
    
    This endpoint returns the current state and basic information about a job,
    similar to the get_job endpoint but with a focus on the current status.
    
    Example usage by LLM:
    - Check if a job is still running
    - Verify a job's status before taking action
    - Monitor job state changes
    """
    try:
        job = await client_manager.get_job(job_id)
        
        status_data = {
            "job_id": job.job_id,
            "job_name": job.job_name,
            "current_state": job.current_state,
            "runner_type": job.runner_type,
            "job_type": job.job_type,
            "update_time": job.update_time
        }
        
        return LLMToolResponse(
            success=True,
            data=status_data,
            message=f"Job {job_id} is in state {job.current_state.value}",
            error=None
        )
    except ValueError as e:
        return LLMToolResponse(
            success=False,
            data=None,
            message=f"Failed to get status for job {job_id}",
            error=str(e)
        )
    except Exception as e:
        logger.error(f"Error getting job status: {str(e)}")
        return LLMToolResponse(
            success=False,
            data=None,
            message=f"Failed to get status for job {job_id}",
            error=str(e)
        ) 