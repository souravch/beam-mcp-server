"""
Runners API Router for the Apache Beam MCP Server.

This module provides endpoints for runner management operations.
"""

import logging
from fastapi import APIRouter, Depends, Path, Request

from ..models import (
    RunnerType, RunnerList, RunnerInfo, RunnerScalingParameters,
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

# Runner Management Endpoints
@router.get("/runners", response_model=LLMToolResponse, summary="List available runners")
async def list_runners(
    client_manager: BeamClientManager = Depends(get_client_manager)
):
    """
    List all available pipeline runners.
    
    This endpoint returns information about all available runner types, including
    their capabilities, configuration examples, and supported job types.
    
    Example usage by LLM:
    - Find which runners support streaming jobs
    - Get configuration examples for a specific runner
    - Compare features between different runners
    """
    try:
        runners = await client_manager.list_runners()
        
        return LLMToolResponse(
            success=True,
            data=runners,
            message=f"Successfully retrieved {len(runners.runners)} available runners",
            error=None
        )
    except Exception as e:
        logger.error(f"Error listing runners: {str(e)}")
        return LLMToolResponse(
            success=False,
            data=None,
            message="Failed to list runners",
            error=str(e)
        )

@router.get("/runners/{runner_type}", response_model=LLMToolResponse, summary="Get runner details")
async def get_runner(
    runner_type: RunnerType = Path(..., description="Runner type to get details for"),
    client_manager: BeamClientManager = Depends(get_client_manager)
):
    """
    Get detailed information about a specific runner.
    
    This endpoint returns comprehensive details about a runner, including its
    capabilities, configuration examples, and supported job types.
    
    Example usage by LLM:
    - Get configuration examples for Dataflow
    - Check what features are supported by Flink
    - Find out which job types are supported by Spark
    """
    try:
        # Get all runners and filter
        all_runners = await client_manager.list_runners()
        
        for runner in all_runners.runners:
            if runner.runner_type == runner_type:
                return LLMToolResponse(
                    success=True,
                    data=runner,
                    message=f"Successfully retrieved details for {runner_type.value} runner",
                    error=None
                )
        
        return LLMToolResponse(
            success=False,
            data=None,
            message=f"Runner type {runner_type.value} not found or not enabled",
            error="Runner not found"
        )
    except Exception as e:
        logger.error(f"Error getting runner details: {str(e)}")
        return LLMToolResponse(
            success=False,
            data=None,
            message=f"Failed to get details for runner {runner_type.value}",
            error=str(e)
        )

@router.post("/runners/{runner_type}/scale", response_model=LLMToolResponse, summary="Scale runner resources")
async def scale_runner(
    runner_type: RunnerType = Path(..., description="Runner type to scale"),
    scale_params: RunnerScalingParameters = None,
    client_manager: BeamClientManager = Depends(get_client_manager)
):
    """
    Scale resources for a specific runner type.
    
    This endpoint allows you to adjust the default scaling parameters for a runner type,
    which will affect new jobs created with that runner. This is useful for preparing
    for expected load changes.
    
    Example usage by LLM:
    - Increase maximum worker count for Dataflow
    - Adjust autoscaling algorithm for Spark
    - Set a higher target CPU utilization for scaling
    """
    try:
        # Check if runner type is enabled
        client_manager.get_client(runner_type)
        
        # In a real implementation, this would update the runner configuration
        # Here we just return a mock response
        if scale_params is None:
            scale_params = RunnerScalingParameters()
            
        scale_description = ""
        if scale_params.min_workers is not None:
            scale_description += f", min_workers={scale_params.min_workers}"
        if scale_params.max_workers is not None:
            scale_description += f", max_workers={scale_params.max_workers}"
        if scale_params.scaling_algorithm is not None:
            scale_description += f", algorithm={scale_params.scaling_algorithm}"
        
        if scale_description:
            scale_description = f" with{scale_description[1:]}"  # Remove first comma
        
        return LLMToolResponse(
            success=True,
            data={
                "runner_type": runner_type,
                "status": "SCALING",
                "min_workers": scale_params.min_workers,
                "max_workers": scale_params.max_workers,
                "scaling_algorithm": scale_params.scaling_algorithm,
                "target_cpu_utilization": scale_params.target_cpu_utilization,
                "target_throughput_per_worker": scale_params.target_throughput_per_worker
            },
            message=f"Successfully applied scaling configuration to {runner_type.value} runner{scale_description}",
            error=None
        )
    except ValueError as e:
        return LLMToolResponse(
            success=False,
            data=None,
            message=f"Failed to scale runner {runner_type.value}",
            error=str(e)
        )
    except Exception as e:
        logger.error(f"Error scaling runner: {str(e)}")
        return LLMToolResponse(
            success=False,
            data=None,
            message=f"Failed to scale runner {runner_type.value}",
            error=str(e)
        ) 