"""
Runners API Router for the Apache Beam MCP Server.

This module provides endpoints for runner management operations.
"""

import logging
from fastapi import APIRouter, Depends, Path, Request

from ..models import (
    RunnerType, RunnerList, Runner, RunnerScalingParameters,
    LLMToolResponse
)
from ..core import BeamClientManager

logger = logging.getLogger(__name__)

# Create router
router = APIRouter()

# Helper function to get client manager
async def get_client_manager(request: Request) -> BeamClientManager:
    """Get client manager from request state."""
    return request.app.state.client_manager

# Runner Management Endpoints
@router.get("", response_model=LLMToolResponse, summary="List available runners")
async def list_runners(
    client_manager: BeamClientManager = Depends(get_client_manager)
):
    """List available runners."""
    try:
        runners = await client_manager.list_runners()
        return LLMToolResponse(
            success=True,
            data=runners.model_dump(),
            message="Successfully retrieved runners"
        )
    except Exception as e:
        logger.error(f"Error listing runners: {str(e)}")
        return LLMToolResponse(
            success=False,
            data=None,
            message=f"Failed to list runners: {str(e)}",
            error=str(e)
        )

@router.get("/{runner_type}", response_model=LLMToolResponse, summary="Get runner details")
async def get_runner(
    runner_type: RunnerType = Path(..., description="Runner type to get details for"),
    client_manager: BeamClientManager = Depends(get_client_manager)
):
    """Get details for a specific runner."""
    try:
        runner = await client_manager.get_runner(runner_type)
        return LLMToolResponse(
            success=True,
            data=runner.model_dump(),
            message=f"Successfully retrieved runner details for {runner_type}"
        )
    except Exception as e:
        logger.error(f"Error getting runner details: {str(e)}")
        return LLMToolResponse(
            success=False,
            data=None,
            message=f"Failed to get runner details: {str(e)}",
            error=str(e)
        )

@router.post("/{runner_type}/scale", response_model=LLMToolResponse, summary="Scale runner resources")
async def scale_runner(
    runner_type: RunnerType = Path(..., description="Runner type to scale"),
    scale_params: RunnerScalingParameters = None,
    client_manager: BeamClientManager = Depends(get_client_manager)
):
    """Scale runner resources."""
    try:
        runner = await client_manager.scale_runner(runner_type, scale_params)
        return LLMToolResponse(
            success=True,
            data=runner.model_dump(),
            message=f"Successfully scaled runner {runner_type}"
        )
    except Exception as e:
        logger.error(f"Error scaling runner: {str(e)}")
        return LLMToolResponse(
            success=False,
            data=None,
            message=f"Failed to scale runner: {str(e)}",
            error=str(e)
        ) 