"""
Runners API Router for the Apache Beam MCP Server.

This module provides endpoints for runner management operations.
"""

import logging
import traceback
from fastapi import APIRouter, Depends, Path, Request
from typing import Optional, Callable, Any
import uuid

from ..models import (
    RunnerType, RunnerList, Runner, RunnerScalingParameters,
    LLMToolResponse, RunnerStatus, RunnerCapability
)
from ..core import BeamClientManager

# Import authentication dependencies if available
try:
    from ..auth import require_read, require_write
except ImportError:
    # Create dummy auth functions for backward compatibility
    from fastapi import Depends
    
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

# Create router
router = APIRouter()

# Helper function to get client manager
async def get_client_manager(request: Request) -> BeamClientManager:
    """Get client manager from request state."""
    return request.app.state.client_manager

# Runner Management Endpoints
@router.get("", summary="List available runners")
async def list_runners(
    client_manager: BeamClientManager = Depends(get_client_manager),
    user = Depends(require_read)
):
    """
    List available runners.
    
    Returns:
        LLMToolResponse with a list of available runners
    """
    try:
        print("===== API ENDPOINT: list_runners called =====")
        logger.info("list_runners endpoint called")
        
        # Print out the client_manager config and clients to debug
        print(f"API - runner config keys: {list(client_manager.config['runners'].keys())}")
        for r_name, r_config in client_manager.config['runners'].items():
            print(f"API - runner {r_name} enabled: {r_config.get('enabled', False)}")
        print(f"API - client keys: {list(client_manager.clients.keys())}")
        
        # DIRECT IMPLEMENTATION - bypass client_manager.list_runners()
        print("API - Using direct implementation instead of client_manager.list_runners()")
        
        from ..models.runner import RunnerList, Runner, RunnerType, RunnerStatus, RunnerCapability
        
        # Create runners directly based on configuration
        direct_runners = []
        
        # Check direct runner
        if 'direct' in client_manager.config['runners'] and client_manager.config['runners']['direct'].get('enabled', False):
            direct_runners.append(Runner(
                name="Apache Beam Direct Runner",
                runner_type=RunnerType.DIRECT,
                status=RunnerStatus.AVAILABLE,
                capabilities=[
                    RunnerCapability.BATCH,
                ],
                metadata={
                    "description": "Local direct runner for development and testing",
                    "location": "localhost"
                },
                mcp_resource_id=f"runner-direct-{uuid.uuid4()}",
                description="Apache Beam Direct Runner for local development and testing",
                version="2.50.0"
            ))
        
        # Check dataflow runner
        if 'dataflow' in client_manager.config['runners'] and client_manager.config['runners']['dataflow'].get('enabled', False):
            direct_runners.append(Runner(
                name="Google Cloud Dataflow",
                runner_type=RunnerType.DATAFLOW,
                status=RunnerStatus.AVAILABLE,
                capabilities=[
                    RunnerCapability.BATCH,
                    RunnerCapability.STREAMING,
                    RunnerCapability.AUTOSCALING,
                ],
                metadata={
                    "description": "Google Cloud Dataflow runner",
                    "location": client_manager.config['runners']['dataflow'].get('region', 'us-central1'),
                    "project": client_manager.config['runners']['dataflow'].get('project_id', 'default-project')
                },
                mcp_resource_id=f"runner-dataflow-{uuid.uuid4()}",
                description="Google Cloud Dataflow runner for GCP",
                version="2.50.0"
            ))
        
        # Check Spark runner
        if 'spark' in client_manager.config['runners'] and client_manager.config['runners']['spark'].get('enabled', False):
            direct_runners.append(Runner(
                name="Apache Spark",
                runner_type=RunnerType.SPARK,
                status=RunnerStatus.AVAILABLE,
                capabilities=[
                    RunnerCapability.BATCH
                ],
                metadata={
                    "description": "Apache Spark runner",
                    "location": client_manager.config['runners']['spark'].get('spark_master', 'local[*]')
                },
                mcp_resource_id=f"runner-spark-{uuid.uuid4()}",
                description="Apache Spark runner for distributed processing",
                version="3.2.0"
            ))
        
        # Check Flink runner
        if 'flink' in client_manager.config['runners'] and client_manager.config['runners']['flink'].get('enabled', False):
            direct_runners.append(Runner(
                name="Apache Flink",
                runner_type=RunnerType.FLINK,
                status=RunnerStatus.AVAILABLE,
                capabilities=[
                    RunnerCapability.BATCH,
                ],
                metadata={
                    "description": "Apache Flink runner",
                    "location": client_manager.config['runners']['flink'].get('jobmanager_address', 'localhost:8081')
                },
                mcp_resource_id=f"runner-flink-{uuid.uuid4()}",
                description="Apache Flink runner for stream and batch processing",
                version="1.17.0"
            ))
        
        # Create response with direct runners
        try:
            direct_response = RunnerList(
                runners=direct_runners,
                default_runner=client_manager.config.get('default_runner', 'direct'),
                mcp_resource_id=f"runner-list-{uuid.uuid4()}",
                mcp_total_runners=len(direct_runners)
            )
            
            return {
                "success": True,
                "data": {
                    "runners": [runner.model_dump() for runner in direct_runners],
                    "default_runner": client_manager.config.get('default_runner', 'direct'),
                    "total_count": len(direct_runners)
                }
            }
        except Exception as e:
            logger.error(f"Error listing runners: {e}")
            
            # If the model validation fails, return a simpler response
            return {
                "success": False,
                "data": {
                    "runners": [],
                    "default_runner": client_manager.config.get('default_runner', 'direct'),
                    "total_count": 0
                },
                "error": str(e)
            }
    except Exception as e:
        logger.error(f"Error listing runners: {e}")
        tb = traceback.format_exc()
        logger.error(tb)
        return LLMToolResponse(
            success=False,
            message=f"Failed to list runners: {str(e)}",
            data={
                "error": str(e),
                "traceback": tb
            }
        )

@router.get("/debug-test", summary="Test endpoint that directly calls list_runners")
async def test_list_runners(
    client_manager: BeamClientManager = Depends(get_client_manager),
    user = Depends(require_read)
):
    """
    Test endpoint that directly calls list_runners.
    
    Returns:
        LLMToolResponse with a list of available runners
    """
    try:
        # Call list_runners directly
        print("===== API ENDPOINT: test_list_runners called =====")
        print("Calling client_manager.list_runners() directly via API endpoint")
        
        # Get client manager from app state
        print(f"client_manager ID: {id(client_manager)}")
        print(f"client_manager class: {type(client_manager).__name__}")
        print(f"runner config keys: {list(client_manager.config['runners'].keys()) if 'runners' in client_manager.config else []}")
        print(f"client keys: {list(client_manager.clients.keys())}")
        
        # Call list_runners directly
        runners_list = await client_manager.list_runners()
        
        runner_count = len(runners_list.runners)
        print(f"Found {runner_count} runners")
        for i, runner in enumerate(runners_list.runners):
            print(f"Runner {i+1}: {runner.name} (type={runner.runner_type})")
        
        # Format response
        return LLMToolResponse(
            success=True,
            message=f"Test successful - found {runner_count} runners",
            data={
                "runner_count": runner_count,
                "runners": [
                    {
                        "name": runner.name,
                        "type": runner.runner_type, 
                        "status": runner.status
                    }
                    for runner in runners_list.runners
                ],
                "raw_data": runners_list.model_dump() if hasattr(runners_list, "model_dump") else str(runners_list)
            }
        )
    except Exception as e:
        logger.error(f"Error in test_list_runners: {str(e)}")
        tb = traceback.format_exc()
        logger.error(tb)
        return LLMToolResponse(
            success=False,
            message=f"Test failed: {str(e)}",
            data={
                "error": str(e),
                "traceback": tb
            }
        )

@router.get("/{runner_type}", response_model=LLMToolResponse, summary="Get runner details")
async def get_runner(
    runner_type: RunnerType = Path(..., description="Runner type to get details for"),
    client_manager: BeamClientManager = Depends(get_client_manager),
    user = Depends(require_read)
):
    """
    Get details for a specific runner.
    
    Args:
        runner_type: Runner type to get details for
        client_manager: Client manager
        user: Authentication user
    
    Returns:
        LLMToolResponse with runner details
    """
    try:
        runner = await client_manager.get_runner(runner_type)
        if not runner:
            return LLMToolResponse(
                success=False,
                message=f"Runner {runner_type} not found or not available",
                data=None
            )
        
        return LLMToolResponse(
            success=True,
            message=f"Successfully retrieved runner {runner_type}",
            data=runner.model_dump() if hasattr(runner, "model_dump") else runner
        )
    except Exception as e:
        logger.error(f"Error getting runner {runner_type}: {str(e)}")
        return LLMToolResponse(
            success=False,
            message=f"Failed to get runner: {str(e)}",
            data=None
        )

@router.post("/{runner_type}/scale", response_model=LLMToolResponse, summary="Scale runner resources")
async def scale_runner(
    runner_type: RunnerType = Path(..., description="Runner type to scale"),
    scale_params: RunnerScalingParameters = None,
    client_manager: BeamClientManager = Depends(get_client_manager),
    user = Depends(require_write)
):
    """
    Scale runner resources.
    
    Args:
        runner_type: Runner type to scale
        scale_params: Scaling parameters
        client_manager: Client manager
        user: Authentication user
    
    Returns:
        LLMToolResponse with scaling result
    """
    try:
        # Mock implementation for now
        from pydantic import BaseModel
        from typing import Optional
        
        class ScaleRequest(BaseModel):
            min_workers: Optional[int] = None
            max_workers: Optional[int] = None
            scaling_algorithm: Optional[str] = None
            target_cpu_utilization: Optional[float] = None
            target_throughput_per_worker: Optional[int] = None
        
        if scale_params is None:
            scale_params = ScaleRequest()
        
        # Check if runner exists and supports scaling
        runner = await client_manager.get_runner(runner_type)
        if not runner:
            return LLMToolResponse(
                success=False,
                message=f"Runner {runner_type} not found or not available",
                data=None
            )
        
        # Only Dataflow supports scaling for now
        if runner_type != RunnerType.DATAFLOW:
            return LLMToolResponse(
                success=False,
                message=f"Scaling not supported for runner type {runner_type}",
                data=None
            )
        
        # Call client manager to scale
        result = await client_manager.scale_runner(runner_type, scale_params)
        
        return LLMToolResponse(
            success=True,
            message=f"Successfully scaled runner {runner_type}",
            data=result
        )
    except Exception as e:
        logger.error(f"Error scaling runner {runner_type}: {str(e)}")
        return LLMToolResponse(
            success=False,
            message=f"Failed to scale runner: {str(e)}",
            data=None
        ) 