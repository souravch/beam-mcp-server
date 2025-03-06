"""
Resources router for the MCP API.

This module provides the following endpoints for managing datasets, files, ML models, 
and other assets that can be used in data pipelines:

- GET /resources - List all resources
  Usage: GET /api/v1/resources/
  Filters: ?resource_type=dataset&status=available
  Response: List of available resources matching the filters

- GET /resources/{resource_id} - Get a specific resource
  Usage: GET /api/v1/resources/dataset-123
  Response: Detailed information about the specified resource

- POST /resources - Create a new resource
  Usage: POST /api/v1/resources/
  Payload:
    {
      "name": "New Dataset",
      "description": "A new example dataset",
      "resource_type": "dataset",
      "location": "gs://my-bucket/datasets/new-dataset.csv",
      "format": "csv",
      "schema": {
        "fields": [
          {"name": "id", "type": "integer"},
          {"name": "name", "type": "string"}
        ]
      },
      "metadata": {"rows": 1000}
    }
  Response: Created resource details with auto-generated ID

- PUT /resources/{resource_id} - Update an existing resource
  Usage: PUT /api/v1/resources/dataset-123
  Payload: Same format as POST, with updated fields
  Response: Updated resource details

- DELETE /resources/{resource_id} - Delete a resource
  Usage: DELETE /api/v1/resources/dataset-123
  Response: Success confirmation

RESPONSE FORMAT:
All API responses follow a standard format:
{
  "success": true|false,  # Whether the operation succeeded
  "message": "Human-readable message",
  "data": { ... }  # Resource object or DUMMY_RESOURCE for errors
}

IMPORTANT NOTES:
1. Resources are identified by 'mcp_resource_id', not 'id'.
2. Error responses include a dummy resource in the 'data' field (never null).
3. Response models may exclude unset or null fields.
4. Always check the 'success' field to determine if the operation was successful.

Example error response:
{
  "success": false,
  "message": "Resource with this name already exists",
  "data": {
    "mcp_resource_id": "error",
    "mcp_resource_type": "resource",
    "name": "Error",
    "description": "Error response",
    "resource_type": "custom",
    "location": "null://error",
    "status": "error",
    "metadata": {}
  }
}
"""
import logging
from typing import Dict, List, Optional, Any

from fastapi import APIRouter, Depends, Path, Query, status
from pydantic import ValidationError

from ..core.resource_registry import ResourceRegistry
from ..models.resources import Resource, ResourceDefinition, ResourceType, ResourceStatus
from ..models.common import LLMToolResponse
from .dependencies import get_resource_registry

# Define a dummy resource for error responses
DUMMY_RESOURCE = Resource(
    mcp_resource_id="error",
    mcp_resource_type="resource",
    name="Error",
    description="Error response",
    resource_type=ResourceType.CUSTOM,
    location="null://error",
    status=ResourceStatus.ERROR,
    metadata={}
)

# Setup router
router = APIRouter()
logger = logging.getLogger(__name__)

@router.get("/", response_model=LLMToolResponse[List[Resource]], response_model_exclude_unset=True, response_model_exclude_none=True)
async def list_resources(
    resource_type: Optional[ResourceType] = Query(None, description="Filter resources by type"),
    status: Optional[ResourceStatus] = Query(None, description="Filter resources by status"),
    resource_registry: ResourceRegistry = Depends(get_resource_registry)
) -> LLMToolResponse[List[Resource]]:
    """
    List all available resources with optional filtering by type and status.
    
    Args:
        resource_type: Optional filter by resource type
        status: Optional filter by resource status
        resource_registry: Resource registry dependency
        
    Returns:
        LLMToolResponse containing a list of resources
    """
    try:
        resources = resource_registry.list_resources(resource_type=resource_type, status=status)
        return LLMToolResponse(
            success=True,
            message="Resources retrieved successfully",
            data=resources
        )
    except Exception as e:
        logger.error(f"Error listing resources: {str(e)}")
        return LLMToolResponse(
            success=False,
            message=f"Failed to list resources: {str(e)}",
            data=[DUMMY_RESOURCE]
        )

@router.get("/{resource_id}", response_model=LLMToolResponse[Resource], response_model_exclude_unset=True, response_model_exclude_none=True)
async def get_resource(
    resource_id: str = Path(..., description="The ID of the resource to retrieve"),
    resource_registry: ResourceRegistry = Depends(get_resource_registry)
) -> LLMToolResponse[Resource]:
    """
    Get a specific resource by ID.
    
    Args:
        resource_id: The ID of the resource to retrieve
        resource_registry: Resource registry dependency
        
    Returns:
        LLMToolResponse containing the resource details
    """
    try:
        resource = resource_registry.get_resource(resource_id)
        if not resource:
            return LLMToolResponse(
                success=False,
                message=f"Resource with ID '{resource_id}' not found",
                data=DUMMY_RESOURCE
            )
        
        return LLMToolResponse(
            success=True,
            message=f"Resource '{resource_id}' retrieved successfully",
            data=resource
        )
    except Exception as e:
        logger.error(f"Error retrieving resource {resource_id}: {str(e)}")
        return LLMToolResponse(
            success=False,
            message=f"Failed to retrieve resource: {str(e)}",
            data=DUMMY_RESOURCE
        )

@router.post("/", response_model=LLMToolResponse[Resource], response_model_exclude_unset=True, response_model_exclude_none=True, status_code=status.HTTP_201_CREATED)
async def create_resource(
    resource_definition: ResourceDefinition,
    resource_registry: ResourceRegistry = Depends(get_resource_registry)
) -> LLMToolResponse[Resource]:
    """
    Create a new resource with the provided definition.
    
    Args:
        resource_definition: The definition for the new resource
        resource_registry: Resource registry dependency
        
    Returns:
        LLMToolResponse containing the created resource
    """
    try:
        resource = resource_registry.create_resource(resource_definition)
        return LLMToolResponse(
            success=True,
            message=f"Resource '{resource.name}' created successfully",
            data=resource
        )
    except ValueError as e:
        logger.error(f"Error creating resource: {str(e)}")
        return LLMToolResponse(
            success=False,
            message=str(e),
            data=DUMMY_RESOURCE
        )
    except ValidationError as e:
        logger.error(f"Validation error creating resource: {str(e)}")
        return LLMToolResponse(
            success=False,
            message=f"Invalid resource definition: {str(e)}",
            data=DUMMY_RESOURCE
        )
    except Exception as e:
        logger.error(f"Unexpected error creating resource: {str(e)}")
        return LLMToolResponse(
            success=False,
            message=f"Failed to create resource: {str(e)}",
            data=DUMMY_RESOURCE
        )

@router.put("/{resource_id}", response_model=LLMToolResponse[Resource], response_model_exclude_unset=True, response_model_exclude_none=True)
async def update_resource(
    resource_id: str = Path(..., description="The ID of the resource to update"),
    updates: Dict[str, Any] = None,
    resource_registry: ResourceRegistry = Depends(get_resource_registry)
) -> LLMToolResponse[Resource]:
    """
    Update an existing resource by ID.
    
    Args:
        resource_id: The ID of the resource to update
        updates: The updates to apply to the resource
        resource_registry: Resource registry dependency
        
    Returns:
        LLMToolResponse containing the updated resource
    """
    try:
        resource = resource_registry.update_resource(resource_id, updates or {})
        if not resource:
            return LLMToolResponse(
                success=False,
                message=f"Resource with ID '{resource_id}' not found",
                data=DUMMY_RESOURCE
            )
        
        return LLMToolResponse(
            success=True,
            message=f"Resource '{resource_id}' updated successfully",
            data=resource
        )
    except ValueError as e:
        logger.error(f"Error updating resource {resource_id}: {str(e)}")
        return LLMToolResponse(
            success=False,
            message=str(e),
            data=DUMMY_RESOURCE
        )
    except ValidationError as e:
        logger.error(f"Validation error updating resource {resource_id}: {str(e)}")
        return LLMToolResponse(
            success=False,
            message=f"Invalid update data: {str(e)}",
            data=DUMMY_RESOURCE
        )
    except Exception as e:
        logger.error(f"Unexpected error updating resource {resource_id}: {str(e)}")
        return LLMToolResponse(
            success=False,
            message=f"Failed to update resource: {str(e)}",
            data=DUMMY_RESOURCE
        )

@router.delete("/{resource_id}", response_model=LLMToolResponse[Resource], response_model_exclude_unset=True, response_model_exclude_none=True)
async def delete_resource(
    resource_id: str = Path(..., description="The ID of the resource to delete"),
    resource_registry: ResourceRegistry = Depends(get_resource_registry)
) -> LLMToolResponse[Resource]:
    """
    Delete a resource by ID.
    
    Args:
        resource_id: The ID of the resource to delete
        resource_registry: Resource registry dependency
        
    Returns:
        LLMToolResponse with deletion confirmation
    """
    try:
        deleted = resource_registry.delete_resource(resource_id)
        if not deleted:
            return LLMToolResponse(
                success=False,
                message=f"Resource with ID '{resource_id}' not found",
                data=DUMMY_RESOURCE
            )
        
        return LLMToolResponse(
            success=True,
            message=f"Resource '{resource_id}' deleted successfully",
            data=DUMMY_RESOURCE  # Return dummy resource as the real one is deleted
        )
    except Exception as e:
        logger.error(f"Error deleting resource {resource_id}: {str(e)}")
        return LLMToolResponse(
            success=False,
            message=f"Failed to delete resource: {str(e)}",
            data=DUMMY_RESOURCE
        ) 