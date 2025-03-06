"""
Contexts router for the MCP API.

This module provides the following endpoints for managing execution contexts
that can be used to run Beam pipelines:

- GET /contexts - List all execution contexts
  Usage: GET /api/v1/contexts/
  Filters: ?context_type=dataflow&status=active
  Response: List of available contexts matching the filters

- GET /contexts/{context_id} - Get a specific context
  Usage: GET /api/v1/contexts/dataflow-default
  Response: Detailed information about the specified context

- POST /contexts - Create a new context
  Usage: POST /api/v1/contexts/
  Payload:
    {
      "name": "Dataflow Production",
      "description": "Production execution context for Dataflow jobs",
      "context_type": "dataflow",
      "parameters": {
        "region": "us-central1",
        "project": "my-beam-project",
        "temp_location": "gs://my-bucket/temp"
      },
      "resources": {
        "cpu": "2",
        "memory": "4GB"
      },
      "metadata": {
        "environment": "production"
      }
    }
  Response: Created context details with auto-generated ID

- PUT /contexts/{context_id} - Update an existing context
  Usage: PUT /api/v1/contexts/dataflow-default
  Payload: Same format as POST, with updated fields
  Response: Updated context details

- DELETE /contexts/{context_id} - Delete a context
  Usage: DELETE /api/v1/contexts/dataflow-default
  Response: Success confirmation

RESPONSE FORMAT:
All API responses follow a standard format:
{
  "success": true|false,  # Whether the operation succeeded
  "message": "Human-readable message",
  "data": { ... }  # Context object or DUMMY_CONTEXT for errors
}

IMPORTANT NOTES:
1. Contexts are identified by 'mcp_resource_id', not 'id'.
2. Error responses include a dummy context in the 'data' field (never null).
3. Response models may exclude unset or null fields.
4. Always check the 'success' field to determine if the operation was successful.

Example error response:
{
  "success": false,
  "message": "Context with this name already exists",
  "data": {
    "mcp_resource_id": "error",
    "mcp_resource_type": "context",
    "name": "Error",
    "description": "Error response",
    "context_type": "custom",
    "status": "error",
    "parameters": {},
    "resources": {},
    "metadata": {}
  }
}
"""
import logging
from typing import Dict, List, Optional, Any, Union

from fastapi import APIRouter, Depends, Path, Query, status
from pydantic import ValidationError

from ..core.context_registry import ContextRegistry
from ..models.contexts import Context, ContextDefinition, ContextType, ContextStatus
from ..models.common import LLMToolResponse
from .dependencies import get_context_registry

# Setup router
router = APIRouter()
logger = logging.getLogger(__name__)

def _transform_context_for_response(context: Context) -> Dict:
    """Transform a Context object to include 'id' field for response compatibility."""
    if not context:
        return None
    
    context_dict = context.dict()
    context_dict["id"] = context.mcp_resource_id
    return context_dict

def _transform_contexts_for_response(contexts: List[Context]) -> List[Dict]:
    """Transform a list of Context objects for response compatibility."""
    return [_transform_context_for_response(context) for context in contexts]

@router.get("/", response_model=LLMToolResponse[List[Dict]])
async def list_contexts(
    context_type: Optional[ContextType] = Query(None, description="Filter contexts by type"),
    status: Optional[ContextStatus] = Query(None, description="Filter contexts by status"),
    context_registry: ContextRegistry = Depends(get_context_registry)
) -> LLMToolResponse[List[Dict]]:
    """
    List all available execution contexts with optional filtering by type and status.
    
    Args:
        context_type: Optional filter by context type
        status: Optional filter by context status
        context_registry: Context registry dependency
        
    Returns:
        LLMToolResponse containing a list of contexts
    """
    try:
        contexts = context_registry.list_contexts(context_type=context_type, status=status)
        return LLMToolResponse(
            success=True,
            message="Execution contexts retrieved successfully",
            data=_transform_contexts_for_response(contexts)
        )
    except Exception as e:
        logger.error(f"Error listing contexts: {str(e)}")
        return LLMToolResponse(
            success=False,
            message=f"Failed to list contexts: {str(e)}",
            data=[],
            error=str(e)
        )

@router.get("/{context_id}", response_model=LLMToolResponse[Dict])
async def get_context(
    context_id: str = Path(..., description="The ID of the context to retrieve"),
    context_registry: ContextRegistry = Depends(get_context_registry)
) -> LLMToolResponse[Dict]:
    """
    Get a specific execution context by ID.
    
    Args:
        context_id: The ID of the context to retrieve
        context_registry: Context registry dependency
        
    Returns:
        LLMToolResponse containing the context details
    """
    try:
        context = context_registry.get_context(context_id)
        if not context:
            return LLMToolResponse(
                success=False,
                message=f"Execution context with ID '{context_id}' not found",
                data={},
                error=f"Context not found: {context_id}"
            )
        
        return LLMToolResponse(
            success=True,
            message=f"Execution context '{context_id}' retrieved successfully",
            data=_transform_context_for_response(context)
        )
    except Exception as e:
        logger.error(f"Error retrieving context {context_id}: {str(e)}")
        return LLMToolResponse(
            success=False,
            message=f"Failed to retrieve execution context: {str(e)}",
            data={},
            error=str(e)
        )

@router.post("/", response_model=LLMToolResponse[Dict], status_code=status.HTTP_201_CREATED)
async def create_context(
    context_definition: ContextDefinition,
    context_registry: ContextRegistry = Depends(get_context_registry)
) -> LLMToolResponse[Dict]:
    """
    Create a new execution context with the provided definition.
    
    Args:
        context_definition: The definition for the new context
        context_registry: Context registry dependency
        
    Returns:
        LLMToolResponse containing the created context
    """
    try:
        context = context_registry.create_context(context_definition)
        return LLMToolResponse(
            success=True,
            message=f"Execution context '{context.name}' created successfully",
            data=_transform_context_for_response(context)
        )
    except ValueError as e:
        logger.error(f"Error creating context: {str(e)}")
        # Return error response but keep 201 status code as specified in the tests
        return LLMToolResponse(
            success=False,
            message=str(e),
            data={},
            error=str(e)
        )
    except ValidationError as e:
        logger.error(f"Validation error creating context: {str(e)}")
        return LLMToolResponse(
            success=False,
            message=f"Invalid context definition: {str(e)}",
            data={},
            error=str(e)
        )
    except Exception as e:
        logger.error(f"Error creating context: {str(e)}")
        return LLMToolResponse(
            success=False,
            message=f"Failed to create execution context: {str(e)}",
            data={},
            error=str(e)
        )

@router.put("/{context_id}", response_model=LLMToolResponse[Dict])
async def update_context(
    context_id: str = Path(..., description="The ID of the context to update"),
    updates: Dict[str, Any] = None,
    context_registry: ContextRegistry = Depends(get_context_registry)
) -> LLMToolResponse[Dict]:
    """
    Update an existing execution context by ID.
    
    Args:
        context_id: The ID of the context to update
        updates: The updates to apply to the context
        context_registry: Context registry dependency
        
    Returns:
        LLMToolResponse containing the updated context
    """
    if updates is None:
        updates = {}
    
    try:
        # Get the existing context first to verify it exists
        existing_context = context_registry.get_context(context_id)
        if not existing_context:
            return LLMToolResponse(
                success=False,
                message=f"Execution context with ID '{context_id}' not found",
                data={},
                error=f"Context not found: {context_id}"
            )
        
        # Update the context
        updated_context = context_registry.update_context(context_id, updates)
        
        return LLMToolResponse(
            success=True,
            message=f"Execution context '{context_id}' updated successfully",
            data=_transform_context_for_response(updated_context)
        )
    except ValueError as e:
        logger.error(f"Error updating context {context_id}: {str(e)}")
        return LLMToolResponse(
            success=False,
            message=str(e),
            data={},
            error=str(e)
        )
    except ValidationError as e:
        logger.error(f"Validation error updating context {context_id}: {str(e)}")
        return LLMToolResponse(
            success=False,
            message=f"Invalid context update: {str(e)}",
            data={},
            error=str(e)
        )
    except Exception as e:
        logger.error(f"Error updating context {context_id}: {str(e)}")
        return LLMToolResponse(
            success=False,
            message=f"Failed to update execution context: {str(e)}",
            data={},
            error=str(e)
        )

@router.delete("/{context_id}", response_model=LLMToolResponse[None])
async def delete_context(
    context_id: str = Path(..., description="The ID of the context to delete"),
    context_registry: ContextRegistry = Depends(get_context_registry)
) -> LLMToolResponse[None]:
    """
    Delete an execution context by ID.
    
    Args:
        context_id: The ID of the context to delete
        context_registry: Context registry dependency
        
    Returns:
        LLMToolResponse with success status
    """
    try:
        # First check if the context exists
        existing_context = context_registry.get_context(context_id)
        if not existing_context:
            return LLMToolResponse(
                success=False,
                message=f"Execution context with ID '{context_id}' not found",
                data=None,
                error=f"Context not found: {context_id}"
            )
            
        # Delete the context
        deleted = context_registry.delete_context(context_id)
        
        if deleted:
            return LLMToolResponse(
                success=True,
                message=f"Execution context '{context_id}' deleted successfully",
                data=None
            )
        else:
            return LLMToolResponse(
                success=False,
                message=f"Failed to delete execution context '{context_id}'",
                data=None,
                error="Delete operation failed"
            )
    except Exception as e:
        logger.error(f"Error deleting context {context_id}: {str(e)}")
        return LLMToolResponse(
            success=False,
            message=f"Failed to delete execution context: {str(e)}",
            data=None,
            error=str(e)
        ) 