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
from typing import Dict, List, Optional, Any, Callable

from fastapi import APIRouter, Depends, Path, Query, status
from pydantic import ValidationError

from ..core.context_registry import ContextRegistry
from ..models.contexts import Context, ContextDefinition, ContextType, ContextStatus
from ..models.common import LLMToolResponse
from .dependencies import get_context_registry

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

# Define a dummy context for error responses
DUMMY_CONTEXT = Context(
    mcp_resource_id="error",
    mcp_resource_type="context",
    name="Error",
    description="Error response",
    context_type=ContextType.CUSTOM,
    status=ContextStatus.ERROR,
    parameters={},
    resources={},
    metadata={}
)

# Setup router
router = APIRouter()
logger = logging.getLogger(__name__)

@router.get("/", response_model=LLMToolResponse[List[Context]], response_model_exclude_unset=True, response_model_exclude_none=True)
async def list_contexts(
    context_type: Optional[ContextType] = Query(None, description="Filter contexts by type"),
    status: Optional[ContextStatus] = Query(None, description="Filter contexts by status"),
    context_registry: ContextRegistry = Depends(get_context_registry),
    user = Depends(require_read)
) -> LLMToolResponse[List[Context]]:
    """
    List all available contexts with optional filtering by type and status.
    
    Args:
        context_type: Optional filter by context type
        status: Optional filter by context status
        context_registry: Context registry dependency
        user: Authentication user dependency
        
    Returns:
        LLMToolResponse containing a list of contexts
    """
    try:
        contexts = context_registry.list_contexts(context_type=context_type, status=status)
        return LLMToolResponse(
            success=True,
            message="Contexts retrieved successfully",
            data=contexts
        )
    except Exception as e:
        logger.error(f"Error listing contexts: {str(e)}")
        return LLMToolResponse(
            success=False,
            message=f"Failed to list contexts: {str(e)}",
            data=[DUMMY_CONTEXT]
        )

@router.get("/{context_id}", response_model=LLMToolResponse[Context], response_model_exclude_unset=True, response_model_exclude_none=True)
async def get_context(
    context_id: str = Path(..., description="The ID of the context to retrieve"),
    context_registry: ContextRegistry = Depends(get_context_registry),
    user = Depends(require_read)
) -> LLMToolResponse[Context]:
    """
    Get a specific context by ID.
    
    Args:
        context_id: The ID of the context to retrieve
        context_registry: Context registry dependency
        user: Authentication user dependency
        
    Returns:
        LLMToolResponse containing the context details
    """
    try:
        context = context_registry.get_context(context_id)
        if not context:
            return LLMToolResponse(
                success=False,
                message=f"Context with ID '{context_id}' not found",
                data=DUMMY_CONTEXT
            )
        
        return LLMToolResponse(
            success=True,
            message=f"Context '{context_id}' retrieved successfully",
            data=context
        )
    except Exception as e:
        logger.error(f"Error retrieving context {context_id}: {str(e)}")
        return LLMToolResponse(
            success=False,
            message=f"Failed to retrieve context: {str(e)}",
            data=DUMMY_CONTEXT
        )

@router.post("/", response_model=LLMToolResponse[Context], response_model_exclude_unset=True, response_model_exclude_none=True, status_code=status.HTTP_201_CREATED)
async def create_context(
    context_definition: ContextDefinition,
    context_registry: ContextRegistry = Depends(get_context_registry),
    user = Depends(require_write)
) -> LLMToolResponse[Context]:
    """
    Create a new context with the provided definition.
    
    Args:
        context_definition: The definition for the new context
        context_registry: Context registry dependency
        user: Authentication user dependency
        
    Returns:
        LLMToolResponse containing the created context
    """
    try:
        context = context_registry.create_context(context_definition)
        return LLMToolResponse(
            success=True,
            message=f"Context '{context.name}' created successfully",
            data=context
        )
    except ValueError as e:
        logger.error(f"Error creating context: {str(e)}")
        return LLMToolResponse(
            success=False,
            message=str(e),
            data=DUMMY_CONTEXT
        )
    except ValidationError as e:
        logger.error(f"Validation error creating context: {str(e)}")
        return LLMToolResponse(
            success=False,
            message=f"Invalid context definition: {str(e)}",
            data=DUMMY_CONTEXT
        )
    except Exception as e:
        logger.error(f"Unexpected error creating context: {str(e)}")
        return LLMToolResponse(
            success=False,
            message=f"Failed to create context: {str(e)}",
            data=DUMMY_CONTEXT
        )

@router.put("/{context_id}", response_model=LLMToolResponse[Context], response_model_exclude_unset=True, response_model_exclude_none=True)
async def update_context(
    context_id: str = Path(..., description="The ID of the context to update"),
    updates: Dict[str, Any] = None,
    context_registry: ContextRegistry = Depends(get_context_registry),
    user = Depends(require_write)
) -> LLMToolResponse[Context]:
    """
    Update an existing context by ID.
    
    Args:
        context_id: The ID of the context to update
        updates: The updates to apply to the context
        context_registry: Context registry dependency
        user: Authentication user dependency
        
    Returns:
        LLMToolResponse containing the updated context
    """
    try:
        context = context_registry.update_context(context_id, updates or {})
        if not context:
            return LLMToolResponse(
                success=False,
                message=f"Context with ID '{context_id}' not found",
                data=DUMMY_CONTEXT
            )
        
        return LLMToolResponse(
            success=True,
            message=f"Context '{context_id}' updated successfully",
            data=context
        )
    except ValueError as e:
        logger.error(f"Error updating context {context_id}: {str(e)}")
        return LLMToolResponse(
            success=False,
            message=str(e),
            data=DUMMY_CONTEXT
        )
    except ValidationError as e:
        logger.error(f"Validation error updating context {context_id}: {str(e)}")
        return LLMToolResponse(
            success=False,
            message=f"Invalid update data: {str(e)}",
            data=DUMMY_CONTEXT
        )
    except Exception as e:
        logger.error(f"Unexpected error updating context {context_id}: {str(e)}")
        return LLMToolResponse(
            success=False,
            message=f"Failed to update context: {str(e)}",
            data=DUMMY_CONTEXT
        )

@router.delete("/{context_id}", response_model=LLMToolResponse[Context], response_model_exclude_unset=True, response_model_exclude_none=True)
async def delete_context(
    context_id: str = Path(..., description="The ID of the context to delete"),
    context_registry: ContextRegistry = Depends(get_context_registry),
    user = Depends(require_write)
) -> LLMToolResponse[Context]:
    """
    Delete a context by ID.
    
    Args:
        context_id: The ID of the context to delete
        context_registry: Context registry dependency
        user: Authentication user dependency
        
    Returns:
        LLMToolResponse with deletion confirmation
    """
    try:
        deleted = context_registry.delete_context(context_id)
        if not deleted:
            return LLMToolResponse(
                success=False,
                message=f"Context with ID '{context_id}' not found",
                data=DUMMY_CONTEXT
            )
        
        return LLMToolResponse(
            success=True,
            message=f"Context '{context_id}' deleted successfully",
            data=DUMMY_CONTEXT  # Return dummy context as the real one is deleted
        )
    except Exception as e:
        logger.error(f"Error deleting context {context_id}: {str(e)}")
        return LLMToolResponse(
            success=False,
            message=f"Failed to delete context: {str(e)}",
            data=DUMMY_CONTEXT
        ) 