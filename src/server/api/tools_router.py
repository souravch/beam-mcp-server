"""
Tools router for the MCP API.

This module provides the following endpoints for managing AI models, transformation functions, 
and data processors that can be used in data pipelines:

- GET /tools - List all tools
  Usage: GET /api/v1/tools/
  Filters: ?tool_type=model&status=active
  Response: List of available tools matching the filters

- GET /tools/{tool_id} - Get a specific tool
  Usage: GET /api/v1/tools/text-tokenizer
  Response: Detailed information about the specified tool

- POST /tools - Create a new tool
  Usage: POST /api/v1/tools/
  Payload:
    {
      "name": "New Tool",
      "description": "A new text processing tool",
      "tool_type": "processor",
      "version": "1.0.0",
      "parameters": {
        "input": {"type": "string", "description": "Input text"}
      },
      "capabilities": ["text-processing"],
      "metadata": {"author": "Beam Team"}
    }
  Response: Created tool details with auto-generated ID

- PUT /tools/{tool_id} - Update an existing tool
  Usage: PUT /api/v1/tools/text-tokenizer
  Payload: Same format as POST, with updated fields
  Response: Updated tool details

- DELETE /tools/{tool_id} - Delete a tool
  Usage: DELETE /api/v1/tools/text-tokenizer
  Response: Success confirmation

- POST /tools/{tool_id}/invoke - Invoke a tool
  Usage: POST /api/v1/tools/text-tokenizer/invoke
  Payload: 
    {
      "text": "Hello world"
    }
  Response: Tool execution results

RESPONSE FORMAT:
All API responses follow a standard format:
{
  "success": true|false,  # Whether the operation succeeded
  "message": "Human-readable message",
  "data": { ... }  # Tool object or DUMMY_TOOL for errors
}

IMPORTANT NOTES:
1. Tools are identified by 'mcp_resource_id', not 'id'.
2. Error responses include a dummy tool in the 'data' field (never null).
3. Response models may exclude unset or null fields.
4. Always check the 'success' field to determine if the operation was successful.

Example error response:
{
  "success": false,
  "message": "Tool with this name already exists",
  "data": {
    "mcp_resource_id": "error",
    "mcp_resource_type": "tool",
    "name": "Error",
    "description": "Error response",
    "tool_type": "processor",
    "version": "0.0.0",
    "status": "inactive",
    "parameters": {},
    "capabilities": [],
    "metadata": {}
  }
}
"""
import logging
from typing import Dict, List, Optional, Any, Union

from fastapi import APIRouter, Depends, HTTPException, Path, Query, status
from pydantic import ValidationError

from ..core.tool_registry import ToolRegistry
from ..models.tools import Tool, ToolDefinition, ToolType, ToolStatus
from ..models.common import LLMToolResponse
from .dependencies import get_tool_registry

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

# Setup router
router = APIRouter()
logger = logging.getLogger(__name__)

# Dummy Tool for error responses
DUMMY_TOOL = Tool(
    mcp_resource_id="error",
    name="Error",
    description="Error response",
    tool_type="processor",  # Use string to avoid import issues
    version="0.0.0",
    status="inactive",
    parameters={},
    capabilities=[],
    metadata={}
)

@router.get("/", response_model=LLMToolResponse[List[Tool]], response_model_exclude_unset=True, response_model_exclude_none=True)
async def list_tools(
    tool_type: Optional[str] = None,
    tool_registry: ToolRegistry = Depends(get_tool_registry),
    user = Depends(require_read)
) -> LLMToolResponse[List[Tool]]:
    """
    List all available tools, optionally filtered by type.
    
    Args:
        tool_type: Optional filter by tool type
        tool_registry: Tool registry dependency
        
    Returns:
        LLMToolResponse containing a list of tools
    """
    try:
        tools = await tool_registry.list_tools(tool_type=tool_type)
        return LLMToolResponse(
            success=True,
            message=f"Retrieved {len(tools)} tools",
            data=tools
        )
    except Exception as e:
        logger.error(f"Error listing tools: {str(e)}")
        return LLMToolResponse(
            success=False,
            message=f"Failed to list tools: {str(e)}",
            data=[]
        )

@router.get("/{tool_id}", response_model=LLMToolResponse[Tool], response_model_exclude_unset=True, response_model_exclude_none=True)
async def get_tool(
    tool_id: str = Path(..., description="The ID of the tool to retrieve"),
    tool_registry: ToolRegistry = Depends(get_tool_registry),
    user = Depends(require_read)
) -> LLMToolResponse[Tool]:
    """
    Get a specific tool by ID.
    
    Args:
        tool_id: The ID of the tool to retrieve
        tool_registry: Tool registry dependency
        
    Returns:
        LLMToolResponse containing the tool
    """
    try:
        tool = await tool_registry.get_tool(tool_id)
        if not tool:
            return LLMToolResponse(
                success=False,
                message=f"Tool with ID '{tool_id}' not found",
                data=DUMMY_TOOL
            )
        return LLMToolResponse(
            success=True,
            message=f"Retrieved tool '{tool_id}'",
            data=tool
        )
    except Exception as e:
        logger.error(f"Error retrieving tool {tool_id}: {str(e)}")
        return LLMToolResponse(
            success=False,
            message=f"Failed to retrieve tool: {str(e)}",
            data=DUMMY_TOOL
        )

@router.post("/", response_model=LLMToolResponse[Tool], response_model_exclude_unset=True, response_model_exclude_none=True, status_code=status.HTTP_201_CREATED)
async def create_tool(
    tool_definition: ToolDefinition,
    tool_registry: ToolRegistry = Depends(get_tool_registry),
    user = Depends(require_write)
) -> LLMToolResponse[Tool]:
    """
    Create a new tool with the provided definition.
    
    Args:
        tool_definition: The definition for the new tool
        tool_registry: Tool registry dependency
        
    Returns:
        LLMToolResponse containing the created tool
    """
    try:
        tool = await tool_registry.create_tool(tool_definition)
        return LLMToolResponse(
            success=True,
            message=f"Tool '{tool.name}' created successfully",
            data=tool
        )
    except ValueError as e:
        logger.error(f"Error creating tool: {str(e)}")
        return LLMToolResponse(
            success=False,
            message=str(e),
            data=DUMMY_TOOL
        )
    except ValidationError as e:
        logger.error(f"Validation error creating tool: {str(e)}")
        return LLMToolResponse(
            success=False,
            message=f"Invalid tool definition: {str(e)}",
            data=DUMMY_TOOL
        )
    except Exception as e:
        logger.error(f"Unexpected error creating tool: {str(e)}")
        return LLMToolResponse(
            success=False,
            message=f"Failed to create tool: {str(e)}",
            data=DUMMY_TOOL
        )

@router.put("/{tool_id}", response_model=LLMToolResponse[Tool], response_model_exclude_unset=True, response_model_exclude_none=True)
async def update_tool(
    tool_id: str = Path(..., description="The ID of the tool to update"),
    tool_definition: ToolDefinition = None,
    tool_registry: ToolRegistry = Depends(get_tool_registry),
    user = Depends(require_write)
) -> LLMToolResponse[Tool]:
    """
    Update an existing tool.
    
    Args:
        tool_id: The ID of the tool to update
        tool_definition: The updated tool definition
        tool_registry: Tool registry dependency
        
    Returns:
        LLMToolResponse containing the updated tool
    """
    try:
        updated_tool = await tool_registry.update_tool(tool_id, tool_definition)
        if not updated_tool:
            return LLMToolResponse(
                success=False,
                message=f"Tool with ID '{tool_id}' not found",
                data=DUMMY_TOOL
            )
        return LLMToolResponse(
            success=True,
            message=f"Tool '{tool_id}' updated successfully",
            data=updated_tool
        )
    except ValueError as e:
        logger.error(f"Error updating tool {tool_id}: {str(e)}")
        return LLMToolResponse(
            success=False,
            message=str(e),
            data=DUMMY_TOOL
        )
    except ValidationError as e:
        logger.error(f"Validation error updating tool {tool_id}: {str(e)}")
        return LLMToolResponse(
            success=False,
            message=f"Invalid tool definition: {str(e)}",
            data=DUMMY_TOOL
        )
    except Exception as e:
        logger.error(f"Unexpected error updating tool {tool_id}: {str(e)}")
        return LLMToolResponse(
            success=False,
            message=f"Failed to update tool: {str(e)}",
            data=DUMMY_TOOL
        )

@router.delete("/{tool_id}", response_model=LLMToolResponse[None], response_model_exclude_unset=True, response_model_exclude_none=True)
async def delete_tool(
    tool_id: str = Path(..., description="The ID of the tool to delete"),
    tool_registry: ToolRegistry = Depends(get_tool_registry),
    user = Depends(require_write)
) -> LLMToolResponse[None]:
    """
    Delete a tool.
    
    Args:
        tool_id: The ID of the tool to delete
        tool_registry: Tool registry dependency
        
    Returns:
        LLMToolResponse indicating success or failure
    """
    try:
        success = await tool_registry.delete_tool(tool_id)
        if not success:
            return LLMToolResponse(
                success=False,
                message=f"Tool with ID '{tool_id}' not found",
                data=None
            )
        return LLMToolResponse(
            success=True,
            message=f"Tool '{tool_id}' deleted successfully",
            data=None
        )
    except Exception as e:
        logger.error(f"Error deleting tool {tool_id}: {str(e)}")
        return LLMToolResponse(
            success=False,
            message=f"Failed to delete tool: {str(e)}",
            data=None
        )

@router.post("/{tool_id}/invoke", response_model=LLMToolResponse[Dict[str, Any]], response_model_exclude_unset=True, response_model_exclude_none=True)
async def invoke_tool(
    tool_id: str = Path(..., description="The ID of the tool to invoke"),
    parameters: Dict[str, Any] = None,
    tool_registry: ToolRegistry = Depends(get_tool_registry),
    user = Depends(require_read)
) -> LLMToolResponse[Dict[str, Any]]:
    """
    Invoke a tool with the provided parameters.
    
    Args:
        tool_id: The ID of the tool to invoke
        parameters: Parameters for the tool invocation
        tool_registry: Tool registry dependency
        
    Returns:
        LLMToolResponse containing the tool invocation result
    """
    try:
        # Create a context object if needed
        context = {}
        
        result = await tool_registry.invoke_tool(tool_id, parameters, context)
        return LLMToolResponse(
            success=True,
            message=f"Tool '{tool_id}' invoked successfully",
            data=result
        )
    except ValueError as e:
        logger.error(f"Parameter error invoking tool {tool_id}: {str(e)}")
        return LLMToolResponse(
            success=False,
            message=str(e),
            data={}
        )
    except Exception as e:
        logger.error(f"Unexpected error invoking tool {tool_id}: {str(e)}")
        return LLMToolResponse(
            success=False,
            message=f"Failed to invoke tool: {str(e)}",
            data={}
        ) 