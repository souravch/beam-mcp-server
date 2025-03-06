"""
Tool Registry for managing MCP tools.

This module provides a registry for managing tools that can be used
in data pipelines, following the MCP standard.
"""

import logging
import uuid
from typing import Dict, List, Optional, Any, Union
from datetime import datetime

from ..models.tools import Tool, ToolDefinition, ToolType, ToolStatus
from ..models.context import MCPContext

logger = logging.getLogger(__name__)

class ToolRegistry:
    """Registry for managing MCP tools."""
    
    def __init__(self):
        """Initialize tool registry."""
        self._tools: Dict[str, Tool] = {}
        self._init_default_tools()
    
    def _init_default_tools(self):
        """Initialize default tools."""
        # Add default transformation tools
        default_tools = [
            ToolDefinition(
                name="text-tokenizer",
                description="Tokenizes text into words or subwords",
                tool_type=ToolType.TRANSFORMATION,
                version="1.0.0",
                parameters={
                    "text": {
                        "type": "string",
                        "description": "Text to tokenize",
                        "required": True
                    },
                    "lowercase": {
                        "type": "boolean",
                        "description": "Convert text to lowercase before tokenizing",
                        "default": True
                    },
                    "min_token_length": {
                        "type": "integer",
                        "description": "Minimum token length to keep",
                        "default": 1
                    }
                },
                capabilities=["text-processing", "preprocessing"]
            ),
            ToolDefinition(
                name="beam-wordcount",
                description="Counts words in a text corpus using Apache Beam",
                tool_type=ToolType.PROCESSOR,
                version="1.0.0",
                parameters={
                    "input_path": {
                        "type": "string",
                        "description": "Path to input data",
                        "required": True
                    },
                    "output_path": {
                        "type": "string",
                        "description": "Path to output data",
                        "required": True
                    },
                    "runner": {
                        "type": "string",
                        "description": "Beam runner to use",
                        "enum": ["direct", "dataflow", "spark", "flink"],
                        "default": "direct"
                    }
                },
                capabilities=["data-processing", "beam-pipeline"]
            )
        ]
        
        # Add tools to registry
        for tool_def in default_tools:
            tool_id = f"{tool_def.name}-{str(uuid.uuid4())[:8]}"
            tool = Tool(
                mcp_resource_id=tool_id,
                name=tool_def.name,
                description=tool_def.description,
                tool_type=tool_def.tool_type,
                version=tool_def.version,
                parameters=tool_def.parameters,
                capabilities=tool_def.capabilities,
                metadata=tool_def.metadata,
                mcp_created_at=datetime.utcnow(),
                mcp_updated_at=datetime.utcnow()
            )
            self._tools[tool_id] = tool
            logger.info(f"Added default tool: {tool.name} (ID: {tool_id})")
    
    async def list_tools(
        self,
        tool_type: Optional[ToolType] = None,
        status: Optional[ToolStatus] = None
    ) -> List[Tool]:
        """
        List all tools, optionally filtered by type and status.
        
        Args:
            tool_type: Optional filter by tool type
            status: Optional filter by tool status
            
        Returns:
            List[Tool]: List of tools
        """
        tools = list(self._tools.values())
        
        # Apply filters
        if tool_type:
            tools = [t for t in tools if t.tool_type == tool_type]
        
        if status:
            tools = [t for t in tools if t.status == status]
        
        return tools
    
    async def get_tool(self, tool_id: str) -> Optional[Tool]:
        """
        Get a tool by ID.
        
        Args:
            tool_id: Tool ID
            
        Returns:
            Optional[Tool]: Tool if found, None otherwise
        """
        return self._tools.get(tool_id)
    
    async def create_tool(
        self,
        tool_definition: ToolDefinition,
        created_by: Optional[str] = None
    ) -> Tool:
        """
        Create a new tool.
        
        Args:
            tool_definition: Tool definition
            created_by: User who created the tool
            
        Returns:
            Tool: Created tool
            
        Raises:
            ValueError: If tool name already exists
        """
        # Check if tool name already exists
        if any(t.name == tool_definition.name for t in self._tools.values()):
            raise ValueError(f"Tool with name '{tool_definition.name}' already exists")
        
        # Create tool
        tool_id = f"{tool_definition.name}-{str(uuid.uuid4())[:8]}"
        tool = Tool(
            mcp_resource_id=tool_id,
            name=tool_definition.name,
            description=tool_definition.description,
            tool_type=tool_definition.tool_type,
            version=tool_definition.version,
            parameters=tool_definition.parameters,
            capabilities=tool_definition.capabilities,
            metadata=tool_definition.metadata,
            mcp_created_at=datetime.utcnow(),
            mcp_updated_at=datetime.utcnow(),
            mcp_created_by=created_by
        )
        
        # Add to registry
        self._tools[tool_id] = tool
        logger.info(f"Created tool: {tool.name} (ID: {tool_id})")
        
        return tool
    
    async def update_tool(
        self,
        tool_id: str,
        updates: Dict[str, Any],
        updated_by: Optional[str] = None
    ) -> Optional[Tool]:
        """
        Update a tool.
        
        Args:
            tool_id: Tool ID
            updates: Updates to apply
            updated_by: User who updated the tool
            
        Returns:
            Optional[Tool]: Updated tool if found, None otherwise
        """
        # Get tool
        tool = await self.get_tool(tool_id)
        
        if not tool:
            return None
        
        # Apply updates
        for key, value in updates.items():
            if hasattr(tool, key) and key not in ('mcp_resource_id', 'mcp_resource_type'):
                setattr(tool, key, value)
        
        # Update metadata
        tool.mcp_updated_at = datetime.utcnow()
        
        if updated_by:
            tool.mcp_updated_by = updated_by
        
        tool.mcp_generation += 1
        
        logger.info(f"Updated tool: {tool.name} (ID: {tool_id})")
        
        return tool
    
    async def delete_tool(self, tool_id: str) -> bool:
        """
        Delete a tool.
        
        Args:
            tool_id: Tool ID
            
        Returns:
            bool: True if tool was deleted, False otherwise
        """
        if tool_id in self._tools:
            del self._tools[tool_id]
            logger.info(f"Deleted tool: {tool_id}")
            return True
        
        return False
    
    async def invoke_tool(
        self,
        tool_id: str,
        parameters: Dict[str, Any],
        context: MCPContext
    ) -> Dict[str, Any]:
        """
        Invoke a tool with parameters.
        
        Args:
            tool_id: Tool ID
            parameters: Tool parameters
            context: MCP context
            
        Returns:
            Dict[str, Any]: Tool invocation result
            
        Raises:
            ValueError: If tool not found or parameters invalid
            Exception: If tool invocation fails
        """
        # Get tool
        tool = await self.get_tool(tool_id)
        
        if not tool:
            raise ValueError(f"Tool not found: {tool_id}")
        
        # Validate parameters
        self._validate_parameters(tool, parameters)
        
        # In a real implementation, we would actually invoke the tool here.
        # For this example, we'll return a simple result.
        logger.info(f"Invoking tool: {tool.name} (ID: {tool_id})")
        
        # Add context information to result
        result = {
            "tool_id": tool_id,
            "tool_name": tool.name,
            "timestamp": datetime.utcnow().isoformat(),
            "parameters": parameters,
            "context": {
                "session_id": context.session_id,
                "transaction_id": context.transaction_id,
                "trace_id": context.trace_id,
                "user_id": context.user_id
            },
            # Simulate a tool result
            "result": {
                "status": "success",
                "processing_time_ms": 123.45,
                "output": f"Sample output from {tool.name}"
            }
        }
        
        return result
    
    def _validate_parameters(self, tool: Tool, parameters: Dict[str, Any]) -> None:
        """
        Validate tool parameters.
        
        Args:
            tool: Tool to validate parameters for
            parameters: Parameters to validate
            
        Raises:
            ValueError: If parameters are invalid
        """
        tool_params = tool.parameters
        
        # Check required parameters
        for param_name, param_schema in tool_params.items():
            if param_schema.get("required", False) and param_name not in parameters:
                raise ValueError(f"Missing required parameter: {param_name}")
        
        # Validate parameter types (simplified)
        for param_name, param_value in parameters.items():
            if param_name in tool_params:
                param_schema = tool_params[param_name]
                param_type = param_schema.get("type")
                
                # Basic type checking
                if param_type == "string" and not isinstance(param_value, str):
                    raise ValueError(f"Parameter {param_name} must be a string")
                elif param_type == "integer" and not isinstance(param_value, int):
                    raise ValueError(f"Parameter {param_name} must be an integer")
                elif param_type == "boolean" and not isinstance(param_value, bool):
                    raise ValueError(f"Parameter {param_name} must be a boolean")
                
                # Check enum values
                if "enum" in param_schema and param_value not in param_schema["enum"]:
                    valid_values = ", ".join(param_schema["enum"])
                    raise ValueError(f"Parameter {param_name} must be one of: {valid_values}") 