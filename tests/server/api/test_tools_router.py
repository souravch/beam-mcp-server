"""
Tests for the tools router.

This module contains tests for the tools router endpoints:
- GET /tools
- GET /tools/{tool_id}
- POST /tools
- PUT /tools/{tool_id}
- DELETE /tools/{tool_id}
- POST /tools/{tool_id}/invoke
"""
import json
import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock, AsyncMock
import asyncio
import uuid

from src.server.app import create_app
from src.server.models.tools import ToolDefinition, ToolType, ToolStatus, Tool
from src.server.core.tool_registry import ToolRegistry
from src.server.api import tools_router

@pytest.fixture
def app():
    """Create a test app instance."""
    test_config = {
        "testing": True,
        "runners": {},  # Empty runners dictionary to avoid KeyError
        "api": {
            "prefix": "/api/v1",
            "cors_origins": ["*"],
            "base_url": "http://localhost:8888"
        }
    }
    return create_app(test_config)

@pytest.fixture
def client(app):
    """Create a test client."""
    return TestClient(app)

@pytest.fixture
def mock_tool_registry():
    """Create a mock tool registry."""
    registry = MagicMock(spec=ToolRegistry)
    
    # Create default tools that match the Tool model format
    tool1 = Tool(
        mcp_resource_id="text-tokenizer",
        name="Text Tokenizer",
        description="Tokenizes text and returns tokens",
        tool_type="processor",
        version="1.0.0",
        status="active",
        parameters={"text": {"type": "string", "description": "Text to tokenize"}},
        capabilities=["tokenization", "text-processing"],
        metadata={"tags": ["text", "nlp"]}
    )
    
    tool2 = Tool(
        mcp_resource_id="beam-wordcount",
        name="Beam WordCount",
        description="Counts words in a text using Apache Beam",
        tool_type="transformation",
        version="1.0.0",
        status="active",
        parameters={"text": {"type": "string", "description": "Text to process"}},
        capabilities=["text-processing", "analytics"],
        metadata={"tags": ["text", "beam"]}
    )
    
    # Configure list_tools and get_tool to return awaitable results
    async def mock_list_tools(*args, **kwargs):
        return [tool1, tool2]
    
    async def mock_get_tool(tool_id):
        if tool_id == "text-tokenizer":
            return tool1
        elif tool_id == "beam-wordcount":
            return tool2
        return None
    
    registry.list_tools = AsyncMock(side_effect=mock_list_tools)
    registry.get_tool = AsyncMock(side_effect=mock_get_tool)
    
    # Setup create_tool as an AsyncMock
    async def mock_create_tool(tool_def):
        return Tool(
            mcp_resource_id=f"{tool_def.name.lower().replace(' ', '-')}-{uuid.uuid4().hex[:8]}",
            name=tool_def.name,
            description=tool_def.description,
            tool_type=tool_def.tool_type,
            version=tool_def.version,
            status="active",
            parameters=tool_def.parameters,
            capabilities=tool_def.capabilities,
            metadata=tool_def.metadata
        )
    
    registry.create_tool = AsyncMock(side_effect=mock_create_tool)
    
    # Setup update_tool, delete_tool, and invoke_tool as AsyncMocks
    async def mock_update_tool(tool_id, tool_def):
        tool = await mock_get_tool(tool_id)
        if not tool:
            return None
        
        return Tool(
            mcp_resource_id=tool.mcp_resource_id,
            name=tool_def.name,
            description=tool_def.description,
            tool_type=tool_def.tool_type,
            version=tool_def.version,
            status=tool.status,
            parameters=tool_def.parameters,
            capabilities=tool_def.capabilities,
            metadata=tool_def.metadata
        )
    
    async def mock_delete_tool(tool_id):
        return tool_id in ["text-tokenizer", "beam-wordcount"]
    
    async def mock_invoke_tool(tool_id, parameters, context=None):
        if tool_id == "text-tokenizer":
            return {"tokens": parameters.get("text", "").split()}
        elif tool_id == "beam-wordcount":
            text = parameters.get("text", "")
            return {"counts": {word: text.count(word) for word in set(text.split())}}
        return None
    
    registry.update_tool = AsyncMock(side_effect=mock_update_tool)
    registry.delete_tool = AsyncMock(side_effect=mock_delete_tool)
    registry.invoke_tool = AsyncMock(side_effect=mock_invoke_tool)
    
    return registry

@pytest.mark.asyncio
async def test_list_tools(client, mock_tool_registry):
    """Test listing all tools."""
    # Patch the app.state.tool_registry directly
    client.app.state.tool_registry = mock_tool_registry
    
    response = client.get("/api/v1/tools/")
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert len(data["data"]) == 2
    
    # Update the assertion to match the actual format of the tools returned from the mock
    assert data["data"][0]["mcp_resource_id"] == "text-tokenizer"
    assert data["data"][1]["mcp_resource_id"] == "beam-wordcount"
    assert data["data"][0]["name"] == "Text Tokenizer"
    assert data["data"][1]["name"] == "Beam WordCount"

@pytest.mark.asyncio
async def test_list_tools_with_filter(client, mock_tool_registry):
    """Test listing tools with filter."""
    # Configure mock to return filtered results
    filtered_tool = Tool(
        mcp_resource_id="text-tokenizer",
        name="Text Tokenizer",
        description="Tokenizes text into words",
        tool_type="processor",
        version="1.0.0",
        status="active",
        parameters={"text": {"type": "string", "description": "Text to tokenize"}},
        capabilities=["tokenization", "text-processing"],
        metadata={"tags": ["text", "nlp"]}
    )
    
    # Override the list_tools method for this test
    async def mock_filtered_list(*args, **kwargs):
        return [filtered_tool]
    
    mock_tool_registry.list_tools = AsyncMock(side_effect=mock_filtered_list)
    
    # Patch the app.state.tool_registry directly
    client.app.state.tool_registry = mock_tool_registry
    
    response = client.get("/api/v1/tools/?tool_type=processor")
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert len(data["data"]) == 1
    assert data["data"][0]["mcp_resource_id"] == "text-tokenizer"
    assert data["data"][0]["name"] == "Text Tokenizer"

@pytest.mark.asyncio
async def test_get_tool(client, mock_tool_registry):
    """Test getting a tool by ID."""
    # Patch the app.state.tool_registry directly
    client.app.state.tool_registry = mock_tool_registry
    
    response = client.get("/api/v1/tools/text-tokenizer")
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert data["data"]["mcp_resource_id"] == "text-tokenizer"
    assert data["data"]["name"] == "Text Tokenizer"
    assert data["data"]["tool_type"] == "processor"

@pytest.mark.asyncio
async def test_get_nonexistent_tool(client, mock_tool_registry):
    """Test getting a nonexistent tool."""
    # Configure mock for nonexistent tool
    async def mock_get_nonexistent(tool_id):
        return None
    
    mock_tool_registry.get_tool = AsyncMock(side_effect=mock_get_nonexistent)
    
    # Patch the app.state.tool_registry directly
    client.app.state.tool_registry = mock_tool_registry
    
    response = client.get("/api/v1/tools/nonexistent")
    assert response.status_code == 200  # Should return 200 with success=False
    data = response.json()
    assert data["success"] is False
    assert "not found" in data["message"]
    
    # Check the data field contains the dummy tool
    assert data["data"]["mcp_resource_id"] == "error"
    assert data["data"]["name"] == "Error"
    assert data["data"]["description"] == "Error response"

@pytest.mark.asyncio
async def test_create_tool_validation_error(client, mock_tool_registry):
    """Test validation error when creating a tool."""
    from src.server.models.tools import ToolDefinition
    
    # Configure mock to raise ValueError
    async def mock_create_error(tool_def):
        raise ValueError("Tool with this name already exists")
    
    mock_tool_registry.create_tool = AsyncMock(side_effect=mock_create_error)
    
    # Create a sample tool definition
    tool_definition = {
        "name": "Duplicate Tool",
        "description": "A duplicate tool",
        "tool_type": "processor",
        "version": "1.0.0",
        "parameters": {},
        "capabilities": [],
        "metadata": {}
    }
    
    # Test directly through the API
    # Patch the app.state.tool_registry directly
    client.app.state.tool_registry = mock_tool_registry
    
    response = client.post(
        "/api/v1/tools/",
        json=tool_definition
    )
    
    # Print response for debugging
    print(f"Response status: {response.status_code}")
    print(f"Response data: {response.json()}")
    
    # Verify the response
    assert response.status_code == 201  # Still returns 201 for validation errors
    data = response.json()
    assert data["success"] is False
    assert "Tool with this name already exists" in data["message"]
    
    # Check the data field contains the dummy tool
    assert data["data"]["mcp_resource_id"] == "error"
    assert data["data"]["name"] == "Error"
    assert data["data"]["description"] == "Error response"

@pytest.mark.asyncio
async def test_update_tool(client, mock_tool_registry):
    """Test updating a tool."""
    tool_update = {
        "name": "Updated Text Tokenizer",
        "description": "Updated tokenizer",
        "tool_type": "processor",
        "version": "1.1.0",
        "parameters": {},
        "capabilities": ["tokenization", "text-processing"],
        "metadata": {"author": "Beam Team", "updated": True}
    }
    
    # Patch the app.state.tool_registry directly
    client.app.state.tool_registry = mock_tool_registry
    
    response = client.put(
        "/api/v1/tools/text-tokenizer",
        json=tool_update
    )
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert data["data"]["mcp_resource_id"] == "text-tokenizer"
    assert data["data"]["name"] == "Updated Text Tokenizer"
    assert data["data"]["description"] == "Updated tokenizer"

@pytest.mark.asyncio
async def test_update_nonexistent_tool(client, mock_tool_registry):
    """Test updating a nonexistent tool."""
    # Configure mock to return None for nonexistent tool
    async def mock_update_nonexistent(tool_id, tool_def):
        return None
    
    mock_tool_registry.update_tool = AsyncMock(side_effect=mock_update_nonexistent)
    
    tool_update = {
        "name": "Updated Tool",
        "description": "Updated description",
        "tool_type": "processor",
        "version": "1.1.0",
        "parameters": {},
        "capabilities": [],
        "metadata": {}
    }
    
    # Patch the app.state.tool_registry directly
    client.app.state.tool_registry = mock_tool_registry
    
    response = client.put(
        "/api/v1/tools/nonexistent",
        json=tool_update
    )
    assert response.status_code == 200  # Should return 200 with success=False
    data = response.json()
    assert data["success"] is False
    assert "not found" in data["message"]
    
    # Check the data field contains the dummy tool
    assert data["data"]["mcp_resource_id"] == "error"
    assert data["data"]["name"] == "Error"
    assert data["data"]["description"] == "Error response"

@pytest.mark.asyncio
async def test_delete_tool(client, mock_tool_registry):
    """Test deleting a tool."""
    # Patch the app.state.tool_registry directly
    client.app.state.tool_registry = mock_tool_registry
    
    response = client.delete("/api/v1/tools/text-tokenizer")
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert "deleted successfully" in data["message"]
    # Delete response might not include a 'data' key, or it might be null
    
    # Check that the mock was called with the right parameters
    # We need to await this since it's an AsyncMock
    calls = mock_tool_registry.delete_tool.call_args_list
    assert len(calls) == 1
    assert calls[0][0][0] == "text-tokenizer"

@pytest.mark.asyncio
async def test_delete_nonexistent_tool(client, mock_tool_registry):
    """Test deleting a nonexistent tool."""
    # Configure mock to return False for nonexistent tool
    async def mock_delete_nonexistent(tool_id):
        return False
    
    mock_tool_registry.delete_tool = AsyncMock(side_effect=mock_delete_nonexistent)
    
    # Patch the app.state.tool_registry directly
    client.app.state.tool_registry = mock_tool_registry
    
    response = client.delete("/api/v1/tools/nonexistent")
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is False
    assert "not found" in data["message"]
    # Delete response might not include a 'data' key, or it might be null

@pytest.mark.asyncio
async def test_invoke_tool(client, mock_tool_registry):
    """Test invoking a tool."""
    parameters = {
        "text": "Hello world"
    }
    
    # Patch the app.state.tool_registry directly
    client.app.state.tool_registry = mock_tool_registry
    
    response = client.post(
        "/api/v1/tools/text-tokenizer/invoke",
        json=parameters
    )
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert "tokens" in data["data"]
    assert data["data"]["tokens"] == ["Hello", "world"]
    
    # Check that the mock was called with the right parameters
    calls = mock_tool_registry.invoke_tool.call_args_list
    assert len(calls) == 1
    assert calls[0][0][0] == "text-tokenizer"
    assert calls[0][0][1] == parameters

@pytest.mark.asyncio
async def test_invoke_tool_parameter_error(client, mock_tool_registry):
    """Test error when invoking a tool with invalid parameters."""
    # Configure mock to raise ValueError for invalid parameters
    async def mock_invoke_error(tool_id, parameters, context=None):
        raise ValueError("Invalid parameter: missing required 'text'")
    
    mock_tool_registry.invoke_tool = AsyncMock(side_effect=mock_invoke_error)
    
    # Patch the app.state.tool_registry directly
    client.app.state.tool_registry = mock_tool_registry
    
    response = client.post(
        "/api/v1/tools/text-tokenizer/invoke",
        json={}
    )
    assert response.status_code == 200  # Should return 200 with success=False
    data = response.json()
    assert data["success"] is False
    assert "Invalid parameter" in data["message"]
    # Empty dict for error response in invoke
    assert data["data"] == {}

@pytest.mark.asyncio
async def test_create_tool(client, mock_tool_registry):
    """Test creating a new tool."""
    tool_definition = {
        "name": "New Tool",
        "description": "A new test tool",
        "tool_type": "processor",  # Using correct field name and type
        "version": "1.0.0",
        "parameters": {
            "input": {"type": "string", "description": "Input text"}
        },
        "capabilities": ["testing"],
        "metadata": {"author": "Test Author"}
    }
    
    # Patch the app.state.tool_registry directly
    client.app.state.tool_registry = mock_tool_registry
    
    response = client.post(
        "/api/v1/tools/",
        json=tool_definition
    )
    assert response.status_code == 201
    data = response.json()
    assert data["success"] is True
    assert data["data"]["name"] == "New Tool"
    
    # Check that the mock was called with the right parameters
    calls = mock_tool_registry.create_tool.call_args_list
    assert len(calls) == 1
    # The first argument to create_tool should be a ToolDefinition object
    assert calls[0][0][0].name == "New Tool"
    assert calls[0][0][0].description == "A new test tool"
    assert calls[0][0][0].tool_type == "processor" 