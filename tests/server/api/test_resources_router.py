"""
Tests for the resources router.

This module contains tests for the resources router endpoints:
- GET /resources
- GET /resources/{resource_id}
- POST /resources
- PUT /resources/{resource_id}
- DELETE /resources/{resource_id}
"""
import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock
import json
import asyncio

from src.server.app import create_app
from src.server.models.resources import ResourceDefinition, ResourceType, ResourceStatus, Resource
from src.server.core.resource_registry import ResourceRegistry

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
def mock_resource_registry():
    """Create a mock resource registry."""
    registry = MagicMock(spec=ResourceRegistry)
    
    # Setup default resources for list_resources
    resource1 = {
        "id": "dataset-123",
        "name": "Example Dataset",
        "description": "An example dataset for testing",
        "resource_type": ResourceType.DATASET,
        "location": "gs://beam-examples/datasets/example.csv",
        "format": "csv",
        "status": ResourceStatus.AVAILABLE,
        "size_bytes": 102400,
        "schema": {
            "fields": [
                {"name": "id", "type": "integer", "description": "Unique identifier"},
                {"name": "name", "type": "string", "description": "Name field"},
                {"name": "value", "type": "float", "description": "Value field"}
            ]
        },
        "metadata": {
            "rows": 1000,
            "created_by": "system"
        },
        # Add required MCP fields
        "mcp_resource_id": "dataset-123",
        "mcp_resource_type": "resource",
        "mcp_created_at": "2023-08-15T10:30:00Z",
        "mcp_updated_at": "2023-08-15T10:30:00Z",
        "mcp_state": "ACTIVE",
        "mcp_generation": 1,
        "mcp_labels": {},
        "mcp_annotations": {}
    }
    
    resource2 = {
        "id": "model-456",
        "name": "Example ML Model",
        "description": "A pre-trained example model",
        "resource_type": ResourceType.MODEL,
        "location": "gs://beam-examples/models/example-model.pkl",
        "format": "pickle",
        "status": ResourceStatus.AVAILABLE,
        "size_bytes": 50000000,
        "metadata": {
            "framework": "scikit-learn",
            "algorithm": "RandomForest",
            "created_by": "system"
        },
        # Add required MCP fields
        "mcp_resource_id": "model-456",
        "mcp_resource_type": "resource",
        "mcp_created_at": "2023-08-15T10:30:00Z",
        "mcp_updated_at": "2023-08-15T10:30:00Z",
        "mcp_state": "ACTIVE",
        "mcp_generation": 1,
        "mcp_labels": {},
        "mcp_annotations": {}
    }
    
    resource3 = {
        "id": "config-789",
        "name": "Example Config",
        "description": "An example configuration",
        "resource_type": ResourceType.CONFIG,
        "location": "gs://beam-examples/configs/example-config.json",
        "format": "json",
        "status": ResourceStatus.AVAILABLE,
        "size_bytes": 1024,
        "metadata": {
            "created_by": "system"
        },
        # Add required MCP fields
        "mcp_resource_id": "config-789",
        "mcp_resource_type": "resource",
        "mcp_created_at": "2023-08-15T10:30:00Z",
        "mcp_updated_at": "2023-08-15T10:30:00Z",
        "mcp_state": "ACTIVE",
        "mcp_generation": 1,
        "mcp_labels": {},
        "mcp_annotations": {}
    }
    
    registry.list_resources.return_value = [resource1, resource2, resource3]
    registry.get_resource.return_value = resource1
    
    # Setup create_resource
    def mock_create_resource_fixed(resource_def):
        # Convert ResourceDefinition attributes to a resource dictionary
        return {
            "mcp_resource_id": f"{resource_def.resource_type.value}-789",
            "mcp_resource_type": "resource",
            "mcp_created_at": "2023-08-15T10:30:00Z",
            "mcp_updated_at": "2023-08-15T10:30:00Z",
            "mcp_state": "ACTIVE",
            "mcp_generation": 1,
            "mcp_labels": {},
            "mcp_annotations": {},
            "name": resource_def.name,
            "description": resource_def.description,
            "resource_type": resource_def.resource_type,
            "location": resource_def.location,
            "format": resource_def.format,
            "status": ResourceStatus.AVAILABLE,
            "schema": resource_def.schema,
            "metadata": {
                **resource_def.metadata,
                "created_at": "2023-08-15T10:30:00Z"
            }
        }
    
    registry.create_resource.side_effect = mock_create_resource_fixed
    
    # Setup update_resource
    registry.update_resource.return_value = {
        "id": "dataset-123",
        "name": "Updated Dataset",
        "description": "An updated dataset",
        "resource_type": ResourceType.DATASET,
        "location": "gs://beam-examples/datasets/updated-example.csv",
        "format": "csv",
        "status": ResourceStatus.AVAILABLE,
        "size_bytes": 102400,
        "schema": {
            "fields": [
                {"name": "id", "type": "integer", "description": "Unique identifier"},
                {"name": "name", "type": "string", "description": "Name field"},
                {"name": "value", "type": "float", "description": "Value field"}
            ]
        },
        "metadata": {
            "rows": 1000,
            "created_by": "system",
            "updated_at": "2023-08-15T11:30:00Z"
        },
        # Add required MCP fields
        "mcp_resource_id": "dataset-123",
        "mcp_resource_type": "resource",
        "mcp_created_at": "2023-08-15T10:30:00Z",
        "mcp_updated_at": "2023-08-15T11:30:00Z",
        "mcp_state": "ACTIVE",
        "mcp_generation": 2,
        "mcp_labels": {},
        "mcp_annotations": {}
    }
    
    # Setup delete_resource
    registry.delete_resource.return_value = True
    
    return registry

def test_list_resources(client, mock_resource_registry):
    """Test listing resources."""
    # Patch the app.state.resource_registry directly
    client.app.state.resource_registry = mock_resource_registry
    
    response = client.get("/api/v1/resources/")
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert len(data["data"]) == 3
    assert data["data"][0]["name"] == "Example Dataset"
    assert data["data"][1]["name"] == "Example ML Model"
    assert data["data"][2]["name"] == "Example Config"

def test_list_resources_with_filter(client, mock_resource_registry):
    """Test listing resources with filter."""
    # Configure mock to return filtered results
    filtered_resource = {
        "id": "dataset-123",
        "name": "Example Dataset",
        "description": "An example dataset for testing",
        "resource_type": ResourceType.DATASET,
        "location": "gs://beam-examples/datasets/example.csv",
        "format": "csv",
        "status": ResourceStatus.AVAILABLE,
        "metadata": {
            "rows": 1000
        },
        # Add required MCP fields
        "mcp_resource_id": "dataset-123",
        "mcp_resource_type": "resource",
        "mcp_created_at": "2023-08-15T10:30:00Z",
        "mcp_updated_at": "2023-08-15T10:30:00Z",
        "mcp_state": "ACTIVE",
        "mcp_generation": 1,
        "mcp_labels": {},
        "mcp_annotations": {}
    }
    mock_resource_registry.list_resources.return_value = [filtered_resource]
    
    # Patch the app.state.resource_registry directly
    client.app.state.resource_registry = mock_resource_registry
    
    response = client.get("/api/v1/resources/?resource_type=dataset")
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert len(data["data"]) == 1
    assert data["data"][0]["name"] == "Example Dataset"
    assert data["data"][0]["resource_type"] == "dataset"

def test_get_resource(client, mock_resource_registry):
    """Test getting a resource by ID."""
    # Patch the app.state.resource_registry directly
    client.app.state.resource_registry = mock_resource_registry
    
    response = client.get("/api/v1/resources/dataset-123")
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert data["data"]["mcp_resource_id"] == "dataset-123"
    assert data["data"]["name"] == "Example Dataset"

def test_get_nonexistent_resource(client, mock_resource_registry):
    """Test getting a nonexistent resource."""
    mock_resource_registry.get_resource.return_value = None
    
    # Patch the app.state.resource_registry directly
    client.app.state.resource_registry = mock_resource_registry
    
    response = client.get("/api/v1/resources/nonexistent")
    assert response.status_code == 200  # Still returns 200 with error status
    data = response.json()
    assert data["success"] is False
    assert "not found" in data["message"]
    # Now expect a dummy resource instead of None
    assert data["data"]["mcp_resource_id"] == "error"
    assert data["data"]["name"] == "Error"

@pytest.mark.asyncio
async def test_create_resource(client, mock_resource_registry):
    """Test creating a new resource."""
    from src.server.api import resources_router
    from src.server.models.resources import ResourceDefinition, Resource
    
    # Create a sample resource definition
    resource_definition = ResourceDefinition(
        name="New Dataset",
        description="A new test dataset",
        resource_type="dataset",
        location="gs://beam-examples/datasets/new-dataset.csv",
        format="csv",
        schema={
            "fields": [
                {"name": "id", "type": "integer", "description": "Unique identifier"},
                {"name": "name", "type": "string", "description": "Name field"}
            ]
        },
        metadata={
            "rows": 500,
            "created_by": "test_user"
        }
    )
    
    # Define mock for create_resource
    def mock_create_resource_fixed(resource_def):
        # Return a proper Resource object, not a dictionary
        return Resource(
            mcp_resource_id=f"{resource_def.resource_type.value}-789",
            mcp_resource_type="resource",
            name=resource_def.name,
            description=resource_def.description,
            resource_type=resource_def.resource_type,
            location=resource_def.location,
            format=resource_def.format,
            status=ResourceStatus.AVAILABLE,
            schema=resource_def.schema,
            metadata={
                **resource_def.metadata,
                "created_at": "2023-08-15T10:30:00Z"
            }
        )
    
    # Replace the mock with our fixed version
    mock_resource_registry.create_resource.side_effect = mock_create_resource_fixed
    
    # Call the function directly without going through the API
    response = await resources_router.create_resource(
        resource_definition=resource_definition,
        resource_registry=mock_resource_registry
    )
    
    # Verify the response structure is correct
    assert response.success is True
    assert "created successfully" in response.message
    assert response.data.name == "New Dataset"
    assert response.data.mcp_resource_id == "dataset-789"
    
    # Make sure the mock was called
    mock_resource_registry.create_resource.assert_called_once_with(resource_definition)

@pytest.mark.asyncio
async def test_create_resource_validation_error(client, mock_resource_registry):
    """Test validation error when creating a resource."""
    from src.server.api import resources_router
    from src.server.models.resources import ResourceDefinition
    
    # Configure mock to raise ValueError
    mock_resource_registry.create_resource.side_effect = ValueError("Resource with this name already exists")
    
    # Create a sample resource definition
    resource_definition = ResourceDefinition(
        name="Duplicate Resource",
        description="A duplicate resource",
        resource_type="dataset",
        location="gs://beam-examples/duplicate.csv",
        format="csv",
        metadata={}
    )
    
    # Call the function directly without going through the API
    response = await resources_router.create_resource(
        resource_definition=resource_definition,
        resource_registry=mock_resource_registry
    )
    
    # Verify the response structure is correct
    assert response.success is False
    assert "Resource with this name already exists" in response.message
    assert response.data.mcp_resource_id == "error"
    assert response.data.name == "Error"
    
    # Make sure the mock was called
    mock_resource_registry.create_resource.assert_called_once_with(resource_definition)

def test_update_resource(client, mock_resource_registry):
    """Test updating a resource."""
    resource_update = {
        "name": "Updated Dataset",
        "description": "An updated dataset",
        "location": "gs://beam-examples/datasets/updated-example.csv"
    }
    
    # Patch the app.state.resource_registry directly
    client.app.state.resource_registry = mock_resource_registry
    
    response = client.put(
        "/api/v1/resources/dataset-123",
        json=resource_update
    )
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert data["data"]["name"] == "Updated Dataset"
    assert data["data"]["description"] == "An updated dataset"
    assert data["data"]["location"] == "gs://beam-examples/datasets/updated-example.csv"
    mock_resource_registry.update_resource.assert_called_once()

def test_update_nonexistent_resource(client, mock_resource_registry):
    """Test updating a nonexistent resource."""
    mock_resource_registry.update_resource.return_value = None
    
    resource_update = {
        "name": "Updated Resource",
        "description": "Updated description"
    }
    
    # Patch the app.state.resource_registry directly
    client.app.state.resource_registry = mock_resource_registry
    
    response = client.put(
        "/api/v1/resources/nonexistent",
        json=resource_update
    )
    assert response.status_code == 200  # Still returns 200 with error status
    data = response.json()
    assert data["success"] is False
    assert "not found" in data["message"]
    # Now expect a dummy resource instead of None
    assert data["data"]["mcp_resource_id"] == "error"
    assert data["data"]["name"] == "Error"

def test_delete_resource(client, mock_resource_registry):
    """Test deleting a resource."""
    # Patch the app.state.resource_registry directly
    client.app.state.resource_registry = mock_resource_registry
    
    response = client.delete("/api/v1/resources/dataset-123")
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert "deleted successfully" in data["message"]
    # Now expect a dummy resource instead of None
    assert data["data"]["mcp_resource_id"] == "error"
    assert data["data"]["name"] == "Error"
    mock_resource_registry.delete_resource.assert_called_once_with("dataset-123")

def test_delete_nonexistent_resource(client, mock_resource_registry):
    """Test deleting a nonexistent resource."""
    mock_resource_registry.delete_resource.return_value = False
    
    # Patch the app.state.resource_registry directly
    client.app.state.resource_registry = mock_resource_registry
    
    response = client.delete("/api/v1/resources/nonexistent")
    assert response.status_code == 200  # Still returns 200 with error status
    data = response.json()
    assert data["success"] is False
    assert "not found" in data["message"]
    # Now expect a dummy resource instead of None
    assert data["data"]["mcp_resource_id"] == "error"
    assert data["data"]["name"] == "Error" 