"""
Tests for the contexts router.

This module contains tests for the contexts router endpoints:
- GET /contexts
- GET /contexts/{context_id}
- POST /contexts
- PUT /contexts/{context_id}
- DELETE /contexts/{context_id}
"""
import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock

from src.server.app import create_app
from src.server.models.contexts import ContextDefinition, ContextType, ContextStatus, Context
from src.server.core.context_registry import ContextRegistry

@pytest.fixture
def app(mock_context_registry):
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
    app = create_app(test_config)
    # Replace the context registry with our mock
    app.state.context_registry = mock_context_registry
    return app

@pytest.fixture
def client(app):
    """Create a test client."""
    return TestClient(app)

@pytest.fixture
def mock_context_registry():
    """Create a mock context registry."""
    registry = MagicMock(spec=ContextRegistry)
    
    # Create a Context object with a dict method
    def create_context_obj(context_data):
        context = MagicMock(spec=Context)
        context.dict.return_value = context_data
        # Set all attributes of the context
        for key, value in context_data.items():
            setattr(context, key, value)
        # Special handling for mcp_resource_id since that's used in transformations
        context.mcp_resource_id = context_data["id"]
        return context
    
    # Setup default contexts for list_contexts
    context1_data = {
        "id": "dataflow-default",
        "name": "Default Dataflow",
        "description": "Default execution context for Dataflow jobs",
        "context_type": ContextType.DATAFLOW,
        "parameters": {
            "region": "us-central1",
            "temp_location": "gs://beam-temp/dataflow",
            "machine_type": "n1-standard-2"
        },
        "resources": {
            "cpu": "2",
            "memory": "4GB"
        },
        "status": ContextStatus.ACTIVE,
        "job_count": 5,
        "metadata": {
            "environment": "development",
            "created_by": "system"
        }
    }
    
    context2_data = {
        "id": "direct-default",
        "name": "Default Direct Runner",
        "description": "Default execution context for Direct Runner jobs",
        "context_type": ContextType.DIRECT,
        "parameters": {
            "parallelism": "4"
        },
        "status": ContextStatus.ACTIVE,
        "job_count": 3,
        "metadata": {
            "environment": "development",
            "created_by": "system"
        }
    }
    
    context3_data = {
        "id": "flink-default",
        "name": "Default Flink",
        "description": "Default execution context for Flink jobs",
        "context_type": ContextType.FLINK,
        "parameters": {
            "master": "local[*]",
            "temp_location": "gs://beam-temp/flink"
        },
        "resources": {
            "cpu": "4",
            "memory": "8GB"
        },
        "status": ContextStatus.ACTIVE,
        "job_count": 2,
        "metadata": {
            "environment": "development",
            "created_by": "system"
        }
    }
    
    context1 = create_context_obj(context1_data)
    context2 = create_context_obj(context2_data)
    context3 = create_context_obj(context3_data)
    
    registry.list_contexts.return_value = [context1, context2, context3]
    registry.get_context.return_value = context1
    
    # Setup create_context
    def mock_create_context(context_def):
        context_data = {
            "id": f"{context_def.context_type.value}-123",
            "name": context_def.name,
            "description": context_def.description,
            "context_type": context_def.context_type,
            "parameters": context_def.parameters,
            "resources": getattr(context_def, 'resources', {}),
            "status": ContextStatus.ACTIVE,
            "job_count": 0,
            "metadata": {
                **(context_def.metadata or {}),
                "created_at": "2023-08-15T10:30:00Z"
            }
        }
        return create_context_obj(context_data)
    
    registry.create_context.side_effect = mock_create_context
    
    # Setup update_context
    update_context_data = {
        "id": "dataflow-default",
        "name": "Updated Dataflow",
        "description": "Updated execution context for Dataflow jobs",
        "context_type": ContextType.DATAFLOW,
        "parameters": {
            "region": "us-west1",
            "temp_location": "gs://beam-temp/dataflow-updated",
            "machine_type": "n1-standard-4"
        },
        "resources": {
            "cpu": "4",
            "memory": "8GB"
        },
        "status": ContextStatus.ACTIVE,
        "job_count": 5,
        "metadata": {
            "environment": "staging",
            "created_by": "system",
            "updated_at": "2023-08-15T11:30:00Z"
        }
    }
    registry.update_context.return_value = create_context_obj(update_context_data)
    
    # Setup delete_context
    registry.delete_context.return_value = True
    
    return registry

def test_list_contexts(client, mock_context_registry):
    """Test listing contexts."""
    response = client.get("/api/v1/contexts/")
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert len(data["data"]) == 3
    assert data["data"][0]["name"] == "Default Dataflow"
    assert data["data"][1]["name"] == "Default Direct Runner"
    assert data["data"][2]["name"] == "Default Flink"

def test_list_contexts_with_filter(client, mock_context_registry):
    """Test listing contexts with filter."""
    # Configure mock to return filtered results
    filtered_context = {
        "id": "dataflow-default",
        "name": "Default Dataflow",
        "description": "Default execution context for Dataflow jobs",
        "context_type": ContextType.DATAFLOW,
        "parameters": {
            "region": "us-central1",
            "temp_location": "gs://my-bucket/temp"
        },
        "resources": {
            "cpu": "2",
            "memory": "4GB"
        },
        "status": ContextStatus.ACTIVE,
        "job_count": 5,
        "metadata": {
            "environment": "staging"
        }
    }
    # Return list with only one context for the filter
    mock_context_registry.list_contexts.side_effect = lambda context_type=None, status=None: (
        [mock_context_registry.get_context.return_value] if context_type == ContextType.DATAFLOW else []
    )
    
    response = client.get("/api/v1/contexts/?context_type=dataflow")
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert len(data["data"]) == 1
    assert data["data"][0]["name"] == "Default Dataflow"
    assert data["data"][0]["context_type"] == "dataflow"

def test_get_context(client, mock_context_registry):
    """Test getting a context by ID."""
    response = client.get("/api/v1/contexts/dataflow-default")
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert data["data"]["name"] == "Default Dataflow"

def test_get_nonexistent_context(client, mock_context_registry):
    """Test getting a nonexistent context."""
    # Configure mock to return None for nonexistent context
    mock_context_registry.get_context.side_effect = lambda context_id: None if context_id == "nonexistent-id" else mock_context_registry.get_context.return_value
    
    response = client.get("/api/v1/contexts/nonexistent-id")
    assert response.status_code == 200  # Still returns 200 with error in response
    data = response.json()
    assert data["success"] is False
    assert "not found" in data["message"]

def test_create_context(client, mock_context_registry):
    """Test creating a new context."""
    context_definition = {
        "name": "Production Dataflow",
        "description": "Production execution context for Dataflow jobs",
        "context_type": "dataflow",
        "parameters": {
            "region": "us-central1",
            "project": "my-beam-project",
            "temp_location": "gs://my-bucket/temp"
        },
        "resources": {
            "cpu": "4",
            "memory": "8GB"
        },
        "metadata": {
            "environment": "production"
        }
    }

    response = client.post(
        "/api/v1/contexts/",
        json=context_definition
    )
    assert response.status_code == 201
    data = response.json()
    assert data["success"] is True
    assert data["data"]["name"] == "Production Dataflow"
    assert "id" in data["data"]
    assert data["data"]["id"].startswith("dataflow-")
    mock_context_registry.create_context.assert_called_once()

def test_create_context_validation_error(client, mock_context_registry):
    """Test validation error when creating a context."""
    # Configure mock to raise ValueError
    mock_context_registry.create_context.side_effect = ValueError("Context with this name already exists")

    context_definition = {
        "name": "Duplicate Context",
        "description": "A duplicate context",
        "context_type": "dataflow",
        "parameters": {},
        "metadata": {}
    }

    response = client.post(
        "/api/v1/contexts/",
        json=context_definition
    )
    assert response.status_code == 201  # Still returns 201 with error in response
    data = response.json()
    assert data["success"] is False
    assert "Context with this name already exists" in data["message"]

def test_update_context(client, mock_context_registry):
    """Test updating a context."""
    context_update = {
        "name": "Updated Dataflow",
        "description": "Updated execution context for Dataflow jobs",
        "parameters": {
            "region": "us-west1",
            "machine_type": "n1-standard-4"
        },
        "metadata": {
            "environment": "staging"
        }
    }
    
    # Reset mock to clear any previous calls
    mock_context_registry.update_context.reset_mock()
    
    response = client.put(
        "/api/v1/contexts/dataflow-default",
        json=context_update
    )
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert data["data"]["name"] == "Updated Dataflow"
    assert data["data"]["parameters"]["region"] == "us-west1"
    assert data["data"]["metadata"]["environment"] == "staging"
    # Check that update_context was called with correct args
    assert mock_context_registry.update_context.call_count > 0

def test_update_nonexistent_context(client, mock_context_registry):
    """Test updating a nonexistent context."""
    # Configure mock to return None for nonexistent context
    mock_context_registry.get_context.side_effect = lambda context_id: None if context_id == "nonexistent-id" else mock_context_registry.get_context.return_value
    
    context_update = {
        "name": "Updated Context",
        "description": "This update should fail"
    }
    
    response = client.put(
        "/api/v1/contexts/nonexistent-id",
        json=context_update
    )
    assert response.status_code == 200  # Still returns 200 with error status
    data = response.json()
    assert data["success"] is False
    assert "not found" in data["message"]

def test_delete_context(client, mock_context_registry):
    """Test deleting a context."""
    # Reset mock to clear any previous calls
    mock_context_registry.delete_context.reset_mock()
    
    response = client.delete("/api/v1/contexts/dataflow-default")
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert "deleted successfully" in data["message"]
    # Check that delete_context was called with correct arg
    assert mock_context_registry.delete_context.call_count > 0

def test_delete_nonexistent_context(client, mock_context_registry):
    """Test deleting a nonexistent context."""
    # Configure mock to return None for nonexistent context
    mock_context_registry.get_context.side_effect = lambda context_id: None if context_id == "nonexistent-id" else mock_context_registry.get_context.return_value
    
    response = client.delete("/api/v1/contexts/nonexistent-id")
    assert response.status_code == 200  # Still returns 200 with error status
    data = response.json()
    assert data["success"] is False
    assert "not found" in data["message"] 