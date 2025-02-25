"""
Unit tests for the health endpoint.
"""

import os
import pytest
from fastapi.testclient import TestClient
from datetime import datetime

from src.server.app import create_app

@pytest.fixture
def client():
    """Create a test client using TestClient."""
    # Set environment variables for testing
    os.environ["ENVIRONMENT"] = "testing"
    
    # Create the app
    app = create_app()
    
    # Create a test client
    client = TestClient(app)
    
    return client

def test_health_endpoint(client):
    """Test the health endpoint."""
    # Make a request to the health endpoint
    response = client.get("/health")
    
    # Check the response status code
    assert response.status_code == 200
    
    # Parse the response JSON
    data = response.json()
    
    # Check the response data
    assert data["status"] == "healthy"
    assert "timestamp" in data
    assert "version" in data
    assert data["service"] == "beam-mcp"
    assert data["environment"] == "testing"
    assert "error" not in data or data["error"] is None

def test_llm_health_endpoint(client):
    """Test the LLM-friendly health endpoint."""
    # Make a request to the LLM health endpoint
    response = client.get("/health/llm")
    
    # Check the response status code
    assert response.status_code == 200
    
    # Parse the response JSON
    data = response.json()
    
    # Check the response format
    assert data["success"] is True
    assert "data" in data
    assert "message" in data
    assert data["error"] is None
    
    # Check the health data
    health_data = data["data"]
    assert health_data["status"] == "healthy"
    assert "timestamp" in health_data
    assert "version" in health_data
    assert health_data["service"] == "beam-mcp"
    assert health_data["environment"] == "testing" 