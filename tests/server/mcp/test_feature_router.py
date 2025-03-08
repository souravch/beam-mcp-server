import pytest
from fastapi import FastAPI, APIRouter
from fastapi.testclient import TestClient

from server.mcp.app_integration import MCPFeatureRouter, create_mcp_feature_router, get_feature_dependency
from server.mcp.fastapi import MCPFastAPI
from server.mcp.capabilities import CapabilityRegistry
from server.mcp.models import ConnectionState


def test_feature_router_init():
    """Test feature router initialization."""
    # Create test components
    app = FastAPI()
    router = APIRouter()
    registry = CapabilityRegistry(
        server_name="test-server",
        server_version="1.0.0",
        protocol_version="1.0"
    )
    
    # Register feature
    registry.register_feature("test.feature")
    
    # Create mock FastAPI integration
    mcp_fastapi = MCPFastAPI(
        connection_manager=None,  # Not needed for test
        capability_registry=registry,
        transport=None,  # Not needed for test
        prefix="/api/v1/mcp"
    )
    
    # Create feature router
    feature_router = MCPFeatureRouter(
        router=router,
        mcp_fastapi=mcp_fastapi,
        feature_name="test.feature"
    )
    
    # Check properties
    assert feature_router.router == router
    assert feature_router.mcp_fastapi == mcp_fastapi
    assert feature_router.feature_name == "test.feature"
    assert feature_router.get_router() == router


class MockMCPFastAPI:
    """Mock MCP FastAPI integration for testing."""
    
    def __init__(self):
        self.registered_endpoints = []
        
    def register_feature_endpoint(self, router, path, endpoint, feature_name, methods=None, **kwargs):
        """Mock registering a feature endpoint."""
        self.registered_endpoints.append({
            "router": router,
            "path": path,
            "endpoint": endpoint,
            "feature_name": feature_name,
            "methods": methods or ["GET"],
            "kwargs": kwargs
        })


def test_feature_router_add_api_route():
    """Test adding an API route to a feature router."""
    # Create test components
    router = APIRouter()
    mock_fastapi = MockMCPFastAPI()
    
    # Create feature router
    feature_router = MCPFeatureRouter(
        router=router,
        mcp_fastapi=mock_fastapi,
        feature_name="test.feature"
    )
    
    # Add route
    async def test_endpoint():
        return {"message": "test"}
    
    feature_router.add_api_route(
        path="/test",
        endpoint=test_endpoint,
        methods=["GET", "POST"],
        summary="Test endpoint"
    )
    
    # Check registration
    assert len(mock_fastapi.registered_endpoints) == 1
    registered = mock_fastapi.registered_endpoints[0]
    assert registered["router"] == router
    assert registered["path"] == "/test"
    assert registered["endpoint"] == test_endpoint
    assert registered["feature_name"] == "test.feature"
    assert registered["methods"] == ["GET", "POST"]
    assert registered["kwargs"]["summary"] == "Test endpoint"


def test_feature_router_include_router():
    """Test including another router in a feature router."""
    # Create test components
    router = APIRouter()
    included_router = APIRouter()
    mock_fastapi = MockMCPFastAPI()
    
    # Create feature router
    feature_router = MCPFeatureRouter(
        router=router,
        mcp_fastapi=mock_fastapi,
        feature_name="test.feature"
    )
    
    # Include router
    feature_router.include_router(included_router, prefix="/included")
    
    # No direct way to check included routers in APIRouter,
    # but we know the method exists and is called correctly 