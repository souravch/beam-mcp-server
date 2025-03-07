"""
Integration tests for the authentication system.

These tests verify that the authentication system works correctly with the API endpoints.
They test:
1. Authentication disabled (default)
2. Authentication enabled with valid tokens
3. Authentication enabled with invalid tokens
4. Authentication bypass for development
5. Role-based access control (admin vs viewer)
"""

import pytest
import os
from unittest.mock import patch, MagicMock
from fastapi import FastAPI, Depends
from fastapi.testclient import TestClient
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Callable

# Import the auth module
from src.server.auth import (
    get_auth_config, 
    require_read, 
    require_write,
    Operation, 
    Role,
    UserSession
)
from src.server.auth.config import AuthConfig
from src.server.auth.jwt import create_tokens, TokenData
from src.server.auth.dependencies import get_current_user, get_current_token


# Create a test app
@pytest.fixture
def mock_auth_app():
    """Create a test FastAPI app with auth endpoints."""
    app = FastAPI()
    
    # Test read endpoint
    @app.get("/test/read")
    async def test_read(user: UserSession = Depends(require_read)):
        return {"success": True, "message": "Read access granted", "user": user.user_id, "roles": user.roles}
    
    # Test write endpoint
    @app.post("/test/write")
    async def test_write(user: UserSession = Depends(require_write)):
        return {"success": True, "message": "Write access granted", "user": user.user_id, "roles": user.roles}
    
    # Public endpoint (no auth)
    @app.get("/test/public")
    async def test_public():
        return {"success": True, "message": "Public access"}
    
    return app


@pytest.fixture
def admin_token_pair():
    """Create an admin token pair."""
    auth_config = AuthConfig(
        enabled=True,
        jwt_secret_key="test-secret-key",
        jwt_algorithm="HS256"
    )
    
    with patch("src.server.auth.jwt.get_auth_config", return_value=auth_config):
        tokens = create_tokens(
            user_id="test-admin",
            email="admin@example.com",
            name="Test Admin",
            roles=[Role.ADMIN],
            provider="test"
        )
        
        return tokens


@pytest.fixture
def viewer_token_pair():
    """Create a viewer token pair."""
    auth_config = AuthConfig(
        enabled=True,
        jwt_secret_key="test-secret-key",
        jwt_algorithm="HS256"
    )
    
    with patch("src.server.auth.jwt.get_auth_config", return_value=auth_config):
        tokens = create_tokens(
            user_id="test-viewer",
            email="viewer@example.com",
            name="Test Viewer",
            roles=[Role.VIEWER],
            provider="test"
        )
        
        return tokens


@pytest.fixture
def expired_token_pair():
    """Create an expired token pair."""
    auth_config = AuthConfig(
        enabled=True,
        jwt_secret_key="test-secret-key",
        jwt_algorithm="HS256",
        access_token_expire_minutes=-30  # Expired 30 minutes ago
    )
    
    with patch("src.server.auth.jwt.get_auth_config", return_value=auth_config):
        tokens = create_tokens(
            user_id="test-expired",
            email="expired@example.com",
            name="Test Expired",
            roles=[Role.ADMIN],
            provider="test"
        )
        
        return tokens


# Test authentication disabled (default)
def test_auth_disabled(mock_auth_app):
    """Test that authentication is disabled by default."""
    client = TestClient(mock_auth_app)
    
    # Mock auth config with auth disabled
    auth_config = AuthConfig(enabled=False)
    
    with patch("src.server.auth.dependencies.get_auth_config", return_value=auth_config):
        # Test read endpoint
        response = client.get("/test/read")
        assert response.status_code == 200
        assert response.json()["success"] is True
        
        # Test write endpoint
        response = client.post("/test/write")
        assert response.status_code == 200
        assert response.json()["success"] is True
        
        # Test public endpoint
        response = client.get("/test/public")
        assert response.status_code == 200
        assert response.json()["success"] is True


# Test authentication enabled with valid tokens
def test_auth_enabled_valid_tokens(mock_auth_app, admin_token_pair, viewer_token_pair):
    """Test authentication with valid tokens."""
    client = TestClient(mock_auth_app)
    
    # Mock auth config with auth enabled
    auth_config = AuthConfig(
        enabled=True,
        jwt_secret_key="test-secret-key",
        jwt_algorithm="HS256"
    )
    
    with patch("src.server.auth.dependencies.get_auth_config", return_value=auth_config), \
         patch("src.server.auth.jwt.get_auth_config", return_value=auth_config):
        
        # Test read endpoint with admin token
        response = client.get(
            "/test/read",
            headers={"Authorization": f"Bearer {admin_token_pair.access_token}"}
        )
        assert response.status_code == 200
        assert response.json()["success"] is True
        assert response.json()["user"] == "test-admin"
        assert Role.ADMIN in response.json()["roles"]
        
        # Test write endpoint with admin token
        response = client.post(
            "/test/write",
            headers={"Authorization": f"Bearer {admin_token_pair.access_token}"}
        )
        assert response.status_code == 200
        assert response.json()["success"] is True
        assert response.json()["user"] == "test-admin"
        assert Role.ADMIN in response.json()["roles"]
        
        # Test read endpoint with viewer token
        response = client.get(
            "/test/read",
            headers={"Authorization": f"Bearer {viewer_token_pair.access_token}"}
        )
        assert response.status_code == 200
        assert response.json()["success"] is True
        assert response.json()["user"] == "test-viewer"
        assert Role.VIEWER in response.json()["roles"]
        
        # Test public endpoint (no token)
        response = client.get("/test/public")
        assert response.status_code == 200
        assert response.json()["success"] is True


# Test authentication enabled with invalid tokens
def test_auth_enabled_invalid_tokens(mock_auth_app, expired_token_pair):
    """Test authentication with invalid tokens."""
    client = TestClient(mock_auth_app)
    
    # Mock auth config with auth enabled
    auth_config = AuthConfig(
        enabled=True,
        jwt_secret_key="test-secret-key",
        jwt_algorithm="HS256"
    )
    
    with patch("src.server.auth.dependencies.get_auth_config", return_value=auth_config), \
         patch("src.server.auth.jwt.get_auth_config", return_value=auth_config):
        
        # Test read endpoint with invalid token
        response = client.get(
            "/test/read",
            headers={"Authorization": "Bearer invalid-token"}
        )
        assert response.status_code == 401  # Unauthorized
        
        # Test write endpoint with invalid token
        response = client.post(
            "/test/write",
            headers={"Authorization": "Bearer invalid-token"}
        )
        assert response.status_code == 401  # Unauthorized
        
        # Test read endpoint with expired token
        response = client.get(
            "/test/read",
            headers={"Authorization": f"Bearer {expired_token_pair.access_token}"}
        )
        assert response.status_code == 401  # Unauthorized
        
        # Test write endpoint with expired token
        response = client.post(
            "/test/write",
            headers={"Authorization": f"Bearer {expired_token_pair.access_token}"}
        )
        assert response.status_code == 401  # Unauthorized
        
        # Test endpoints with no token
        response = client.get("/test/read")
        assert response.status_code == 401  # Unauthorized
        
        response = client.post("/test/write")
        assert response.status_code == 401  # Unauthorized
        
        # Test public endpoint (no token)
        response = client.get("/test/public")
        assert response.status_code == 200  # Public endpoints are always accessible
        assert response.json()["success"] is True


# Test role-based access control
def test_rbac(mock_auth_app, admin_token_pair, viewer_token_pair):
    """Test role-based access control."""
    client = TestClient(mock_auth_app)
    
    # Mock auth config with auth enabled
    auth_config = AuthConfig(
        enabled=True,
        jwt_secret_key="test-secret-key",
        jwt_algorithm="HS256"
    )
    
    with patch("src.server.auth.dependencies.get_auth_config", return_value=auth_config), \
         patch("src.server.auth.jwt.get_auth_config", return_value=auth_config):
        
        # Test read endpoint with viewer token (should succeed)
        response = client.get(
            "/test/read",
            headers={"Authorization": f"Bearer {viewer_token_pair.access_token}"}
        )
        assert response.status_code == 200
        assert response.json()["success"] is True
        assert response.json()["user"] == "test-viewer"
        assert Role.VIEWER in response.json()["roles"]
        
        # Test write endpoint with viewer token (should fail)
        response = client.post(
            "/test/write",
            headers={"Authorization": f"Bearer {viewer_token_pair.access_token}"}
        )
        assert response.status_code == 403  # Forbidden
        
        # Test write endpoint with admin token (should succeed)
        response = client.post(
            "/test/write",
            headers={"Authorization": f"Bearer {admin_token_pair.access_token}"}
        )
        assert response.status_code == 200
        assert response.json()["success"] is True
        assert response.json()["user"] == "test-admin"
        assert Role.ADMIN in response.json()["roles"]


# Test auth bypass for development
def test_auth_bypass(mock_auth_app):
    """Test authentication bypass for development."""
    client = TestClient(mock_auth_app)
    
    # Mock auth config with auth enabled and bypass enabled
    auth_config = AuthConfig(
        enabled=True,
        jwt_secret_key="test-secret-key",
        jwt_algorithm="HS256",
        allow_test_bypass=True,
        test_bypass_token="bypass-token"
    )
    
    with patch("src.server.auth.dependencies.get_auth_config", return_value=auth_config), \
         patch("src.server.auth.jwt.get_auth_config", return_value=auth_config):
        
        # Test read endpoint with bypass token
        response = client.get(
            "/test/read",
            headers={"Authorization": "Bearer bypass-token"}
        )
        assert response.status_code == 200
        assert response.json()["success"] is True
        assert response.json()["user"] == "test-user"
        assert Role.ADMIN in response.json()["roles"]  # Bypass token gets admin role
        
        # Test write endpoint with bypass token
        response = client.post(
            "/test/write",
            headers={"Authorization": "Bearer bypass-token"}
        )
        assert response.status_code == 200
        assert response.json()["success"] is True
        assert response.json()["user"] == "test-user"
        assert Role.ADMIN in response.json()["roles"] 