import pytest
from unittest.mock import patch, MagicMock, AsyncMock
from fastapi import FastAPI, status
from fastapi.testclient import TestClient
from datetime import datetime

from src.server.auth.router import router
from src.server.auth.models import UserInfo, TokenResponse, UserSession, Role
from src.server.auth.jwt import TokenPair


# Create a test FastAPI app
app = FastAPI()
app.include_router(router, prefix="/api/v1")


@pytest.fixture
def client():
    """Test client for the FastAPI app."""
    return TestClient(app)


@pytest.fixture
def mock_auth_config():
    """Mock the auth config for testing."""
    config = MagicMock()
    config.enabled = True
    config.jwt_secret_key = "test-secret-key"
    config.jwt_algorithm = "HS256"
    config.access_token_expire_minutes = 30
    config.refresh_token_expire_days = 7
    config.allow_test_bypass = True
    config.test_bypass_token = "test-bypass-token"
    
    # OAuth providers
    config.oauth2_providers = {
        "google": MagicMock(),
        "github": MagicMock()
    }
    
    return config


@pytest.fixture
def mock_tokens():
    """Mock token pair for testing."""
    return TokenPair(
        access_token="test-access-token",
        refresh_token="test-refresh-token",
        token_type="bearer",
        expires_in=1800
    )


@pytest.fixture
def mock_user_info():
    """Mock user info for testing."""
    return UserInfo(
        user_id="test-user",
        email="test@example.com",
        name="Test User",
        roles=["admin"],
        provider="google",
        created_at=datetime.utcnow(),
        last_login=datetime.utcnow()
    )


def test_login_oauth_google_mock_response(client, mock_auth_config):
    """Test initiating Google OAuth login flow by mocking the response."""
    mock_url = "https://accounts.google.com/o/oauth2/auth?client_id=google-id&redirect_uri=http%3A%2F%2Flocalhost%2Fcallback&scope=openid+email+profile&response_type=code&state=test-state"
    
    # Instead of testing the actual redirect, we'll mock the response
    with patch("src.server.auth.router.get_auth_config", return_value=mock_auth_config), \
         patch("src.server.auth.router.get_oauth2_login_url", return_value=mock_url), \
         patch("src.server.auth.router.secrets.token_urlsafe", return_value="test-state"), \
         patch("src.server.auth.router.RedirectResponse") as mock_redirect:
        
        # Mock the RedirectResponse to return a simple dict
        mock_redirect.return_value = {"location": mock_url}
        
        # Call the endpoint
        client.get("/api/v1/auth/login/google?redirect_uri=/dashboard")
        
        # Check that RedirectResponse was called with the correct URL
        mock_redirect.assert_called_once_with(url=mock_url)


def test_login_oauth_auth_disabled(client, mock_auth_config):
    """Test initiating OAuth login flow when auth is disabled."""
    mock_auth_config.enabled = False
    
    with patch("src.server.auth.router.get_auth_config", return_value=mock_auth_config):
        response = client.get("/api/v1/auth/login/google")
        
        assert response.status_code == status.HTTP_501_NOT_IMPLEMENTED
        assert response.json()["detail"] == "Authentication is not enabled" 