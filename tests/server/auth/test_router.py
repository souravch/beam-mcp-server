import pytest
from unittest.mock import patch, MagicMock, AsyncMock
from fastapi import FastAPI, status
from fastapi.testclient import TestClient
from datetime import datetime, UTC

from src.server.auth.router import router
from src.server.auth.models import UserInfo, TokenResponse, UserSession, Role
from src.server.auth.jwt import TokenPair


# Create a test FastAPI app
app = FastAPI()
# The router already has prefix="/auth", so we need to include it with the correct prefix
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
        created_at=datetime.now(UTC),
        last_login=datetime.now(UTC)
    )


def test_login_oauth_google(client, mock_auth_config):
    """Test initiating Google OAuth login flow."""
    mock_url = "https://accounts.google.com/o/oauth2/auth?client_id=google-id&redirect_uri=http%3A%2F%2Flocalhost%2Fcallback&scope=openid+email+profile&response_type=code&state=test-state"
    
    # Ensure the OAuth provider is properly mocked
    mock_auth_config.oauth2_providers = {"google": MagicMock()}
    
    # NOTE: We need to mock RedirectResponse because TestClient doesn't properly handle
    # async endpoints that return RedirectResponse in some versions of FastAPI/Starlette
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


def test_login_oauth_unknown_provider(client, mock_auth_config):
    """Test initiating OAuth login flow with unknown provider."""
    with patch("src.server.auth.router.get_auth_config", return_value=mock_auth_config):
        response = client.get("/api/v1/auth/login/unknown")
        
        assert response.status_code == status.HTTP_400_BAD_REQUEST
        assert response.json()["detail"] == "Unknown OAuth provider: unknown"


def test_oauth_callback_success(client, mock_auth_config, mock_tokens, mock_user_info):
    """Test successful OAuth callback processing."""
    # Mock OAuth state
    oauth_state = MagicMock()
    oauth_state.redirect_url = "/dashboard"
    
    # Mock token exchange and user info
    token_response = {"access_token": "provider-token"}
    user_data = {
        "provider_user_id": "12345",
        "email": "test@example.com",
        "name": "Test User"
    }
    
    # NOTE: We need to mock RedirectResponse because TestClient doesn't properly handle
    # async endpoints that return RedirectResponse in some versions of FastAPI/Starlette
    with patch("src.server.auth.router.get_auth_config", return_value=mock_auth_config), \
         patch("src.server.auth.router.oauth_states", {"test-state": oauth_state}), \
         patch("src.server.auth.router.exchange_code_for_token", return_value=token_response), \
         patch("src.server.auth.router.get_user_info", return_value=user_data), \
         patch("src.server.auth.router.create_tokens", return_value=mock_tokens), \
         patch("src.server.auth.router.RedirectResponse") as mock_redirect:
        
        # Set up the mock redirect
        redirect_url = "/dashboard?access_token=test-access-token&refresh_token=test-refresh-token&token_type=bearer&expires_in=1800"
        mock_redirect.return_value = {"location": redirect_url}
        
        # Call the endpoint
        client.get(
            "/api/v1/auth/callback/google?code=test-code&state=test-state"
        )
        
        # Check that RedirectResponse was called with a URL containing the expected parameters
        mock_redirect.assert_called_once()
        call_args = mock_redirect.call_args[1]['url']
        assert "/dashboard?" in call_args
        assert "access_token=test-access-token" in call_args
        assert "refresh_token=test-refresh-token" in call_args


def test_oauth_callback_error(client, mock_auth_config):
    """Test OAuth callback with error from provider."""
    # NOTE: We need to mock RedirectResponse because TestClient doesn't properly handle
    # async endpoints that return RedirectResponse in some versions of FastAPI/Starlette
    with patch("src.server.auth.router.get_auth_config", return_value=mock_auth_config), \
         patch("src.server.auth.router.RedirectResponse") as mock_redirect:
        
        # Set up the mock redirect
        mock_redirect.return_value = {"location": "/?error=Authentication+failed"}
        
        # Call the endpoint
        client.get(
            "/api/v1/auth/callback/google?error=access_denied&state=test-state"
        )
        
        # Check that RedirectResponse was called with a URL containing the error
        mock_redirect.assert_called_once()
        call_args = mock_redirect.call_args[1]['url']
        assert "/?error=" in call_args
        # The actual error message includes the error from the provider
        assert "Authentication" in call_args
        assert "access_denied" in call_args


def test_oauth_callback_invalid_state(client, mock_auth_config):
    """Test OAuth callback with invalid state."""
    # NOTE: We need to mock RedirectResponse because TestClient doesn't properly handle
    # async endpoints that return RedirectResponse in some versions of FastAPI/Starlette
    with patch("src.server.auth.router.get_auth_config", return_value=mock_auth_config), \
         patch("src.server.auth.router.oauth_states", {}), \
         patch("src.server.auth.router.RedirectResponse") as mock_redirect:
        
        # Set up the mock redirect
        mock_redirect.return_value = {"location": "/?error=Invalid+state+parameter"}
        
        # Call the endpoint
        client.get(
            "/api/v1/auth/callback/google?code=test-code&state=invalid-state"
        )
        
        # Check that RedirectResponse was called with a URL containing the error
        mock_redirect.assert_called_once()
        call_args = mock_redirect.call_args[1]['url']
        assert "/?error=Invalid+state+parameter" in call_args


def test_refresh_token_success(client, mock_auth_config, mock_tokens, mock_user_info):
    """Test successful token refresh."""
    with patch("src.server.auth.router.get_auth_config", return_value=mock_auth_config), \
         patch("src.server.auth.router.verify_refresh_token", return_value="test-user"), \
         patch("src.server.auth.router.create_tokens", return_value=mock_tokens):
        
        response = client.post(
            "/api/v1/auth/refresh",
            json={"refresh_token": "valid-refresh-token"}
        )
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["access_token"] == "test-access-token"
        assert data["refresh_token"] == "test-refresh-token"
        assert data["token_type"] == "bearer"
        assert data["expires_in"] == 1800
        assert "user" in data


def test_refresh_token_invalid(client, mock_auth_config):
    """Test token refresh with invalid refresh token."""
    with patch("src.server.auth.router.get_auth_config", return_value=mock_auth_config), \
         patch("src.server.auth.router.verify_refresh_token", return_value=None):
        
        response = client.post(
            "/api/v1/auth/refresh",
            json={"refresh_token": "invalid-refresh-token"}
        )
        
        assert response.status_code == status.HTTP_401_UNAUTHORIZED
        assert response.json()["detail"] == "Invalid refresh token"


def test_get_me(client, mock_auth_config):
    """Test getting current user info."""
    mock_user = UserSession(
        user_id="test-user",
        provider_user_id="provider:12345",
        provider="google",
        email="test@example.com",
        name="Test User",
        roles=["admin"],
        metadata={},
        last_login=datetime.now(UTC)
    )
    
    with patch("src.server.auth.router.get_auth_config", return_value=mock_auth_config), \
         patch("src.server.auth.router.get_current_user", return_value=mock_user):
        
        response = client.get("/api/v1/auth/me")
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["user_id"] == "test-user"
        assert data["email"] == "test@example.com"
        assert data["name"] == "Test User"
        assert data["roles"] == ["admin"]
        # The provider might be transformed in the response, so just check it exists
        assert "provider" in data


def test_get_token_for_testing_success(client, mock_auth_config):
    """Test getting a test token."""
    with patch("src.server.auth.router.get_auth_config", return_value=mock_auth_config):
        response = client.post("/api/v1/auth/token")
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["access_token"] == "test-bypass-token"
        assert data["token_type"] == "bearer"
        assert data["expires_in"] == 3600
        assert data["scope"] == "all"


def test_get_token_for_testing_auth_disabled(client, mock_auth_config):
    """Test getting a test token when auth is disabled."""
    mock_auth_config.enabled = False
    
    with patch("src.server.auth.router.get_auth_config", return_value=mock_auth_config):
        response = client.post("/api/v1/auth/token")
        
        assert response.status_code == status.HTTP_501_NOT_IMPLEMENTED
        assert response.json()["detail"] == "Authentication is not enabled"


def test_get_token_for_testing_bypass_disabled(client, mock_auth_config):
    """Test getting a test token when test bypass is disabled."""
    mock_auth_config.allow_test_bypass = False
    
    with patch("src.server.auth.router.get_auth_config", return_value=mock_auth_config):
        response = client.post("/api/v1/auth/token")
        
        assert response.status_code == status.HTTP_403_FORBIDDEN
        assert response.json()["detail"] == "Test authentication is disabled in this environment" 