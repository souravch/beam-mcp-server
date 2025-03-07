import pytest
from fastapi import HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials
from unittest.mock import patch, MagicMock, AsyncMock
from datetime import datetime

from src.server.auth.dependencies import (
    get_current_token,
    get_current_user,
    require_permission,
    Operation,
    RateLimiter
)
from src.server.auth.jwt import TokenData
from src.server.auth.models import Role


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
    
    # Rate limiting settings
    config.rate_limit = MagicMock()
    config.rate_limit.enabled = True
    config.rate_limit.max_requests = 100
    config.rate_limit.window_seconds = 3600
    
    return config


@pytest.fixture
def mock_request():
    """Mock a FastAPI request."""
    request = MagicMock()
    request.client = MagicMock()
    request.client.host = "127.0.0.1"
    request.url = MagicMock()
    request.url.path = "/api/v1/resources"
    
    return request


@pytest.fixture
def mock_token_data():
    """Mock token data for testing."""
    return TokenData(
        sub="test-user",
        email="test@example.com",
        name="Test User",
        roles=["admin"],
        jti="test-jti",
        provider="google"
    )


@pytest.mark.asyncio
async def test_get_current_token_valid(mock_auth_config, mock_request, mock_token_data):
    """Test getting the current token with valid credentials."""
    with patch("src.server.auth.dependencies.get_auth_config", return_value=mock_auth_config), \
         patch("src.server.auth.dependencies.verify_access_token", return_value=mock_token_data):
        
        credentials = HTTPAuthorizationCredentials(scheme="bearer", credentials="valid-token")
        token_data = await get_current_token(mock_request, credentials)
        
        assert token_data is not None
        assert token_data.sub == "test-user"
        assert token_data.email == "test@example.com"
        assert token_data.name == "Test User"
        assert token_data.roles == ["admin"]
        assert token_data.provider == "google"


@pytest.mark.asyncio
async def test_get_current_token_no_credentials(mock_auth_config, mock_request):
    """Test getting the current token with no credentials."""
    with patch("src.server.auth.dependencies.get_auth_config", return_value=mock_auth_config):
        token_data = await get_current_token(mock_request, None)
        assert token_data is None


@pytest.mark.asyncio
async def test_get_current_token_auth_disabled(mock_auth_config, mock_request):
    """Test getting the current token when auth is disabled."""
    mock_auth_config.enabled = False
    with patch("src.server.auth.dependencies.get_auth_config", return_value=mock_auth_config):
        credentials = HTTPAuthorizationCredentials(scheme="bearer", credentials="valid-token")
        token_data = await get_current_token(mock_request, credentials)
        assert token_data is None


@pytest.mark.asyncio
async def test_get_current_token_rate_limited(mock_auth_config, mock_request):
    """Test getting the current token when rate limited."""
    with patch("src.server.auth.dependencies.get_auth_config", return_value=mock_auth_config), \
         patch("src.server.auth.dependencies.rate_limiter.is_rate_limited", return_value=True):
        
        credentials = HTTPAuthorizationCredentials(scheme="bearer", credentials="valid-token")
        
        with pytest.raises(HTTPException) as excinfo:
            await get_current_token(mock_request, credentials)
        
        assert excinfo.value.status_code == status.HTTP_429_TOO_MANY_REQUESTS
        assert "Rate limit exceeded" in excinfo.value.detail


@pytest.mark.asyncio
async def test_get_current_token_invalid(mock_auth_config, mock_request):
    """Test getting the current token with invalid credentials."""
    with patch("src.server.auth.dependencies.get_auth_config", return_value=mock_auth_config), \
         patch("src.server.auth.dependencies.verify_access_token", return_value=None):
        
        credentials = HTTPAuthorizationCredentials(scheme="bearer", credentials="invalid-token")
        token_data = await get_current_token(mock_request, credentials)
        
        assert token_data is None


@pytest.mark.asyncio
async def test_get_current_user_valid(mock_auth_config, mock_token_data):
    """Test getting the current user with valid token data."""
    with patch("src.server.auth.dependencies.get_auth_config", return_value=mock_auth_config):
        user = await get_current_user(mock_token_data)
        
        assert user is not None
        assert user.user_id == "test-user"
        assert user.email == "test@example.com"
        assert user.name == "Test User"
        assert user.roles == ["admin"]
        assert user.provider == "google"


@pytest.mark.asyncio
async def test_get_current_user_no_token(mock_auth_config):
    """Test getting the current user with no token data."""
    with patch("src.server.auth.dependencies.get_auth_config", return_value=mock_auth_config):
        with pytest.raises(HTTPException) as excinfo:
            await get_current_user(None)
        
        assert excinfo.value.status_code == status.HTTP_401_UNAUTHORIZED
        assert "Not authenticated" in excinfo.value.detail


@pytest.mark.asyncio
async def test_get_current_user_auth_disabled(mock_auth_config):
    """Test getting the current user when auth is disabled."""
    mock_auth_config.enabled = False
    with patch("src.server.auth.dependencies.get_auth_config", return_value=mock_auth_config):
        user = await get_current_user(None)
        
        assert user is not None
        assert user.user_id == "test-user"
        assert user.email == "test@example.com"
        assert user.name == "Test User"
        assert user.roles == ["admin"]
        assert user.provider == "local"


@pytest.mark.asyncio
async def test_require_permission_read_admin(mock_auth_config):
    """Test requiring read permission with admin role."""
    mock_user = MagicMock()
    mock_user.roles = [Role.ADMIN]
    
    with patch("src.server.auth.dependencies.get_auth_config", return_value=mock_auth_config):
        require_read_fn = require_permission(Operation.READ)
        user = await require_read_fn(mock_user)
        
        assert user is mock_user


@pytest.mark.asyncio
async def test_require_permission_read_viewer(mock_auth_config):
    """Test requiring read permission with viewer role."""
    mock_user = MagicMock()
    mock_user.roles = [Role.VIEWER]
    
    with patch("src.server.auth.dependencies.get_auth_config", return_value=mock_auth_config):
        require_read_fn = require_permission(Operation.READ)
        user = await require_read_fn(mock_user)
        
        assert user is mock_user


@pytest.mark.asyncio
async def test_require_permission_write_admin(mock_auth_config):
    """Test requiring write permission with admin role."""
    mock_user = MagicMock()
    mock_user.roles = [Role.ADMIN]
    
    with patch("src.server.auth.dependencies.get_auth_config", return_value=mock_auth_config):
        require_write_fn = require_permission(Operation.WRITE)
        user = await require_write_fn(mock_user)
        
        assert user is mock_user


@pytest.mark.asyncio
async def test_require_permission_write_viewer(mock_auth_config):
    """Test requiring write permission with viewer role."""
    mock_user = MagicMock()
    mock_user.roles = [Role.VIEWER]
    
    with patch("src.server.auth.dependencies.get_auth_config", return_value=mock_auth_config):
        require_write_fn = require_permission(Operation.WRITE)
        
        with pytest.raises(HTTPException) as excinfo:
            await require_write_fn(mock_user)
        
        assert excinfo.value.status_code == status.HTTP_403_FORBIDDEN
        assert "Insufficient permissions" in excinfo.value.detail
        assert "Write access required" in excinfo.value.detail


@pytest.mark.asyncio
async def test_require_permission_auth_disabled(mock_auth_config):
    """Test requiring permission when auth is disabled."""
    mock_auth_config.enabled = False
    mock_user = MagicMock()
    mock_user.roles = [Role.VIEWER]
    
    with patch("src.server.auth.dependencies.get_auth_config", return_value=mock_auth_config):
        require_write_fn = require_permission(Operation.WRITE)
        user = await require_write_fn(mock_user)
        
        assert user is mock_user


@pytest.mark.asyncio
async def test_optional_auth_valid_token(mock_auth_config, mock_token_data):
    """Test optional auth with a valid token."""
    mock_func = AsyncMock()
    
    with patch("src.server.auth.dependencies.get_auth_config", return_value=mock_auth_config):
        wrapper = optional_auth(mock_func)
        await wrapper(token=mock_token_data)
        
        # Check that the function was called with the user
        mock_func.assert_called_once()
        call_kwargs = mock_func.call_args.kwargs
        assert "user" in call_kwargs
        assert call_kwargs["user"] is not None
        assert call_kwargs["user"].user_id == "test-user"
        assert call_kwargs["user"].email == "test@example.com"
        assert call_kwargs["user"].roles == ["admin"]


@pytest.mark.asyncio
async def test_optional_auth_no_token(mock_auth_config):
    """Test optional auth with no token."""
    mock_func = AsyncMock()
    
    with patch("src.server.auth.dependencies.get_auth_config", return_value=mock_auth_config):
        wrapper = optional_auth(mock_func)
        await wrapper(token=None)
        
        # Check that the function was called with user=None
        mock_func.assert_called_once()
        call_kwargs = mock_func.call_args.kwargs
        assert "user" in call_kwargs
        assert call_kwargs["user"] is None


@pytest.mark.asyncio
async def test_optional_auth_auth_disabled(mock_auth_config):
    """Test optional auth when auth is disabled."""
    mock_auth_config.enabled = False
    mock_func = AsyncMock()
    
    with patch("src.server.auth.dependencies.get_auth_config", return_value=mock_auth_config):
        wrapper = optional_auth(mock_func)
        await wrapper(token=None)
        
        # Check that the function was called with the default admin user
        mock_func.assert_called_once()
        call_kwargs = mock_func.call_args.kwargs
        assert "user" in call_kwargs
        assert call_kwargs["user"] is not None
        assert call_kwargs["user"].user_id == "test-user"
        assert call_kwargs["user"].email == "test@example.com"
        assert call_kwargs["user"].roles == ["admin"]


def test_rate_limiter():
    """Test the rate limiter functionality."""
    limiter = RateLimiter()
    
    # First request should not be rate limited
    assert not limiter.is_rate_limited("127.0.0.1", "/api/v1/resources")
    
    # Add many requests to exceed the limit
    config = MagicMock()
    config.enabled = True
    config.rate_limit = MagicMock()
    config.rate_limit.enabled = True
    config.rate_limit.max_requests = 5
    config.rate_limit.window_seconds = 3600
    
    with patch("src.server.auth.dependencies.get_auth_config", return_value=config):
        # Add 5 requests (which is the limit)
        for i in range(5):
            assert not limiter.is_rate_limited("127.0.0.1", "/api/v1/resources")
        
        # The 6th request should be rate limited
        assert limiter.is_rate_limited("127.0.0.1", "/api/v1/resources")
        
        # Different endpoint should not be rate limited
        assert not limiter.is_rate_limited("127.0.0.1", "/api/v1/tools")
        
        # Different IP should not be rate limited
        assert not limiter.is_rate_limited("192.168.1.1", "/api/v1/resources") 