import pytest
import os
from datetime import datetime, timedelta
from jose import jwt
from unittest.mock import patch, MagicMock

from src.server.auth.jwt import create_tokens, verify_access_token, verify_refresh_token, TokenData
from src.server.auth.config import AuthConfig, OAuth2ProviderConfig


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
    
    return config


@pytest.mark.parametrize("user_id,email,name,roles,provider", [
    ("test-user", "test@example.com", "Test User", ["admin"], "google"),
    ("another-user", None, None, ["viewer"], "github"),
    ("third-user", "third@example.com", "Third User", ["admin", "viewer"], "unknown"),
])
def test_create_tokens(mock_auth_config, user_id, email, name, roles, provider):
    """Test creating JWT token pairs."""
    with patch("src.server.auth.jwt.get_auth_config", return_value=mock_auth_config):
        # Create tokens
        tokens = create_tokens(user_id, email, name, roles, provider)
        
        # Verify token structure
        assert tokens.access_token is not None
        assert tokens.refresh_token is not None
        assert tokens.token_type == "bearer"
        assert tokens.expires_in == mock_auth_config.access_token_expire_minutes * 60
        
        # Decode access token to verify contents
        payload = jwt.decode(
            tokens.access_token, 
            mock_auth_config.jwt_secret_key, 
            algorithms=[mock_auth_config.jwt_algorithm]
        )
        
        assert payload["sub"] == user_id
        assert payload["email"] == email
        assert payload["name"] == name
        assert payload["roles"] == roles
        assert payload["provider"] == provider
        assert payload["type"] == "access"
        assert "jti" in payload
        assert "exp" in payload
        assert "iat" in payload
        
        # Decode refresh token to verify contents
        payload = jwt.decode(
            tokens.refresh_token, 
            mock_auth_config.jwt_secret_key, 
            algorithms=[mock_auth_config.jwt_algorithm]
        )
        
        assert payload["sub"] == user_id
        assert payload["type"] == "refresh"
        assert "jti" in payload
        assert "exp" in payload
        assert "iat" in payload


def test_create_tokens_auth_disabled(mock_auth_config):
    """Test creating tokens when auth is disabled."""
    mock_auth_config.enabled = False
    with patch("src.server.auth.jwt.get_auth_config", return_value=mock_auth_config):
        with pytest.raises(ValueError, match="Authentication is not properly configured"):
            create_tokens("test-user", "test@example.com", "Test User", ["admin"], "google")


def test_create_tokens_no_secret_key(mock_auth_config):
    """Test creating tokens when no secret key is set."""
    mock_auth_config.jwt_secret_key = None
    with patch("src.server.auth.jwt.get_auth_config", return_value=mock_auth_config):
        with pytest.raises(ValueError, match="Authentication is not properly configured"):
            create_tokens("test-user", "test@example.com", "Test User", ["admin"], "google")


def test_verify_access_token(mock_auth_config):
    """Test verifying access tokens."""
    with patch("src.server.auth.jwt.get_auth_config", return_value=mock_auth_config):
        # Create tokens
        tokens = create_tokens(
            "test-user", 
            "test@example.com", 
            "Test User", 
            ["admin"], 
            "google"
        )
        
        # Verify access token
        token_data = verify_access_token(tokens.access_token)
        
        assert token_data is not None
        assert token_data.sub == "test-user"
        assert token_data.email == "test@example.com"
        assert token_data.name == "Test User"
        assert token_data.roles == ["admin"]
        assert token_data.provider == "google"
        assert token_data.jti is not None


def test_verify_access_token_auth_disabled(mock_auth_config):
    """Test verifying access tokens when auth is disabled."""
    mock_auth_config.enabled = False
    with patch("src.server.auth.jwt.get_auth_config", return_value=mock_auth_config):
        token_data = verify_access_token("invalid-token")
        assert token_data is None


def test_verify_access_token_invalid(mock_auth_config):
    """Test verifying an invalid access token."""
    with patch("src.server.auth.jwt.get_auth_config", return_value=mock_auth_config):
        token_data = verify_access_token("invalid-token")
        assert token_data is None


def test_verify_access_token_test_bypass(mock_auth_config):
    """Test verifying a test bypass token."""
    with patch("src.server.auth.jwt.get_auth_config", return_value=mock_auth_config):
        token_data = verify_access_token("test-bypass-token")
        
        assert token_data is not None
        assert token_data.sub == "test-user"
        assert token_data.email == "test@example.com"
        assert token_data.name == "Test User"
        assert token_data.roles == ["admin"]
        assert token_data.provider == "test"
        assert token_data.jti is not None


def test_verify_refresh_token(mock_auth_config):
    """Test verifying refresh tokens."""
    with patch("src.server.auth.jwt.get_auth_config", return_value=mock_auth_config):
        # Create tokens
        tokens = create_tokens(
            "test-user", 
            "test@example.com", 
            "Test User", 
            ["admin"], 
            "google"
        )
        
        # Verify refresh token
        user_id = verify_refresh_token(tokens.refresh_token)
        
        assert user_id == "test-user"


def test_verify_refresh_token_auth_disabled(mock_auth_config):
    """Test verifying refresh tokens when auth is disabled."""
    mock_auth_config.enabled = False
    with patch("src.server.auth.jwt.get_auth_config", return_value=mock_auth_config):
        user_id = verify_refresh_token("invalid-token")
        assert user_id is None


def test_verify_refresh_token_invalid(mock_auth_config):
    """Test verifying an invalid refresh token."""
    with patch("src.server.auth.jwt.get_auth_config", return_value=mock_auth_config):
        user_id = verify_refresh_token("invalid-token")
        assert user_id is None


def test_verify_refresh_token_wrong_type(mock_auth_config):
    """Test verifying a token of the wrong type."""
    with patch("src.server.auth.jwt.get_auth_config", return_value=mock_auth_config):
        # Create tokens
        tokens = create_tokens(
            "test-user", 
            "test@example.com", 
            "Test User", 
            ["admin"], 
            "google"
        )
        
        # Try to verify access token as refresh token
        user_id = verify_refresh_token(tokens.access_token)
        
        assert user_id is None 