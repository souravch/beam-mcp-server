import pytest
import json
from unittest.mock import patch, MagicMock, AsyncMock

from src.server.auth.oauth2 import (
    get_oauth2_login_url,
    exchange_code_for_token,
    get_user_info,
    OAuth2Error,
    _get_provider_user_id,
    _get_user_email,
    _get_user_name
)
from src.server.auth.config import OAuth2ProviderConfig


@pytest.fixture
def mock_auth_config():
    """Mock the auth config for testing."""
    config = MagicMock()
    config.enabled = True
    
    # Set up OAuth providers
    google_provider = OAuth2ProviderConfig(
        client_id="google-client-id",
        client_secret="google-client-secret",
        authorize_url="https://accounts.google.com/o/oauth2/auth",
        token_url="https://oauth2.googleapis.com/token",
        userinfo_url="https://www.googleapis.com/oauth2/v1/userinfo",
        scope=["openid", "email", "profile"]
    )
    
    github_provider = OAuth2ProviderConfig(
        client_id="github-client-id",
        client_secret="github-client-secret",
        authorize_url="https://github.com/login/oauth/authorize",
        token_url="https://github.com/login/oauth/access_token",
        userinfo_url="https://api.github.com/user",
        scope=["user:email"]
    )
    
    config.oauth2_providers = {
        "google": google_provider,
        "github": github_provider
    }
    
    return config


@pytest.mark.asyncio
async def test_get_oauth2_login_url_google(mock_auth_config):
    """Test getting OAuth2 login URL for Google."""
    with patch("src.server.auth.oauth2.get_auth_config", return_value=mock_auth_config):
        url = await get_oauth2_login_url(
            "google", 
            "http://localhost:8888/callback", 
            "test-state"
        )
        
        assert "https://accounts.google.com/o/oauth2/auth" in url
        assert "client_id=google-client-id" in url
        assert "redirect_uri=http%3A%2F%2Flocalhost%3A8888%2Fcallback" in url
        assert "scope=openid+email+profile" in url
        assert "response_type=code" in url
        assert "state=test-state" in url


@pytest.mark.asyncio
async def test_get_oauth2_login_url_github(mock_auth_config):
    """Test getting OAuth2 login URL for GitHub."""
    with patch("src.server.auth.oauth2.get_auth_config", return_value=mock_auth_config):
        url = await get_oauth2_login_url(
            "github", 
            "http://localhost:8888/callback", 
            "test-state"
        )
        
        assert "https://github.com/login/oauth/authorize" in url
        assert "client_id=github-client-id" in url
        assert "redirect_uri=http%3A%2F%2Flocalhost%3A8888%2Fcallback" in url
        assert "scope=user%3Aemail" in url
        assert "response_type=code" in url
        assert "state=test-state" in url


@pytest.mark.asyncio
async def test_get_oauth2_login_url_auth_disabled(mock_auth_config):
    """Test getting OAuth2 login URL when auth is disabled."""
    mock_auth_config.enabled = False
    with patch("src.server.auth.oauth2.get_auth_config", return_value=mock_auth_config):
        with pytest.raises(OAuth2Error, match="Authentication is not enabled"):
            await get_oauth2_login_url(
                "google", 
                "http://localhost:8888/callback", 
                "test-state"
            )


@pytest.mark.asyncio
async def test_get_oauth2_login_url_unknown_provider(mock_auth_config):
    """Test getting OAuth2 login URL for an unknown provider."""
    with patch("src.server.auth.oauth2.get_auth_config", return_value=mock_auth_config):
        with pytest.raises(OAuth2Error, match="Unknown OAuth2 provider: unknown"):
            await get_oauth2_login_url(
                "unknown", 
                "http://localhost:8888/callback", 
                "test-state"
            )


@pytest.mark.asyncio
async def test_exchange_code_for_token_google(mock_auth_config):
    """Test exchanging authorization code for token with Google."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "access_token": "google-access-token",
        "token_type": "Bearer",
        "expires_in": 3600,
        "refresh_token": "google-refresh-token",
        "id_token": "google-id-token"
    }
    
    mock_client = AsyncMock()
    mock_client.__aenter__.return_value.post.return_value = mock_response
    
    with patch("src.server.auth.oauth2.get_auth_config", return_value=mock_auth_config), \
         patch("src.server.auth.oauth2.httpx.AsyncClient", return_value=mock_client):
        
        result = await exchange_code_for_token(
            "google", 
            "auth-code", 
            "http://localhost:8888/callback"
        )
        
        # Check result
        assert result == mock_response.json.return_value
        
        # Check request was made correctly
        mock_client.__aenter__.return_value.post.assert_called_once()
        call_args = mock_client.__aenter__.return_value.post.call_args
        
        assert call_args[0][0] == "https://oauth2.googleapis.com/token"
        assert call_args[1]["data"] == {
            "client_id": "google-client-id",
            "client_secret": "google-client-secret",
            "code": "auth-code",
            "redirect_uri": "http://localhost:8888/callback",
            "grant_type": "authorization_code"
        }
        assert call_args[1]["json"] is None
        assert call_args[1]["headers"] == {"Accept": "application/json"}


@pytest.mark.asyncio
async def test_exchange_code_for_token_github(mock_auth_config):
    """Test exchanging authorization code for token with GitHub."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "access_token": "github-access-token",
        "token_type": "bearer",
        "scope": "user:email"
    }
    
    mock_client = AsyncMock()
    mock_client.__aenter__.return_value.post.return_value = mock_response
    
    with patch("src.server.auth.oauth2.get_auth_config", return_value=mock_auth_config), \
         patch("src.server.auth.oauth2.httpx.AsyncClient", return_value=mock_client):
        
        result = await exchange_code_for_token(
            "github", 
            "auth-code", 
            "http://localhost:8888/callback"
        )
        
        # Check result
        assert result == mock_response.json.return_value
        
        # Check request was made correctly
        mock_client.__aenter__.return_value.post.assert_called_once()
        call_args = mock_client.__aenter__.return_value.post.call_args
        
        assert call_args[0][0] == "https://github.com/login/oauth/access_token"
        assert call_args[1]["data"] is None
        assert call_args[1]["json"] == {
            "client_id": "github-client-id",
            "client_secret": "github-client-secret",
            "code": "auth-code",
            "redirect_uri": "http://localhost:8888/callback",
            "grant_type": "authorization_code"
        }
        assert call_args[1]["headers"] == {"Accept": "application/json"}


@pytest.mark.asyncio
async def test_exchange_code_for_token_error(mock_auth_config):
    """Test error when exchanging authorization code for token."""
    mock_response = MagicMock()
    mock_response.status_code = 400
    mock_response.text = "Bad Request"
    
    mock_client = AsyncMock()
    mock_client.__aenter__.return_value.post.return_value = mock_response
    
    with patch("src.server.auth.oauth2.get_auth_config", return_value=mock_auth_config), \
         patch("src.server.auth.oauth2.httpx.AsyncClient", return_value=mock_client):
        
        with pytest.raises(OAuth2Error, match="Failed to exchange code for token: Bad Request"):
            await exchange_code_for_token(
                "google", 
                "invalid-code", 
                "http://localhost:8888/callback"
            )


@pytest.mark.asyncio
async def test_get_user_info_google(mock_auth_config):
    """Test getting user info from Google."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "id": "123456789",
        "email": "test@example.com",
        "verified_email": True,
        "name": "Test User",
        "given_name": "Test",
        "family_name": "User",
        "picture": "https://example.com/photo.jpg",
        "locale": "en"
    }
    
    mock_client = AsyncMock()
    mock_client.__aenter__.return_value.get.return_value = mock_response
    
    with patch("src.server.auth.oauth2.get_auth_config", return_value=mock_auth_config), \
         patch("src.server.auth.oauth2.httpx.AsyncClient", return_value=mock_client):
        
        result = await get_user_info("google", "google-access-token")
        
        # Check result
        assert result["provider_user_id"] == "123456789"
        assert result["email"] == "test@example.com"
        assert result["name"] == "Test User"
        assert result["raw"] == mock_response.json.return_value
        
        # Check request was made correctly
        mock_client.__aenter__.return_value.get.assert_called_once()
        call_args = mock_client.__aenter__.return_value.get.call_args
        
        assert call_args[0][0] == "https://www.googleapis.com/oauth2/v1/userinfo"
        assert call_args[1]["headers"] == {
            "Authorization": "Bearer google-access-token",
            "Accept": "application/json"
        }


@pytest.mark.asyncio
async def test_get_user_info_github(mock_auth_config):
    """Test getting user info from GitHub."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "login": "testuser",
        "id": 123456,
        "node_id": "abc123",
        "avatar_url": "https://github.com/images/photo.jpg",
        "url": "https://api.github.com/users/testuser",
        "html_url": "https://github.com/testuser",
        "name": "Test User",
        "email": "test@example.com"
    }
    
    mock_client = AsyncMock()
    mock_client.__aenter__.return_value.get.return_value = mock_response
    
    with patch("src.server.auth.oauth2.get_auth_config", return_value=mock_auth_config), \
         patch("src.server.auth.oauth2.httpx.AsyncClient", return_value=mock_client):
        
        result = await get_user_info("github", "github-access-token")
        
        # Check result
        assert result["provider_user_id"] == "123456"
        assert result["email"] == "test@example.com"
        assert result["name"] == "Test User"
        assert result["raw"] == mock_response.json.return_value
        
        # Check request was made correctly
        mock_client.__aenter__.return_value.get.assert_called_once()
        call_args = mock_client.__aenter__.return_value.get.call_args
        
        assert call_args[0][0] == "https://api.github.com/user"
        assert call_args[1]["headers"] == {
            "Authorization": "Bearer github-access-token",
            "Accept": "application/json"
        }


@pytest.mark.asyncio
async def test_get_user_info_github_no_email(mock_auth_config):
    """Test getting user info from GitHub when email is not in profile."""
    # First response for user profile without email
    user_response = MagicMock()
    user_response.status_code = 200
    user_response.json.return_value = {
        "login": "testuser",
        "id": 123456,
        "name": "Test User",
        "email": None
    }
    
    # Second response for emails endpoint
    emails_response = MagicMock()
    emails_response.status_code = 200
    emails_response.json.return_value = [
        {
            "email": "private@example.com",
            "primary": True,
            "verified": True,
            "visibility": "private"
        },
        {
            "email": "public@example.com",
            "primary": False,
            "verified": True,
            "visibility": "public"
        }
    ]
    
    mock_client = AsyncMock()
    mock_client.__aenter__.return_value.get.side_effect = [user_response, emails_response]
    
    with patch("src.server.auth.oauth2.get_auth_config", return_value=mock_auth_config), \
         patch("src.server.auth.oauth2.httpx.AsyncClient", return_value=mock_client):
        
        result = await get_user_info("github", "github-access-token")
        
        # Check result
        assert result["provider_user_id"] == "123456"
        assert result["email"] == "private@example.com"  # Should get primary email
        assert result["name"] == "Test User"
        assert result["raw"] == user_response.json.return_value
        
        # Check both requests were made correctly
        assert mock_client.__aenter__.return_value.get.call_count == 2
        
        # First call to get user profile
        first_call = mock_client.__aenter__.return_value.get.call_args_list[0]
        assert first_call[0][0] == "https://api.github.com/user"
        
        # Second call to get emails
        second_call = mock_client.__aenter__.return_value.get.call_args_list[1]
        assert second_call[0][0] == "https://api.github.com/user/emails"


def test_get_provider_user_id():
    """Test extracting provider user ID from user info."""
    # Google
    google_user = {"id": "123456789", "email": "test@example.com"}
    assert _get_provider_user_id("google", google_user) == "123456789"
    
    # GitHub
    github_user = {"id": 987654321, "login": "testuser"}
    assert _get_provider_user_id("github", github_user) == "987654321"
    
    # Unknown provider
    unknown_user = {"user_id": "abc123"}
    assert _get_provider_user_id("unknown", unknown_user) == ""


def test_get_user_email():
    """Test extracting user email from user info."""
    # Google
    google_user = {"id": "123456789", "email": "google@example.com"}
    assert _get_user_email("google", google_user) == "google@example.com"
    
    # GitHub
    github_user = {"id": 987654321, "email": "github@example.com"}
    assert _get_user_email("github", github_user) == "github@example.com"
    
    # No email
    no_email_user = {"id": "123456789"}
    assert _get_user_email("google", no_email_user) is None


def test_get_user_name():
    """Test extracting user name from user info."""
    # Google
    google_user = {"id": "123456789", "name": "Google User"}
    assert _get_user_name("google", google_user) == "Google User"
    
    # GitHub with name
    github_user_with_name = {"id": 987654321, "name": "GitHub User", "login": "githubuser"}
    assert _get_user_name("github", github_user_with_name) == "GitHub User"
    
    # GitHub without name, using login
    github_user_no_name = {"id": 987654321, "login": "githubuser"}
    assert _get_user_name("github", github_user_no_name) == "githubuser"
    
    # No name or login
    no_name_user = {"id": "123456789"}
    assert _get_user_name("google", no_name_user) == "" 