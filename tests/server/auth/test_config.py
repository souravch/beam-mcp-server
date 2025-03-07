import pytest
import os
from unittest.mock import patch, MagicMock

from src.server.auth.config import get_auth_config, AuthConfig, OAuth2ProviderConfig, RateLimitConfig


@patch.dict(os.environ, {
    "MCP_AUTH_ENABLED": "false"
})
def test_get_auth_config_disabled():
    """Test getting auth config when auth is disabled."""
    config = get_auth_config()
    
    assert isinstance(config, AuthConfig)
    assert config.enabled is False
    assert config.jwt_secret_key is None
    assert config.jwt_algorithm == "HS256"
    assert config.access_token_expire_minutes == 30
    assert config.refresh_token_expire_days == 7
    assert config.oauth2_providers == {}
    assert config.allow_test_bypass is False
    assert config.test_bypass_token is None
    assert isinstance(config.rate_limit, RateLimitConfig)
    assert config.rate_limit.enabled is True
    assert config.rate_limit.max_requests == 100
    assert config.rate_limit.window_seconds == 3600


@patch.dict(os.environ, {
    "MCP_AUTH_ENABLED": "true",
    "MCP_JWT_SECRET_KEY": "test-secret-key",
    "MCP_JWT_ALGORITHM": "HS512",
    "MCP_ACCESS_TOKEN_EXPIRE_MINUTES": "60",
    "MCP_REFRESH_TOKEN_EXPIRE_DAYS": "14",
    "MCP_AUTH_ALLOW_TEST_BYPASS": "true",
    "MCP_AUTH_TEST_BYPASS_TOKEN": "test-bypass-token",
    "MCP_RATE_LIMIT_ENABLED": "false",
    "MCP_RATE_LIMIT_MAX_REQUESTS": "200",
    "MCP_RATE_LIMIT_WINDOW_SECONDS": "7200"
})
def test_get_auth_config_enabled():
    """Test getting auth config when auth is enabled with custom settings."""
    config = get_auth_config()
    
    assert isinstance(config, AuthConfig)
    assert config.enabled is True
    assert config.jwt_secret_key == "test-secret-key"
    assert config.jwt_algorithm == "HS512"
    assert config.access_token_expire_minutes == 60
    assert config.refresh_token_expire_days == 14
    assert config.allow_test_bypass is True
    assert config.test_bypass_token == "test-bypass-token"
    assert isinstance(config.rate_limit, RateLimitConfig)
    assert config.rate_limit.enabled is False
    assert config.rate_limit.max_requests == 200
    assert config.rate_limit.window_seconds == 7200


@patch.dict(os.environ, {
    "MCP_AUTH_ENABLED": "true",
    "MCP_JWT_SECRET_KEY": "test-secret-key",
    "MCP_OAUTH2_GOOGLE_CLIENT_ID": "google-client-id",
    "MCP_OAUTH2_GOOGLE_CLIENT_SECRET": "google-client-secret",
    "MCP_OAUTH2_GITHUB_CLIENT_ID": "github-client-id",
    "MCP_OAUTH2_GITHUB_CLIENT_SECRET": "github-client-secret"
})
def test_get_auth_config_with_oauth_providers():
    """Test getting auth config with OAuth providers configured."""
    config = get_auth_config()
    
    assert isinstance(config, AuthConfig)
    assert config.enabled is True
    assert "google" in config.oauth2_providers
    assert "github" in config.oauth2_providers
    
    # Check Google provider
    google = config.oauth2_providers["google"]
    assert isinstance(google, OAuth2ProviderConfig)
    assert google.client_id == "google-client-id"
    assert google.client_secret == "google-client-secret"
    assert google.authorize_url == "https://accounts.google.com/o/oauth2/auth"
    assert google.token_url == "https://oauth2.googleapis.com/token"
    assert google.userinfo_url == "https://www.googleapis.com/oauth2/v1/userinfo"
    assert google.scope == ["openid", "email", "profile"]
    
    # Check GitHub provider
    github = config.oauth2_providers["github"]
    assert isinstance(github, OAuth2ProviderConfig)
    assert github.client_id == "github-client-id"
    assert github.client_secret == "github-client-secret"
    assert github.authorize_url == "https://github.com/login/oauth/authorize"
    assert github.token_url == "https://github.com/login/oauth/access_token"
    assert github.userinfo_url == "https://api.github.com/user"
    assert github.scope == ["user:email"]


def test_oauth2_provider_config():
    """Test OAuth2ProviderConfig model."""
    provider = OAuth2ProviderConfig(
        client_id="test-client-id",
        client_secret="test-client-secret",
        authorize_url="https://example.com/auth",
        token_url="https://example.com/token",
        userinfo_url="https://example.com/userinfo",
        scope=["profile", "email"]
    )
    
    assert provider.client_id == "test-client-id"
    assert provider.client_secret == "test-client-secret"
    assert provider.authorize_url == "https://example.com/auth"
    assert provider.token_url == "https://example.com/token"
    assert provider.userinfo_url == "https://example.com/userinfo"
    assert provider.scope == ["profile", "email"]


def test_rate_limit_config():
    """Test RateLimitConfig model."""
    rate_limit = RateLimitConfig(
        enabled=False,
        max_requests=50,
        window_seconds=1800
    )
    
    assert rate_limit.enabled is False
    assert rate_limit.max_requests == 50
    assert rate_limit.window_seconds == 1800


def test_auth_config():
    """Test AuthConfig model."""
    rate_limit = RateLimitConfig(enabled=False, max_requests=50, window_seconds=1800)
    google_provider = OAuth2ProviderConfig(
        client_id="google-id",
        client_secret="google-secret",
        authorize_url="https://google.com/auth",
        token_url="https://google.com/token",
        userinfo_url="https://google.com/userinfo"
    )
    
    config = AuthConfig(
        enabled=True,
        jwt_secret_key="test-key",
        jwt_algorithm="HS384",
        access_token_expire_minutes=45,
        refresh_token_expire_days=10,
        oauth2_providers={"google": google_provider},
        allow_test_bypass=True,
        test_bypass_token="bypass-token",
        rate_limit=rate_limit
    )
    
    assert config.enabled is True
    assert config.jwt_secret_key == "test-key"
    assert config.jwt_algorithm == "HS384"
    assert config.access_token_expire_minutes == 45
    assert config.refresh_token_expire_days == 10
    assert len(config.oauth2_providers) == 1
    assert "google" in config.oauth2_providers
    assert config.oauth2_providers["google"] == google_provider
    assert config.allow_test_bypass is True
    assert config.test_bypass_token == "bypass-token"
    assert config.rate_limit == rate_limit 