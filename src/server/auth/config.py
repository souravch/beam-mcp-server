from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
import os
from functools import lru_cache

class OAuth2ProviderConfig(BaseModel):
    """Configuration for an OAuth2 provider."""
    client_id: str
    client_secret: str
    authorize_url: str
    token_url: str
    userinfo_url: str
    scope: List[str] = ["openid", "email", "profile"]
    
class RateLimitConfig(BaseModel):
    """Configuration for rate limiting."""
    enabled: bool = True
    max_requests: int = 100
    window_seconds: int = 3600  # 1 hour

class AuthConfig(BaseModel):
    """Configuration for authentication."""
    enabled: bool = Field(
        default=False,
        description="Whether authentication is enabled. Default is False."
    )
    
    # JWT settings
    jwt_secret_key: Optional[str] = None
    jwt_algorithm: str = "HS256"
    access_token_expire_minutes: int = 30
    refresh_token_expire_days: int = 7
    
    # OAuth2 providers
    oauth2_providers: Dict[str, OAuth2ProviderConfig] = {}
    
    # Testing bypass
    allow_test_bypass: bool = False
    test_bypass_token: Optional[str] = None
    
    # Rate limiting
    rate_limit: RateLimitConfig = RateLimitConfig()

@lru_cache()
def get_auth_config() -> AuthConfig:
    """Get the authentication configuration from environment variables."""
    
    # Check if auth is enabled
    auth_enabled = os.getenv("MCP_AUTH_ENABLED", "false").lower() == "true"
    
    # Only load other config if auth is enabled
    if auth_enabled:
        jwt_secret_key = os.getenv("MCP_JWT_SECRET_KEY")
        jwt_algorithm = os.getenv("MCP_JWT_ALGORITHM", "HS256")
        access_token_expire_minutes = int(os.getenv("MCP_ACCESS_TOKEN_EXPIRE_MINUTES", "30"))
        refresh_token_expire_days = int(os.getenv("MCP_REFRESH_TOKEN_EXPIRE_DAYS", "7"))
        
        # OAuth2 providers
        oauth2_providers = {}
        
        # Google provider
        if os.getenv("MCP_OAUTH2_GOOGLE_CLIENT_ID"):
            oauth2_providers["google"] = OAuth2ProviderConfig(
                client_id=os.getenv("MCP_OAUTH2_GOOGLE_CLIENT_ID", ""),
                client_secret=os.getenv("MCP_OAUTH2_GOOGLE_CLIENT_SECRET", ""),
                authorize_url="https://accounts.google.com/o/oauth2/auth",
                token_url="https://oauth2.googleapis.com/token",
                userinfo_url="https://www.googleapis.com/oauth2/v1/userinfo",
                scope=["openid", "email", "profile"]
            )
            
        # GitHub provider
        if os.getenv("MCP_OAUTH2_GITHUB_CLIENT_ID"):
            oauth2_providers["github"] = OAuth2ProviderConfig(
                client_id=os.getenv("MCP_OAUTH2_GITHUB_CLIENT_ID", ""),
                client_secret=os.getenv("MCP_OAUTH2_GITHUB_CLIENT_SECRET", ""),
                authorize_url="https://github.com/login/oauth/authorize",
                token_url="https://github.com/login/oauth/access_token",
                userinfo_url="https://api.github.com/user",
                scope=["user:email"]
            )
        
        # Testing bypass settings
        allow_test_bypass = os.getenv("MCP_AUTH_ALLOW_TEST_BYPASS", "false").lower() == "true"
        test_bypass_token = os.getenv("MCP_AUTH_TEST_BYPASS_TOKEN")
        
        # Rate limiting
        rate_limit_enabled = os.getenv("MCP_RATE_LIMIT_ENABLED", "true").lower() == "true"
        rate_limit_max = int(os.getenv("MCP_RATE_LIMIT_MAX_REQUESTS", "100"))
        rate_limit_window = int(os.getenv("MCP_RATE_LIMIT_WINDOW_SECONDS", "3600"))
        
        rate_limit = RateLimitConfig(
            enabled=rate_limit_enabled,
            max_requests=rate_limit_max,
            window_seconds=rate_limit_window
        )
        
        return AuthConfig(
            enabled=auth_enabled,
            jwt_secret_key=jwt_secret_key,
            jwt_algorithm=jwt_algorithm,
            access_token_expire_minutes=access_token_expire_minutes,
            refresh_token_expire_days=refresh_token_expire_days,
            oauth2_providers=oauth2_providers,
            allow_test_bypass=allow_test_bypass,
            test_bypass_token=test_bypass_token,
            rate_limit=rate_limit
        )
    
    # Return default disabled config
    return AuthConfig(enabled=False) 