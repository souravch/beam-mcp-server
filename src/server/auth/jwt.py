from datetime import datetime, timedelta, UTC
from typing import Optional, Dict, Any
from jose import jwt, JWTError
from pydantic import BaseModel, EmailStr
from uuid import uuid4
import secrets

from .config import get_auth_config


class TokenData(BaseModel):
    """Data contained in a JWT token."""
    sub: str  # Subject (user ID)
    email: Optional[EmailStr] = None
    name: Optional[str] = None
    roles: list[str] = []
    jti: str  # JWT ID (unique identifier for this token)
    provider: str  # OAuth provider (google, github, etc.)


class TokenPair(BaseModel):
    """A pair of access and refresh tokens."""
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    expires_in: int  # Seconds until expiration


def create_tokens(
    user_id: str,
    email: Optional[str] = None,
    name: Optional[str] = None,
    roles: list[str] = [],
    provider: str = "unknown"
) -> TokenPair:
    """
    Create a new JWT access token and refresh token pair.
    
    Args:
        user_id: The unique user identifier
        email: User's email address
        name: User's display name
        roles: List of roles assigned to the user
        provider: The OAuth provider used for authentication
        
    Returns:
        TokenPair containing access and refresh tokens
    """
    auth_config = get_auth_config()
    
    # Ensure auth is enabled and JWT key is set
    if not auth_config.enabled or not auth_config.jwt_secret_key:
        raise ValueError("Authentication is not properly configured")
    
    # Create unique token identifiers
    access_jti = str(uuid4())
    refresh_jti = str(uuid4())
    
    # Create the JWT payload
    now = datetime.now(UTC)
    
    # Access token payload (shorter expiry)
    access_expires = now + timedelta(minutes=auth_config.access_token_expire_minutes)
    access_payload = {
        "sub": user_id,
        "email": email,
        "name": name,
        "roles": roles,
        "jti": access_jti,
        "provider": provider,
        "type": "access",
        "exp": access_expires,
        "iat": now,
    }
    
    # Refresh token payload (longer expiry)
    refresh_expires = now + timedelta(days=auth_config.refresh_token_expire_days)
    refresh_payload = {
        "sub": user_id,
        "jti": refresh_jti,
        "type": "refresh",
        "exp": refresh_expires,
        "iat": now,
    }
    
    # Encode the tokens
    access_token = jwt.encode(
        access_payload, 
        auth_config.jwt_secret_key, 
        algorithm=auth_config.jwt_algorithm
    )
    
    refresh_token = jwt.encode(
        refresh_payload, 
        auth_config.jwt_secret_key, 
        algorithm=auth_config.jwt_algorithm
    )
    
    return TokenPair(
        access_token=access_token,
        refresh_token=refresh_token,
        expires_in=auth_config.access_token_expire_minutes * 60
    )


def verify_access_token(token: str) -> Optional[TokenData]:
    """
    Verify a JWT access token and extract the token data.
    
    Args:
        token: The JWT token to verify
        
    Returns:
        TokenData if valid, None otherwise
    """
    auth_config = get_auth_config()
    
    # Skip verification if auth is disabled
    if not auth_config.enabled:
        return None
    
    # Handle test bypass token
    if auth_config.allow_test_bypass and auth_config.test_bypass_token and token == auth_config.test_bypass_token:
        return TokenData(
            sub="test-user",
            email="test@example.com",
            name="Test User",
            roles=["admin"],
            jti=str(uuid4()),
            provider="test"
        )
    
    try:
        payload = jwt.decode(
            token, 
            auth_config.jwt_secret_key, 
            algorithms=[auth_config.jwt_algorithm]
        )
        
        # Ensure this is an access token
        if payload.get("type") != "access":
            return None
        
        token_data = TokenData(
            sub=payload["sub"],
            email=payload.get("email"),
            name=payload.get("name"),
            roles=payload.get("roles", []),
            jti=payload["jti"],
            provider=payload.get("provider", "unknown")
        )
        
        return token_data
        
    except JWTError:
        return None


def verify_refresh_token(token: str) -> Optional[str]:
    """
    Verify a JWT refresh token and extract the user ID.
    
    Args:
        token: The refresh token to verify
        
    Returns:
        User ID if valid, None otherwise
    """
    auth_config = get_auth_config()
    
    # Skip verification if auth is disabled
    if not auth_config.enabled:
        return None
    
    try:
        payload = jwt.decode(
            token, 
            auth_config.jwt_secret_key, 
            algorithms=[auth_config.jwt_algorithm]
        )
        
        # Ensure this is a refresh token
        if payload.get("type") != "refresh":
            return None
        
        # Return the user ID
        return payload["sub"]
        
    except JWTError:
        return None 