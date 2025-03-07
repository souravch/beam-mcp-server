from fastapi import Depends, HTTPException, status, Request
from fastapi.security import OAuth2AuthorizationCodeBearer, HTTPBearer, HTTPAuthorizationCredentials
from typing import Optional, List, Callable, Dict, Any
import time
from datetime import datetime, UTC
from enum import Enum

from .jwt import verify_access_token, TokenData
from .config import get_auth_config
from .models import UserSession, Role


# Security schemes
security = HTTPBearer(auto_error=False)


class Operation(str, Enum):
    """Operation types for authorization."""
    READ = "read"
    WRITE = "write"


class RateLimiter:
    """Simple in-memory rate limiter for API endpoints."""
    
    def __init__(self):
        self.requests = {}  # {ip: [(timestamp, endpoint), ...]}
    
    def is_rate_limited(self, ip: str, endpoint: str) -> bool:
        """Check if a request is rate limited."""
        auth_config = get_auth_config()
        
        # Skip if rate limiting is disabled
        if not auth_config.enabled or not auth_config.rate_limit.enabled:
            return False
        
        now = time.time()
        window = auth_config.rate_limit.window_seconds
        max_requests = auth_config.rate_limit.max_requests
        
        # Clean up old requests
        if ip in self.requests:
            self.requests[ip] = [
                (ts, ep) for ts, ep in self.requests[ip] 
                if now - ts < window
            ]
        
        # Count requests for this endpoint
        if ip in self.requests:
            endpoint_count = sum(1 for ts, ep in self.requests[ip] if ep == endpoint)
            if endpoint_count >= max_requests:
                return True
        
        # Add this request
        if ip not in self.requests:
            self.requests[ip] = []
        self.requests[ip].append((now, endpoint))
        
        return False


# Create a global rate limiter
rate_limiter = RateLimiter()


async def get_current_token(
    request: Request,
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security)
) -> Optional[TokenData]:
    """Get and validate the current token from the request."""
    auth_config = get_auth_config()
    
    # Skip auth if disabled
    if not auth_config.enabled:
        return None
    
    # Check for rate limiting
    client_ip = request.client.host if request.client else "unknown"
    endpoint = request.url.path
    
    if rate_limiter.is_rate_limited(client_ip, endpoint):
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Rate limit exceeded. Please try again later."
        )
    
    # Continue with auth if no credentials, return None
    if not credentials:
        return None
    
    token = credentials.credentials
    token_data = verify_access_token(token)
    
    if not token_data:
        return None
    
    return token_data


async def get_current_user(token: Optional[TokenData] = Depends(get_current_token)) -> UserSession:
    """
    Get the current authenticated user.
    
    Returns a UserSession object if authenticated, or raises a 401 exception if not.
    """
    auth_config = get_auth_config()
    
    # Skip auth if disabled
    if not auth_config.enabled:
        # Return a default test user with admin role
        return UserSession(
            user_id="test-user",
            provider_user_id="test-user-id",
            provider="local",
            email="test@example.com",
            name="Test User",
            roles=[Role.ADMIN],
            metadata={},
            last_login=datetime.now(UTC)
        )
    
    if not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    # Convert token data to user session
    user = UserSession(
        user_id=token.sub,
        provider_user_id=token.sub,
        provider=token.provider,
        email=token.email,
        name=token.name,
        roles=token.roles,
        metadata={},
        last_login=datetime.now(UTC)
    )
    
    return user


def require_permission(operation: Operation):
    """
    Create a dependency that requires the user to have permission for an operation.
    
    Args:
        operation: The operation (READ or WRITE)
    
    Logic:
        - ADMIN role can perform any operation (READ or WRITE)
        - VIEWER role can only perform READ operations
    """
    
    async def _require_permission(
        user: UserSession = Depends(get_current_user),
    ) -> UserSession:
        auth_config = get_auth_config()
        
        # Skip auth if disabled
        if not auth_config.enabled:
            return user
        
        # Check operation permissions
        if operation == Operation.READ:
            # Any authenticated user can read (both ADMIN and VIEWER)
            return user
        elif operation == Operation.WRITE:
            # Only ADMIN can write
            if Role.ADMIN in user.roles:
                return user
            else:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Insufficient permissions. Write access required."
                )
        else:
            # Invalid operation
            raise ValueError(f"Invalid operation: {operation}")
        
        return user
    
    return _require_permission


# Convenience functions for operation permissions
require_read = require_permission(Operation.READ)
require_write = require_permission(Operation.WRITE)


def optional_auth(func: Callable) -> Callable:
    """
    Decorator for endpoints that can work with or without authentication.
    
    If authentication is provided and valid, the user will be passed to the endpoint.
    If no authentication is provided or it's invalid, None will be passed.
    """
    
    async def wrapper(
        token: Optional[TokenData] = Depends(get_current_token),
        *args, **kwargs
    ):
        auth_config = get_auth_config()
        
        # Skip auth if disabled
        if not auth_config.enabled:
            # Return a default test user with admin role
            user = UserSession(
                user_id="test-user",
                provider_user_id="test-user-id",
                provider="local",
                email="test@example.com",
                name="Test User",
                roles=[Role.ADMIN],
                metadata={},
                last_login=datetime.now(UTC)
            )
            return await func(user=user, *args, **kwargs)
        
        if not token:
            return await func(user=None, *args, **kwargs)
        
        # Convert token data to user session
        user = UserSession(
            user_id=token.sub,
            provider_user_id=token.sub,
            provider=token.provider,
            email=token.email,
            name=token.name,
            roles=token.roles,
            metadata={},
            last_login=datetime.now(UTC)
        )
        
        return await func(user=user, *args, **kwargs)
    
    return wrapper 