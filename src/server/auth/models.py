from pydantic import BaseModel, EmailStr, Field
from typing import Optional, List, Dict, Any
from datetime import datetime
from enum import Enum


class Role(str, Enum):
    """
    User roles for role-based access control.
    
    Just two roles:
    - ADMIN: Has read/write access to everything
    - VIEWER: Has read-only access to everything
    """
    ADMIN = "admin"
    VIEWER = "viewer"


class UserBase(BaseModel):
    """Base model for user data."""
    email: EmailStr
    name: Optional[str] = None
    roles: List[str] = [Role.VIEWER]  # Default to viewer role
    

class UserInfo(UserBase):
    """User information returned to clients."""
    user_id: str
    provider: str  # OAuth provider
    created_at: datetime
    last_login: Optional[datetime] = None
    

class UserSession(BaseModel):
    """User session information."""
    user_id: str
    provider_user_id: str
    provider: str
    email: EmailStr
    name: Optional[str] = None
    roles: List[str] = [Role.VIEWER]
    metadata: Dict[str, Any] = {}
    last_login: datetime


class TokenResponse(BaseModel):
    """Response model for token requests."""
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    expires_in: int  # Seconds
    user: UserInfo


class RefreshRequest(BaseModel):
    """Request model for token refresh."""
    refresh_token: str


class LoginRequest(BaseModel):
    """Request model for direct login (development only)."""
    username: str
    password: str


class OAuthState(BaseModel):
    """State information for OAuth flow."""
    provider: str
    redirect_url: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    nonce: str  # Random value to prevent CSRF 