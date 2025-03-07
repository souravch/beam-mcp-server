from fastapi import APIRouter, Depends, HTTPException, status, Request, Response, Query
from fastapi.responses import RedirectResponse, JSONResponse
from typing import Optional, Dict, Any
import secrets
import time
import logging
from datetime import datetime, UTC
from urllib.parse import urlencode, quote

from .config import get_auth_config
from .models import (
    UserInfo, 
    TokenResponse, 
    RefreshRequest, 
    LoginRequest,
    OAuthState
)
from .jwt import create_tokens, verify_refresh_token
from .oauth2 import (
    get_oauth2_login_url, 
    exchange_code_for_token, 
    get_user_info, 
    OAuth2Error
)
from .dependencies import get_current_user, UserSession


# Create router
router = APIRouter(prefix="/auth", tags=["auth"])

# Store OAuth state temporarily
# In a production environment, this should be in Redis or similar
oauth_states: Dict[str, OAuthState] = {}

# Logger
logger = logging.getLogger(__name__)


@router.get("/login/{provider}")
async def login_oauth(
    provider: str, 
    request: Request,
    redirect_uri: Optional[str] = Query(None),
):
    """
    Initiate the OAuth login flow.
    
    Args:
        provider: The OAuth provider (e.g., "google", "github")
        redirect_uri: Where to redirect after successful authentication
    """
    auth_config = get_auth_config()
    
    if not auth_config.enabled:
        return JSONResponse(
            status_code=status.HTTP_501_NOT_IMPLEMENTED,
            content={"detail": "Authentication is not enabled"}
        )
    
    if provider not in auth_config.oauth2_providers:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Unknown OAuth provider: {provider}"
        )
    
    # Generate a random state for CSRF protection
    state = secrets.token_urlsafe(32)
    
    # Store state with redirect URL
    base_url = str(request.base_url).rstrip("/")
    callback_uri = f"{base_url}/api/v1/auth/callback/{provider}"
    
    # Store state
    final_redirect = redirect_uri or "/"
    oauth_states[state] = OAuthState(
        provider=provider,
        redirect_url=final_redirect,
        timestamp=datetime.now(UTC),
        nonce=state
    )
    
    # Clean up old states (older than 10 minutes)
    now = datetime.now(UTC)
    for k, v in list(oauth_states.items()):
        if (now - v.timestamp).total_seconds() > 600:
            oauth_states.pop(k, None)
    
    try:
        # Get provider login URL
        login_url = await get_oauth2_login_url(provider, callback_uri, state)
        
        # Redirect to provider login
        return RedirectResponse(url=login_url)
    except OAuth2Error as e:
        logger.error(f"OAuth error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@router.get("/callback/{provider}")
async def oauth_callback(
    provider: str,
    request: Request,
    code: Optional[str] = None,
    state: Optional[str] = None,
    error: Optional[str] = None,
):
    """
    Handle the OAuth callback from the provider.
    
    Args:
        provider: The OAuth provider (e.g., "google", "github")
        code: The authorization code from the provider
        state: The state parameter for CSRF protection
        error: Error message if authorization failed
    """
    auth_config = get_auth_config()
    
    if not auth_config.enabled:
        return JSONResponse(
            status_code=status.HTTP_501_NOT_IMPLEMENTED,
            content={"detail": "Authentication is not enabled"}
        )
    
    # Handle authentication errors
    if error:
        logger.error(f"OAuth error from provider: {error}")
        return RedirectResponse(
            url=f"/?error={quote(f'Authentication failed: {error}')}"
        )
    
    # Validate state to prevent CSRF
    if not state or state not in oauth_states:
        return RedirectResponse(
            url="/?error=Invalid+state+parameter"
        )
    
    # Get stored state and remove it
    stored_state = oauth_states.pop(state)
    
    # Validate code
    if not code:
        return RedirectResponse(
            url="/?error=No+authorization+code+provided"
        )
    
    # Exchange code for token
    try:
        base_url = str(request.base_url).rstrip("/")
        callback_uri = f"{base_url}/api/v1/auth/callback/{provider}"
        
        token_response = await exchange_code_for_token(provider, code, callback_uri)
        provider_access_token = token_response.get("access_token")
        
        if not provider_access_token:
            logger.error(f"No access token in provider response: {token_response}")
            return RedirectResponse(
                url="/?error=Failed+to+get+access+token"
            )
        
        # Get user info from provider
        user_data = await get_user_info(provider, provider_access_token)
        
        if not user_data.get("email"):
            logger.error(f"No email in user data: {user_data}")
            return RedirectResponse(
                url="/?error=Could+not+retrieve+email+from+provider"
            )
        
        # Create a unique user ID from provider and provider user ID
        user_id = f"{provider}:{user_data['provider_user_id']}"
        
        # Create JWT tokens
        tokens = create_tokens(
            user_id=user_id,
            email=user_data["email"],
            name=user_data["name"],
            roles=["viewer"],  # Default role for new users
            provider=provider
        )
        
        # Create response with tokens
        # In a production app, you'd typically set these as cookies
        # For now, we'll redirect with tokens in query params
        redirect_url = stored_state.redirect_url or "/"
        query_params = {
            "access_token": tokens.access_token,
            "refresh_token": tokens.refresh_token,
            "token_type": tokens.token_type,
            "expires_in": tokens.expires_in
        }
        
        redirect_with_params = f"{redirect_url}?{urlencode(query_params)}"
        return RedirectResponse(url=redirect_with_params)
    
    except OAuth2Error as e:
        logger.error(f"OAuth error: {str(e)}")
        return RedirectResponse(
            url=f"/?error={quote(f'Authentication failed: {str(e)}')}"
        )
    except Exception as e:
        logger.exception(f"Unexpected error in OAuth callback: {str(e)}")
        return RedirectResponse(
            url="/?error=Internal+server+error"
        )


@router.post("/refresh", response_model=TokenResponse)
async def refresh_token(request: RefreshRequest):
    """
    Refresh the access token using a refresh token.
    
    Args:
        request: Refresh token request
    """
    auth_config = get_auth_config()
    
    if not auth_config.enabled:
        raise HTTPException(
            status_code=status.HTTP_501_NOT_IMPLEMENTED,
            detail="Authentication is not enabled"
        )
    
    # Verify refresh token
    user_id = verify_refresh_token(request.refresh_token)
    
    if not user_id:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid refresh token"
        )
    
    # Parse user ID to get provider
    parts = user_id.split(":", 1)
    provider = parts[0] if len(parts) > 1 else "unknown"
    
    # Create new tokens
    # In a production app, you'd typically look up the user in a database
    # to get their email, name, roles, etc.
    tokens = create_tokens(
        user_id=user_id,
        email=None,  # We don't have this without a DB
        name=None,   # We don't have this without a DB
        roles=["viewer"],  # Default role
        provider=provider
    )
    
    # Create user info
    user = UserInfo(
        user_id=user_id,
        email="user@example.com",  # Placeholder
        name="User",               # Placeholder
        roles=["viewer"],          # Default role
        provider=provider,
        created_at=datetime.now(UTC),
        last_login=datetime.now(UTC)
    )
    
    return TokenResponse(
        access_token=tokens.access_token,
        refresh_token=tokens.refresh_token,
        token_type=tokens.token_type,
        expires_in=tokens.expires_in,
        user=user
    )


@router.get("/me", response_model=UserInfo)
async def get_me(user: UserSession = Depends(get_current_user)):
    """Get the current user's information."""
    return UserInfo(
        user_id=user.user_id,
        email=user.email,
        name=user.name,
        roles=user.roles,
        provider=user.provider,
        created_at=datetime.now(UTC),
        last_login=user.last_login
    )


@router.post("/token")
async def get_token_for_testing(
    request: Request,
    response: Response,
):
    """
    Get a test token for development.
    This endpoint only works if test bypass is enabled.
    """
    auth_config = get_auth_config()
    
    if not auth_config.enabled:
        raise HTTPException(
            status_code=status.HTTP_501_NOT_IMPLEMENTED,
            detail="Authentication is not enabled"
        )
    
    if not auth_config.allow_test_bypass:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Test authentication is disabled in this environment"
        )
    
    # Return test bypass token
    if auth_config.test_bypass_token:
        return {
            "access_token": auth_config.test_bypass_token,
            "token_type": "bearer",
            "expires_in": 3600,
            "scope": "all"
        }
    else:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Test bypass token is not configured"
        ) 