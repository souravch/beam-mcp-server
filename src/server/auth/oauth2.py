from typing import Optional, Dict, Any, List
import httpx
import json
from urllib.parse import urlencode

from .config import get_auth_config, OAuth2ProviderConfig


class OAuth2Error(Exception):
    """Exception raised for OAuth2 errors."""
    pass


async def get_oauth2_login_url(provider_id: str, redirect_uri: str, state: str) -> str:
    """
    Get the login URL for an OAuth2 provider.
    
    Args:
        provider_id: The provider identifier (e.g., "google", "github")
        redirect_uri: The URI to redirect to after authentication
        state: A random state string to prevent CSRF
        
    Returns:
        The full authorization URL to redirect the user to
    """
    auth_config = get_auth_config()
    
    if not auth_config.enabled:
        raise OAuth2Error("Authentication is not enabled")
    
    if provider_id not in auth_config.oauth2_providers:
        raise OAuth2Error(f"Unknown OAuth2 provider: {provider_id}")
    
    provider = auth_config.oauth2_providers[provider_id]
    
    params = {
        "client_id": provider.client_id,
        "redirect_uri": redirect_uri,
        "scope": " ".join(provider.scope),
        "response_type": "code",
        "state": state
    }
    
    return f"{provider.authorize_url}?{urlencode(params)}"


async def exchange_code_for_token(
    provider_id: str, 
    code: str, 
    redirect_uri: str
) -> Dict[str, Any]:
    """
    Exchange an authorization code for access tokens.
    
    Args:
        provider_id: The provider identifier (e.g., "google", "github")
        code: The authorization code from the OAuth2 callback
        redirect_uri: The redirect URI used in the initial request
        
    Returns:
        The token response from the provider
    """
    auth_config = get_auth_config()
    
    if not auth_config.enabled:
        raise OAuth2Error("Authentication is not enabled")
    
    if provider_id not in auth_config.oauth2_providers:
        raise OAuth2Error(f"Unknown OAuth2 provider: {provider_id}")
    
    provider = auth_config.oauth2_providers[provider_id]
    
    payload = {
        "client_id": provider.client_id,
        "client_secret": provider.client_secret,
        "code": code,
        "redirect_uri": redirect_uri,
        "grant_type": "authorization_code"
    }
    
    headers = {"Accept": "application/json"}
    
    async with httpx.AsyncClient() as client:
        response = await client.post(
            provider.token_url,
            data=payload if provider_id != "github" else None,
            json=payload if provider_id == "github" else None,
            headers=headers
        )
        
        if response.status_code != 200:
            raise OAuth2Error(f"Failed to exchange code for token: {response.text}")
        
        # GitHub returns content-type text/plain, but the content is JSON
        if provider_id == "github" and response.headers.get("content-type") == "application/x-www-form-urlencoded":
            return dict(x.split("=") for x in response.text.split("&"))
        
        return response.json()


async def get_user_info(provider_id: str, access_token: str) -> Dict[str, Any]:
    """
    Get user information from the OAuth2 provider.
    
    Args:
        provider_id: The provider identifier (e.g., "google", "github")
        access_token: The OAuth2 access token
        
    Returns:
        User information from the provider
    """
    auth_config = get_auth_config()
    
    if not auth_config.enabled:
        raise OAuth2Error("Authentication is not enabled")
    
    if provider_id not in auth_config.oauth2_providers:
        raise OAuth2Error(f"Unknown OAuth2 provider: {provider_id}")
    
    provider = auth_config.oauth2_providers[provider_id]
    
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json"
    }
    
    async with httpx.AsyncClient() as client:
        response = await client.get(provider.userinfo_url, headers=headers)
        
        if response.status_code != 200:
            raise OAuth2Error(f"Failed to get user info: {response.text}")
        
        user_info = response.json()
        
        # Normalize user info across providers
        normalized = {
            "provider_user_id": _get_provider_user_id(provider_id, user_info),
            "email": _get_user_email(provider_id, user_info),
            "name": _get_user_name(provider_id, user_info),
            "raw": user_info
        }
        
        # For GitHub, make an additional request to get emails if not present
        if provider_id == "github" and not normalized["email"]:
            emails_response = await client.get(
                "https://api.github.com/user/emails",
                headers=headers
            )
            
            if emails_response.status_code == 200:
                emails = emails_response.json()
                primary_email = next((e["email"] for e in emails if e["primary"]), None)
                if primary_email:
                    normalized["email"] = primary_email
        
        return normalized


def _get_provider_user_id(provider_id: str, user_info: Dict[str, Any]) -> str:
    """Extract the provider-specific user ID from user info."""
    if provider_id == "google":
        return user_info.get("id", "")
    elif provider_id == "github":
        return str(user_info.get("id", ""))
    return ""


def _get_user_email(provider_id: str, user_info: Dict[str, Any]) -> Optional[str]:
    """Extract the user's email from provider-specific user info."""
    if provider_id == "google":
        return user_info.get("email")
    elif provider_id == "github":
        return user_info.get("email")
    return None


def _get_user_name(provider_id: str, user_info: Dict[str, Any]) -> str:
    """Extract the user's name from provider-specific user info."""
    if provider_id == "google":
        return user_info.get("name", "")
    elif provider_id == "github":
        return user_info.get("name", "") or user_info.get("login", "")
    return "" 