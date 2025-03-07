# Authentication Module for Apache Beam MCP Server

This module provides a comprehensive OAuth 2.0 authentication system for the Apache Beam MCP Server.

## Features

- **OAuth 2.0 Integration**: Support for Google and GitHub identity providers
- **JWT Tokens**: Access and refresh tokens for secure authentication
- **Role-Based Access Control**: Simple RBAC model with Admin (read/write) and Viewer (read-only) roles
- **Rate Limiting**: Prevents abuse of the API
- **Development Bypass**: Easy authentication bypass for development environments
- **Disabled by Default**: Authentication is disabled by default for easy development

## Configuration

Authentication is configured using environment variables. The following environment variables are supported:

### Basic Authentication Settings

```bash
# Enable authentication (default is false)
MCP_AUTH_ENABLED=true

# JWT settings
MCP_JWT_SECRET_KEY=your-secure-random-key
MCP_JWT_ALGORITHM=HS256
MCP_ACCESS_TOKEN_EXPIRE_MINUTES=30
MCP_REFRESH_TOKEN_EXPIRE_DAYS=7
```

### OAuth Provider Configuration

```bash
# Google OAuth
MCP_OAUTH2_GOOGLE_CLIENT_ID=your-google-client-id
MCP_OAUTH2_GOOGLE_CLIENT_SECRET=your-google-client-secret

# GitHub OAuth
MCP_OAUTH2_GITHUB_CLIENT_ID=your-github-client-id
MCP_OAUTH2_GITHUB_CLIENT_SECRET=your-github-client-secret
```

### Test Bypass for Development

```bash
MCP_AUTH_ALLOW_TEST_BYPASS=true
MCP_AUTH_TEST_BYPASS_TOKEN=your-test-bypass-token
```

### Rate Limiting Configuration

```bash
MCP_RATE_LIMIT_ENABLED=true
MCP_RATE_LIMIT_MAX_REQUESTS=100
MCP_RATE_LIMIT_WINDOW_SECONDS=3600
```

## Usage

### Initializing the Authentication System

The authentication system is automatically initialized when the application starts. If authentication is enabled, the API endpoints will require authentication.

### Securing API Endpoints

API endpoints can be secured using the `require_read` and `require_write` decorators:

```python
from src.server.auth import require_read, require_write, UserSession
from fastapi import Depends

@router.get("/resources", response_model=ResponseModel)
@require_read
async def list_resources(user: UserSession = Depends(get_current_user)):
    # Only authenticated users can access this endpoint
    # This endpoint requires read permission

@router.post("/resources", response_model=ResponseModel)
@require_write
async def create_resource(resource: ResourceModel, user: UserSession = Depends(get_current_user)):
    # Only authenticated users with write permission can access this endpoint
    # This endpoint requires write permission
```

### Authentication Flow

The authentication flow works as follows:

1. The user is redirected to the OAuth provider's login page
2. After successful authentication, the user is redirected back to the application with an authorization code
3. The application exchanges the authorization code for access and refresh tokens
4. The access token is used to authenticate API requests
5. When the access token expires, the refresh token is used to obtain a new access token

### Testing Authentication

You can test the authentication system using the provided test bypass token:

```bash
curl -X GET "http://localhost:8888/api/v1/resources" \
  -H "Authorization: Bearer your-test-bypass-token"
```

## Role-Based Access Control

The authentication system uses a simplified RBAC model with two roles:

- **Admin**: Has read and write access to all resources
- **Viewer**: Has read-only access to all resources

The role is stored in the JWT token and is used to determine if the user has permission to access a specific endpoint.

## Rate Limiting

The authentication system includes rate limiting to prevent abuse of the API. The rate limiting is configured using environment variables and applies to each IP address.

## Development Notes

When adding new API endpoints, make sure to secure them using the appropriate decorator (`require_read` or `require_write`).

If you need to modify the authentication system, make sure to run the comprehensive test suite to ensure that everything works correctly.

## Testing Notes

### Testing Async Endpoints with RedirectResponse

When testing async endpoints that return a RedirectResponse (like OAuth login endpoints), you may encounter issues with the FastAPI TestClient. This is because TestClient doesn't properly handle async endpoints that return RedirectResponse in some versions of FastAPI/Starlette.

To work around this issue, you can mock the RedirectResponse class instead of relying on the actual redirect behavior:

```python
from unittest.mock import patch

def test_login_oauth_endpoint(client):
    """Test the login OAuth endpoint."""
    # Mock dependencies
    mock_auth_config = MagicMock()
    mock_auth_config.enabled = True
    mock_auth_config.oauth2_providers = {"google": MagicMock()}
    
    mock_url = "https://accounts.google.com/o/oauth2/auth?client_id=google-id&redirect_uri=http%3A%2F%2Flocalhost%2Fcallback&scope=openid+email+profile&response_type=code&state=test-state"
    
    # Mock RedirectResponse and other dependencies
    with patch("src.server.auth.router.get_auth_config", return_value=mock_auth_config), \
         patch("src.server.auth.router.get_oauth2_login_url", return_value=mock_url), \
         patch("src.server.auth.router.secrets.token_urlsafe", return_value="test-state"), \
         patch("src.server.auth.router.RedirectResponse") as mock_redirect:
        
        # Mock the RedirectResponse to return a simple dict
        mock_redirect.return_value = {"location": mock_url}
        
        # Call the endpoint
        client.get("/api/v1/auth/login/google?redirect_uri=/dashboard")
        
        # Check that RedirectResponse was called with the correct URL
        mock_redirect.assert_called_once_with(url=mock_url)
```

This approach allows you to test the logic of the endpoint without being affected by the TestClient's handling of redirects. 