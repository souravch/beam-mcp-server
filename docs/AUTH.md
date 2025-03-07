# Authentication Setup Guide

This guide explains how to set up and configure the authentication system for the Apache Beam MCP Server.

## Overview

The MCP Server uses OAuth 2.0 with JWT (JSON Web Tokens) for authentication. The authentication system includes:

- OAuth 2.0 integration with Google and GitHub
- JWT tokens with refresh token support
- Rate limiting to prevent abuse
- Test bypass option for development environments
- Simplified RBAC with Admin (read/write) and Viewer (read-only) roles

**By default, authentication is disabled** for easy development and testing. To enable it, you need to configure the necessary environment variables.

## Configuration

### Basic Authentication Settings

To enable authentication, set these environment variables:

```bash
# Enable authentication (default is false)
MCP_AUTH_ENABLED=true

# JWT settings
MCP_JWT_SECRET_KEY=your-secure-random-key
MCP_JWT_ALGORITHM=HS256
MCP_ACCESS_TOKEN_EXPIRE_MINUTES=30
MCP_REFRESH_TOKEN_EXPIRE_DAYS=7
```

The `MCP_JWT_SECRET_KEY` should be a secure random string. You can generate one using:

```bash
python -c "import secrets; print(secrets.token_hex(32))"
```

### OAuth Provider Configuration

Configure at least one OAuth provider:

#### Google OAuth

1. Create OAuth credentials in the [Google Cloud Console](https://console.cloud.google.com/apis/credentials)
2. Set these environment variables:

```bash
MCP_OAUTH2_GOOGLE_CLIENT_ID=your-google-client-id
MCP_OAUTH2_GOOGLE_CLIENT_SECRET=your-google-client-secret
```

3. Add the following redirect URL to your Google OAuth app:
   `http://localhost:8888/api/v1/auth/callback/google` (for local development)
   `https://your-domain.com/api/v1/auth/callback/google` (for production)

#### GitHub OAuth

1. Create an OAuth App in your [GitHub settings](https://github.com/settings/developers)
2. Set these environment variables:

```bash
MCP_OAUTH2_GITHUB_CLIENT_ID=your-github-client-id
MCP_OAUTH2_GITHUB_CLIENT_SECRET=your-github-client-secret
```

3. Add the following redirect URL to your GitHub OAuth app:
   `http://localhost:8888/api/v1/auth/callback/github` (for local development)
   `https://your-domain.com/api/v1/auth/callback/github` (for production)

### Test Bypass for Development

For development environments, you can enable a test bypass token:

```bash
MCP_AUTH_ALLOW_TEST_BYPASS=true
MCP_AUTH_TEST_BYPASS_TOKEN=your-test-bypass-token
```

This token can be used in the `Authorization` header without going through the OAuth flow:

```
Authorization: Bearer your-test-bypass-token
```

### Rate Limiting Configuration

Rate limiting helps prevent abuse of your API:

```bash
MCP_RATE_LIMIT_ENABLED=true
MCP_RATE_LIMIT_MAX_REQUESTS=100
MCP_RATE_LIMIT_WINDOW_SECONDS=3600
```

This configuration limits each IP address to 100 requests per hour per endpoint.

## Role-Based Access Control

The authentication system uses a simplified RBAC model with two roles:

- **Admin**: Has read and write access to all resources
- **Viewer**: Has read-only access to all resources

Read operations include GET requests to list or retrieve resources.
Write operations include POST, PUT, DELETE requests to create, update, or delete resources.

## Authentication Flow

### Web Application Flow

For web applications, the OAuth flow works as follows:

1. Direct users to `/api/v1/auth/login/{provider}?redirect_uri={your_redirect_uri}`
   - `provider`: Either `google` or `github`
   - `redirect_uri`: Where to redirect after successful authentication

2. After authentication, the user will be redirected to your application with tokens in the query parameters:

```
https://your-redirect-uri?access_token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...&refresh_token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...&token_type=bearer&expires_in=1800
```

3. Your application should store these tokens and use the access token for subsequent API requests.

### Using Access Tokens

Include the access token in the `Authorization` header of your requests:

```
Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
```

Example with curl:
```bash
curl http://localhost:8888/api/v1/resources \
  -H "Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."
```

Example with Python:
```python
import requests

headers = {
    "Authorization": f"Bearer {access_token}"
}
response = requests.get("http://localhost:8888/api/v1/resources", headers=headers)
```

### Token Refresh

Access tokens expire after 30 minutes by default. Use the refresh token to obtain a new access token:

```bash
curl -X POST http://localhost:8888/api/v1/auth/refresh \
  -H "Content-Type: application/json" \
  -d '{"refresh_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."}'
```

Response:
```json
{
  "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "refresh_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "token_type": "bearer",
  "expires_in": 1800,
  "user": {
    "user_id": "google:123456789",
    "email": "user@example.com",
    "name": "John Doe",
    "roles": ["viewer"],
    "provider": "google",
    "created_at": "2023-10-31T12:00:00Z",
    "last_login": "2023-10-31T12:00:00Z"
  }
}
```

### User Information

To get information about the currently authenticated user:

```bash
curl http://localhost:8888/api/v1/auth/me \
  -H "Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."
```

## Configuration Examples

### Docker Compose

```yaml
version: '3'
services:
  mcp-server:
    image: apache/beam-mcp-server
    environment:
      - MCP_AUTH_ENABLED=true
      - MCP_JWT_SECRET_KEY=your-secure-random-key
      - MCP_OAUTH2_GOOGLE_CLIENT_ID=your-google-client-id
      - MCP_OAUTH2_GOOGLE_CLIENT_SECRET=your-google-client-secret
    ports:
      - 8888:8888
```

### Kubernetes

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: mcp-auth-secrets
type: Opaque
data:
  jwt-secret-key: base64-encoded-secret-key
  google-client-secret: base64-encoded-google-secret
  github-client-secret: base64-encoded-github-secret
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: mcp-server
spec:
  template:
    spec:
      containers:
      - name: mcp-server
        image: apache/beam-mcp-server
        env:
        - name: MCP_AUTH_ENABLED
          value: "true"
        - name: MCP_JWT_SECRET_KEY
          valueFrom:
            secretKeyRef:
              name: mcp-auth-secrets
              key: jwt-secret-key
        - name: MCP_OAUTH2_GOOGLE_CLIENT_ID
          value: your-google-client-id
        - name: MCP_OAUTH2_GOOGLE_CLIENT_SECRET
          valueFrom:
            secretKeyRef:
              name: mcp-auth-secrets
              key: google-client-secret
```

## Security Considerations

1. **JWT Secret Key**: Use a secure random string for `MCP_JWT_SECRET_KEY`. Never share this key.

2. **OAuth Secrets**: Treat OAuth client secrets as sensitive information. Use environment variables or secure storage.

3. **HTTPS**: Always use HTTPS in production to protect tokens in transit.

4. **Token Storage**: Store access and refresh tokens securely in your client applications.

5. **Test Bypass**: Only enable test bypass (`MCP_AUTH_ALLOW_TEST_BYPASS`) in development environments.

## Troubleshooting

### Authentication Not Working

- Check if `MCP_AUTH_ENABLED` is set to `true`
- Verify that at least one OAuth provider is configured correctly
- Ensure the JWT secret key is set

### OAuth Redirect Issues

- Confirm the redirect URIs are correctly registered in your OAuth provider settings
- Check for URL encoding issues in the redirect_uri parameter

### Token Validation Problems

- Verify the JWT token is valid and not expired
- Check the clock synchronization between your server and clients

### Permission Denied

- Ensure the user has the appropriate role for the operation (Admin for write operations)
- Check the token payload for the correct roles 