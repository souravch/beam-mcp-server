"""
Authentication module for the MCP Server.

This module provides OAuth 2.0 authentication for the MCP Server API.
Features:
- Google and GitHub OAuth integration
- JWT tokens with refresh token support
- Rate limiting
- Test bypass for development
- Simplified RBAC with Admin (read/write) and Viewer (read-only) roles

By default, authentication is disabled. To enable it, set the MCP_AUTH_ENABLED
environment variable to "true" and configure at least one OAuth provider.
"""

from .config import get_auth_config
from .dependencies import (
    get_current_user, 
    require_permission,
    require_read,
    require_write,
    optional_auth,
    Operation
)
from .models import Role, UserSession

# Expose public API
__all__ = [
    'get_auth_config',
    'get_current_user',
    'require_permission',
    'require_read',
    'require_write',
    'optional_auth',
    'Operation',
    'Role',
    'UserSession'
] 