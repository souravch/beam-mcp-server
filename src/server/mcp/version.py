"""Semantic versioning utilities for MCP protocol."""

import re
from enum import Enum
from typing import Tuple, Optional, Union, List


class VersionCompatibility(Enum):
    """Version compatibility modes."""
    EXACT = "exact"          # Versions must be exactly the same
    COMPATIBLE = "compatible"  # Client version must be compatible with server
    DOWNGRADE = "downgrade"    # Server allows client to use an older version
    UPGRADE = "upgrade"        # Server allows client to use a newer version
    ANY = "any"              # Any version is accepted


def parse_semantic_version(version: str) -> Tuple[int, int, int, Optional[str]]:
    """
    Parse a semantic version string into components.
    
    Args:
        version: Version string (e.g., "1.2.3" or "1.2.3-alpha.1")
        
    Returns:
        Tuple of (major, minor, patch, prerelease)
        
    Raises:
        ValueError: If version is not a valid semantic version
    """
    # Regular expression for semantic version
    # Matches: MAJOR.MINOR.PATCH[-PRERELEASE]
    pattern = r'^(\d+)\.(\d+)\.(\d+)(?:-([a-zA-Z0-9.-]+))?$'
    match = re.match(pattern, version)
    
    if not match:
        raise ValueError(f"Invalid semantic version: {version}")
    
    major = int(match.group(1))
    minor = int(match.group(2))
    patch = int(match.group(3))
    prerelease = match.group(4)
    
    return major, minor, patch, prerelease


def is_version_compatible(
    client_version: str, 
    server_version: str,
    mode: VersionCompatibility = VersionCompatibility.COMPATIBLE
) -> bool:
    """
    Check if the client version is compatible with the server version.
    
    Args:
        client_version: Client version string
        server_version: Server version string
        mode: Compatibility mode
        
    Returns:
        True if compatible, False otherwise
    """
    # Handle non-semantic versions for backward compatibility
    if not (
        all(c in "0123456789." for c in client_version) and 
        all(c in "0123456789." for c in server_version)
    ):
        # For non-semantic versions, default to exact matching
        return client_version == server_version if mode == VersionCompatibility.EXACT else True
    
    try:
        client_major, client_minor, client_patch, client_prerelease = parse_semantic_version(client_version)
        server_major, server_minor, server_patch, server_prerelease = parse_semantic_version(server_version)
        
        # Handle different compatibility modes
        if mode == VersionCompatibility.EXACT:
            # Versions must be exactly the same
            return (
                client_major == server_major and
                client_minor == server_minor and
                client_patch == server_patch and
                client_prerelease == server_prerelease
            )
        
        elif mode == VersionCompatibility.COMPATIBLE:
            # Major versions must match, client minor/patch can be <= server
            return (
                client_major == server_major and
                ((client_minor < server_minor) or
                 (client_minor == server_minor and client_patch <= server_patch))
            )
        
        elif mode == VersionCompatibility.DOWNGRADE:
            # Client version can be older (any version)
            return (
                (client_major < server_major) or
                (client_major == server_major and client_minor < server_minor) or
                (client_major == server_major and client_minor == server_minor and client_patch <= server_patch)
            )
        
        elif mode == VersionCompatibility.UPGRADE:
            # Client version can be newer (any version)
            return (
                (client_major > server_major) or
                (client_major == server_major and client_minor > server_minor) or
                (client_major == server_major and client_minor == server_minor and client_patch >= server_patch)
            )
        
        elif mode == VersionCompatibility.ANY:
            # Any version is accepted
            return True
        
        return False
    
    except ValueError:
        # If versions are not valid semantic versions, default to exact matching
        return client_version == server_version


def compatible_version_range(version: str) -> Tuple[str, str]:
    """
    Get the compatible version range for a given version.
    
    Args:
        version: Version string
        
    Returns:
        Tuple of (min_version, max_version) representing the compatible range
    """
    try:
        major, minor, patch, prerelease = parse_semantic_version(version)
        min_version = f"{major}.0.0"
        max_version = f"{major}.{minor+1}.0"
        return min_version, max_version
    except ValueError:
        # For non-semantic versions, return exact version as range
        return version, version 