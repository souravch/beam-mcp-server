import pytest

from server.mcp.version import (
    VersionCompatibility,
    parse_semantic_version,
    is_version_compatible,
    compatible_version_range
)


def test_parse_semantic_version():
    """Test parsing semantic versions."""
    # Simple version
    major, minor, patch, prerelease = parse_semantic_version("1.2.3")
    assert major == 1
    assert minor == 2
    assert patch == 3
    assert prerelease is None
    
    # Version with prerelease
    major, minor, patch, prerelease = parse_semantic_version("1.2.3-alpha.1")
    assert major == 1
    assert minor == 2
    assert patch == 3
    assert prerelease == "alpha.1"
    
    # Two-part version (assumes patch is 0)
    major, minor, patch, prerelease = parse_semantic_version("1.2")
    assert major == 1
    assert minor == 2
    assert patch == 0
    assert prerelease is None
    
    # One-part version (assumes minor and patch are 0)
    major, minor, patch, prerelease = parse_semantic_version("1")
    assert major == 1
    assert minor == 0
    assert patch == 0
    assert prerelease is None
    
    # Invalid version
    with pytest.raises(ValueError):
        parse_semantic_version("not.a.version")


def test_is_version_compatible_exact():
    """Test exact version compatibility."""
    # Exact match
    assert is_version_compatible(
        "1.2.3", "1.2.3", VersionCompatibility.EXACT
    )
    
    # Different patch version
    assert not is_version_compatible(
        "1.2.4", "1.2.3", VersionCompatibility.EXACT
    )
    
    # Different minor version
    assert not is_version_compatible(
        "1.3.3", "1.2.3", VersionCompatibility.EXACT
    )
    
    # Different major version
    assert not is_version_compatible(
        "2.2.3", "1.2.3", VersionCompatibility.EXACT
    )
    
    # With prerelease
    assert is_version_compatible(
        "1.2.3-alpha.1", "1.2.3-alpha.1", VersionCompatibility.EXACT
    )
    
    # Different prerelease
    assert not is_version_compatible(
        "1.2.3-alpha.2", "1.2.3-alpha.1", VersionCompatibility.EXACT
    )


def test_is_version_compatible_compatible():
    """Test semver-compatible version compatibility."""
    # Exact match
    assert is_version_compatible(
        "1.2.3", "1.2.3", VersionCompatibility.COMPATIBLE
    )
    
    # Same major, higher minor
    assert is_version_compatible(
        "1.3.0", "1.2.3", VersionCompatibility.COMPATIBLE
    )
    
    # Same major, same minor, higher patch
    assert is_version_compatible(
        "1.2.4", "1.2.3", VersionCompatibility.COMPATIBLE
    )
    
    # Same major, same minor, lower patch
    assert is_version_compatible(
        "1.2.2", "1.2.3", VersionCompatibility.COMPATIBLE
    )
    
    # Same major, lower minor
    assert is_version_compatible(
        "1.1.0", "1.2.3", VersionCompatibility.COMPATIBLE
    )
    
    # Different major
    assert not is_version_compatible(
        "2.0.0", "1.2.3", VersionCompatibility.COMPATIBLE
    )
    
    # With prerelease
    assert is_version_compatible(
        "1.2.3-alpha.1", "1.2.3", VersionCompatibility.COMPATIBLE
    )


def test_is_version_compatible_downgrade():
    """Test downgrade compatibility."""
    # Exact match
    assert is_version_compatible(
        "1.2.3", "1.2.3", VersionCompatibility.DOWNGRADE
    )
    
    # Lower version
    assert is_version_compatible(
        "1.2.2", "1.2.3", VersionCompatibility.DOWNGRADE
    )
    assert is_version_compatible(
        "1.1.0", "1.2.3", VersionCompatibility.DOWNGRADE
    )
    assert is_version_compatible(
        "0.9.0", "1.2.3", VersionCompatibility.DOWNGRADE
    )
    
    # Higher version
    assert not is_version_compatible(
        "1.2.4", "1.2.3", VersionCompatibility.DOWNGRADE
    )
    assert not is_version_compatible(
        "1.3.0", "1.2.3", VersionCompatibility.DOWNGRADE
    )
    assert not is_version_compatible(
        "2.0.0", "1.2.3", VersionCompatibility.DOWNGRADE
    )


def test_is_version_compatible_upgrade():
    """Test upgrade compatibility."""
    # Exact match
    assert is_version_compatible(
        "1.2.3", "1.2.3", VersionCompatibility.UPGRADE
    )
    
    # Higher version
    assert is_version_compatible(
        "1.2.4", "1.2.3", VersionCompatibility.UPGRADE
    )
    assert is_version_compatible(
        "1.3.0", "1.2.3", VersionCompatibility.UPGRADE
    )
    assert is_version_compatible(
        "2.0.0", "1.2.3", VersionCompatibility.UPGRADE
    )
    
    # Lower version
    assert not is_version_compatible(
        "1.2.2", "1.2.3", VersionCompatibility.UPGRADE
    )
    assert not is_version_compatible(
        "1.1.0", "1.2.3", VersionCompatibility.UPGRADE
    )
    assert not is_version_compatible(
        "0.9.0", "1.2.3", VersionCompatibility.UPGRADE
    )


def test_is_version_compatible_any():
    """Test any version compatibility."""
    # All versions should be compatible
    assert is_version_compatible(
        "1.2.3", "1.2.3", VersionCompatibility.ANY
    )
    assert is_version_compatible(
        "1.2.4", "1.2.3", VersionCompatibility.ANY
    )
    assert is_version_compatible(
        "1.3.0", "1.2.3", VersionCompatibility.ANY
    )
    assert is_version_compatible(
        "2.0.0", "1.2.3", VersionCompatibility.ANY
    )
    assert is_version_compatible(
        "1.2.2", "1.2.3", VersionCompatibility.ANY
    )
    assert is_version_compatible(
        "1.1.0", "1.2.3", VersionCompatibility.ANY
    )
    assert is_version_compatible(
        "0.9.0", "1.2.3", VersionCompatibility.ANY
    )


def test_compatible_version_range():
    """Test getting compatible version range."""
    # Simple version
    min_version, max_version = compatible_version_range("1.2.3")
    assert min_version == "1.0.0"
    assert max_version == "2.0.0"
    
    # Specific version
    min_version, max_version = compatible_version_range("2.3.4")
    assert min_version == "2.0.0"
    assert max_version == "3.0.0"
    
    # With prerelease
    min_version, max_version = compatible_version_range("1.2.3-alpha.1")
    assert min_version == "1.0.0"
    assert max_version == "2.0.0" 