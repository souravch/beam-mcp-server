import pytest
from typing import Dict, Any

from server.mcp.models import FeatureCapabilityLevel, FeatureCapabilityConfig
from server.mcp.capabilities import CapabilityRegistry
from server.mcp.version import VersionCompatibility


def test_basic_server_capabilities():
    """Test that the server can declare its capabilities."""
    # Create registry
    registry = CapabilityRegistry(
        server_name="test-server",
        server_version="1.0.0",
        protocol_version="1.0"
    )
    
    # Register some capabilities
    registry.register_feature("test.feature1", properties={"key1": "value1"})
    registry.register_feature("test.feature2", properties={"key2": "value2"})
    
    # Get capabilities
    capabilities = registry.get_capabilities()
    
    # Check server info
    assert capabilities.server.name == "test-server"
    assert capabilities.server.version == "1.0.0"
    assert capabilities.protocol_version == "1.0"
    
    # Check features
    assert "test.feature1" in capabilities.capabilities
    assert capabilities.capabilities["test.feature1"]["key1"] == "value1"
    assert "test.feature2" in capabilities.capabilities
    assert capabilities.capabilities["test.feature2"]["key2"] == "value2"


def test_capability_compatibility_basic():
    """Test basic capability compatibility checking."""
    # Create registry
    registry = CapabilityRegistry(
        server_name="test-server",
        server_version="1.0.0",
        protocol_version="1.0"
    )
    
    # Register some capabilities
    registry.register_feature("test.feature1", properties={"key1": "value1"})
    registry.register_feature("test.feature2", properties={"key2": "value2"})
    
    # Create client capabilities
    client_capabilities = {
        "protocol_version": "1.0",
        "capabilities": {
            "test.feature1": {"key1": "value1"},
            "test.feature2": {"key2": "value2"}
        }
    }
    
    # Check compatibility
    compatible, _ = registry.is_compatible_with(client_capabilities)
    assert compatible


def test_capability_compatibility_missing_feature():
    """Test capability compatibility when client is missing required feature."""
    # Create registry
    registry = CapabilityRegistry(
        server_name="test-server",
        server_version="1.0.0",
        protocol_version="1.0"
    )
    
    # Register some capabilities
    registry.register_feature(
        "test.feature1", 
        properties={"key1": "value1"},
        level=FeatureCapabilityLevel.REQUIRED
    )
    registry.register_feature(
        "test.feature2", 
        properties={"key2": "value2"},
        level=FeatureCapabilityLevel.OPTIONAL
    )
    
    # Create client capabilities with missing required feature
    client_capabilities = {
        "protocol_version": "1.0",
        "capabilities": {
            "test.feature2": {"key2": "value2"}
        }
    }
    
    # Check compatibility
    compatible, reasons = registry.is_compatible_with(client_capabilities)
    assert not compatible
    assert len(reasons) > 0
    assert "test.feature1" in reasons[0]


def test_capability_compatibility_version_mismatch():
    """Test capability compatibility when protocol versions don't match."""
    # Create registry
    registry = CapabilityRegistry(
        server_name="test-server",
        server_version="1.0.0",
        protocol_version="1.0"
    )
    
    # Create client capabilities with different protocol version
    client_capabilities = {
        "protocol_version": "2.0",
        "capabilities": {}
    }
    
    # Check compatibility
    compatible, reasons = registry.is_compatible_with(client_capabilities)
    assert not compatible
    assert len(reasons) > 0
    assert "protocol_version" in reasons[0]


def test_capability_level():
    """Test feature capability levels."""
    # Create registry
    registry = CapabilityRegistry(
        server_name="test-server",
        server_version="1.0.0",
        protocol_version="1.0"
    )
    
    # Register features with different levels
    registry.register_feature(
        "test.required", 
        properties={"key": "value"},
        level=FeatureCapabilityLevel.REQUIRED
    )
    registry.register_feature(
        "test.preferred", 
        properties={"key": "value"},
        level=FeatureCapabilityLevel.PREFERRED
    )
    registry.register_feature(
        "test.optional", 
        properties={"key": "value"},
        level=FeatureCapabilityLevel.OPTIONAL
    )
    registry.register_feature(
        "test.experimental", 
        properties={"key": "value"},
        level=FeatureCapabilityLevel.EXPERIMENTAL
    )
    
    # Client with only required feature
    client_required_only = {
        "protocol_version": "1.0",
        "capabilities": {
            "test.required": {"key": "value"}
        }
    }
    
    # Check compatibility
    compatible, _ = registry.is_compatible_with(client_required_only)
    assert compatible
    
    # Client missing required feature
    client_missing_required = {
        "protocol_version": "1.0",
        "capabilities": {
            "test.preferred": {"key": "value"}
        }
    }
    
    # Check compatibility
    compatible, _ = registry.is_compatible_with(client_missing_required)
    assert not compatible


def test_version_compatibility():
    """Test feature version compatibility."""
    # Create registry
    registry = CapabilityRegistry(
        server_name="test-server",
        server_version="1.0.0",
        protocol_version="1.0"
    )
    
    # Register features with version compatibility
    registry.register_feature(
        "test.exact", 
        properties={"version": "1.2.3"},
        version_compatibility=VersionCompatibility.EXACT
    )
    registry.register_feature(
        "test.compatible", 
        properties={"version": "2.3.4"},
        version_compatibility=VersionCompatibility.COMPATIBLE
    )
    registry.register_feature(
        "test.any", 
        properties={"version": "3.4.5"},
        version_compatibility=VersionCompatibility.ANY
    )
    
    # Client with exact versions
    client_exact = {
        "protocol_version": "1.0",
        "capabilities": {
            "test.exact": {"version": "1.2.3"},
            "test.compatible": {"version": "2.3.4"},
            "test.any": {"version": "3.4.5"}
        }
    }
    
    # Check compatibility
    compatible, _ = registry.is_compatible_with(client_exact)
    assert compatible
    
    # Client with compatible versions
    client_compatible = {
        "protocol_version": "1.0",
        "capabilities": {
            "test.exact": {"version": "1.2.3"},
            "test.compatible": {"version": "2.3.5"},
            "test.any": {"version": "9.9.9"}
        }
    }
    
    # Check compatibility
    compatible, _ = registry.is_compatible_with(client_compatible)
    assert compatible
    
    # Client with incompatible versions
    client_incompatible = {
        "protocol_version": "1.0",
        "capabilities": {
            "test.exact": {"version": "1.2.4"},
            "test.compatible": {"version": "2.3.4"},
            "test.any": {"version": "3.4.5"}
        }
    }
    
    # Check compatibility
    compatible, reasons = registry.is_compatible_with(client_incompatible)
    assert not compatible
    assert "1.2.4" in reasons[0]
    assert "1.2.3" in reasons[0]


def test_required_properties():
    """Test required properties for features."""
    # Create registry
    registry = CapabilityRegistry(
        server_name="test-server",
        server_version="1.0.0",
        protocol_version="1.0"
    )
    
    # Register feature with required properties
    registry.register_feature(
        "test.feature", 
        properties={"required": "value", "optional": "value"},
        required_properties={"required"}
    )
    
    # Client with all properties
    client_complete = {
        "protocol_version": "1.0",
        "capabilities": {
            "test.feature": {"required": "value", "optional": "different"}
        }
    }
    
    # Check compatibility
    compatible, _ = registry.is_compatible_with(client_complete)
    assert compatible
    
    # Client missing required property
    client_missing = {
        "protocol_version": "1.0",
        "capabilities": {
            "test.feature": {"optional": "value"}
        }
    }
    
    # Check compatibility
    compatible, reasons = registry.is_compatible_with(client_missing)
    assert not compatible
    assert "required" in reasons[0]


def test_is_feature_enabled():
    """Test checking if a feature is enabled for a connection."""
    # Create registry
    registry = CapabilityRegistry(
        server_name="test-server",
        server_version="1.0.0",
        protocol_version="1.0"
    )
    
    # Register features
    registry.register_feature("test.feature1")
    registry.register_feature("test.feature2")
    
    # Connection with feature1
    connection = {
        "capabilities": {
            "test.feature1": {}
        }
    }
    
    # Check if features are enabled
    assert registry.is_feature_enabled("test.feature1", connection)
    assert not registry.is_feature_enabled("test.feature2", connection)
    assert not registry.is_feature_enabled("test.nonexistent", connection) 