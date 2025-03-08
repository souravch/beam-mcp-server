from typing import Dict, Tuple, List, Any, Optional, Set
from .models import (
    FeatureCapability, 
    ServerCapabilities, 
    FeatureCapabilityConfig, 
    FeatureCapabilityLevel
)
from .errors import incompatible_capabilities, protocol_version_mismatch
from .version import is_version_compatible, VersionCompatibility


class CapabilityRegistry:
    """Manages server capabilities and capability negotiation."""
    
    def __init__(self, server_name: str, server_version: str):
        """Initialize the capability registry."""
        self._features: Dict[str, FeatureCapability] = {}
        self._server_info = {
            "name": server_name,
            "version": server_version
        }
        self._protocol_version = "1.0"
        self._required_features: Set[str] = set()
    
    def register_feature(
        self, 
        name: str, 
        version: str, 
        supported: bool = True, 
        properties: Optional[Dict[str, Any]] = None,
        config: Optional[FeatureCapabilityConfig] = None
    ) -> None:
        """
        Register a server feature capability.
        
        Args:
            name: Feature name
            version: Feature version
            supported: Whether the feature is supported
            properties: Feature properties
            config: Feature capability configuration
        """
        # Create feature capability
        feature = FeatureCapability(
            supported=supported,
            version=version,
            properties=properties or {},
            config=config
        )
        
        # Register feature
        self._features[name] = feature
        
        # Add to required features if necessary
        if config and config.level == FeatureCapabilityLevel.REQUIRED:
            self._required_features.add(name)
    
    def set_feature_property(
        self, 
        feature_name: str, 
        property_name: str, 
        property_value: Any
    ) -> None:
        """
        Set a property for a feature capability.
        
        Args:
            feature_name: Feature name
            property_name: Property name
            property_value: Property value
            
        Raises:
            KeyError: If the feature does not exist
        """
        if feature_name not in self._features:
            raise KeyError(f"Feature not found: {feature_name}")
        
        self._features[feature_name].properties[property_name] = property_value
    
    def set_feature_required_properties(
        self, 
        feature_name: str, 
        required_properties: List[str]
    ) -> None:
        """
        Set required properties for a feature capability.
        
        Args:
            feature_name: Feature name
            required_properties: List of required property names
            
        Raises:
            KeyError: If the feature does not exist
        """
        if feature_name not in self._features:
            raise KeyError(f"Feature not found: {feature_name}")
        
        if self._features[feature_name].config is None:
            self._features[feature_name].config = FeatureCapabilityConfig()
        
        self._features[feature_name].config.required_properties = set(required_properties)
    
    def set_feature_version_compatibility(
        self, 
        feature_name: str, 
        compatibility: VersionCompatibility
    ) -> None:
        """
        Set version compatibility mode for a feature capability.
        
        Args:
            feature_name: Feature name
            compatibility: Version compatibility mode
            
        Raises:
            KeyError: If the feature does not exist
        """
        if feature_name not in self._features:
            raise KeyError(f"Feature not found: {feature_name}")
        
        if self._features[feature_name].config is None:
            self._features[feature_name].config = FeatureCapabilityConfig()
        
        self._features[feature_name].config.version_compatibility = compatibility
    
    def set_feature_level(
        self, 
        feature_name: str, 
        level: FeatureCapabilityLevel
    ) -> None:
        """
        Set capability level for a feature.
        
        Args:
            feature_name: Feature name
            level: Capability level
            
        Raises:
            KeyError: If the feature does not exist
        """
        if feature_name not in self._features:
            raise KeyError(f"Feature not found: {feature_name}")
        
        if self._features[feature_name].config is None:
            self._features[feature_name].config = FeatureCapabilityConfig()
        
        self._features[feature_name].config.level = level
        
        # Update required features set
        if level == FeatureCapabilityLevel.REQUIRED:
            self._required_features.add(feature_name)
        else:
            self._required_features.discard(feature_name)
    
    def get_capabilities(self) -> ServerCapabilities:
        """Get the complete server capabilities object."""
        return ServerCapabilities(
            protocol_version=self._protocol_version,
            server=self._server_info,
            capabilities=self._features
        )
    
    def is_compatible_with(self, client_capabilities: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """
        Check if client capabilities are compatible with server.
        
        Args:
            client_capabilities: Client capabilities dictionary
            
        Returns:
            Tuple of (is_compatible, list_of_incompatibility_reasons)
        """
        incompatibility_reasons = []
        
        # Check protocol version compatibility
        client_version = client_capabilities.get("protocol_version")
        if not client_version:
            incompatibility_reasons.append("Client did not specify protocol version")
            return False, incompatibility_reasons
        
        if client_version != self._protocol_version:
            incompatibility_reasons.append(
                f"Incompatible protocol version: {client_version} (server: {self._protocol_version})"
            )
            return False, incompatibility_reasons
        
        # Check required client capabilities
        client_caps = client_capabilities.get("capabilities", {})
        if not isinstance(client_caps, dict):
            incompatibility_reasons.append("Client capabilities must be an object")
            return False, incompatibility_reasons
        
        # Check all required features are supported by client
        for feature_name in self._required_features:
            if feature_name not in client_caps:
                incompatibility_reasons.append(f"Required feature not supported by client: {feature_name}")
                continue
            
            # Check feature version compatibility and required properties
            self._check_feature_compatibility(
                feature_name, 
                client_caps.get(feature_name, {}), 
                incompatibility_reasons
            )
        
        # For each feature the client requests, ensure we support it
        for feature_name, feature_details in client_caps.items():
            if feature_name not in self._features:
                incompatibility_reasons.append(f"Server does not support feature: {feature_name}")
                continue
                
            server_feature = self._features[feature_name]
            if not server_feature.supported:
                incompatibility_reasons.append(f"Server has disabled feature: {feature_name}")
                continue
                
            # We already checked required features, now check optional features
            if feature_name not in self._required_features:
                self._check_feature_compatibility(
                    feature_name, 
                    feature_details, 
                    incompatibility_reasons
                )
        
        return len(incompatibility_reasons) == 0, incompatibility_reasons
    
    def _check_feature_compatibility(
        self, 
        feature_name: str, 
        client_feature: Any, 
        incompatibility_reasons: List[str]
    ) -> None:
        """
        Check compatibility of a specific feature.
        
        Args:
            feature_name: Feature name
            client_feature: Client feature details
            incompatibility_reasons: List to append incompatibility reasons to
        """
        server_feature = self._features[feature_name]
        
        # Skip if client feature is not a dictionary or boolean
        if not isinstance(client_feature, (dict, bool)):
            incompatibility_reasons.append(
                f"Invalid client feature format for {feature_name}: {type(client_feature)}"
            )
            return
        
        # If client feature is a boolean, check if it's enabled
        if isinstance(client_feature, bool):
            if not client_feature and server_feature.config.level == FeatureCapabilityLevel.REQUIRED:
                incompatibility_reasons.append(
                    f"Required feature is disabled by client: {feature_name}"
                )
            return
        
        # Check feature version compatibility if client specifies a version
        if "version" in client_feature:
            client_feature_version = client_feature["version"]
            server_feature_version = server_feature.version
            compatibility_mode = (
                server_feature.config.version_compatibility
                if server_feature.config else
                VersionCompatibility.COMPATIBLE
            )
            
            if not is_version_compatible(
                client_feature_version, 
                server_feature_version,
                compatibility_mode
            ):
                incompatibility_reasons.append(
                    f"Incompatible feature version for {feature_name}: "
                    f"client {client_feature_version}, server {server_feature_version} "
                    f"(mode: {compatibility_mode.value})"
                )
        
        # Check required properties if server specifies them
        if server_feature.config and server_feature.config.required_properties:
            client_properties = client_feature.get("properties", {})
            for required_prop in server_feature.config.required_properties:
                if required_prop not in client_properties:
                    incompatibility_reasons.append(
                        f"Required property missing for feature {feature_name}: {required_prop}"
                    )
    
    def is_feature_enabled(self, feature_name: str, connection_capabilities: Dict[str, Any]) -> bool:
        """
        Check if a feature is enabled based on connection capabilities.
        
        Args:
            feature_name: Feature name
            connection_capabilities: Connection capabilities
            
        Returns:
            True if feature is enabled, False otherwise
        """
        # Feature must be registered and supported by server
        if feature_name not in self._features or not self._features[feature_name].supported:
            return False
        
        # Get client capabilities
        client_caps = connection_capabilities.get("capabilities", {})
        if not isinstance(client_caps, dict):
            return False
        
        # Check if client supports the feature
        if feature_name not in client_caps:
            return False
        
        # If client feature is a boolean, check if it's enabled
        client_feature = client_caps[feature_name]
        if isinstance(client_feature, bool):
            return client_feature
        
        # If client feature is a dictionary, check if it has "supported" field
        if isinstance(client_feature, dict) and "supported" in client_feature:
            return client_feature["supported"]
        
        # Otherwise, assume feature is enabled
        return True 