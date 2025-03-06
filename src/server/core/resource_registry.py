"""
Resource Registry for managing MCP resources.

This module provides a registry for managing resources that can be used
in data pipelines, following the MCP standard.
"""

import logging
import uuid
from typing import Dict, List, Optional, Any
from datetime import datetime

from ..models.resources import Resource, ResourceDefinition, ResourceType, ResourceStatus
from ..models.context import MCPContext

logger = logging.getLogger(__name__)

class ResourceRegistry:
    """Registry for managing MCP resources."""
    
    def __init__(self):
        """Initialize the resource registry."""
        self._resources: Dict[str, Resource] = {}
        self._init_default_resources()
        logger.info("ResourceRegistry initialized")
    
    def _init_default_resources(self):
        """Initialize default resources."""
        # Example dataset
        example_dataset = Resource(
            mcp_resource_id="example-dataset",
            name="Example Dataset",
            description="An example dataset for testing",
            resource_type=ResourceType.DATASET,
            location="gs://beam-examples/datasets/example.csv",
            format="csv",
            size_bytes=102400,
            schema={
                "fields": [
                    {"name": "id", "type": "integer", "description": "Unique identifier"},
                    {"name": "name", "type": "string", "description": "Name field"},
                    {"name": "value", "type": "float", "description": "Value field"}
                ]
            },
            metadata={
                "rows": 1000,
                "created_by": "system"
            }
        )
        
        # Example model
        example_model = Resource(
            mcp_resource_id="example-model",
            name="Example ML Model",
            description="A pre-trained example model",
            resource_type=ResourceType.MODEL,
            location="gs://beam-examples/models/example-model.pkl",
            format="pickle",
            status=ResourceStatus.AVAILABLE,
            metadata={
                "accuracy": 0.95,
                "created_by": "system"
            }
        )
        
        # Example configuration
        example_config = Resource(
            mcp_resource_id="example-config",
            name="Example Config",
            description="An example configuration file",
            resource_type=ResourceType.CONFIG,
            location="gs://beam-examples/configs/example-config.json",
            format="json",
            metadata={
                "environment": "development",
                "created_by": "system"
            }
        )
        
        # Add resources to registry
        self._resources[example_dataset.mcp_resource_id] = example_dataset
        self._resources[example_model.mcp_resource_id] = example_model
        self._resources[example_config.mcp_resource_id] = example_config
        
        logger.info(f"Initialized {len(self._resources)} default resources")
    
    def list_resources(
        self,
        resource_type: Optional[ResourceType] = None,
        status: Optional[ResourceStatus] = None
    ) -> List[Resource]:
        """
        List all available resources with optional filtering.
        
        Args:
            resource_type: Optional filter by resource type
            status: Optional filter by resource status
            
        Returns:
            List of resources
        """
        resources = list(self._resources.values())
        
        # Apply filters
        if resource_type:
            resources = [r for r in resources if r.resource_type == resource_type]
        
        if status:
            resources = [r for r in resources if r.status == status]
            
        logger.info(f"Listed {len(resources)} resources")
        return resources
    
    def get_resource(self, resource_id: str) -> Optional[Resource]:
        """
        Get a resource by ID.
        
        Args:
            resource_id: Resource ID
            
        Returns:
            Resource if found, None otherwise
        """
        resource = self._resources.get(resource_id)
        if not resource:
            logger.warning(f"Resource not found: {resource_id}")
            return None
            
        return resource
    
    def create_resource(
        self,
        resource_definition: ResourceDefinition,
        created_by: Optional[str] = None
    ) -> Resource:
        """
        Create a new resource.
        
        Args:
            resource_definition: Resource definition
            created_by: Creator ID
            
        Returns:
            Created resource
            
        Raises:
            ValueError: If a resource with the same name already exists
        """
        # Check if resource with same name exists
        for existing in self._resources.values():
            if existing.name == resource_definition.name:
                raise ValueError(f"Resource with name '{resource_definition.name}' already exists")
        
        # Generate ID
        resource_id = f"{resource_definition.resource_type.value}-{uuid.uuid4().hex[:8]}"
        
        # Create resource
        resource = Resource(
            mcp_resource_id=resource_id,
            name=resource_definition.name,
            description=resource_definition.description,
            resource_type=resource_definition.resource_type,
            location=resource_definition.location,
            format=resource_definition.format,
            schema=resource_definition.schema,
            metadata={
                **resource_definition.metadata,
                "created_at": datetime.utcnow().isoformat(),
                "created_by": created_by or "system"
            }
        )
        
        # Add to registry
        self._resources[resource_id] = resource
        logger.info(f"Created resource: {resource_id}")
        
        return resource
    
    def update_resource(
        self,
        resource_id: str,
        updates: Dict[str, Any],
        updated_by: Optional[str] = None
    ) -> Optional[Resource]:
        """
        Update an existing resource.
        
        Args:
            resource_id: Resource ID
            updates: Updates to apply
            updated_by: Updater ID
            
        Returns:
            Updated resource if found, None otherwise
            
        Raises:
            ValueError: If trying to update immutable fields
        """
        resource = self.get_resource(resource_id)
        if not resource:
            return None
        
        # Check for immutable fields
        immutable_fields = ["id", "mcp_resource_id"]
        for field in immutable_fields:
            if field in updates:
                raise ValueError(f"Cannot update immutable field: {field}")
        
        # Apply updates
        resource_dict = resource.dict()
        for key, value in updates.items():
            if hasattr(resource, key):
                resource_dict[key] = value
        
        # Update metadata
        if "metadata" not in resource_dict:
            resource_dict["metadata"] = {}
        
        resource_dict["metadata"]["updated_at"] = datetime.utcnow().isoformat()
        if updated_by:
            resource_dict["metadata"]["updated_by"] = updated_by
        
        # Create updated resource
        updated_resource = Resource(**resource_dict)
        self._resources[resource_id] = updated_resource
        
        logger.info(f"Updated resource: {resource_id}")
        return updated_resource
    
    def delete_resource(self, resource_id: str) -> bool:
        """
        Delete a resource.
        
        Args:
            resource_id: Resource ID
            
        Returns:
            True if deleted, False if not found
        """
        if resource_id not in self._resources:
            logger.warning(f"Cannot delete: resource not found: {resource_id}")
            return False
        
        del self._resources[resource_id]
        logger.info(f"Deleted resource: {resource_id}")
        return True 