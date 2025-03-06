"""
Context Registry for managing MCP execution contexts.

This module provides a registry for managing execution contexts that can be used
to run data pipelines, following the MCP standard.
"""

import logging
import uuid
from typing import Dict, List, Optional, Any
from datetime import datetime

from ..models.contexts import Context, ContextDefinition, ContextType, ContextStatus

logger = logging.getLogger(__name__)

class ContextRegistry:
    """Registry for managing MCP execution contexts."""
    
    def __init__(self):
        """Initialize the context registry."""
        self._contexts: Dict[str, Context] = {}
        self._init_default_contexts()
        logger.info("ContextRegistry initialized")
    
    def _init_default_contexts(self):
        """Initialize default execution contexts."""
        logger.info("Initializing default contexts")
        
        # Default Flink context - Now the first default context
        flink_context = Context(
            mcp_resource_id="flink-default",
            name="Default Flink",
            description="Default execution context for Flink jobs",
            context_type=ContextType.FLINK,
            parameters={
                "master": "local",
                "temp_location": "gs://beam-temp/flink"
            },
            resources={
                "cpu": "2",
                "memory": "4GB"
            },
            status=ContextStatus.ACTIVE,
            metadata={
                "environment": "development",
                "created_by": "system"
            }
        )
        
        # Default Dataflow context - Now second
        dataflow_context = Context(
            mcp_resource_id="dataflow-default",
            name="Default Dataflow",
            description="Default execution context for Dataflow jobs",
            context_type=ContextType.DATAFLOW,
            parameters={
                "region": "us-central1",
                "temp_location": "gs://beam-temp/dataflow",
                "machine_type": "n1-standard-2"
            },
            resources={
                "cpu": "2",
                "memory": "4GB"
            },
            metadata={
                "environment": "development",
                "created_by": "system"
            }
        )
        
        # Default Direct Runner context
        direct_context = Context(
            mcp_resource_id="direct-default",
            name="Default Direct Runner",
            description="Default execution context for Direct Runner jobs",
            context_type=ContextType.DIRECT,
            parameters={
                "temp_location": "gs://beam-temp/direct"
            },
            resources={
                "cpu": "1",
                "memory": "2GB"
            },
            metadata={
                "environment": "development",
                "created_by": "system"
            }
        )
        
        # Add contexts to registry
        self._contexts[dataflow_context.mcp_resource_id] = dataflow_context
        self._contexts[direct_context.mcp_resource_id] = direct_context
        self._contexts[flink_context.mcp_resource_id] = flink_context
        
        logger.info(f"Initialized {len(self._contexts)} default contexts")
    
    def list_contexts(
        self,
        context_type: Optional[ContextType] = None,
        status: Optional[ContextStatus] = None
    ) -> List[Context]:
        """
        List all available execution contexts with optional filtering.
        
        Args:
            context_type: Optional filter by context type
            status: Optional filter by context status
            
        Returns:
            List of contexts
        """
        contexts = list(self._contexts.values())
        
        # Apply filters
        if context_type:
            contexts = [c for c in contexts if c.context_type == context_type]
        
        if status:
            contexts = [c for c in contexts if c.status == status]
            
        logger.info(f"Listed {len(contexts)} contexts")
        return contexts
    
    def get_context(self, context_id: str) -> Optional[Context]:
        """
        Get a context by ID.
        
        Args:
            context_id: Context ID
            
        Returns:
            Context if found, None otherwise
        """
        context = self._contexts.get(context_id)
        if not context:
            logger.warning(f"Context not found: {context_id}")
            return None
            
        return context
    
    def create_context(
        self,
        context_definition: ContextDefinition,
        created_by: Optional[str] = None
    ) -> Context:
        """
        Create a new execution context.
        
        Args:
            context_definition: Context definition
            created_by: Creator ID
            
        Returns:
            Created context
            
        Raises:
            ValueError: If a context with the same name already exists
        """
        # Check if context with same name exists
        for existing in self._contexts.values():
            if existing.name == context_definition.name:
                raise ValueError(f"Context with name '{context_definition.name}' already exists")
        
        # Generate ID
        context_id = f"{context_definition.context_type.value}-{uuid.uuid4().hex[:8]}"
        
        # Create context
        context = Context(
            mcp_resource_id=context_id,
            name=context_definition.name,
            description=context_definition.description,
            context_type=context_definition.context_type,
            parameters=context_definition.parameters,
            resources=context_definition.resources,
            metadata={
                **context_definition.metadata,
                "created_at": datetime.utcnow().isoformat(),
                "created_by": created_by or "system"
            }
        )
        
        # Add to registry
        self._contexts[context_id] = context
        logger.info(f"Created context: {context_id}")
        
        return context
    
    def update_context(
        self,
        context_id: str,
        updates: Dict[str, Any],
        updated_by: Optional[str] = None
    ) -> Optional[Context]:
        """
        Update an existing context.
        
        Args:
            context_id: Context ID
            updates: Updates to apply
            updated_by: Updater ID
            
        Returns:
            Updated context if found, None otherwise
            
        Raises:
            ValueError: If trying to update immutable fields
        """
        context = self.get_context(context_id)
        if not context:
            return None
        
        # Check for immutable fields
        immutable_fields = ["mcp_resource_id", "mcp_resource_type"]
        for field in immutable_fields:
            if field in updates:
                raise ValueError(f"Cannot update immutable field: {field}")
        
        # Apply updates
        context_dict = context.dict()
        for key, value in updates.items():
            if hasattr(context, key):
                context_dict[key] = value
        
        # Update metadata
        if "metadata" not in context_dict:
            context_dict["metadata"] = {}
        
        context_dict["metadata"]["updated_at"] = datetime.utcnow().isoformat()
        if updated_by:
            context_dict["metadata"]["updated_by"] = updated_by
        
        # Create updated context
        updated_context = Context(**context_dict)
        self._contexts[context_id] = updated_context
        
        logger.info(f"Updated context: {context_id}")
        return updated_context
    
    def delete_context(self, context_id: str) -> bool:
        """
        Delete a context.
        
        Args:
            context_id: Context ID
            
        Returns:
            True if deleted, False if not found
        """
        if context_id not in self._contexts:
            logger.warning(f"Cannot delete: context not found: {context_id}")
            return False
        
        del self._contexts[context_id]
        logger.info(f"Deleted context: {context_id}")
        return True
    
    def record_context_usage(self, context_id: str) -> bool:
        """
        Record that a context was used for job execution.
        
        Args:
            context_id: Context ID
            
        Returns:
            True if updated, False if context not found
        """
        context = self.get_context(context_id)
        if not context:
            return False
        
        # Update context with usage information
        updates = {
            "last_used": datetime.utcnow().isoformat(),
            "job_count": context.job_count + 1
        }
        
        self.update_context(context_id, updates)
        logger.info(f"Recorded usage for context: {context_id}")
        return True 