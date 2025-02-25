"""
Context models for the Apache Beam MCP server.

This module defines data models for MCP context handling.
"""

from typing import Dict, Any, Optional
from pydantic import BaseModel, Field
import uuid

class MCPContext(BaseModel):
    """
    Context information for MCP operations.
    
    The MCP context allows for maintaining state between operations,
    tracking requests, and providing consistent behavior for stateful
    operations.
    """
    session_id: str = Field(
        default_factory=lambda: str(uuid.uuid4()),
        description="Session identifier"
    )
    trace_id: Optional[str] = Field(
        None, 
        description="Trace identifier for distributed tracing"
    )
    transaction_id: Optional[str] = Field(
        None, 
        description="Transaction identifier for multi-step operations"
    )
    parameters: Dict[str, Any] = Field(
        default_factory=dict, 
        description="Context parameters"
    )
    user_id: Optional[str] = Field(
        None, 
        description="User identifier"
    )
    version: str = Field(
        "1.0", 
        description="MCP context version"
    )
    
    def with_param(self, key: str, value: Any) -> "MCPContext":
        """
        Add a parameter to the context.
        
        Args:
            key (str): Parameter key
            value (Any): Parameter value
            
        Returns:
            MCPContext: Context with new parameter
        """
        context = self.copy()
        context.parameters[key] = value
        return context
    
    def with_transaction(self, transaction_id: str) -> "MCPContext":
        """
        Start a new transaction.
        
        Args:
            transaction_id (str): Transaction ID
            
        Returns:
            MCPContext: Context with transaction
        """
        context = self.copy()
        context.transaction_id = transaction_id
        return context
    
    def with_trace(self, trace_id: str) -> "MCPContext":
        """
        Set trace ID for distributed tracing.
        
        Args:
            trace_id (str): Trace ID
            
        Returns:
            MCPContext: Context with trace
        """
        context = self.copy()
        context.trace_id = trace_id
        return context 