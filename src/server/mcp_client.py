#!/usr/bin/env python3
"""
MCP Client implementation for interacting with the Beam MCP server.
"""

import asyncio
import httpx
import json
import uuid
from typing import Dict, Any, Optional, List, Union
from dataclasses import dataclass

@dataclass
class Context:
    """Context object for MCP client."""
    session_id: str
    user_id: Optional[str] = None
    trace_id: Optional[str] = None
    transaction_id: Optional[str] = None
    parameters: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.parameters is None:
            self.parameters = {}

class MCPError(Exception):
    """Exception raised for MCP client errors."""
    
    def __init__(self, message: str, status_code: Optional[int] = None, data: Optional[Dict] = None):
        self.message = message
        self.status_code = status_code
        self.data = data or {}
        super().__init__(self.message)

class MCPClient:
    """Client for interacting with the Beam MCP server."""
    
    def __init__(
        self, 
        base_url: str, 
        session_id: Optional[str] = None,
        user_id: Optional[str] = None
    ):
        """
        Initialize the MCP client.
        
        Args:
            base_url (str): Base URL of the MCP server
            session_id (str, optional): Session ID for the client
            user_id (str, optional): User ID for the client
        """
        self.base_url = base_url.rstrip('/')
        self.api_prefix = "/api/v1"
        self.context = Context(
            session_id=session_id or str(uuid.uuid4()),
            user_id=user_id
        )
    
    def _get_headers(self) -> Dict[str, str]:
        """
        Get the headers for HTTP requests.
        
        Returns:
            Dict[str, str]: HTTP headers
        """
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "MCP-Session-ID": self.context.session_id
        }
        
        if self.context.user_id:
            headers["MCP-User-ID"] = self.context.user_id
            
        if self.context.trace_id:
            headers["MCP-Trace-ID"] = self.context.trace_id
            
        if self.context.transaction_id:
            headers["MCP-Transaction-ID"] = self.context.transaction_id
            
        return headers
    
    async def get_manifest(self) -> Dict[str, Any]:
        """
        Get the server's tool manifest.
        
        Returns:
            Dict[str, Any]: Tool manifest
        """
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.base_url}{self.api_prefix}/manifest",
                headers=self._get_headers()
            )
            
            if response.status_code != 200:
                raise MCPError(
                    f"Failed to get manifest: {response.text}",
                    status_code=response.status_code
                )
                
            return response.json()
    
    async def get_health(self) -> Dict[str, Any]:
        """
        Get the server's health status.
        
        Returns:
            Dict[str, Any]: Health status
        """
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.base_url}{self.api_prefix}/health",
                headers=self._get_headers()
            )
            
            if response.status_code != 200:
                raise MCPError(
                    f"Failed to get health status: {response.text}",
                    status_code=response.status_code
                )
                
            return response.json()
    
    async def get_context(self) -> Context:
        """
        Get the current MCP context.
        
        Returns:
            Context: Current context
        """
        return self.context
    
    async def invoke_tool(
        self, 
        tool_name: str, 
        parameters: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Invoke a tool on the MCP server.
        
        Args:
            tool_name (str): Name of the tool to invoke
            parameters (Dict[str, Any]): Tool parameters
            
        Returns:
            Dict[str, Any]: Tool response
        """
        # Map tool_name to the appropriate API endpoint
        endpoint_map = {
            "list_runners": "runners",
            "create_job": "jobs",
            "list_jobs": "jobs",
            "get_job": f"jobs/{parameters.get('job_id', '')}",
            "cancel_job": f"jobs/{parameters.get('job_id', '')}/cancel",
            "drain_job": f"jobs/{parameters.get('job_id', '')}/drain",
            "create_savepoint": f"jobs/{parameters.get('job_id', '')}/savepoints",
            "list_savepoints": f"jobs/{parameters.get('job_id', '')}/savepoints",
            "get_savepoint": f"jobs/{parameters.get('job_id', '')}/savepoints/{parameters.get('savepoint_id', '')}",
            "get_metrics": f"jobs/{parameters.get('job_id', '')}/metrics",
        }
        
        endpoint = endpoint_map.get(tool_name)
        if not endpoint:
            raise MCPError(f"Unknown tool name: {tool_name}")
        
        url = f"{self.base_url}{self.api_prefix}/{endpoint}"
        
        method_map = {
            "list_runners": "GET",
            "create_job": "POST",
            "list_jobs": "GET",
            "get_job": "GET",
            "cancel_job": "POST",
            "drain_job": "POST",
            "create_savepoint": "POST",
            "list_savepoints": "GET",
            "get_savepoint": "GET",
            "get_metrics": "GET",
        }
        
        method = method_map.get(tool_name, "POST")
        
        async with httpx.AsyncClient() as client:
            if method == "GET":
                # For GET requests, convert parameters to query params
                # Exclude job_id, savepoint_id from query params as they're in the URL
                query_params = {k: v for k, v in parameters.items() 
                              if k not in ('job_id', 'savepoint_id')}
                response = await client.get(
                    url,
                    headers=self._get_headers(),
                    params=query_params
                )
            else:
                # For POST requests, send parameters in the request body
                # Exclude job_id, savepoint_id from body as they're in the URL
                body_params = {k: v for k, v in parameters.items() 
                             if k not in ('job_id', 'savepoint_id')}
                response = await client.post(
                    url,
                    headers=self._get_headers(),
                    json=body_params
                )
            
            if response.status_code not in (200, 201, 202):
                raise MCPError(
                    f"Failed to invoke tool '{tool_name}': {response.text}",
                    status_code=response.status_code
                )
            
            return response.json() 