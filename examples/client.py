#!/usr/bin/env python3
"""
Example client for the Beam MCP server.

This module demonstrates how to use the Beam MCP server API.
"""

import argparse
import json
import requests
import uuid
from typing import Dict, Any, Optional, List
from enum import Enum
import time
import asyncio
from mcp import MCPClient, Context
from mcp.core import MCPRequest, MCPResponse

class RunnerType(str, Enum):
    """Type of runner for executing Apache Beam pipelines."""
    DATAFLOW = "dataflow"
    SPARK = "spark"
    FLINK = "flink"
    DIRECT = "direct"

class JobType(str, Enum):
    """Type of job execution."""
    BATCH = "BATCH"
    STREAMING = "STREAMING"

class MCPContext:
    """MCP context for maintaining state between operations."""
    
    def __init__(
        self, 
        session_id: Optional[str] = None,
        trace_id: Optional[str] = None,
        transaction_id: Optional[str] = None,
        user_id: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize MCP context.
        
        Args:
            session_id (str, optional): Session ID
            trace_id (str, optional): Trace ID
            transaction_id (str, optional): Transaction ID
            user_id (str, optional): User ID
            parameters (Dict[str, Any], optional): Context parameters
        """
        self.session_id = session_id or str(uuid.uuid4())
        self.trace_id = trace_id
        self.transaction_id = transaction_id
        self.user_id = user_id
        self.parameters = parameters or {}
    
    def to_headers(self) -> Dict[str, str]:
        """
        Convert context to request headers.
        
        Returns:
            Dict[str, str]: Request headers
        """
        headers = {
            "MCP-Session-ID": self.session_id
        }
        
        if self.trace_id:
            headers["MCP-Trace-ID"] = self.trace_id
        
        if self.transaction_id:
            headers["MCP-Transaction-ID"] = self.transaction_id
            
        if self.user_id:
            headers["MCP-User-ID"] = self.user_id
            
        return headers
    
    def with_param(self, key: str, value: Any) -> "MCPContext":
        """
        Add a parameter to the context.
        
        Args:
            key (str): Parameter key
            value (Any): Parameter value
            
        Returns:
            MCPContext: Context with new parameter
        """
        self.parameters[key] = value
        return self
    
    def start_transaction(self) -> "MCPContext":
        """
        Start a new transaction.
        
        Returns:
            MCPContext: Context with transaction
        """
        self.transaction_id = str(uuid.uuid4())
        return self
    
    def end_transaction(self) -> "MCPContext":
        """
        End the current transaction.
        
        Returns:
            MCPContext: Context without transaction
        """
        self.transaction_id = None
        return self

class BeamMCPClient:
    """Client for interacting with the Beam MCP server."""
    
    def __init__(self, base_url: str, session_id: Optional[str] = None, user_id: Optional[str] = None):
        """
        Initialize the client.
        
        Args:
            base_url (str): Base URL of the Beam MCP server
            session_id (str, optional): Session ID
            user_id (str, optional): User ID
        """
        self.client = MCPClient(
            base_url=base_url,
            session_id=session_id,
            user_id=user_id
        )
    
    async def get_manifest(self) -> Dict:
        """Get the server's tool manifest."""
        return await self.client.get_manifest()
    
    async def get_context(self) -> Context:
        """Get the current MCP context."""
        return await self.client.get_context()
    
    async def list_runners(self) -> List[Dict]:
        """List available Dataflow runners."""
        response = await self.client.invoke_tool(
            tool_name="list_runners",
            parameters={}
        )
        return response.data
    
    async def create_job(self, job_params: Dict) -> Dict:
        """Create a new Dataflow job."""
        response = await self.client.invoke_tool(
            tool_name="create_job",
            parameters=job_params
        )
        return response.data
    
    async def list_jobs(self) -> List[Dict]:
        """List all Dataflow jobs."""
        response = await self.client.invoke_tool(
            tool_name="list_jobs",
            parameters={}
        )
        return response.data
    
    async def get_job(self, job_id: str) -> Dict:
        """Get details of a specific job."""
        response = await self.client.invoke_tool(
            tool_name="get_job",
            parameters={"job_id": job_id}
        )
        return response.data
    
    async def cancel_job(self, job_id: str) -> Dict:
        """Cancel a running job."""
        response = await self.client.invoke_tool(
            tool_name="cancel_job",
            parameters={"job_id": job_id}
        )
        return response.data
    
    async def drain_job(self, job_id: str) -> Dict:
        """Drain a running job."""
        response = await self.client.invoke_tool(
            tool_name="drain_job",
            parameters={"job_id": job_id}
        )
        return response.data
    
    async def create_savepoint(self, job_id: str, savepoint_params: Dict) -> Dict:
        """Create a savepoint for a job."""
        params = {"job_id": job_id, **savepoint_params}
        response = await self.client.invoke_tool(
            tool_name="create_savepoint",
            parameters=params
        )
        return response.data
    
    async def list_savepoints(self, job_id: str) -> List[Dict]:
        """List savepoints for a job."""
        response = await self.client.invoke_tool(
            tool_name="list_savepoints",
            parameters={"job_id": job_id}
        )
        return response.data
    
    async def get_savepoint(self, job_id: str, savepoint_id: str) -> Dict:
        """Get details of a specific savepoint."""
        response = await self.client.invoke_tool(
            tool_name="get_savepoint",
            parameters={
                "job_id": job_id,
                "savepoint_id": savepoint_id
            }
        )
        return response.data
    
    async def get_metrics(self, job_id: str) -> Dict:
        """Get metrics for a job."""
        response = await self.client.invoke_tool(
            tool_name="get_metrics",
            parameters={"job_id": job_id}
        )
        return response.data

async def main():
    """Example usage of the BeamMCPClient."""
    client = BeamMCPClient(
        base_url="http://localhost:8000",
        session_id="example-session",
        user_id="example-user"
    )
    
    # Get server manifest
    manifest = await client.get_manifest()
    print("Server Manifest:", manifest)
    
    # Get MCP context
    context = await client.get_context()
    print("MCP Context:", context)
    
    # List available runners
    runners = await client.list_runners()
    print("Available Runners:", runners)
    
    # Create a job
    job_params = {
        "job_name": "example-job",
        "pipeline_path": "examples/wordcount.py",
        "runner": "dataflow",
        "project": "my-project",
        "region": "us-central1",
        "temp_location": "gs://my-bucket/temp"
    }
    
    try:
        job = await client.create_job(job_params)
        print("Created Job:", job)
        
        # Get job details
        job_details = await client.get_job(job["job_id"])
        print("Job Details:", job_details)
        
        # List all jobs
        jobs = await client.list_jobs()
        print("All Jobs:", jobs)
        
        # Create a savepoint
        savepoint = await client.create_savepoint(
            job["job_id"],
            {"savepoint_path": "gs://my-bucket/savepoints"}
        )
        print("Created Savepoint:", savepoint)
        
        # Get job metrics
        metrics = await client.get_metrics(job["job_id"])
        print("Job Metrics:", metrics)
        
        # Cancel the job
        result = await client.cancel_job(job["job_id"])
        print("Cancel Result:", result)
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(main()) 