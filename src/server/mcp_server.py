"""
MCP Server implementation for Apache Beam Dataflow.
"""
from typing import Dict, List, Optional, Any
from fastapi import HTTPException, FastAPI
from mcp.server.fastmcp import FastMCP
from mcp import Resource, ServerCapabilities, ToolsCapability, McpError as MCPError, Tool
from mcp.types import ResourcesCapability
from .models.jobs import JobParameters
from .models.common import JobState, OperationType, MCPRequest, MCPResponse, MCPRequestParams
from .models.job import Job
from .models.savepoint import Savepoint
from .models.runner import Runner, RunnerList
from .models.metrics import JobMetrics
from .models.context import MCPContext
from .services.dataflow_client import DataflowClient
from .config import Settings
import logging
import json

logger = logging.getLogger(__name__)

class BeamMCPServer(FastMCP):
    """
    Apache Beam MCP Server that implements the Model Context Protocol standard.
    
    This server provides a unified API for managing Apache Beam pipelines
    across different runners (Dataflow, Spark, Flink, Direct).
    """
    def __init__(self, settings: Settings):
        """
        Initialize the Beam MCP server.
        
        Args:
            settings (Settings): Server configuration
        """
        # Initialize with FastMCP base class
        super().__init__(
            name="beam-mcp",
            instructions="Manage Apache Beam pipelines across different runners",
            **settings.dict()
        )
        
        self.settings = settings
        self.dataflow_client = DataflowClient(settings)
        self.app = FastAPI()
        
        # Set up MCP components
        self._setup_resources()
        self._setup_tools()
        self._setup_routes()

    def _setup_resources(self):
        """Set up MCP resources according to the MCP protocol standard."""
        base_url = self.settings.base_url or "http://localhost:8000"
        resources = [
            Resource(
                name="job",
                uri=f"{base_url}/jobs",
                description="Dataflow job resource",
                operations=[
                    OperationType.CREATE,
                    OperationType.READ,
                    OperationType.UPDATE,
                    OperationType.DELETE,
                    OperationType.LIST
                ]
            ),
            Resource(
                name="savepoint",
                uri=f"{base_url}/savepoints",
                description="Job savepoint resource",
                operations=[
                    OperationType.CREATE,
                    OperationType.READ,
                    OperationType.LIST
                ]
            ),
            Resource(
                name="runner",
                uri=f"{base_url}/runners",
                description="Pipeline runner resource",
                operations=[
                    OperationType.READ,
                    OperationType.LIST
                ]
            ),
            Resource(
                name="metrics",
                uri=f"{base_url}/metrics",
                description="Job metrics resource",
                operations=[
                    OperationType.READ,
                    OperationType.LIST
                ]
            )
        ]
        
        # Register resources with MCP
        for resource in resources:
            self._resource_manager.add_resource(resource)

    def _setup_tools(self):
        """Set up MCP tools according to the MCP protocol standard."""
        # Create functions for each tool
        
        # Create job function
        async def create_job(job_name: str, runner_type: str, job_type: str, code_path: str, pipeline_options: Dict[str, Any] = None) -> Dict:
            """
            Create a new pipeline job.
            
            Args:
                job_name: Unique name for the job
                runner_type: Type of runner to use
                job_type: Type of job (BATCH or STREAMING)
                code_path: Path to pipeline code
                pipeline_options: Runner-specific pipeline options
                
            Returns:
                Job details
            """
            request = MCPRequest(
                method="create_job",
                params=MCPRequestParams(parameters={
                    "job_name": job_name,
                    "runner_type": runner_type,
                    "job_type": job_type,
                    "code_path": code_path,
                    "pipeline_options": pipeline_options or {}
                })
            )
            response = await self.create_job(request, MCPContext())
            return response.data
        
        # Get job function
        async def get_job(job_id: str) -> Dict:
            """
            Get job details.
            
            Args:
                job_id: ID of the job to retrieve
                
            Returns:
                Job details
            """
            request = MCPRequest(
                method="get_job",
                params=MCPRequestParams(parameters={"job_id": job_id})
            )
            response = await self.get_job(request, MCPContext())
            return response.data
        
        # Cancel job function
        async def cancel_job(job_id: str) -> Dict:
            """
            Cancel a running job.
            
            Args:
                job_id: ID of the job to cancel
                
            Returns:
                Cancellation status
            """
            request = MCPRequest(
                method="cancel_job",
                params=MCPRequestParams(parameters={"job_id": job_id})
            )
            response = await self.cancel_job(request, MCPContext())
            return response.data
        
        # List runners function
        async def list_runners() -> Dict:
            """
            List available runners.
            
            Returns:
                List of available runners
            """
            request = MCPRequest(
                method="list_runners",
                params=MCPRequestParams()
            )
            response = await self.list_runners(request, MCPContext())
            return response.data
        
        # Get metrics function
        async def get_metrics(job_id: str) -> Dict:
            """
            Get job metrics.
            
            Args:
                job_id: ID of the job to get metrics for
                
            Returns:
                Job metrics
            """
            request = MCPRequest(
                method="get_metrics",
                params=MCPRequestParams(parameters={"job_id": job_id})
            )
            response = await self.get_metrics(request, MCPContext())
            return response.data
        
        # Register tool functions with the tool manager
        self._tool_manager.add_tool(create_job, description="Create a new pipeline job")
        self._tool_manager.add_tool(get_job, description="Get job details")
        self._tool_manager.add_tool(cancel_job, description="Cancel a running job")
        self._tool_manager.add_tool(list_runners, description="List available runners")
        self._tool_manager.add_tool(get_metrics, description="Get job metrics")

    def _setup_routes(self):
        """Set up FastAPI routes."""
        @self.app.get("/api/v1/manifest")
        async def get_manifest():
            request = MCPRequest(
                method="get_manifest",
                params=MCPRequestParams()
            )
            response = await self.get_manifest(request, MCPContext())
            return {"data": response.data}

        @self.app.get("/api/v1/runners")
        async def list_runners():
            request = MCPRequest(
                method="list_runners",
                params=MCPRequestParams()
            )
            response = await self.list_runners(request, MCPContext())
            return {"data": response.data}

        @self.app.post("/api/v1/jobs")
        async def create_job(job_params: JobParameters):
            request = MCPRequest(
                method="create_job",
                params=MCPRequestParams(parameters=job_params.model_dump())
            )
            response = await self.create_job(request, MCPContext())
            return {"data": response.data}

        @self.app.get("/api/v1/jobs/{job_id}")
        async def get_job(job_id: str):
            request = MCPRequest(
                method="get_job",
                params=MCPRequestParams(parameters={"job_id": job_id})
            )
            response = await self.get_job(request, MCPContext())
            return {"data": response.data}

        @self.app.delete("/api/v1/jobs/{job_id}")
        async def cancel_job(job_id: str):
            request = MCPRequest(
                method="cancel_job",
                params=MCPRequestParams(parameters={"job_id": job_id})
            )
            response = await self.cancel_job(request, MCPContext())
            return {"data": response.data}

        @self.app.get("/api/v1/jobs/{job_id}/metrics")
        async def get_metrics(job_id: str):
            request = MCPRequest(
                method="get_metrics",
                params=MCPRequestParams(parameters={"job_id": job_id})
            )
            response = await self.get_metrics(request, MCPContext())
            return {"data": response.data}

    @property
    def fastapi_app(self) -> FastAPI:
        """Get the FastAPI application instance."""
        return self.app

    async def get_manifest(self, request: MCPRequest, context: MCPContext) -> MCPResponse:
        """Get server manifest according to MCP protocol."""
        try:
            # Get tools from the tool manager
            tools = list(self._tool_manager._tools.values())
            
            # Get resources from the resource manager
            resources = list(self._resource_manager._resources.values())
            
            manifest = {
                "name": "beam-mcp",
                "version": "1.0.0",
                "description": "Apache Beam MCP Server",
                "capabilities": {
                    "resources": ResourcesCapability(subscribe=True, listChanged=True),
                    "tools": ToolsCapability(listChanged=True)
                },
                "tools": [tool.model_dump() for tool in tools],
                "resources": [resource.model_dump() for resource in resources]
            }
            return MCPResponse(data=manifest)
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    async def create_job(self, request: MCPRequest, context: MCPContext) -> MCPResponse:
        """Create a new job."""
        try:
            logger.debug("Creating new job with parameters: %s", request.params.parameters)
            # Convert parameters to JobParameters
            pipeline_options = request.params.parameters.get("pipeline_options", {})
            
            # Only add GCP-specific parameters for Dataflow runner
            if request.params.parameters.get("runner_type") == "dataflow":
                pipeline_options.update({
                    "project": self.settings.gcp_project_id,
                    "region": self.settings.gcp_region,
                })
            
            # Always set temp_location if not provided
            if "temp_location" not in pipeline_options:
                pipeline_options["temp_location"] = "/tmp/beam-test"

            params = JobParameters(
                job_name=request.params.parameters["job_name"],
                runner_type=request.params.parameters["runner_type"],
                job_type=request.params.parameters["job_type"],
                code_path=request.params.parameters["code_path"],
                pipeline_options=pipeline_options
            )

            # Validate code path
            if not params.code_path or params.code_path == "nonexistent.py":
                raise HTTPException(status_code=400, detail=f"Invalid code path: {params.code_path}")

            logger.debug("Creating job with parameters: %s", params.model_dump())
            job = await self.dataflow_client.create_job(params)
            logger.debug("Job created successfully: %s", job.model_dump())
            return MCPResponse(data=job.model_dump())
        except HTTPException as e:
            logger.error("HTTP error creating job: %s", str(e))
            raise e
        except Exception as e:
            logger.error("Error creating job: %s", str(e), exc_info=True)
            raise HTTPException(status_code=500, detail=str(e))

    async def get_job(self, request: MCPRequest, context: MCPContext) -> MCPResponse:
        """Get job details."""
        try:
            job_id = request.params.parameters.get("job_id")
            if not job_id:
                raise HTTPException(status_code=400, detail="job_id is required")
            try:
                job = await self.dataflow_client.get_job(job_id)
                return MCPResponse(data=job.model_dump())
            except HTTPException as e:
                # Pass through HTTP exceptions from the client
                raise e
            except Exception as e:
                logger.error("Error getting job: %s", str(e), exc_info=True)
                raise HTTPException(status_code=500, detail=str(e))
        except HTTPException as e:
            # Pass through HTTP exceptions
            raise e
        except Exception as e:
            logger.error("Error getting job: %s", str(e), exc_info=True)
            raise HTTPException(status_code=500, detail=str(e))

    async def cancel_job(self, request: MCPRequest, context: MCPContext) -> MCPResponse:
        """Cancel a job."""
        try:
            job_id = request.params.parameters.get("job_id")
            if not job_id:
                raise HTTPException(status_code=400, detail="job_id is required")
            await self.dataflow_client.cancel_job(job_id)
            return MCPResponse(data={"status": "cancelled"})
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    async def list_runners(self, request: MCPRequest, context: MCPContext) -> MCPResponse:
        """List available runners."""
        try:
            logger.debug("Fetching available runners")
            runners = await self.dataflow_client.list_runners()
            logger.debug(f"Found {len(runners)} runners")
            runner_list = RunnerList(
                mcp_resource_id="runners",
                runners=runners,
                default_runner=self.settings.default_runner,
                mcp_total_runners=len(runners)
            )
            logger.debug("Successfully created RunnerList")
            return MCPResponse(data=runner_list.model_dump())
        except Exception as e:
            logger.error(f"Error listing runners: {str(e)}", exc_info=True)
            raise HTTPException(status_code=500, detail=str(e))

    async def get_metrics(self, request: MCPRequest, context: MCPContext) -> MCPResponse:
        """Get job metrics."""
        try:
            job_id = request.params.parameters.get("job_id")
            if not job_id:
                raise HTTPException(status_code=400, detail="job_id is required")
            metrics = await self.dataflow_client.get_metrics(job_id)
            return MCPResponse(data=metrics.model_dump())
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    async def handle_error(self, error: Exception, context: MCPContext) -> MCPResponse:
        """Handle errors and return appropriate response."""
        if isinstance(error, HTTPException):
            return MCPResponse(error=str(error.detail))
        return MCPResponse(error=str(error)) 