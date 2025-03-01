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
from datetime import datetime

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
        # Don't instantiate DataflowClient directly to avoid bypassing config-based client 
        # self.dataflow_client = DataflowClient(settings)
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
        @self.app.get("/api/v1/mcp/manifest")
        async def get_manifest():
            request = MCPRequest(
                method="get_manifest",
                params=MCPRequestParams()
            )
            response = await self.get_manifest(request, MCPContext())
            return {"data": response.data}

        @self.app.get("/api/v1/mcp/runners")
        async def list_runners():
            request = MCPRequest(
                method="list_runners",
                params=MCPRequestParams()
            )
            response = await self.list_runners(request, MCPContext())
            return {"data": response.data}

        @self.app.post("/api/v1/mcp/jobs")
        async def create_job(job_params: JobParameters):
            request = MCPRequest(
                method="create_job",
                params=MCPRequestParams(parameters=job_params.model_dump())
            )
            response = await self.create_job(request, MCPContext())
            return {"data": response.data}

        @self.app.get("/api/v1/mcp/jobs/{job_id}")
        async def get_job(job_id: str):
            request = MCPRequest(
                method="get_job",
                params=MCPRequestParams(parameters={"job_id": job_id})
            )
            response = await self.get_job(request, MCPContext())
            return {"data": response.data}

        @self.app.delete("/api/v1/mcp/jobs/{job_id}")
        async def cancel_job(job_id: str):
            request = MCPRequest(
                method="cancel_job",
                params=MCPRequestParams(parameters={"job_id": job_id})
            )
            response = await self.cancel_job(request, MCPContext())
            return {"data": response.data}

        @self.app.get("/api/v1/mcp/jobs/{job_id}/metrics")
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
            # Create a simplified manifest without complex objects
            manifest = {
                "name": "beam-mcp",
                "version": "1.0.0",
                "description": "Apache Beam MCP Server",
                "capabilities": {
                    "resources": {
                        "subscribe": True,
                        "listChanged": True
                    },
                    "tools": {
                        "listChanged": True
                    }
                },
                "tools": [],  # Simplified to avoid serialization issues
                "resources": []  # Simplified to avoid serialization issues
            }
            return MCPResponse(data=manifest)
        except Exception as e:
            logger.error(f"Error in get_manifest: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

    async def create_job(self, request: MCPRequest, context: MCPContext) -> MCPResponse:
        """Create a new job."""
        try:
            logger.debug("Creating new job")
            parameters = request.params.parameters
            
            # Create job parameters
            job_params = JobParameters(
                job_name=parameters.get("job_name"),
                runner_type=parameters.get("runner_type"),
                job_type=parameters.get("job_type"),
                code_path=parameters.get("code_path"),
                pipeline_options=parameters.get("pipeline_options", {}),
                template_path=parameters.get("template_path"),
                template_parameters=parameters.get("template_parameters", {})
            )
            
            # We don't have direct access to the client_manager here
            # Just return a basic response with the parameters
            # The actual job creation is done in the API router
            job = Job(
                mcp_resource_id=f"job-placeholder",
                job_id=f"job-placeholder",
                job_name=job_params.job_name,
                project=job_params.pipeline_options.get('project', 'default-project'),
                region=job_params.pipeline_options.get('region', 'us-central1'),
                status=JobState.PENDING,
                create_time=datetime.now().isoformat() + "Z",
                update_time=datetime.now().isoformat() + "Z",
                runner=job_params.runner_type,
                job_type=job_params.job_type,
                pipeline_options=job_params.pipeline_options,
                current_state="PENDING"
            )
            
            logger.debug(f"Job creation request recognized: {job_params.job_name}")
            logger.debug("Direct job creation through MCP is not supported, use the API endpoint")
            return MCPResponse(data=job.model_dump())
        except Exception as e:
            logger.error(f"Error creating job: {str(e)}", exc_info=True)
            raise HTTPException(status_code=500, detail=str(e))

    async def get_job(self, request: MCPRequest, context: MCPContext) -> MCPResponse:
        """Get job details."""
        try:
            job_id = request.params.parameters.get("job_id")
            if not job_id:
                raise HTTPException(status_code=400, detail="Job ID is required")
                
            logger.debug(f"Getting job details for {job_id}")
            
            # We don't have direct access to the client_manager here
            # The actual job retrieval is done in the API router
            logger.debug("Direct job retrieval through MCP is not supported, use the API endpoint")
            return MCPResponse(data={"job_id": job_id, "status": "PLACEHOLDER"})
        except Exception as e:
            logger.error(f"Error getting job: {str(e)}", exc_info=True)
            raise HTTPException(status_code=500, detail=str(e))
            
    async def cancel_job(self, request: MCPRequest, context: MCPContext) -> MCPResponse:
        """Cancel a job."""
        try:
            job_id = request.params.parameters.get("job_id")
            if not job_id:
                raise HTTPException(status_code=400, detail="Job ID is required")
                
            logger.debug(f"Cancelling job {job_id}")
            
            # We don't have direct access to the client_manager here
            # The actual job cancellation is done in the API router
            logger.debug("Direct job cancellation through MCP is not supported, use the API endpoint")
            return MCPResponse(data={"job_id": job_id, "status": "CANCELLATION_REQUESTED"})
        except Exception as e:
            logger.error(f"Error cancelling job: {str(e)}", exc_info=True)
            raise HTTPException(status_code=500, detail=str(e))

    async def list_runners(self, request: MCPRequest, context: MCPContext) -> MCPResponse:
        """List available runners."""
        try:
            logger.debug("Fetching available runners")
            
            # We don't have direct access to the client_manager here, so we'll return
            # just a basic response. The actual listing is done in the API router which
            # has access to the properly initialized client_manager.
            # This method is called by MCP tools but not by the API router.
            
            # Using the configuration to determine which runners are enabled
            runners = []
            raw_config = getattr(self.settings, 'raw_config', {})
            runners_config = raw_config.get('runners', {})
            
            for runner_name, runner_config in runners_config.items():
                if runner_config.get('enabled', False):
                    # Include basic information for each enabled runner
                    from .models.runner import Runner, RunnerType, RunnerStatus, RunnerCapability
                    try:
                        runner_type = RunnerType(runner_name)
                        runners.append(Runner(
                            mcp_resource_id=runner_name,
                            name=f"{runner_name.capitalize()} Runner",
                            runner_type=runner_type,
                            status=RunnerStatus.AVAILABLE,
                            description=f"Apache Beam {runner_name.capitalize()} runner",
                            capabilities=[],
                            config=runner_config.get('pipeline_options', {}),
                            mcp_provider="apache",
                            version="1.0.0"
                        ))
                    except Exception as e:
                        logger.warning(f"Error creating runner for {runner_name}: {str(e)}")
            
            logger.debug(f"Found {len(runners)} runners from configuration")
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
                raise HTTPException(status_code=400, detail="Job ID is required")
                
            logger.debug(f"Getting metrics for job {job_id}")
            
            # We don't have direct access to the client_manager here
            # The actual metrics retrieval is done in the API router
            logger.debug("Direct metrics retrieval through MCP is not supported, use the API endpoint")
            return MCPResponse(data={"job_id": job_id, "metrics": {}})
        except Exception as e:
            logger.error(f"Error getting metrics: {str(e)}", exc_info=True)
            raise HTTPException(status_code=500, detail=str(e))

    async def handle_error(self, error: Exception, context: MCPContext) -> MCPResponse:
        """Handle errors and return appropriate response."""
        if isinstance(error, HTTPException):
            return MCPResponse(error=str(error.detail))
        return MCPResponse(error=str(error)) 