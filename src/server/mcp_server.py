from typing import Dict, List, Optional
from fastapi import HTTPException
from mcp import FastMCP, Resource, Tool, Context, Capability
from mcp.core import MCPResponse, MCPRequest, MCPError
from mcp.protocol import ToolManifest, ToolCapability, OperationType

from .models.job import Job, JobParameters, JobStatus
from .models.savepoint import Savepoint, SavepointRequest
from .models.runner import Runner
from .services.dataflow_client import DataflowClient
from .config import Settings

class DataflowMCPServer(FastMCP):
    def __init__(self, settings: Settings):
        super().__init__(
            name="dataflow-mcp",
            version="1.0.0",
            description="Apache Beam Dataflow MCP Server",
            capabilities=[
                Capability.TOOL_DISCOVERY,    # Support tool discovery
                Capability.CONTEXT_MANAGEMENT, # Support context management
                Capability.STATE_MANAGEMENT,   # Support state management
                Capability.ASYNC_OPERATIONS,   # Support async operations
                Capability.BATCH_OPERATIONS,   # Support batch operations
                Capability.STREAMING,          # Support streaming operations
                Capability.MONITORING,         # Support monitoring
                Capability.LOGGING,           # Support logging
                Capability.ERROR_HANDLING     # Support error handling
            ]
        )
        self.settings = settings
        self.dataflow_client = DataflowClient(settings)
        
        # Register resources
        self.register_resources([
            Resource(
                name="job",
                description="Dataflow job resource",
                operations=[
                    OperationType.CREATE,
                    OperationType.READ,
                    OperationType.UPDATE,
                    OperationType.DELETE,
                    OperationType.LIST
                ],
                schema=Job.schema()
            ),
            Resource(
                name="savepoint",
                description="Dataflow savepoint resource",
                operations=[
                    OperationType.CREATE,
                    OperationType.READ,
                    OperationType.LIST
                ],
                schema=Savepoint.schema()
            ),
            Resource(
                name="runner",
                description="Dataflow runner resource",
                operations=[OperationType.LIST],
                schema=Runner.schema()
            ),
            Resource(
                name="metrics",
                description="Job metrics resource",
                operations=[OperationType.READ],
                schema=None
            )
        ])
        
        # Register tools with proper MCP tool capabilities
        self.register_tools([
            Tool(
                name="create_job",
                description="Create a new Dataflow job",
                resource="job",
                operation=OperationType.CREATE,
                handler=self.create_job,
                capabilities=[
                    ToolCapability.ASYNC,
                    ToolCapability.IDEMPOTENT,
                    ToolCapability.REQUIRES_CONTEXT
                ],
                parameters=JobParameters.schema(),
                returns=Job.schema()
            ),
            Tool(
                name="list_jobs",
                description="List all Dataflow jobs",
                resource="job",
                operation=OperationType.LIST,
                handler=self.list_jobs,
                capabilities=[
                    ToolCapability.PAGINATED,
                    ToolCapability.FILTERABLE
                ]
            ),
            Tool(
                name="get_job",
                description="Get details of a specific job",
                resource="job",
                operation=OperationType.READ,
                handler=self.get_job,
                capabilities=[ToolCapability.CACHEABLE]
            ),
            Tool(
                name="cancel_job",
                description="Cancel a running job",
                resource="job",
                operation=OperationType.DELETE,
                handler=self.cancel_job,
                capabilities=[
                    ToolCapability.ASYNC,
                    ToolCapability.REQUIRES_CONTEXT
                ]
            ),
            Tool(
                name="drain_job",
                description="Drain a running job",
                resource="job",
                operation=OperationType.UPDATE,
                handler=self.drain_job,
                capabilities=[
                    ToolCapability.ASYNC,
                    ToolCapability.REQUIRES_CONTEXT
                ]
            ),
            Tool(
                name="create_savepoint",
                description="Create a savepoint for a job",
                resource="savepoint",
                operation=OperationType.CREATE,
                handler=self.create_savepoint,
                capabilities=[
                    ToolCapability.ASYNC,
                    ToolCapability.IDEMPOTENT
                ],
                parameters=SavepointRequest.schema()
            ),
            Tool(
                name="list_savepoints",
                description="List savepoints for a job",
                resource="savepoint",
                operation=OperationType.LIST,
                handler=self.list_savepoints,
                capabilities=[
                    ToolCapability.PAGINATED,
                    ToolCapability.FILTERABLE
                ]
            ),
            Tool(
                name="get_savepoint",
                description="Get details of a specific savepoint",
                resource="savepoint",
                operation=OperationType.READ,
                handler=self.get_savepoint,
                capabilities=[ToolCapability.CACHEABLE]
            ),
            Tool(
                name="list_runners",
                description="List available Dataflow runners",
                resource="runner",
                operation=OperationType.LIST,
                handler=self.list_runners,
                capabilities=[ToolCapability.CACHEABLE]
            ),
            Tool(
                name="get_metrics",
                description="Get metrics for a job",
                resource="metrics",
                operation=OperationType.READ,
                handler=self.get_metrics,
                capabilities=[
                    ToolCapability.STREAMING,
                    ToolCapability.CACHEABLE
                ]
            )
        ])

    async def validate_context(self, context: Context) -> None:
        """Validate the MCP context."""
        if not context.session_id:
            raise MCPError("Missing session_id in context")
        if not context.trace_id:
            context.trace_id = f"trace-{context.session_id}"

    async def create_job(self, request: MCPRequest, context: Context) -> MCPResponse:
        """Create a new Dataflow job."""
        await self.validate_context(context)
        try:
            params = JobParameters(**request.parameters)
            job = await self.dataflow_client.create_job(params)
            return MCPResponse(
                data=job.dict(),
                context=context,
                metadata={
                    "operation_id": f"create-job-{job.job_id}",
                    "async": True
                }
            )
        except Exception as e:
            raise MCPError(str(e))

    async def list_jobs(self, request: MCPRequest, context: Context) -> MCPResponse:
        """List all Dataflow jobs."""
        await self.validate_context(context)
        try:
            jobs = await self.dataflow_client.list_jobs()
            return MCPResponse(
                data=[job.dict() for job in jobs],
                context=context,
                metadata={"total": len(jobs)}
            )
        except Exception as e:
            raise MCPError(str(e))

    async def get_job(self, request: MCPRequest, context: Context) -> MCPResponse:
        try:
            job_id = request.parameters.get("job_id")
            if not job_id:
                raise HTTPException(status_code=400, detail="job_id is required")
            job = await self.dataflow_client.get_job(job_id)
            return MCPResponse(data=job.dict())
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    async def cancel_job(self, request: MCPRequest, context: Context) -> MCPResponse:
        try:
            job_id = request.parameters.get("job_id")
            if not job_id:
                raise HTTPException(status_code=400, detail="job_id is required")
            result = await self.dataflow_client.cancel_job(job_id)
            return MCPResponse(data={"status": "cancelled", "job_id": job_id})
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    async def drain_job(self, request: MCPRequest, context: Context) -> MCPResponse:
        try:
            job_id = request.parameters.get("job_id")
            if not job_id:
                raise HTTPException(status_code=400, detail="job_id is required")
            result = await self.dataflow_client.drain_job(job_id)
            return MCPResponse(data={"status": "draining", "job_id": job_id})
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    async def create_savepoint(self, request: MCPRequest, context: Context) -> MCPResponse:
        try:
            params = SavepointRequest(**request.parameters)
            savepoint = await self.dataflow_client.create_savepoint(params)
            return MCPResponse(data=savepoint.dict())
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    async def list_savepoints(self, request: MCPRequest, context: Context) -> MCPResponse:
        try:
            job_id = request.parameters.get("job_id")
            if not job_id:
                raise HTTPException(status_code=400, detail="job_id is required")
            savepoints = await self.dataflow_client.list_savepoints(job_id)
            return MCPResponse(data=[sp.dict() for sp in savepoints])
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    async def get_savepoint(self, request: MCPRequest, context: Context) -> MCPResponse:
        try:
            job_id = request.parameters.get("job_id")
            savepoint_id = request.parameters.get("savepoint_id")
            if not job_id or not savepoint_id:
                raise HTTPException(status_code=400, detail="job_id and savepoint_id are required")
            savepoint = await self.dataflow_client.get_savepoint(job_id, savepoint_id)
            return MCPResponse(data=savepoint.dict())
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    async def list_runners(self, request: MCPRequest, context: Context) -> MCPResponse:
        try:
            runners = await self.dataflow_client.list_runners()
            return MCPResponse(data=[runner.dict() for runner in runners])
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    async def get_metrics(self, request: MCPRequest, context: Context) -> MCPResponse:
        try:
            job_id = request.parameters.get("job_id")
            if not job_id:
                raise HTTPException(status_code=400, detail="job_id is required")
            metrics = await self.dataflow_client.get_metrics(job_id)
            return MCPResponse(data=metrics)
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    async def handle_error(self, error: Exception, context: Context) -> MCPResponse:
        """Handle errors in a standardized way."""
        if isinstance(error, MCPError):
            return MCPResponse(
                error=error.message,
                context=context,
                metadata={"error_type": "mcp_error"}
            )
        return MCPResponse(
            error=str(error),
            context=context,
            metadata={"error_type": "internal_error"}
        ) 