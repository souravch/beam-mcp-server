"""
Example showing how to use capability-based feature routers.

This example demonstrates how to create and use routers that only expose endpoints
when specific MCP features are enabled by the client.
"""

from fastapi import FastAPI, APIRouter, Depends, HTTPException
from pydantic import BaseModel

from server.mcp.app_integration import (
    integrate_mcp_with_app, 
    create_mcp_feature_router,
    get_feature_dependency
)
from server.mcp.models import ConnectionState
from server.mcp import get_mcp_connection_dependency


# Create app
app = FastAPI(title="MCP Feature Router Example")

# Integrate MCP with app
integrate_mcp_with_app(app)

# Define a model for job creation
class JobRequest(BaseModel):
    name: str
    pipeline: str


# Use feature routers

# 1. Create a router for job management feature
job_router = create_mcp_feature_router(
    app=app,
    feature_name="tool.job_management",
    prefix="/api/v1/jobs",
    tags=["jobs"]
)

# Add endpoints to job router
@job_router.add_api_route(
    path="",
    methods=["GET"],
    endpoint=lambda: {"jobs": [{"id": "123", "name": "Example Job"}]}
)
async def list_jobs():
    """List all jobs (requires job_management feature)."""
    return {"jobs": [{"id": "123", "name": "Example Job"}]}

@job_router.add_api_route(
    path="",
    methods=["POST"],
    endpoint=lambda job: {"id": "456", "name": job.name}
)
async def create_job(job: JobRequest):
    """Create a new job (requires job_management feature)."""
    return {"id": "456", "name": job.name}

# Include the job router in the app
app.include_router(job_router.get_router())


# 2. Create a router for pipeline monitoring feature
pipeline_router = create_mcp_feature_router(
    app=app,
    feature_name="tool.pipeline_monitoring",
    prefix="/api/v1/pipelines",
    tags=["pipelines"]
)

# Add endpoints to pipeline router
@pipeline_router.add_api_route(
    path="",
    methods=["GET"],
    endpoint=lambda: {"pipelines": [{"id": "789", "name": "Example Pipeline"}]}
)
async def list_pipelines():
    """List all pipelines (requires pipeline_monitoring feature)."""
    return {"pipelines": [{"id": "789", "name": "Example Pipeline"}]}

# Include the pipeline router in the app
app.include_router(pipeline_router.get_router())


# 3. Using feature dependency directly
@app.get("/api/v1/runners")
async def list_runners(
    connection: ConnectionState = get_feature_dependency("tool.runner_management")
):
    """List all runners (requires runner_management feature)."""
    return {"runners": [{"id": "abc", "name": "Example Runner"}]}


# Regular endpoint that doesn't require specific features
@app.get("/api/v1/status")
async def get_status():
    """Get server status (no feature requirements)."""
    return {"status": "ok"} 