"""
Manifest API Router for the Apache Beam MCP Server.

This module provides endpoints for tool discovery and MCP protocol metadata.
"""

import logging
import os
from datetime import datetime
from fastapi import APIRouter, Request, Depends

from ..models import LLMToolResponse, MCPContext
from ..models.common import RunnerType, JobType

logger = logging.getLogger(__name__)

# Create router
router = APIRouter()

@router.get("/manifest", response_model=LLMToolResponse, summary="Get tool manifest", tags=["Manifest"])
async def get_manifest(request: Request):
    """
    Get the tool manifest describing this MCP server's capabilities.
    
    Returns a complete description of all operations, parameters, and capabilities
    in a format designed for discovery by LLMs and agents.
    
    This endpoint is used by LLMs and agent frameworks to discover what operations
    this MCP server supports and how to use them.
    """
    manifest = {
        "schema_version": "1.0",
        "tool_type": "mcp_server",
        "name": "beam_mcp",
        "display_name": "Apache Beam MCP Server",
        "description": "Model Context Protocol server for managing Apache Beam data pipelines",
        "capabilities": [
            "job_management",
            "pipeline_monitoring",
            "savepoint_management",
            "runner_management"
        ],
        "runner_types": [rt.value for rt in RunnerType],
        "job_types": [jt.value for jt in JobType],
        "authentication": {
            "type": request.app.state.config.get("security", {}).get("auth_type", "none")
        },
        "operations": {
            "create_job": {
                "description": "Create a new pipeline job",
                "path": "/api/v1/jobs",
                "method": "POST",
                "parameters": {
                    "job_name": {"type": "string", "description": "Unique name for the job"},
                    "runner_type": {"type": "string", "description": "Type of runner to use", "enum": [rt.value for rt in RunnerType]},
                    "job_type": {"type": "string", "description": "Type of job (BATCH or STREAMING)", "enum": [jt.value for jt in JobType]},
                    "pipeline_options": {"type": "object", "description": "Runner-specific pipeline options"},
                    "template_path": {"type": "string", "description": "Path to pipeline template", "optional": True},
                    "template_parameters": {"type": "object", "description": "Parameters for the pipeline template", "optional": True},
                    "code_path": {"type": "string", "description": "Path to pipeline code", "optional": True}
                }
            },
            "list_jobs": {
                "description": "List pipeline jobs",
                "path": "/api/v1/jobs",
                "method": "GET",
                "parameters": {
                    "runner_type": {"type": "string", "description": "Filter by runner type", "optional": True},
                    "job_type": {"type": "string", "description": "Filter by job type", "optional": True},
                    "page_size": {"type": "integer", "description": "Number of jobs to return", "optional": True},
                    "page_token": {"type": "string", "description": "Token for pagination", "optional": True}
                }
            },
            "get_job": {
                "description": "Get pipeline job details",
                "path": "/api/v1/jobs/{job_id}",
                "method": "GET",
                "parameters": {
                    "job_id": {"type": "string", "description": "Job ID to retrieve"}
                }
            },
            "update_job": {
                "description": "Update a pipeline job",
                "path": "/api/v1/jobs/{job_id}",
                "method": "PUT",
                "parameters": {
                    "job_id": {"type": "string", "description": "Job ID to update"},
                    "scaling": {"type": "object", "description": "Scaling parameters", "optional": True},
                    "update_options": {"type": "object", "description": "Runner-specific update options", "optional": True}
                }
            },
            "cancel_job": {
                "description": "Cancel a pipeline job",
                "path": "/api/v1/jobs/{job_id}",
                "method": "DELETE",
                "parameters": {
                    "job_id": {"type": "string", "description": "Job ID to cancel"}
                }
            },
            "get_job_status": {
                "description": "Get pipeline job status",
                "path": "/api/v1/jobs/{job_id}/status",
                "method": "GET",
                "parameters": {
                    "job_id": {"type": "string", "description": "Job ID to get status for"}
                }
            },
            "get_job_metrics": {
                "description": "Get pipeline job metrics",
                "path": "/api/v1/jobs/{job_id}/metrics",
                "method": "GET",
                "parameters": {
                    "job_id": {"type": "string", "description": "Job ID to get metrics for"}
                }
            },
            "get_job_logs": {
                "description": "Get pipeline job logs",
                "path": "/api/v1/jobs/{job_id}/logs",
                "method": "GET",
                "parameters": {
                    "job_id": {"type": "string", "description": "Job ID to get logs for"},
                    "max_results": {"type": "integer", "description": "Maximum number of log entries to return", "optional": True},
                    "page_token": {"type": "string", "description": "Token for pagination", "optional": True}
                }
            },
            "create_savepoint": {
                "description": "Create a savepoint for a streaming job",
                "path": "/api/v1/jobs/{job_id}/savepoints",
                "method": "POST",
                "parameters": {
                    "job_id": {"type": "string", "description": "Job ID to create savepoint for"},
                    "drain": {"type": "boolean", "description": "Whether to drain the pipeline during savepointing", "optional": True},
                    "savepoint_dir": {"type": "string", "description": "Directory to store the savepoint", "optional": True}
                }
            },
            "list_savepoints": {
                "description": "List savepoints for a job",
                "path": "/api/v1/jobs/{job_id}/savepoints",
                "method": "GET",
                "parameters": {
                    "job_id": {"type": "string", "description": "Job ID to list savepoints for"}
                }
            },
            "restore_from_savepoint": {
                "description": "Restore a job from a savepoint",
                "path": "/api/v1/jobs/{job_id}/restore/{savepoint_id}",
                "method": "POST",
                "parameters": {
                    "job_id": {"type": "string", "description": "Job ID to restore"},
                    "savepoint_id": {"type": "string", "description": "Savepoint ID to restore from"}
                }
            },
            "list_runners": {
                "description": "List available runners",
                "path": "/api/v1/runners",
                "method": "GET",
                "parameters": {}
            },
            "get_runner": {
                "description": "Get runner details",
                "path": "/api/v1/runners/{runner_type}",
                "method": "GET",
                "parameters": {
                    "runner_type": {"type": "string", "description": "Runner type to get details for", "enum": [rt.value for rt in RunnerType]}
                }
            },
            "scale_runner": {
                "description": "Scale runner resources",
                "path": "/api/v1/runners/{runner_type}/scale",
                "method": "POST",
                "parameters": {
                    "runner_type": {"type": "string", "description": "Runner type to scale", "enum": [rt.value for rt in RunnerType]},
                    "min_workers": {"type": "integer", "description": "Minimum number of workers", "optional": True},
                    "max_workers": {"type": "integer", "description": "Maximum number of workers", "optional": True},
                    "scaling_algorithm": {"type": "string", "description": "Scaling algorithm to use", "optional": True}
                }
            }
        },
        "examples": [
            {
                "operation": "create_job",
                "description": "Create a Dataflow batch job using a template",
                "request": {
                    "job_name": "wordcount-example",
                    "runner_type": "dataflow",
                    "job_type": "BATCH",
                    "pipeline_options": {
                        "project": "my-gcp-project",
                        "region": "us-central1",
                        "tempLocation": "gs://my-bucket/temp"
                    },
                    "template_path": "gs://dataflow-templates/latest/Word_Count",
                    "template_parameters": {
                        "inputFile": "gs://dataflow-samples/shakespeare/kinglear.txt",
                        "output": "gs://my-bucket/output/results"
                    }
                }
            },
            {
                "operation": "list_jobs",
                "description": "List streaming Flink jobs",
                "request": {
                    "runner_type": "flink",
                    "job_type": "STREAMING"
                }
            }
        ]
    }
    
    return LLMToolResponse(
        success=True,
        data=manifest,
        message="Tool manifest retrieved successfully",
        error=None
    )

@router.get("/context", response_model=LLMToolResponse, summary="Get current context", tags=["Manifest"])
async def get_context(request: Request, mcp_context: MCPContext = Depends()):
    """
    Get the current MCP context.
    
    Returns information about the current context, including session ID,
    trace ID, and any context parameters.
    
    This endpoint is used by LLMs and agent frameworks to manage stateful
    operations and track context across requests.
    """
    return LLMToolResponse(
        success=True,
        data=mcp_context.dict(),
        message="Context retrieved successfully",
        error=None
    ) 