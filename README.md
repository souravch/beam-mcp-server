# Beam MCP Server

A Model Context Protocol (MCP) server for managing Apache Beam pipelines across different runners, including Google Cloud Dataflow, Apache Spark, and Apache Flink.

## Overview

The Beam MCP server provides a unified API for managing data pipelines built with Apache Beam. It abstracts away the differences between various runners (Dataflow, Spark, Flink) and provides a consistent interface for:

- Creating and managing jobs
- Monitoring job status and metrics
- Creating and restoring from savepoints/checkpoints
- Managing and scaling runner resources

This server can be used as a backend for AI/LLM tools that need to interact with data processing pipelines.

## Features

- **Runner-agnostic API**: Consistent API across Dataflow, Spark, and Flink runners
- **LLM-friendly design**: Designed for easy integration with AI tools and agents
- **Comprehensive pipeline management**: Create, update, monitor, and cancel pipelines
- **Savepoint/checkpoint support**: Create and restore from savepoints (for streaming jobs)
- **Metrics and logging**: Retrieve detailed metrics and logs for jobs
- **Runner management**: Scale runner resources based on workload needs
- **FastAPI-based**: Modern, high-performance web framework with automatic OpenAPI documentation

## MCP Protocol Compliance

This server implements the Model Context Protocol (MCP) 1.0 standard, providing:

- **Tool Manifest**: Self-description via `/api/v1/manifest` endpoint
- **Context Handling**: Stateful operations via MCP context
- **Standardized Responses**: Consistent `LLMToolResponse` format
- **Discoverability**: OpenAPI schema with MCP metadata
- **LLM Integration**: Optimized for AI/LLM consumption

### MCP Context Headers

All API endpoints support the following MCP context headers:

- `MCP-Session-ID`: Session identifier
- `MCP-Trace-ID`: Trace identifier for distributed tracing
- `MCP-Transaction-ID`: Transaction identifier for multi-step operations
- `MCP-User-ID`: User identifier

### LLM-Specific Endpoints

- `/api/v1/manifest`: Get tool manifest for discovery
- `/api/v1/context`: Get current MCP context
- `/health/llm`: LLM-friendly health check

## Installation

### Prerequisites

- Python 3.9 or later
- Appropriate SDK for your runner(s):
  - Google Cloud SDK for Dataflow
  - Apache Spark for Spark runner
  - Apache Flink for Flink runner

### From PyPI

```bash
pip install beam-mcp-server
```

### From Source

```bash
git clone https://github.com/yourusername/beam-mcp-server.git
cd beam-mcp-server
pip install -e .
```

## Quick Start

1. Create a configuration file (or use environment variables):

```yaml
# config/beam_mcp_config.yaml
service:
  name: beam-mcp
  type: beam

default_runner: dataflow

runners:
  dataflow:
    enabled: true
    default_project: your-gcp-project
    default_region: us-central1
  spark:
    enabled: true
    spark_master: yarn
  flink:
    enabled: true
    jobmanager_address: localhost:8081
  direct:
    enabled: true

interfaces:
  jobs:
    enabled: true
  metrics:
    enabled: true
  logs:
    enabled: true
  savepoints:
    enabled: true
```

2. Start the server:

```bash
# Using configuration file
beam-mcp-server -c config/beam_mcp_config.yaml

# Using environment variables
export GCP_PROJECT_ID=your-gcp-project
export GCP_REGION=us-central1
export DEFAULT_RUNNER=dataflow
beam-mcp-server
```

3. Access the API documentation:

Open your browser and navigate to `http://localhost:8080/docs` to see the interactive API documentation.

## Docker

### Using Docker

```bash
docker pull yourusername/beam-mcp-server

docker run -p 8080:8080 \
  -v /path/to/config:/app/config \
  -v ~/.config/gcloud:/root/.config/gcloud \
  -e GCP_PROJECT_ID=your-gcp-project \
  yourusername/beam-mcp-server
```

### Using Docker Compose

```yaml
# docker-compose.yaml
version: '3'
services:
  beam-mcp:
    image: yourusername/beam-mcp-server
    ports:
      - 8080:8080
    volumes:
      - ./config:/app/config
      - ~/.config/gcloud:/root/.config/gcloud
    environment:
      - GCP_PROJECT_ID=your-gcp-project
      - GCP_REGION=us-central1
      - DEFAULT_RUNNER=dataflow
```

Run with:
```bash
docker-compose up
```

## Using the API with MCP Context

### MCP Client Example

```python
from beam_mcp_client import BeamMCPClient, MCPContext

# Create a client with a session
client = BeamMCPClient("http://localhost:8080")
context = MCPContext(session_id="my-session")
client = client.with_context(context)

# Start a transaction
client.context.start_transaction()

# Create a job
job_response = client.create_job(
    job_name="my-job",
    runner_type="dataflow",
    job_type="BATCH",
    pipeline_options={...},
    template_path="gs://dataflow-templates/latest/Word_Count",
    template_parameters={...}
)

# Get job status
job_id = job_response["data"]["job_id"]
status_response = client.get_job_status(job_id)

# End the transaction
client.context.end_transaction()
```

### Using curl with MCP Headers

```bash
# Create a job with MCP context
curl -X POST http://localhost:8080/api/v1/jobs \
  -H "Content-Type: application/json" \
  -H "MCP-Session-ID: my-session-id" \
  -H "MCP-Transaction-ID: my-transaction-id" \
  -d '{
    "job_name": "word-count-example",
    "runner_type": "dataflow",
    "job_type": "BATCH",
    "pipeline_options": {
      "project": "your-gcp-project",
      "region": "us-central1",
      "tempLocation": "gs://your-bucket/temp"
    },
    "template_path": "gs://dataflow-templates/latest/Word_Count",
    "template_parameters": {
      "inputFile": "gs://dataflow-samples/shakespeare/kinglear.txt",
      "output": "gs://your-bucket/output/results"
    }
  }'
```

## Configuration Options

### Environment Variables

- `GCP_PROJECT_ID`: Google Cloud project ID (for Dataflow)
- `GCP_REGION`: Google Cloud region (for Dataflow)
- `SPARK_MASTER`: Spark master URL
- `FLINK_JOBMANAGER`: Flink JobManager address
- `DEFAULT_RUNNER`: Default runner to use
- `PORT`: Port to listen on (default: 8080)
- `DEBUG`: Enable debug mode (true/false)
- `ENVIRONMENT`: Environment name (development/production)

### Configuration File

See the `beam_mcp_config.yaml` example in the Quick Start section.

## For LLM Tool Integration

This server includes special endpoints and metadata for easy integration with AI/LLM tools:

- `/api/v1/health/llm`: LLM-friendly health check endpoint
- `/api/v1/manifest`: Get tool manifest for discovery
- `/api/v1/context`: Get current MCP context
- All endpoints return a standardized JSON response format
- OpenAPI schema includes LLM-specific metadata

LLMs can discover capabilities through the manifest endpoint:

```bash
curl http://localhost:8080/api/v1/manifest
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details. 