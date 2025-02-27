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

This server fully implements the Model Context Protocol (MCP) 1.0 standard, providing:

- **Full MCP Standard Implementation**: Complete compliance with the MCP 1.0 protocol specification
- **Tool Registration**: Uses the standard `Tool` class with proper JSON schema
- **Resource Registration**: Standard-compliant resource registration
- **Context Handling**: Complete context support with session, transaction, and trace IDs
- **MCP Transports**: Support for HTTP, STDIO, SSE, and WebSocket transports
- **Tool Manifest**: Self-description via `/api/v1/manifest` endpoint
- **Context Handling**: Stateful operations via MCP context
- **Standardized Responses**: Consistent `LLMToolResponse` format
- **Discoverability**: OpenAPI schema with MCP metadata
- **LLM Integration**: Optimized for AI/LLM consumption

See the [MCP Compliance Documentation](docs/mcp-compliance.md) for detailed information on how the server implements the MCP standard.

### MCP Context Headers

All API endpoints support the following MCP context headers:

- `MCP-Session-ID`: Session identifier
- `MCP-Trace-ID`: Trace identifier for distributed tracing
- `MCP-Transaction-ID`: Transaction identifier for multi-step operations
- `MCP-User-ID`: User identifier

### MCP Endpoints

- **Standard MCP Endpoints**:
  - `/api/v1/manifest`: Get tool manifest for discovery
  - `/api/v1/context`: Get current MCP context
  - `/api/v1/health/mcp`: MCP-specific health check

- **LLM-Specific Endpoints**:
  - `/api/v1/health/llm`: LLM-friendly health check

### Testing MCP Compliance

To verify that the server is fully compliant with the MCP standard:

```bash
# Start the server
python main.py

# Check the manifest endpoint
curl http://localhost:8080/api/v1/manifest

# Test context propagation
curl http://localhost:8080/api/v1/context \
  -H "MCP-Session-ID: test-session-123" \
  -H "MCP-Transaction-ID: test-transaction-456"
  
# Test MCP-specific health check
curl http://localhost:8080/api/v1/health/mcp
```

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

# MCP Settings
mcp:
  version: "1.0"
  server_name: "beam-mcp-server"
  provider: "apache"

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

# Using MCP-specific options (for command-line tools)
beam-mcp-server --mcp-stdio
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

#### MCP-specific Environment Variables

- `BEAM_MCP_MCP_VERSION`: MCP protocol version to use
- `BEAM_MCP_MCP_SERVER_NAME`: MCP server name
- `BEAM_MCP_MCP_PROVIDER`: MCP provider name
- `BEAM_MCP_MCP_LOG_LEVEL`: MCP server log level
- `BEAM_MCP_MCP_STREAMING_SUPPORT`: Enable MCP streaming (true/false)

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

## MCP Command-Line Integration

For command-line tools and scripts, the server supports the STDIO transport:

```bash
# Start the server in STDIO mode
beam-mcp-server --mcp-stdio | your-mcp-tool
```

This allows integration with MCP-compatible tools and scripts through standard input/output.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Multi-Runner Support

The Apache Beam MCP Server supports multiple runners:

- Direct (local development)
- Apache Flink
- Apache Spark
- Google Cloud Dataflow

### Configuration

To configure the server for different runners, modify the configuration file in `config/flink_config.yaml` or create your own configuration file:

```yaml
service:
  name: beam-mcp
  type: beam
  description: Apache Beam MCP Server
  version: 1.0.0

default_runner: direct  # Change to flink, spark, or dataflow as needed

runners:
  # Direct runner configuration
  direct:
    enabled: true
    pipeline_options:
      direct_num_workers: 4
      direct_running_mode: multi_threading
      temp_location: /tmp/beam-direct-temp
      save_main_session: true
      
  # Flink runner configuration
  flink:
    enabled: true
    jobmanager_url: http://localhost:8081
    flink_master: localhost:8081
    rest_url: http://localhost:8081
    pipeline_options:
      parallelism: 4
      checkpointing_interval: 10000
      state_backend: "memory"
      tmp_dir: "/tmp/beam-flink-tmp"
      savepoint_path: "/tmp/beam-flink-savepoints"
      streaming: false
      
  # Spark runner configuration
  spark:
    enabled: true
    spark_master: local[*]
    pipeline_options:
      spark_job_name: beam-mcp-spark-job
      spark_executor_instances: 2
      spark_executor_cores: 2
      spark_executor_memory: 1g
      temp_location: /tmp/beam-spark-temp
      
  # Dataflow runner configuration
  dataflow:
    enabled: true
    pipeline_options:
      project: your-gcp-project-id  # Must be set
      region: us-central1
      temp_location: gs://your-bucket/temp
      staging_location: gs://your-bucket/staging
```

### Starting the Server

To start the server with a specific configuration:

```bash
python main.py --config config/flink_config.yaml --port 8082 --debug
```

### Environment Setup

#### Local Development (Direct Runner)

No additional setup required beyond the installation instructions.

#### Apache Flink

1. Download Apache Flink from https://flink.apache.org/downloads/
2. Start a local Flink cluster:
   ```bash
   ./bin/start-cluster.sh
   ```
3. Verify the Flink dashboard is accessible at http://localhost:8081

#### Apache Spark

1. Download Apache Spark from https://spark.apache.org/downloads/
2. Set the `SPARK_HOME` environment variable:
   ```bash
   export SPARK_HOME=/path/to/spark
   ```
3. For local development, no additional setup is needed as `local[*]` will be used

#### Google Cloud Dataflow

1. Install Google Cloud SDK: https://cloud.google.com/sdk/docs/install
2. Authenticate with GCP:
   ```bash
   gcloud auth login
   gcloud config set project your-project-id
   ```
3. Create a GCS bucket for temporary files
4. Update the configuration with your GCP project ID and bucket

### Verifying All Runners

The repository includes a helpful verification script that will:
1. Check for required installations (Flink and Spark)
2. Start a local Flink cluster if needed
3. Launch the MCP server
4. Run tests for all configured runners (Direct, Flink, and Spark)

To run the verification:

```bash
# Make sure the script is executable
chmod +x scripts/verify_runners.sh

# Run the verification
./scripts/verify_runners.sh
```

The script will provide feedback on which runners are working correctly and help diagnose any issues.

### End-to-End Testing

For comprehensive testing of all server APIs and LLM integration capabilities, use the end-to-end testing script:

```bash
# Make sure the script is executable
chmod +x scripts/run_e2e_tests.sh

# Run the end-to-end tests
./scripts/run_e2e_tests.sh
```

This script:
1. Checks for Flink and Spark installations
2. Starts a local Flink cluster if needed
3. Starts the MCP server
4. Tests all API endpoints
5. Validates response formats for LLM tool integration
6. Submits jobs to all available runners
7. Verifies job status, metrics, and cancellation
8. Provides a detailed test summary

You can customize the test run:

```bash
# Use a custom configuration file
./scripts/run_e2e_tests.sh --config path/to/config.yaml

# Run on a different port
./scripts/run_e2e_tests.sh --port 8083

# Skip Flink cluster check/startup (if you're only testing Direct runner)
./scripts/run_e2e_tests.sh --skip-flink-check
```

These tests ensure that the MCP server is functioning correctly and can be properly integrated with LLM tools.

### Running Examples

The repository includes example pipelines for testing with different runners:

#### Direct Runner

```bash
python examples/run_wordcount_direct.py --output /tmp/direct_wordcount
```

#### Apache Flink

```bash
python examples/run_wordcount_flink.py --flink_master http://localhost:8081 --output /tmp/flink_wordcount
```

#### Apache Spark

```bash
python examples/run_wordcount_spark.py --spark_master local[*] --output /tmp/spark_wordcount
```

#### Google Cloud Dataflow

```bash
python examples/run_wordcount_dataflow.py \
  --project your-gcp-project-id \
  --region us-central1 \
  --staging_location gs://your-bucket/staging \
  --temp_location gs://your-bucket/temp \
  --output gs://your-bucket/output/wordcount
```

### Submitting Jobs Through the MCP Server

You can also submit jobs through the MCP server API:

```bash
curl -X POST http://localhost:8082/api/v1/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "job_name": "wordcount",
    "runner_type": "flink",
    "job_type": "BATCH",
    "code_path": "examples/pipelines/wordcount.py",
    "pipeline_options": {
      "parallelism": 4
    }
  }'
```

Or use the client example:

```python
from examples.client import BeamMCPClient
import asyncio

async def run_job():
    client = BeamMCPClient(base_url="http://localhost:8082")
    job = await client.create_job({
        "job_name": "wordcount",
        "runner_type": "flink",
        "job_type": "BATCH",
        "code_path": "examples/pipelines/wordcount.py",
        "pipeline_options": {
            "parallelism": 4
        }
    })
    print(f"Job created: {job['job_id']}")

asyncio.run(run_job())
```

## Troubleshooting

### Flink Jobs

- Ensure the Flink cluster is running and accessible
- Check the Flink dashboard for job status
- Verify the jobmanager URL is correct in your configuration

### Spark Jobs

- Verify the Spark master URL
- Check Spark logs at `$SPARK_HOME/logs`
- For cluster deployments, ensure proper network connectivity

### Dataflow Jobs

- Ensure your GCP credentials are properly set up
- Verify the project ID and region
- Check the Google Cloud Console Dataflow page for job status

### Common Issues

- **Missing Project ID**: When using Dataflow, ensure the project ID is set in your configuration
- **Path Issues**: Make sure temp_location and other paths are accessible by the runners
- **Network Connectivity**: For remote clusters, verify connectivity between the MCP server and the cluster
- **Job Submission Failures**: Check the server logs for detailed error messages 