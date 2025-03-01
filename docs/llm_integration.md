# LLM Integration Guide

This guide shows how to integrate the Apache Beam MCP Server with Large Language Models (LLMs) for AI-controlled data pipelines.

## Quick Start

```python
import requests
import json

# 1. Fetch the server manifest to discover capabilities
manifest = requests.get("http://localhost:8082/api/v1/manifest").json()

# 2. Set up persistent session ID for context tracking
headers = {"MCP-Session-ID": "llm-session-123"}

# 3. List available runners
runners = requests.get(
    "http://localhost:8082/api/v1/runners", 
    headers=headers
).json()

# 4. Create a data processing job
job = requests.post(
    "http://localhost:8082/api/v1/jobs",
    headers=headers,
    json={
        "job_name": "llm-controlled-wordcount",
        "runner_type": "flink",
        "job_type": "BATCH",
        "code_path": "examples/pipelines/wordcount.py",
        "pipeline_options": {
            "parallelism": 2,
            "input_file": "/tmp/input.txt",
            "output_path": "/tmp/output"
        }
    }
).json()

# 5. Monitor the job
job_status = requests.get(
    f"http://localhost:8082/api/v1/jobs/{job['data']['job_id']}",
    headers=headers
).json()
```

## Integration Architecture

LLMs can interact with the Apache Beam MCP Server in two key ways:

1. **Direct API Integration**: LLM makes API calls based on user requests
2. **Agent-based**: LLM delegates to a specialized agent that manages pipelines

### Sequence Flow

```mermaid
sequenceDiagram
    User->>LLM: "Process my stream of events"
    LLM->>MCP: GET /api/v1/manifest
    MCP-->>LLM: Tool manifest
    LLM->>MCP: GET /api/v1/runners
    MCP-->>LLM: Available runners
    LLM->>MCP: POST /api/v1/jobs (create job)
    MCP-->>LLM: Job details
    
    loop Monitor
        LLM->>MCP: GET /api/v1/jobs/{id}/status
        MCP-->>LLM: Job status
        LLM->>MCP: GET /api/v1/jobs/{id}/metrics
        MCP-->>LLM: Current metrics
    end
    
    LLM->>User: "Your data is being processed. Found 1.2M events..."
```

## Key Integration Points

### 1. Discovery Phase

LLMs should first discover the available tools by calling the manifest endpoint:

```python
manifest = requests.get("http://localhost:8082/api/v1/manifest").json()
```

The manifest includes:
- Available API endpoints
- Expected parameters
- Supported runners
- Job types

### 2. Job Lifecycle Management

LLMs can create and manage jobs with the following endpoints:

| Operation | Endpoint | Method | Description |
|-----------|----------|--------|-------------|
| Create job | `/api/v1/jobs` | POST | Create a new pipeline job |
| List jobs | `/api/v1/jobs` | GET | List all jobs |
| Get job | `/api/v1/jobs/{id}` | GET | Get job details |
| Cancel job | `/api/v1/jobs/{id}` | DELETE | Cancel a job |
| Get metrics | `/api/v1/jobs/{id}/metrics` | GET | Get job metrics |

### 3. Error Handling

LLMs should always check the `success` field and handle errors gracefully:

```python
response = requests.get("http://localhost:8082/api/v1/jobs/invalid-id").json()
if not response["success"]:
    # Handle error case
    print(f"Error: {response['error']}")
```

## Example: LLM-controlled Flink Job

This example shows how to implement an LLM-controlled Flink job that processes data:

```python
def create_flink_job(input_file, output_path, parallelism=2):
    """Create a Flink job based on user request."""
    response = requests.post(
        "http://localhost:8082/api/v1/jobs",
        headers={"MCP-Session-ID": "llm-session-456"},
        json={
            "job_name": "user-requested-analysis",
            "runner_type": "flink",
            "job_type": "BATCH",
            "code_path": "examples/pipelines/wordcount.py",
            "pipeline_options": {
                "parallelism": parallelism,
                "jar_path": "/path/to/beam-examples.jar",
                "entry_class": "org.apache.beam.examples.WordCount",
                "program_args": f"--input {input_file} --output {output_path}"
            }
        }
    )
    return response.json()
```

For a complete example implementation, see `examples/llm_agent.py` in the repository.
  