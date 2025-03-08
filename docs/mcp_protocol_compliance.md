# MCP Protocol Compliance

> **Note:** For a comprehensive guide on using the Beam MCP Server, including LLM integration, 
> please refer to the [User Guide & LLM Integration](./mcp/user_guide_llm_integration.md) document.
> This document focuses specifically on MCP protocol compliance details.

This document explains how the Apache Beam MCP Server implements the [Model Context Protocol (MCP)](https://github.com/llm-mcp/mcp-spec), a standard for AI/LLM tool interactions.

## What is MCP?

The Model Context Protocol provides a standardized way for:
- AI models to discover and use tools
- Tools to describe their capabilities
- Maintaining context between operations
- Handling errors consistently

## Implementation Details

### Core Components

- **MCPContext**: Tracks session state, transactions, and user information
- **LLMToolResponse**: Standard response format for all API endpoints
- **Manifest Endpoint**: Tool discovery via `/api/v1/manifest`

### API Design

All endpoints follow MCP format with:

```
{
  "success": true|false,
  "data": { ... },
  "message": "Human-readable message",
  "error": "Error message if applicable"
}
```

### Context Headers

The server supports the following MCP context headers:

| Header | Purpose | Example |
|--------|---------|---------|
| `MCP-Session-ID` | Track user sessions | `"MCP-Session-ID: session-123"` |
| `MCP-Trace-ID` | Track operations across systems | `"MCP-Trace-ID: trace-abc"` |
| `MCP-Transaction-ID` | Group related operations | `"MCP-Transaction-ID: tx-456"` |
| `MCP-User-ID` | Identify users | `"MCP-User-ID: user-789"` |

## Testing MCP Compliance

Test the server's MCP compliance with these simple commands:

```bash
# Check the manifest endpoint
curl http://localhost:8888/api/v1/manifest

# Check health endpoint
curl http://localhost:8888/api/v1/health/health

# Test context propagation
curl http://localhost:8888/api/v1/runners \
  -H "MCP-Session-ID: test-session-123"
```

## Client Integration Guide

When building clients to work with the MCP server:

1. **Start with the manifest**: Always fetch the manifest to discover available tools
   ```python
   manifest = requests.get("http://localhost:8888/api/v1/manifest").json()
   ```

2. **Maintain context**: Pass the same session ID across requests
   ```python
   headers = {"MCP-Session-ID": "my-session-id"}
   response = requests.get("http://localhost:8888/api/v1/runners", headers=headers)
   ```

3. **Handle errors properly**: Always check the `success` field
   ```python
   response_json = response.json()
   if not response_json["success"]:
     print(f"Error: {response_json['error']}")
   ```

4. **Use transactions** for related operations
   ```python
   headers = {
     "MCP-Session-ID": "my-session-id",
     "MCP-Transaction-ID": "job-creation-tx"
   }
   ```

For a complete Python client example, see the `examples/client.py` file in the repository.

## Real-World Examples

The following examples demonstrate how the MCP standard endpoints enhance Apache Beam's functionality in real-world scenarios.

### End-to-End Beam Pipeline Deployment

This example shows how a data engineering team can use the MCP standard endpoints to manage customer transaction processing pipelines.

#### Step 1: Set Up Execution Contexts

First, create standardized execution contexts for different environments:

```bash
# Create a development context for Dataflow
curl -X POST "http://localhost:8888/api/v1/contexts/" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Dataflow Dev",
    "description": "Development environment for Dataflow jobs",
    "context_type": "dataflow",
    "parameters": {
      "region": "us-central1",
      "project": "beam-analytics-dev",
      "temp_location": "gs://beam-analytics-dev/temp",
      "machine_type": "n1-standard-2"
    },
    "metadata": {
      "environment": "development"
    }
  }'

# Create a production context for Dataflow
curl -X POST "http://localhost:8888/api/v1/contexts/" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Dataflow Prod",
    "description": "Production environment for Dataflow jobs",
    "context_type": "dataflow",
    "parameters": {
      "region": "us-central1",
      "project": "beam-analytics-prod",
      "temp_location": "gs://beam-analytics-prod/temp",
      "machine_type": "n1-standard-4",
      "autoscaling_algorithm": "THROUGHPUT_BASED"
    },
    "metadata": {
      "environment": "production"
    }
  }'
```

These contexts provide standardized environments with pre-configured parameters for running Beam pipelines.

#### Step 2: Register Resources

Next, register the datasets that will be used in the pipelines:

```bash
# Register the input dataset
curl -X POST "http://localhost:8888/api/v1/resources/" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Customer Transactions",
    "description": "Daily customer transaction data",
    "resource_type": "dataset",
    "location": "gs://beam-analytics-data/transactions/daily/*.csv",
    "format": "csv",
    "schema": {
      "fields": [
        {"name": "transaction_id", "type": "string", "description": "Unique transaction ID"},
        {"name": "customer_id", "type": "string", "description": "Customer identifier"},
        {"name": "amount", "type": "float", "description": "Transaction amount"},
        {"name": "timestamp", "type": "datetime", "description": "Transaction time"}
      ]
    },
    "metadata": {
      "update_frequency": "daily",
      "owner": "data-team"
    }
  }'

# Register the output location
curl -X POST "http://localhost:8888/api/v1/resources/" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Aggregated Transactions",
    "description": "Daily aggregated transaction data",
    "resource_type": "dataset",
    "location": "gs://beam-analytics-data/transactions/aggregated/",
    "format": "parquet",
    "metadata": {
      "update_frequency": "daily",
      "owner": "data-team"
    }
  }'
```

#### Step 3: Submit a Job Using Context and Resources

Now submit a Beam job referencing the registered context and resources:

```bash
# Submit a new job using the context and resources
curl -X POST "http://localhost:8888/api/v1/jobs/" \
  -H "Content-Type: application/json" \
  -d '{
    "job_name": "daily-transaction-aggregation",
    "runner": "DataflowRunner",
    "context_id": "dataflow-prod-123",
    "pipeline_options": {
      "input": "resource:customer-transactions-456",
      "output": "resource:aggregated-transactions-789",
      "aggregation_level": "daily"
    },
    "pipeline_script": "gs://beam-analytics-code/pipelines/transaction_aggregation.py"
  }'
```

Notice how the job references:
- A specific execution context via `context_id`
- Input and output resources via resource IDs
- Other pipeline-specific parameters

#### Step 4: Monitor Job Progress

Monitor the job using existing endpoints:

```bash
# Get job status
curl "http://localhost:8888/api/v1/jobs/job-12345"

# Get job metrics
curl "http://localhost:8888/api/v1/metrics/job-12345"
```

### Advanced Example: Data Pipeline with Custom Transformations

This example shows how to use the `/tools` endpoint to incorporate custom data transformations:

```bash
# Register a custom data transformation tool
curl -X POST "http://localhost:8888/api/v1/tools/" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "sentiment-analyzer",
    "description": "Analyzes sentiment in text data",
    "type": "transformation",
    "version": "1.0.0",
    "parameters": {
      "text_column": {
        "type": "string",
        "description": "Column containing text to analyze",
        "required": true
      },
      "output_column": {
        "type": "string",
        "description": "Column to store sentiment scores",
        "default": "sentiment_score"
      },
      "language": {
        "type": "string",
        "description": "Text language",
        "default": "en"
      }
    },
    "capabilities": ["text_processing", "analytics"],
    "metadata": {
      "model": "distilbert-base-uncased",
      "owner": "ml-team"
    }
  }'

# Use the tool in a job submission
curl -X POST "http://localhost:8888/api/v1/jobs/" \
  -H "Content-Type: application/json" \
  -d '{
    "job_name": "customer-feedback-analysis",
    "runner": "DataflowRunner",
    "context_id": "dataflow-prod-123",
    "pipeline_options": {
      "input": "resource:customer-feedback-data-456",
      "output": "resource:feedback-analysis-789",
      "transformations": [
        {
          "tool_id": "sentiment-analyzer-123",
          "parameters": {
            "text_column": "feedback_text",
            "output_column": "sentiment_score",
            "language": "en"
          }
        }
      ]
    },
    "pipeline_script": "gs://beam-analytics-code/pipelines/feedback_analysis.py"
  }'
```

## Benefits of MCP Standard Endpoints

The examples above demonstrate how the MCP endpoints provide real value:

1. **Standardized Execution Environments**: The `/contexts` endpoint allows defining standardized execution environments with pre-configured parameters, making it easier to deploy to different environments consistently.

2. **Centralized Resource Management**: The `/resources` endpoint provides a central registry of datasets, making it easier to track data lineage and ensure pipelines use the correct data.

3. **Simplified Tool Management**: The `/tools` endpoint enables registering, discovering, and using custom data processing tools in a standardized way.

4. **Simplified Job Submission**: By referencing contexts, resources, and tools by ID, job submission becomes simpler and less error-prone.

5. **Integration with Core Beam Functionality**: The MCP endpoints complement rather than replace the core Beam functionality, enhancing it with standardized management capabilities.

6. **Automation Friendly**: These standardized endpoints make it easy to automate pipeline deployment through CI/CD systems or to create higher-level orchestration tools.

For a complete implementation reference, see the API router files in the `src/server/api/` directory.

### Basic API Interaction

Access key MCP endpoints to test server functionality:

```bash
# Get server manifest
curl http://localhost:8888/api/v1/manifest

# Check server health
curl http://localhost:8888/api/v1/health/health

# List available runners
curl http://localhost:8888/api/v1/runners \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer ${API_TOKEN}"
```

### Programmatic Access Example

```python
import requests

# Fetch the server manifest to verify MCP compliance
manifest = requests.get("http://localhost:8888/api/v1/manifest").json()

# Set up authentication
headers = {"Authorization": f"Bearer {api_token}"}

# Access a protected endpoint
response = requests.get("http://localhost:8888/api/v1/runners", headers=headers)
``` 