# [WIP] Beam MCP Server: User Guide & LLM Integration

## Table of Contents

- [\[WIP\] Beam MCP Server: User Guide \& LLM Integration](#wip-beam-mcp-server-user-guide--llm-integration)
  - [Table of Contents](#table-of-contents)
  - [Introduction](#introduction)
    - [Capabilities and Constraints](#capabilities-and-constraints)
  - [Basic Usage: Managing Flink Jobs](#basic-usage-managing-flink-jobs)
    - [Connecting to the MCP Server](#connecting-to-the-mcp-server)
    - [Submitting a Flink Job](#submitting-a-flink-job)
    - [Checking Job Status](#checking-job-status)
    - [Scaling a Job](#scaling-a-job)
    - [Creating Savepoints](#creating-savepoints)
    - [Canceling Jobs](#canceling-jobs)
  - [Simplified Interfaces](#simplified-interfaces)
    - [Python Client Library](#python-client-library)
    - [CLI Tool](#cli-tool)
  - [LLM Integration](#llm-integration)
    - [Integration Patterns](#integration-patterns)
    - [Setting Up LLM Integration](#setting-up-llm-integration)
    - [Tool Definitions for LLMs](#tool-definitions-for-llms)
    - [Building an LLM Agent](#building-an-llm-agent)
    - [Natural Language Pipeline Control](#natural-language-pipeline-control)
    - [Advanced Pipeline Management](#advanced-pipeline-management)
  - [Example Workflows](#example-workflows)
    - [Data-Driven Pipeline Generation](#data-driven-pipeline-generation)
    - [Continuous Monitoring and Optimization](#continuous-monitoring-and-optimization)
  - [Troubleshooting and FAQs](#troubleshooting-and-faqs)
    - [Security Considerations](#security-considerations)
  - [Conclusion](#conclusion)
  - [Alternative LLM Integration with LlamaIndex and Local Models](#alternative-llm-integration-with-llamaindex-and-local-models)
    - [Setting Up LlamaIndex with Local Models](#setting-up-llamaindex-with-local-models)
    - [Using Ollama to Run Local LLMs](#using-ollama-to-run-local-llms)
    - [Integration with Beam MCP Server](#integration-with-beam-mcp-server)
    - [Alternative: Using LlamaFile for Self-Contained Models](#alternative-using-llamafile-for-self-contained-models)
    - [Creating a Function-Calling Agent with Local Models](#creating-a-function-calling-agent-with-local-models)
    - [Handling Structured Output with Pydantic](#handling-structured-output-with-pydantic)
    - [Combining with RAG for Enhanced Pipeline Management](#combining-with-rag-for-enhanced-pipeline-management)
  - [Testing Tool Calling with Local LLMs](#testing-tool-calling-with-local-llms)
    - [Model Selection for Tool Calling Capabilities](#model-selection-for-tool-calling-capabilities)
    - [Hardware Considerations for MacOS](#hardware-considerations-for-macos)
    - [Systematic Testing Framework](#systematic-testing-framework)
    - [Common Challenges and Solutions](#common-challenges-and-solutions)
      - [1. Inconsistent Tool Calling Format](#1-inconsistent-tool-calling-format)
      - [2. Slow Inference on Resource-Constrained Macs](#2-slow-inference-on-resource-constrained-macs)
      - [3. Unreliable Tool Selection](#3-unreliable-tool-selection)
    - [Integration with Real MCP Server](#integration-with-real-mcp-server)
    - [Practical End-to-End Testing Workflow](#practical-end-to-end-testing-workflow)
    - [Performance Benchmarking](#performance-benchmarking)
    - [Critical Nuances for Production Use](#critical-nuances-for-production-use)
- [MCP Protocol Compliance](#mcp-protocol-compliance)

## Introduction

The Beam MCP Server provides a standardized API for deploying, monitoring, and managing Apache Beam pipelines across different runners (Flink, Spark, Dataflow, Direct). This guide covers both basic usage for running Flink jobs and advanced integration with LLMs to create intelligent, self-managing data pipelines.

### Capabilities and Constraints

**Current Capabilities:**
- ✅ Submit Beam jobs to various runners (Flink, Spark, Dataflow, Direct)
- ✅ Check job status
- ✅ Cancel running jobs
- ✅ Create savepoints (Flink checkpoints)
- ✅ Scale jobs (adjust parallelism)
- ✅ View job metrics and logs
- ✅ LLM-powered job management and optimization

**Current Constraints:**
- ⚠️ Authentication is required but may need additional setup
- ⚠️ Complex MCP protocol might be challenging for direct API interaction
- ⚠️ Limited error recovery for failed jobs
- ⚠️ The protocol has multiple phases of implementation

## Basic Usage: Managing Flink Jobs

### Connecting to the MCP Server

The first step is to establish a connection with the MCP server using the Model Context Protocol:

```python
import requests
import json
import sseclient

# Server URL
base_url = "http://localhost:8888"

# Initialize MCP connection
def initialize_mcp():
    response = requests.post(
        f"{base_url}/api/v1/mcp/initialize",
        json={
            "protocol_version": "1.0",
            "client": {
                "name": "beam-flink-client",
                "version": "1.0.0"
            },
            "capabilities": {
                "tool.job_management": {
                    "supported": True,
                    "version": "1.0"
                },
                "beam.runners": {
                    "supported": True,
                    "version": "1.0"
                }
            }
        }
    )
    
    # Get connection ID
    connection_id = response.headers.get("X-MCP-Connection-ID")
    
    # Mark connection as initialized
    requests.post(
        f"{base_url}/api/v1/mcp/initialized",
        headers={"X-MCP-Connection-ID": connection_id}
    )
    
    return connection_id

connection_id = initialize_mcp()
```

### Submitting a Flink Job

Once connected, you can submit a Beam job to run on Flink:

```python
def submit_flink_job(connection_id, job_name, pipeline_path):
    response = requests.post(
        f"{base_url}/api/v1/jobs",
        headers={"X-MCP-Connection-ID": connection_id},
        json={
            "job_name": job_name,
            "runner_type": "flink",
            "job_type": "STREAMING",  # or "BATCH" for batch jobs
            "code_path": pipeline_path,
            "pipeline_options": {
                "parallelism": 4,
                "checkpoint_interval": 60000,  # 60 seconds
                "state_backend": "rocksdb",
                "input_source": "kafka://input-topic",
                "output_sink": "kafka://output-topic"
            }
        }
    )
    
    return response.json()["job_id"]

job_id = submit_flink_job(connection_id, "my-flink-job", "/path/to/pipeline.py")
```

### Checking Job Status

Monitor your job's status:

```python
def check_job_status(connection_id, job_id):
    response = requests.get(
        f"{base_url}/api/v1/jobs/{job_id}",
        headers={"X-MCP-Connection-ID": connection_id}
    )
    
    return response.json()

status = check_job_status(connection_id, job_id)
print(f"Job status: {status['state']}")
print(f"Start time: {status['start_time']}")
print(f"Duration: {status['duration']} seconds")
```

### Scaling a Job

Adjust parallelism for a running job:

```python
def scale_job(connection_id, job_id, parallelism):
    response = requests.post(
        f"{base_url}/api/v1/jobs/{job_id}/scale",
        headers={"X-MCP-Connection-ID": connection_id},
        json={"parallelism": parallelism}
    )
    
    return response.json()

scale_result = scale_job(connection_id, job_id, 8)  # Scale to 8 parallel tasks
```

### Creating Savepoints

Create a savepoint to preserve job state:

```python
def create_savepoint(connection_id, job_id, savepoint_path=None):
    payload = {}
    if savepoint_path:
        payload["savepoint_path"] = savepoint_path
        
    response = requests.post(
        f"{base_url}/api/v1/jobs/{job_id}/savepoint",
        headers={"X-MCP-Connection-ID": connection_id},
        json=payload
    )
    
    return response.json()

savepoint = create_savepoint(connection_id, job_id)
savepoint_path = savepoint["savepoint_path"]
print(f"Savepoint created at: {savepoint_path}")
```

### Canceling Jobs

Cancel a running job:

```python
def cancel_job(connection_id, job_id, savepoint=True):
    response = requests.post(
        f"{base_url}/api/v1/jobs/{job_id}/cancel",
        headers={"X-MCP-Connection-ID": connection_id},
        json={"with_savepoint": savepoint}
    )
    
    return response.json()

cancel_result = cancel_job(connection_id, job_id)
```

When done, properly close the connection:

```python
def close_mcp_connection(connection_id):
    # Send shutdown request
    requests.post(
        f"{base_url}/api/v1/mcp/shutdown",
        headers={"X-MCP-Connection-ID": connection_id},
        json={}
    )
    
    # Send exit notification
    requests.post(
        f"{base_url}/api/v1/mcp/exit",
        headers={"X-MCP-Connection-ID": connection_id}
    )

close_mcp_connection(connection_id)
```

## Simplified Interfaces

### Python Client Library

The above examples use direct HTTP requests, which can be complex. To simplify usage, we provide a Python client library:

```python
# Install with: pip install beam-mcp-client
from beam_mcp_client import MCPClient

# Create client
client = MCPClient("http://localhost:8888")

# Submit job
job_id = client.submit_job(
    name="my-flink-job",
    runner="flink",
    job_type="STREAMING",
    code_path="/path/to/pipeline.py",
    options={
        "parallelism": 4,
        "checkpoint_interval": 60000,
        "state_backend": "rocksdb",
        "input_source": "kafka://input-topic",
        "output_sink": "kafka://output-topic"
    }
)

# Check status
status = client.get_job_status(job_id)

# Scale job
client.scale_job(job_id, 8)

# Create savepoint
savepoint_path = client.create_savepoint(job_id)

# Cancel job
client.cancel_job(job_id)

# Close when done
client.close()
```

### CLI Tool

For even simpler usage, we provide a command-line interface:

```bash
# Install with: pip install beam-mcp-cli
# Configure once
mcp config set-server http://localhost:8888

# Submit job
mcp job submit \
  --name "my-flink-job" \
  --runner flink \
  --type STREAMING \
  --code /path/to/pipeline.py \
  --parallelism 4 \
  --checkpoint-interval 60000 \
  --state-backend rocksdb \
  --option input_source=kafka://input-topic \
  --option output_sink=kafka://output-topic

# Check status
mcp job status JOB_ID

# Scale job
mcp job scale JOB_ID --parallelism 8

# Create savepoint
mcp job savepoint JOB_ID

# Cancel job
mcp job cancel JOB_ID
```

## LLM Integration

This section explores how to integrate Large Language Models (LLMs) with the Beam MCP Server to create intelligent, adaptive data processing pipelines.

### Integration Patterns

The key integration patterns include:

1. **LLM as Orchestrator**: LLMs determine when and how to execute pipelines
2. **Natural Language Pipeline Control**: Translate user requests to API actions
3. **Adaptive Pipeline Optimization**: Analyze performance and adjust parameters
4. **Automated Error Recovery**: Detect, diagnose, and resolve pipeline issues
5. **Content-Aware Data Processing**: Process data differently based on content analysis

Here's a high-level architecture overview:

```
┌───────────┐    ┌───────────┐    ┌───────────┐    ┌───────────┐
│  LLM Tool │    │  MCP API  │    │ Beam MCP  │    │   Flink   │
│  or Agent │━━━━│  Client   │━━━━│  Server   │━━━━│  Cluster  │
└───────────┘    └───────────┘    └───────────┘    └───────────┘
       ↑               ↑                ↑                ↑
       ┃               ┃                ┃                ┃
       ↓               ↓                ↓                ↓
┌───────────┐    ┌───────────┐    ┌───────────┐    ┌───────────┐
│   User    │    │Pipeline &│    │ Job State │    │ Data Sinks │
│  Intent   │    │  Config  │    │  & Logs   │    │ & Sources  │
└───────────┘    └───────────┘    └───────────┘    └───────────┘
```

### Setting Up LLM Integration

First, ensure you have the necessary components:

```python
# Install required packages
# pip install beam-mcp-client openai langchain

from beam_mcp_client import MCPClient
from langchain_openai import ChatOpenAI
from langchain.agents import AgentExecutor, create_openai_functions_agent
from langchain.prompts import ChatPromptTemplate
```

### Tool Definitions for LLMs

Define the tools that allow the LLM to interact with the Beam MCP Server:

```python
from langchain.tools import StructuredTool
from pydantic import BaseModel, Field
from typing import Optional, Dict, List, Any

# Initialize MCP client
mcp_client = MCPClient("http://localhost:8888")

# Model for job submission parameters
class JobSubmissionParams(BaseModel):
    job_name: str = Field(..., description="Name of the job")
    runner_type: str = Field(..., description="Runner type (flink, spark, dataflow, direct)")
    job_type: str = Field(..., description="Job type (BATCH or STREAMING)")
    code_path: str = Field(..., description="Path to the pipeline code")
    parallelism: Optional[int] = Field(None, description="Parallelism level for the job")
    input_source: Optional[str] = Field(None, description="Input data source")
    output_sink: Optional[str] = Field(None, description="Output data sink")
    checkpoint_interval: Optional[int] = Field(None, description="Checkpoint interval in milliseconds")
    additional_options: Optional[Dict[str, Any]] = Field({}, description="Additional pipeline options")

# Define MCP tools
def submit_job_tool(params: JobSubmissionParams):
    """Submit a new Beam job to the specified runner."""
    pipeline_options = {
        "parallelism": params.parallelism,
        "input_source": params.input_source,
        "output_sink": params.output_sink,
        "checkpoint_interval": params.checkpoint_interval,
        **params.additional_options
    }
    
    # Filter out None values
    pipeline_options = {k: v for k, v in pipeline_options.items() if v is not None}
    
    job_id = mcp_client.submit_job(
        name=params.job_name,
        runner=params.runner_type,
        job_type=params.job_type,
        code_path=params.code_path,
        options=pipeline_options
    )
    
    return {"job_id": job_id, "status": "submitted"}

def get_job_status_tool(job_id: str):
    """Get the status of a Beam job."""
    status = mcp_client.get_job_status(job_id)
    return status

def scale_job_tool(job_id: str, parallelism: int):
    """Scale a running Beam job by adjusting parallelism."""
    result = mcp_client.scale_job(job_id, parallelism)
    return result

def cancel_job_tool(job_id: str, with_savepoint: bool = True):
    """Cancel a running Beam job."""
    result = mcp_client.cancel_job(job_id, with_savepoint)
    return result

# Create LangChain tool definitions
tools = [
    StructuredTool.from_function(submit_job_tool),
    StructuredTool.from_function(get_job_status_tool),
    StructuredTool.from_function(scale_job_tool),
    StructuredTool.from_function(cancel_job_tool)
]
```

### Building an LLM Agent

Create an agent that can understand user requests and control pipelines:

```python
# Initialize the LLM
llm = ChatOpenAI(model="gpt-4-turbo")

# Create a system prompt
system_prompt = """
You are a Data Pipeline Assistant that helps users manage Apache Beam jobs. 
You can submit, monitor, scale, and cancel jobs running on various distributed processing systems.

When a user requests a data processing task:
1. Determine the appropriate runner (flink for streaming jobs, dataflow for batch jobs)
2. Submit the job with relevant parameters
3. Monitor job status when asked
4. Suggest scaling for performance issues
5. Help recover from errors when they occur

Always verify the requirements before submitting a job and confirm actions with the user.
"""

# Create the prompt template
prompt = ChatPromptTemplate.from_messages([
    ("system", system_prompt),
    ("human", "{input}")
])

# Create the agent
agent = create_openai_functions_agent(llm, tools, prompt)
agent_executor = AgentExecutor(agent=agent, tools=tools, verbose=True)
```

Here's an example interaction:

```python
# Example interaction
response = agent_executor.invoke({
    "input": "I need to process streaming data from Kafka. The topic is 'user-events' and I want to aggregate by user ID and store results in another Kafka topic 'user-stats'. Use the streaming_aggregation.py pipeline."
})

print(response["output"])
```

The LLM might respond:

```
I'll help you set up a streaming job to process data from Kafka.

First, I need to submit a Beam job using Flink (the recommended runner for streaming jobs). I'll configure it to read from the 'user-events' topic and write to the 'user-stats' topic.

I'm submitting a streaming job with the following configuration:
- Name: kafka-user-aggregation-job
- Runner: flink
- Pipeline: streaming_aggregation.py
- Input: kafka://user-events
- Output: kafka://user-stats
- Parallelism: 4 (default for streaming jobs)
- Checkpoint interval: 60000ms (1 minute)

The job has been submitted successfully! The job ID is JOB-12345.

You can check the job status at any time by asking me for an update. Would you like me to monitor the job status for you?
```

### Natural Language Pipeline Control

Implement a conversational interface for pipeline management:

```python
def process_pipeline_command(user_command: str):
    """Process natural language pipeline management commands."""
    return agent_executor.invoke({"input": user_command})["output"]

# Example usage
commands = [
    "Start processing data from our production Kafka cluster using the user-events pipeline",
    "How is the job doing? Is it still running?",
    "The job seems to be processing slowly, can we scale it up?",
    "I see errors about memory in the logs, can you diagnose what's happening?",
    "Pause the job and create a savepoint",
    "Resume the job from the latest savepoint but with more memory",
    "Shut down all jobs when they finish their current processing"
]

for command in commands:
    print(f"USER: {command}")
    response = process_pipeline_command(command)
    print(f"ASSISTANT: {response}\n")
```

### Advanced Pipeline Management

Create more sophisticated intelligence for managing pipelines:

```python
# Performance monitoring and optimization
def analyze_job_performance(job_id: str):
    """Analyze job performance and suggest optimizations."""
    # Get job metrics
    metrics = mcp_client.get_job_metrics(job_id)
    
    # Get job configuration
    config = mcp_client.get_job_configuration(job_id)
    
    # Prepare context for LLM
    context = {
        "metrics": metrics,
        "configuration": config,
        "job_id": job_id
    }
    
    # Ask LLM for optimization recommendations
    response = llm.invoke(f"""
    Analyze the following Beam job metrics and configuration:
    
    Metrics: {metrics}
    Configuration: {config}
    
    Identify performance issues and recommend specific parameter adjustments 
    to improve throughput, reduce latency, or fix bottlenecks.
    
    Format your response as specific parameter changes with justification.
    """)
    
    # Parse recommendations
    recommendations = response.content
    
    return {
        "job_id": job_id,
        "analysis": recommendations
    }

# Error recovery
def diagnose_and_recover_job(job_id: str, error_logs: str):
    """Diagnose job errors and suggest recovery actions."""
    # Ask LLM to diagnose the issue
    diagnosis = llm.invoke(f"""
    Analyze the following error logs from a Beam job running on Flink:
    
    {error_logs}
    
    Identify the root cause of the failure and suggest specific actions to resolve it.
    Format your response as a diagnosis followed by step-by-step recovery actions.
    """)
    
    return {
        "job_id": job_id,
        "diagnosis": diagnosis.content
    }

# Pipeline code generation
def generate_pipeline_code(requirements: str, file_path: str):
    """Generate a Beam pipeline based on requirements."""
    pipeline_code = llm.invoke(f"""
    Create an Apache Beam pipeline in Python that satisfies these requirements:
    
    {requirements}
    
    The pipeline should follow these best practices:
    1. Include proper error handling and logging
    2. Use type hints for all functions
    3. Have clear documentation for all components
    4. Include comments explaining complex transforms
    5. Be optimized for the specified runner (Flink)
    
    Generate complete, runnable code with all imports and proper main function.
    """)
    
    # Save the generated code
    with open(file_path, "w") as f:
        f.write(pipeline_code.content)
    
    return {
        "file_path": file_path,
        "message": "Pipeline code generated successfully"
    }
```

## Example Workflows

### Data-Driven Pipeline Generation

```python
# Analyze a dataset to determine optimal processing pipeline
def generate_optimal_pipeline(dataset_path, runner_type):
    # Ask LLM to analyze dataset sample and recommend pipeline
    dataset_sample = get_dataset_sample(dataset_path)
    
    pipeline_recommendation = llm.invoke(f"""
    Analyze this dataset sample:
    {dataset_sample}
    
    Recommend an optimal Apache Beam pipeline structure for processing this data on {runner_type}.
    Include specific transform suggestions, parallelism recommendations, and state management approach.
    """)
    
    # Generate the pipeline code based on recommendations
    pipeline_code = generate_pipeline_code(pipeline_recommendation.content, "generated_pipeline.py")
    
    # Submit the generated pipeline
    job_params = JobSubmissionParams(
        job_name="auto-generated-pipeline",
        runner_type=runner_type,
        job_type="BATCH" if runner_type == "dataflow" else "STREAMING",
        code_path="generated_pipeline.py",
        parallelism=4
    )
    
    job_result = submit_job_tool(job_params)
    
    return {
        "job_id": job_result["job_id"],
        "pipeline_details": pipeline_recommendation.content
    }
```

### Continuous Monitoring and Optimization

```python
import asyncio

async def run_optimization_agent(job_id, check_interval=300):
    """Run a continuous monitoring and optimization agent for a job."""
    while True:
        # Get current job status
        status = get_job_status_tool(job_id)
        
        if status["state"] in ["FINISHED", "FAILED", "CANCELED"]:
            print(f"Job {job_id} is no longer running (state: {status['state']})")
            break
        
        # Analyze performance
        analysis = analyze_job_performance(job_id)
        recommendations = analysis["analysis"]
        
        if "increase parallelism" in recommendations.lower():
            # Extract suggested parallelism
            import re
            match = re.search(r"parallelism to (\d+)", recommendations)
            if match:
                new_parallelism = int(match.group(1))
                print(f"Auto-scaling job to parallelism {new_parallelism} based on analysis")
                scale_job_tool(job_id, new_parallelism)
        
        # Check for error patterns
        if "backpressure" in status.get("diagnostics", "").lower():
            print("Detected backpressure issues, requesting diagnosis")
            logs = mcp_client.get_job_logs(job_id, duration_seconds=300)
            diagnosis = diagnose_and_recover_job(job_id, logs)
            print(f"Diagnosis: {diagnosis['diagnosis']}")
        
        # Wait before checking again
        await asyncio.sleep(check_interval)
```

## Troubleshooting and FAQs

| Issue | Solution |
|-------|----------|
| MCP connection errors | Ensure you're handling the connection lifecycle correctly |
| Job submission failures | Check that your pipeline options are valid for Flink |
| Scaling doesn't work | Verify your Flink cluster supports dynamic scaling |
| Authentication errors | Configure authentication settings according to your environment |
| Missing metrics | Ensure metrics collection is enabled in your Flink configuration |
| LLM tool errors | Check the LLM is properly configured to use the MCP tools |
| Generated pipeline fails | Inspect the generated code and adjust as needed |

### Security Considerations

When integrating LLMs with the Beam MCP Server, consider these security best practices:

```python
# Set up secure MCP client with authentication
def create_secure_mcp_client(api_key=None, oauth_token=None):
    """Create a securely authenticated MCP client."""
    auth_headers = {}
    
    if api_key:
        auth_headers["X-API-Key"] = api_key
    
    if oauth_token:
        auth_headers["Authorization"] = f"Bearer {oauth_token}"
    
    # Create client with authentication
    return MCPClient(
        "https://secure-mcp-server.example.com",
        auth_headers=auth_headers,
        verify_ssl=True
    )

# Implement permission checks before executing actions
def check_permission(user_id, action, resource):
    """Check if a user has permission to perform an action."""
    # Query permission service or database
    pass
```

## Conclusion

The Beam MCP Server provides a powerful interface for managing Apache Beam jobs on various runners, with or without LLM integration. This guide has shown both the basic usage patterns and advanced LLM integration capabilities.

By integrating LLMs with the Beam MCP Server, you can create intelligent, self-managing data pipelines that automatically adapt to changing conditions, understand natural language commands, and optimize themselves.

**Next Steps:**
1. Start with a simple example pipeline
2. Integrate the MCP server into your workflow
3. Add LLM capabilities incrementally
4. Contribute feedback to help us improve

For additional help or feature requests, please reach out to our support team or file an issue on our GitHub repository. 

## Alternative LLM Integration with LlamaIndex and Local Models

In addition to LangChain and OpenAI, you can use LlamaIndex with locally deployed models for MCP integration. This approach offers enhanced privacy, reduced costs, and offline capabilities.

### Setting Up LlamaIndex with Local Models

First, install the necessary packages:

```bash
# Install LlamaIndex and relevant integrations
pip install llama-index llama-index-llms-ollama llama-index-embeddings-huggingface
```

### Using Ollama to Run Local LLMs

[Ollama](https://ollama.ai/) provides a simple way to run powerful LLMs locally:

```bash
# Install Ollama following instructions at https://ollama.ai/
# Then pull a model that supports function calling capabilities
ollama pull llama3.1
```

### Integration with Beam MCP Server

Here's how to create a tool-calling agent with LlamaIndex and Ollama:

```python
import json
from typing import Dict, List, Any, Optional
from llama_index.core.tools import FunctionTool
from llama_index.llms.ollama import Ollama
from llama_index.core.agent import ReActAgent
from beam_mcp_client import MCPClient

# Initialize MCP client
mcp_client = MCPClient("http://localhost:8888")

# Create tools for MCP server interaction
def submit_beam_job(
    job_name: str, 
    runner_type: str, 
    job_type: str, 
    code_path: str, 
    parallelism: Optional[int] = 4,
    **kwargs
) -> Dict[str, Any]:
    """
    Submit a new Beam job to the specified runner.
    
    Args:
        job_name: Name of the job
        runner_type: Type of runner (flink, spark, dataflow, direct)
        job_type: Type of job (BATCH or STREAMING)
        code_path: Path to the pipeline code
        parallelism: Parallelism level for the job
        **kwargs: Additional pipeline options
    
    Returns:
        Dict containing job_id and status
    """
    pipeline_options = {
        "parallelism": parallelism,
        **kwargs
    }
    
    job_id = mcp_client.submit_job(
        name=job_name,
        runner=runner_type,
        job_type=job_type,
        code_path=code_path,
        options=pipeline_options
    )
    
    return {"job_id": job_id, "status": "submitted"}

def get_job_status(job_id: str) -> Dict[str, Any]:
    """
    Get the status of a Beam job.
    
    Args:
        job_id: ID of the job to check
    
    Returns:
        Dict containing job status information
    """
    status = mcp_client.get_job_status(job_id)
    return status

# Create LlamaIndex tools
submit_job_tool = FunctionTool.from_defaults(fn=submit_beam_job)
get_status_tool = FunctionTool.from_defaults(fn=get_job_status)

# Initialize the local LLM
llm = Ollama(model="llama3.1", temperature=0.1, request_timeout=120.0)

# Create the agent with tools
agent = ReActAgent.from_tools(
    [submit_job_tool, get_status_tool],
    llm=llm,
    verbose=True,
    system_prompt="""
    You are a Data Pipeline Assistant that helps users manage Apache Beam jobs.
    You can submit and monitor jobs running on various distributed processing systems.
    
    When a user requests a data processing task:
    1. Determine the appropriate runner (flink for streaming jobs, dataflow for batch jobs)
    2. Submit the job with relevant parameters
    3. Monitor job status when asked
    
    Always verify the requirements before submitting a job and confirm actions with the user.
    """
)

# Example usage
response = agent.chat("I need to process streaming data using the wordcount.py pipeline on Flink")
print(response)
```

### Alternative: Using LlamaFile for Self-Contained Models

For an even simpler deployment, you can use LlamaFile, which bundles the model and runtime in a single file:

```bash
# Download a llamafile from HuggingFace
wget https://huggingface.co/jartine/TinyLlama-1.1B-Chat-v1.0-GGUF/resolve/main/TinyLlama-1.1B-Chat-v1.0.Q5_K_M.llamafile

# Make it executable
chmod +x TinyLlama-1.1B-Chat-v1.0.Q5_K_M.llamafile

# Run the server
./TinyLlama-1.1B-Chat-v1.0.Q5_K_M.llamafile --server --nobrowser
```

Then integrate with LlamaIndex:

```python
from llama_index.llms.llamafile import Llamafile

# Initialize the LlamaFile model
llm = Llamafile(temperature=0, seed=0)

# Use the same tools and agent setup as above
agent = ReActAgent.from_tools(
    [submit_job_tool, get_status_tool],
    llm=llm,
    verbose=True,
    # Same system prompt as above
)
```

### Creating a Function-Calling Agent with Local Models

Some local models have been specifically fine-tuned for function/tool calling:

```bash
# Pull a model better suited for function calling
ollama pull openhermes
```

```python
from llama_index.agent.openai import OpenAIAgent
from llama_index.llms.ollama import Ollama

# Configure the llm
llm = Ollama(model="openhermes", temperature=0)

# Create agent (improved function calling)
agent = OpenAIAgent.from_tools(
    [submit_job_tool, get_status_tool],
    llm=llm,
    verbose=True
)

# Run the agent
response = agent.chat("Submit a streaming job named 'my-flink-job' using pipeline.py on Flink")
print(response)
```

### Handling Structured Output with Pydantic

For more reliable structured output with local models:

```python
from pydantic import BaseModel, Field
from llama_index.program.openai import OpenAIPydanticProgram

# Define schema
class JobSubmission(BaseModel):
    job_name: str = Field(..., description="Name of the job")
    runner_type: str = Field(..., description="Type of runner (flink, spark, dataflow)")
    job_type: str = Field(..., description="Type of job (BATCH or STREAMING)")
    code_path: str = Field(..., description="Path to the pipeline code")
    parallelism: int = Field(4, description="Parallelism level")

# Create structured program
extraction_program = OpenAIPydanticProgram.from_defaults(
    output_cls=JobSubmission,
    prompt_template_str="Extract job submission details from this request: {query}",
    llm=llm,
    verbose=True
)

# Process natural language input
job_details = extraction_program.run(query="Create a streaming job for processing events with event_processor.py")

# Submit using extracted details
job_result = submit_beam_job(
    job_name=job_details.job_name,
    runner_type=job_details.runner_type,
    job_type=job_details.job_type,
    code_path=job_details.code_path,
    parallelism=job_details.parallelism
)
```

### Combining with RAG for Enhanced Pipeline Management

Use retrieval-augmented generation to help the model understand pipeline code:

```python
from llama_index.core import VectorStoreIndex, SimpleDirectoryReader
from llama_index.embeddings.huggingface import HuggingFaceEmbedding

# Load documents from pipeline code directory
documents = SimpleDirectoryReader("./pipelines").load_data()

# Create embedding model
embed_model = HuggingFaceEmbedding(model_name="BAAI/bge-small-en-v1.5")

# Create index
index = VectorStoreIndex.from_documents(documents, embed_model=embed_model)

# Create retrieval augmented query engine
query_engine = index.as_query_engine()

# Use RAG to analyze pipeline
pipeline_analysis = query_engine.query("Analyze the streaming_aggregation.py pipeline and explain its functionality")

# Use analysis in agent context
agent_response = agent.chat(
    f"I want to run the streaming_aggregation.py pipeline. Here's what it does: {pipeline_analysis.response}. "
    "Can you help me set up the right parameters?"
)
```

This approach provides the flexibility of local models with the performance needed for effective job management through the Beam MCP Server, particularly in environments where data privacy is paramount or internet connectivity is limited.

## Testing Tool Calling with Local LLMs

Successfully implementing tool calling with local LLMs requires careful testing and optimization. This section provides a systematic approach to ensure your local models can effectively interact with the Beam MCP Server.

### Model Selection for Tool Calling Capabilities

Not all local models perform equally for tool calling. Here's a tiered approach to selecting appropriate models:

```bash
# Tier 1: Specialized Function-Calling Models (Best Performance)
ollama pull functionary:latest      # Purpose-built for function calling
ollama pull gorilla-openfunctions   # Specifically trained for tool use

# Tier 2: Strong General-Purpose Models with Tool Capabilities
ollama pull llama3:8b-instruct      # Good balance for most Mac hardware
ollama pull mistral-nemo:latest     # Excellent reasoning with tool support
ollama pull openhermes:latest       # Strong function calling capabilities
ollama pull nous-hermes2:instruct   # Excellent for structured output

# Tier 3: Large Models (Require Powerful Hardware)
ollama pull llama3:70b-instruct     # Exceptional but requires significant resources
ollama pull mixtral:instruct        # Excellent for complex tool interactions
```

The models listed above have consistently demonstrated stronger tool calling abilities compared to other options. Your choice depends on your Mac's specifications and your specific needs.

### Hardware Considerations for MacOS

Local model performance varies significantly based on hardware:

| Mac Configuration | Recommended Models | Max Context | Notes |
|-------------------|-------------------|-------------|-------|
| M1/M2 (8GB RAM) | llama3:8b, mistral:instruct, openhermes:7b | 4K tokens | Limit batch size; enable Metal acceleration |
| M1/M2 (16GB RAM) | functionary, nous-hermes2, mistral-nemo | 8K tokens | Good balance of performance and capability |
| M1/M2 Pro/Max/Ultra | llama3:70b, mixtral:instruct | 16K+ tokens | Can handle complex multi-tool interactions |
| Intel Mac | mistral:instruct, openhermes:7b | 2-4K tokens | CPU-only inference; reduce context length |

Enable hardware acceleration for optimal performance:

```bash
# For Apple Silicon (M1/M2/M3) 
ollama serve --compute metal

# Check if Metal acceleration is active
ollama show gpu
```

### Systematic Testing Framework

Implement this testing framework to validate tool calling capabilities:

```python
import time
import logging
from typing import Dict, List, Any, Optional
from llama_index.core.tools import FunctionTool
from llama_index.llms.ollama import Ollama
from llama_index.core.agent import ReActAgent

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Tool definitions (using the same as defined previously)
# ...

# Function to test tool calling with various prompts
def test_tool_capabilities(model_name: str, test_prompts: List[str]):
    """Test a model's ability to call tools with various prompts."""
    
    results = {
        "model": model_name,
        "success_rate": 0,
        "avg_response_time": 0,
        "tests": []
    }
    
    # Initialize LLM with appropriate parameters for the model
    llm = Ollama(
        model=model_name, 
        temperature=0.1,
        request_timeout=60.0,
        # Adjust parameters based on model
        model_kwargs={"num_gpu": 1, "num_thread": 4}
    )
    
    # Create agent with tools
    agent = ReActAgent.from_tools(
        [submit_job_tool, get_status_tool],
        llm=llm,
        verbose=True,
        system_prompt="""
        You are a Data Pipeline Assistant that helps users manage Apache Beam jobs.
        
        When users request pipeline operations, you MUST use the appropriate tool:
        - submit_beam_job: To submit new pipeline jobs
        - get_job_status: To check job status
        
        ALWAYS provide your reasoning before using a tool.
        """
    )
    
    successful_tests = 0
    total_time = 0
    
    for i, prompt in enumerate(test_prompts):
        test_result = {
            "prompt": prompt,
            "success": False,
            "tool_used": None,
            "response_time": 0,
            "error": None
        }
        
        logger.info(f"Testing prompt {i+1}/{len(test_prompts)}: {prompt}")
        
        try:
            start_time = time.time()
            response = agent.chat(prompt)
            end_time = time.time()
            response_time = end_time - start_time
            
            # Determine if tool was used correctly based on response content
            response_str = str(response)
            if "Action: submit_beam_job" in response_str and "job_name" in prompt.lower():
                test_result["success"] = True
                test_result["tool_used"] = "submit_beam_job"
            elif "Action: get_job_status" in response_str and "status" in prompt.lower():
                test_result["success"] = True
                test_result["tool_used"] = "get_job_status"
            else:
                test_result["success"] = False
                test_result["tool_used"] = "No tool or incorrect tool"
            
            test_result["response_time"] = response_time
            total_time += response_time
            
            if test_result["success"]:
                successful_tests += 1
                
        except Exception as e:
            test_result["success"] = False
            test_result["error"] = str(e)
            logger.error(f"Error testing prompt: {prompt}\nError: {str(e)}")
        
        results["tests"].append(test_result)
        
    # Calculate summary metrics
    results["success_rate"] = (successful_tests / len(test_prompts)) * 100
    results["avg_response_time"] = total_time / len(test_prompts)
    
    return results

# Standard test prompts covering various scenarios
standard_test_prompts = [
    "Submit a streaming job named 'kafka-processor' using the streaming_pipeline.py code",
    "Check the status of job JOB-12345",
    "I need to run wordcount.py on the Flink runner with 4 parallel tasks",
    "Has my batch processing job finished yet? The job ID is JOB-67890",
    "Create a new Beam pipeline with the user_activity.py file on Dataflow",
    "What's the current state of the job I submitted earlier?",
    "Run the daily aggregation pipeline on the production cluster",
    "Is job JOB-54321 still running or did it complete?",
]

# Run tests with multiple models
def compare_models(model_names: List[str]):
    """Compare multiple models for tool calling capabilities."""
    
    results = []
    
    for model in model_names:
        logger.info(f"Testing model: {model}")
        model_results = test_tool_capabilities(model, standard_test_prompts)
        results.append(model_results)
        
        # Log summary immediately after testing each model
        logger.info(f"Model: {model}")
        logger.info(f"Success Rate: {model_results['success_rate']:.2f}%")
        logger.info(f"Avg Response Time: {model_results['avg_response_time']:.2f}s")
        logger.info("-" * 50)
    
    return results

# Example usage
test_models = ["functionary", "mistral:instruct", "openhermes:latest"]
comparison_results = compare_models(test_models)
```

### Common Challenges and Solutions

#### 1. Inconsistent Tool Calling Format

**Problem**: Local models sometimes fail to produce the exact function call format.

**Solution**: Implement a validation and retry mechanism:

```python
def validate_and_execute_tool_call(agent_response, max_retries=2):
    """Validate tool call format and retry if needed."""
    
    response_str = str(agent_response)
    
    # Check for properly formatted tool calls
    if "Action: " in response_str and "Action Input: " in response_str:
        # Extract tool name and parameters
        action_match = re.search(r"Action: (.*?)(?:\n|$)", response_str)
        params_match = re.search(r"Action Input: (.*?)(?:\n\n|$)", response_str, re.DOTALL)
        
        if action_match and params_match:
            tool_name = action_match.group(1).strip()
            params_str = params_match.group(1).strip()
            
            try:
                # Parse the parameters as JSON
                params = json.loads(params_str)
                
                # Execute the tool based on tool_name
                if tool_name == "submit_beam_job":
                    return submit_beam_job(**params)
                elif tool_name == "get_job_status":
                    return get_job_status(**params)
                else:
                    raise ValueError(f"Unknown tool: {tool_name}")
                    
            except (json.JSONDecodeError, TypeError) as e:
                if max_retries > 0:
                    # Ask the model to fix the format and retry
                    retry_response = agent.chat(
                        f"The function call format was incorrect. Please retry with valid JSON parameters. Error: {str(e)}"
                    )
                    return validate_and_execute_tool_call(retry_response, max_retries - 1)
                else:
                    raise ValueError(f"Failed to parse tool parameters after retries: {str(e)}")
    
    # If we get here, no valid tool call was found
    if max_retries > 0:
        retry_response = agent.chat(
            "You need to use one of your tools to complete this task. Please use the correct format:\n"
            "Action: tool_name\n"
            "Action Input: {\"param1\": \"value1\", \"param2\": value2}"
        )
        return validate_and_execute_tool_call(retry_response, max_retries - 1)
    
    raise ValueError("No valid tool call detected after retries")
```

#### 2. Slow Inference on Resource-Constrained Macs

**Problem**: Local inference can be very slow on older or resource-limited Macs.

**Solution**: Optimize model parameters and quantization level:

```python
# For resource-constrained Macs
llm = Ollama(
    model="mistral:instruct-q4_0",  # Use Q4_0 quantization
    temperature=0.1,
    context_window=2048,  # Reduced context window
    model_kwargs={
        "num_thread": 4,
        "num_gpu": 1,
        "seed": 42,  # Consistent responses
        "num_predict": 512,  # Limit token generation
        "mirostat": 2,  # Dynamic temperature control
    }
)
```

#### 3. Unreliable Tool Selection

**Problem**: Models sometimes struggle to choose the appropriate tool.

**Solution**: Implement a structured decision-making process in the prompt:

```python
agent = ReActAgent.from_tools(
    [submit_job_tool, get_status_tool],
    llm=llm,
    verbose=True,
    system_prompt="""
    You are a Data Pipeline Assistant that manages Apache Beam jobs.
    
    IMPORTANT: Follow this EXACT decision process for every request:
    
    1. DETERMINE REQUEST TYPE:
       - If user wants to CREATE, RUN, SUBMIT, or START a job → Use submit_beam_job
       - If user wants to CHECK, MONITOR, or GET STATUS of a job → Use get_job_status
    
    2. EXTRACT PARAMETERS:
       - For submit_beam_job: job_name, runner_type, job_type, code_path (all required)
       - For get_job_status: job_id (required)
    
    3. FORMAT YOUR RESPONSE:
       First explain your reasoning, then call the tool using EXACTLY this format:
       
       Action: tool_name
       Action Input: {
           "param1": "value1",
           "param2": "value2"
       }
    
    NEVER skip this process. ALWAYS use a tool for pipeline operations.
    """
)
```

### Integration with Real MCP Server

To test with your actual Beam MCP Server, integrate the testing framework with your server:

```python
from beam_mcp_client import MCPClient

# Initialize real MCP client
mcp_client = MCPClient("http://localhost:8888")

# Create real tool functions
def submit_beam_job_real(job_name, runner_type, job_type, code_path, **kwargs):
    """Submit a job to the real MCP server."""
    try:
        job_id = mcp_client.submit_job(
            name=job_name,
            runner=runner_type,
            job_type=job_type,
            code_path=code_path,
            options=kwargs
        )
        return {"job_id": job_id, "status": "submitted"}
    except Exception as e:
        return {"error": str(e)}

def get_job_status_real(job_id):
    """Get job status from real MCP server."""
    try:
        status = mcp_client.get_job_status(job_id)
        return status
    except Exception as e:
        return {"error": str(e)}

# Replace mock tools with real ones in your agent
submit_job_tool_real = FunctionTool.from_defaults(fn=submit_beam_job_real)
get_status_tool_real = FunctionTool.from_defaults(fn=get_job_status_real)

# Create agent with real tools
agent_real = ReActAgent.from_tools(
    [submit_job_tool_real, get_status_tool_real],
    llm=llm,
    verbose=True,
    # Same system prompt as before
)
```

### Practical End-to-End Testing Workflow

For comprehensive testing before production deployment:

1. **Start with simulated tools** to validate model behavior
2. **Use the testing framework** to quantify success rates across different prompt styles
3. **Test with real MCP server** in development environment
4. **Gradually increase complexity** of requests
5. **Monitor and log performance** in production

```python
# Complete end-to-end testing workflow
def progressive_testing_workflow():
    """Run a progressive testing workflow from simulation to production."""
    
    # Phase 1: Test with simulated tools
    logger.info("Phase 1: Testing with simulated tools")
    simulate_results = test_tool_capabilities("functionary", standard_test_prompts)
    
    if simulate_results["success_rate"] < 70:
        logger.error("Simulation phase failed with low success rate. Review model/prompts.")
        return False
    
    # Phase 2: Test with development MCP server
    logger.info("Phase 2: Testing with development MCP server")
    # Set up tools connected to dev server
    # ... 
    # Run tests
    # ...
    
    # Phase 3: Gradual integration with more complex scenarios
    logger.info("Phase 3: Testing complex scenarios")
    complex_scenarios = [
        "I need to process real-time data from our Kafka cluster. The topic is 'user-events' and the data needs to be aggregated by user ID. Use window functions with 1 minute intervals.",
        # Add more complex scenarios
    ]
    
    # ... test with complex scenarios ...
    
    # Phase 4: Production validation (with safeguards)
    logger.info("Phase 4: Limited production validation")
    # ... limited deployment tests ...
    
    return True

# Execute workflow
success = progressive_testing_workflow()
if success:
    logger.info("All testing phases completed successfully. Ready for production.")
else:
    logger.warning("Testing workflow did not complete successfully. Review logs for details.")
```

### Performance Benchmarking

To choose the most effective model for your specific hardware and use case, run this benchmark:

```python
def benchmark_models_for_mcp_tools():
    """Benchmark different models for MCP tool interactions."""
    
    models_to_test = [
        "functionary:latest",
        "llama3:8b-instruct",
        "mistral:instruct",
        "openhermes:latest",
        "nous-hermes2:instruct"
    ]
    
    test_categories = {
        "basic_submission": [
            "Submit a job called 'test-job' using wordcount.py on Flink",
            "Run wordcount.py as a batch job on Dataflow",
        ],
        "status_checking": [
            "Check status of job JOB-12345",
            "Is my job JOB-67890 still running?",
        ],
        "complex_requests": [
            "I need to process streaming data from Kafka using streaming_aggregate.py with 8 parallel tasks and 30 second checkpointing",
            "Set up a daily batch job for processing the transaction data using transaction_processor.py on Spark",
        ]
    }
    
    results = {}
    
    for model in models_to_test:
        model_results = test_tool_capabilities(model, standard_test_prompts)
        results[model] = model_results
    
    # Find best model
    best_model = max(results.items(), key=lambda x: x[1]["success_rate"])
    
    logger.info(f"Benchmark completed. Best model: {best_model[0]} with {best_model[1]['success_rate']:.2f}% success rate")
    return results

# Run benchmark
benchmark_results = benchmark_models_for_mcp_tools()
```

### Critical Nuances for Production Use

When deploying to production, consider these critical factors:

1. **Deterministic behavior**: Set a fixed seed for consistent responses
   ```python
   llm = Ollama(model="functionary", seed=42, temperature=0)
   ```

2. **Timeout handling**: Implement graceful timeout management
   ```python
   llm = Ollama(model="functionary", request_timeout=30.0)
   # Also implement application-level timeout handling
   ```

3. **Error recovery**: Build robust recovery mechanisms for failed tool calls
   ```python
   try:
       result = agent.chat(user_input)
       # Process result
   except Exception as e:
       logger.error(f"Agent error: {str(e)}")
       # Implement fallback strategy
       fallback_response = "I'm currently having trouble processing your request. Please try again with more specific instructions."
   ```

4. **Input validation**: Always validate user input before passing to tools
   ```python
   def sanitize_and_validate_input(user_input):
       # Remove sensitive patterns, validate length, etc.
       return sanitized_input
   ```

5. **Monitoring**: Track tool calling success rates in production
   ```python
   # Implement metrics collection for tracking success/failure
   def log_tool_call_metrics(tool_name, success, response_time):
       # Log to monitoring system
       pass
   ```

By addressing these nuances proactively, you'll ensure a more robust and reliable tool calling experience with local LLMs in your Beam MCP Server integration.

# MCP Protocol Compliance

> **Note:** For a comprehensive guide on using the Beam MCP Server, including LLM integration, 
> please refer to the current document. This section is a reference to the more detailed 
> [MCP Protocol Compliance](../mcp_protocol_compliance.md) document that focuses specifically on MCP protocol compliance details. 