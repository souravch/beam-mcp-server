# MCP Protocol Support for Beam MCP Server

This module implements support for the [Model Context Protocol (MCP)](https://modelcontextprotocol.io/) in the Beam MCP Server. It provides a complete connection lifecycle with proper capability negotiation, allowing clients to establish connections, negotiate capabilities, and exchange messages according to the MCP specification.

## Features

The MCP module provides the following features:

1. **Complete Connection Lifecycle**: Initialization, active management, and termination of MCP connections.
2. **Capability Negotiation**: Server capability declaration, client compatibility validation, and version checking.
3. **Redis-backed Connection State**: Persistent tracking of connections with automatic expiration.
4. **HTTP Transport with SSE**: JSON-RPC communication and server-sent notifications.
5. **FastAPI Integration**: Seamless integration with the FastAPI framework.
6. **Feature-based Route Control**: Dynamically enable/disable endpoints based on client capabilities.
7. **Semantic Version Compatibility**: Robust version compatibility checking for features.
8. **Structured Message Types**: Strongly typed message models with validation.
9. **Comprehensive Error Handling**: Detailed error codes and messages for all operations.
10. **Batch Message Processing**: Support for processing multiple requests in a single call.

## Architecture

The MCP implementation consists of several key components:

1. **Connection Manager**: Tracks connection state throughout the lifecycle, using Redis for persistence
2. **Capability Registry**: Manages server capabilities and checks compatibility with clients
3. **HTTP Transport**: Implements JSON-RPC over HTTP with SSE for notifications
4. **FastAPI Integration**: Provides endpoints for MCP protocol messages

## Configuration

MCP protocol support can be configured using environment variables:

| Environment Variable | Description | Default |
|---------------------|-------------|---------|
| `MCP_ENABLED` | Enable MCP protocol support | `True` |
| `MCP_REDIS_URL` | Redis URL for connection state storage | `redis://localhost:6379/0` |
| `MCP_SERVER_NAME` | Server name for capability information | `beam-mcp-server` |
| `MCP_SERVER_VERSION` | Server version for capability information | `1.0.0` |
| `MCP_PREFIX` | Router prefix for MCP endpoints | `/api/v1/mcp` |
| `MCP_CONNECTION_EXPIRY` | Connection expiration in seconds | `1800` |

## Connection Lifecycle

### 1. Initialization

1. Client sends `initialize` request with protocol version and capabilities
2. Server validates protocol version and capability compatibility
3. Server creates a new connection and returns its capabilities
4. Client sends `initialized` notification
5. Server marks the connection as active

### 2. Message Exchange

Once initialized, clients can:
1. Send JSON-RPC requests via `POST /api/v1/mcp/jsonrpc`
2. Receive server notifications via SSE connection to `GET /api/v1/mcp/events`

### 3. Termination

1. Client sends `shutdown` request
2. Server marks connection as terminating and responds
3. Client sends `exit` notification
4. Server cleans up the connection

## Connection State Management

The connection lifecycle transitions through the following states:

```
UNINITIALIZED --[initialize]--> INITIALIZING --[initialized]--> ACTIVE
                                    |
                                    v
               TERMINATED <--[exit]-- TERMINATING <--[shutdown]-- ACTIVE
```

Connection state is persisted in Redis with automatic expiration.

## Capability Negotiation

The MCP protocol uses capabilities to negotiate supported features between the client and server. The server declares its capabilities, and the client validates its compatibility with these capabilities.

### Server Capability Declaration

Server capabilities are declared during initialization of the MCP module. The server declares:

1. **Protocol Version**: The MCP protocol version supported by the server.
2. **Server Information**: Name and version of the server.
3. **Feature Capabilities**: Set of features supported by the server, with their properties and configuration.

Example:

```python
# Register features
registry.register_feature(
    "core.jsonrpc",  # Feature name
    properties={
        "version": "2.0",
        "methods": ["initialize", "shutdown", "jsonrpc"],
    },
    level=FeatureCapabilityLevel.REQUIRED,  # This feature is required
    version_compatibility=VersionCompatibility.COMPATIBLE  # Use semver compatibility
)
```

### Feature Capability Configuration

Each feature can be configured with the following parameters:

1. **Support Level**: Whether the feature is required, preferred, optional, or experimental.
2. **Version Compatibility**: How client and server versions should match (exact, compatible, any).
3. **Properties**: Feature-specific properties and their values.
4. **Required Properties**: Properties that must be present in the client capabilities.

### Client Compatibility Validation

During connection initialization, the server validates the client's capabilities against its own. The validation includes:

1. **Protocol Version**: The client's protocol version must be compatible with the server's.
2. **Required Features**: The client must support all required features.
3. **Feature Version Compatibility**: For each feature, the client's version must be compatible with the server's based on the specified compatibility mode.
4. **Required Properties**: The client must provide all required properties for each feature.

If any validation fails, the connection is rejected with a detailed error message.

### Feature Availability Check

After connection initialization, the server can check if a specific feature is enabled for a connection:

```python
if is_feature_enabled("tool.job_management", connection):
    # The client supports job management
    # Provide job management functionality
```

## Feature-Based API Endpoints

The MCP module provides a way to create API endpoints that are only available if specific features are enabled.

### Feature Router

You can create a router for endpoints that require a specific feature:

```python
# Create a router for job management feature
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

# Include the job router in the app
app.include_router(job_router.get_router())
```

### Feature Dependency

You can also use a dependency to check if a feature is enabled:

```python
@app.get("/api/v1/runners")
async def list_runners(
    connection: ConnectionState = get_feature_dependency("tool.runner_management")
):
    """List all runners (requires runner_management feature)."""
    return {"runners": [{"id": "abc", "name": "Example Runner"}]}
```

## Using MCP in API Endpoints

API endpoints can access the validated MCP connection using a dependency:

```python
from src.server.mcp import get_mcp_connection_dependency
from fastapi import APIRouter, Depends

router = APIRouter()

@router.post("/some-endpoint")
async def some_endpoint(connection = Depends(get_mcp_connection_dependency())):
    # Access connection.client_info, connection.client_capabilities, etc.
    return {"message": f"Hello, {connection.client_info.name}!"}
```

## Testing

When testing endpoints that use MCP connections, you'll need to mock the connection dependency:

```python
from unittest.mock import MagicMock
from src.server.mcp.models import ConnectionState, ConnectionLifecycleState, ClientInfo

def test_some_endpoint():
    # Mock connection
    mock_connection = ConnectionState(
        id="test-connection",
        client_info=ClientInfo(name="test-client", version="1.0"),
        client_capabilities={},
        state=ConnectionLifecycleState.ACTIVE,
        created_at=datetime.now(UTC),
        last_activity=datetime.now(UTC)
    )
    
    # Pass to endpoint function
    response = some_endpoint(connection=mock_connection)
    
    # Assert response
    assert response["message"] == "Hello, test-client!"
```

## Known Limitations

1. Only HTTP transport is supported; stdio transport is not implemented
2. Connection state is tied to Redis; no alternative storage backends are supported
3. No rate limiting for MCP protocol messages (relies on the authentication module's rate limiting)

## Message Handling

The MCP module provides a robust message handling system with structured message types, validation, and comprehensive error handling.

### Message Types

MCP messages are categorized into several types:

1. **Requests**: Messages from client to server that expect a response
2. **Responses**: Server responses to client requests
3. **Notifications**: Messages that don't expect a response
4. **Errors**: Error responses for failed requests

### Message Structure

Each MCP message consists of two parts:

1. **Metadata**: Information about the message (type, direction, category, timestamps, etc.)
2. **Payload**: The actual content of the message (JSON-RPC request/response)

Example of a complete MCP message:

```python
message = MCPMessage(
    metadata=MessageMetadata(
        type=MessageType.REQUEST,
        direction=MessageDirection.CLIENT_TO_SERVER,
        category=MessageCategory.JSONRPC,
        connection_id="conn-123",
        request_id="req-456"
    ),
    payload=JsonRpcRequest(
        method="get_job_status",
        params={"job_id": "job-789"},
        id="req-456"
    )
)
```

### JSON-RPC Implementation

The MCP module implements JSON-RPC 2.0 with support for:

1. **Single Requests**: Processing individual JSON-RPC requests
2. **Batch Requests**: Processing multiple requests in a single call
3. **Notifications**: Handling messages that don't expect a response
4. **Error Handling**: Detailed error responses for all types of failures

### Error Handling

The MCP module provides comprehensive error handling with:

1. **Standard JSON-RPC Errors**: Parse errors, invalid requests, method not found, etc.
2. **MCP-specific Errors**: Protocol version mismatch, incompatible capabilities, etc.
3. **Transport Errors**: Issues with HTTP/SSE communication
4. **Resource/Tool Errors**: Issues with specific resources or tools
5. **Validation Errors**: Issues with message structure or content

Each error includes:
- A numeric error code
- A descriptive error message
- Optional additional data for debugging

Example of error handling:

```python
try:
    # Process request
    result = await process_request(request)
    return create_jsonrpc_success_response(request.id, result)
except ResourceNotFoundError as e:
    return create_jsonrpc_error_response(
        request.id,
        resource_not_found(e.resource_id).to_dict()
    )
except ValidationError as e:
    return create_jsonrpc_error_response(
        request.id,
        validation_error(str(e)).to_dict()
    )
except Exception as e:
    return create_jsonrpc_error_response(
        request.id,
        internal_error(str(e)).to_dict()
    )
```

### Batch Processing

The MCP module supports processing multiple JSON-RPC requests in a single batch:

```python
# Process batch request
batch_request = JsonRpcBatchRequest(
    requests=[
        JsonRpcRequest(method="method1", id="1"),
        JsonRpcRequest(method="method2", id="2")
    ]
)

# Process each request in batch
responses = []
for request in batch_request.requests:
    response = await process_request(request)
    responses.append(response)

# Return batch response
return JsonRpcBatchResponse(responses=responses)
``` 