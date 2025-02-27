# Apache Beam MCP Server - Model Context Protocol Compliance

This document outlines how the Apache Beam MCP Server implements the Model Context Protocol (MCP) standard. The server is designed to be fully compliant with the MCP 1.0 specification.

## MCP Protocol Overview

The Model Context Protocol (MCP) is a standard for communication between AI models and tools that defines:

1. How tools should describe themselves to models
2. How models should invoke tools
3. How contextual information should be maintained between operations
4. How to handle stateful operations across multiple requests
5. How to handle streaming and event-based communication

## Implementation Details

The Apache Beam MCP Server implements the MCP 1.0 standard with the following components:

### 1. Core MCP Components

- **BeamMCPServer**: Extends `FastMCP` from the official MCP library to provide a full MCP implementation
- **MCPContext**: Implementation of the MCP context model for maintaining state between operations
- **Tool Registration**: Uses the standard `Tool` class from the MCP library for tool registration
- **Resource Registration**: Uses the standard `Resource` class from the MCP library for resource registration

### 2. MCP Protocol Endpoints

The server provides all standard MCP protocol endpoints:

- **Manifest**: `GET /api/v1/manifest` - Returns a detailed manifest of server capabilities and tools
- **Context**: `GET /api/v1/context` - Returns the current MCP context
- **Health**: `GET /api/v1/health/mcp` - MCP-specific health check endpoint

### 3. MCP Request/Response Format

All API endpoints follow the MCP standard request/response format:

- **Request**: Accepts MCP context headers and maintains session state
- **Response**: Returns standardized `LLMToolResponse` format with `success`, `data`, `message`, and `error` fields

### 4. MCP Context Handling

The server implements full MCP context handling with:

- **Session Management**: Unique session IDs for tracking conversations
- **Transaction Support**: Multi-step operations with transaction IDs
- **Context Propagation**: Context is properly propagated through all operations
- **Context Headers**: Supports all standard MCP context headers:
  - `MCP-Session-ID`: Session identifier
  - `MCP-Trace-ID`: Trace identifier for distributed tracing
  - `MCP-Transaction-ID`: Transaction identifier for multi-step operations
  - `MCP-User-ID`: User identifier

### 5. MCP Tool Interface

Tools are implemented according to the MCP standard:

- **Standard Schema**: Tools use JSON Schema for parameter specification
- **Documentation**: Tools include detailed descriptions for LLM consumption
- **Tool Discovery**: Tools are discoverable through the manifest endpoint
- **Standard Invocation**: Tools follow the standard MCP invocation pattern

### 6. MCP Resource Management

Resources are implemented according to the MCP standard:

- **Resource Definition**: Resources have standardized fields
- **Resource Operations**: Resources support standard operations (CREATE, READ, UPDATE, DELETE, LIST)
- **Resource URIs**: Resources have consistent URI patterns
- **Resource State**: Resources maintain proper state through operations

### 7. MCP Transport Options

The server supports multiple MCP transport options:

- **HTTP**: Default transport for RESTful API access
- **STDIO**: Command-line interface for scripting and CLI tools
- **SSE**: Server-Sent Events for streaming updates (optional)
- **WebSocket**: Bidirectional communication (optional)

## Configuration Options

The server provides extensive configuration options for MCP protocol compliance, including:

- `mcp_version`: MCP protocol version (currently 1.0)
- `mcp_server_name`: Name of the MCP server
- `mcp_implementation_name`: Name of the MCP implementation
- `mcp_implementation_version`: Version of the MCP implementation
- `mcp_provider`: Name of the MCP provider
- `mcp_context_headers`: List of supported MCP context headers
- `mcp_streaming_support`: Flag to enable MCP streaming support

## Command-Line Options

The server includes MCP-specific command-line options:

- `--mcp-version`: MCP protocol version to use
- `--mcp-stdio`: Enable MCP stdio transport (for command-line tools)
- `--mcp-sse`: Enable MCP Server-Sent Events transport
- `--mcp-websocket`: Enable MCP WebSocket transport
- `--mcp-log-level`: MCP log level

## Testing MCP Compliance

To test the MCP compliance of the server:

1. Start the server with default options:
   ```
   python main.py
   ```

2. Check the manifest endpoint:
   ```
   curl http://localhost:8080/api/v1/manifest
   ```

3. Check the MCP health endpoint:
   ```
   curl http://localhost:8080/api/v1/health/mcp
   ```

4. Test context propagation:
   ```
   curl http://localhost:8080/api/v1/context \
     -H "MCP-Session-ID: test-session-123" \
     -H "MCP-Transaction-ID: test-transaction-456"
   ```

## Best Practices for MCP Clients

When building clients to interact with the Apache Beam MCP Server:

1. **Maintain Context**: Always pass the MCP context headers between requests
2. **Use Transactions**: Group related operations using transaction IDs
3. **Follow Schemas**: Adhere to the parameter schemas defined in the manifest
4. **Handle Errors**: Check the `success` field in responses and handle errors appropriately
5. **Discover Capabilities**: Use the manifest endpoint to discover available tools and resources

## Future Enhancements

The server will continue to evolve with the MCP standard, with planned enhancements including:

1. **Enhanced Notifications**: Improved event-based communication
2. **Subscription System**: Advanced resource subscription capabilities
3. **Extended Prompts**: More sophisticated prompt template handling
4. **Streaming Improvements**: Enhanced streaming capabilities for real-time data 