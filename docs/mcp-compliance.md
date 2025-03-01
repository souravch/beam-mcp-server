# MCP Compliance Guide

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
curl http://localhost:8082/api/v1/manifest

# Check health endpoint
curl http://localhost:8082/api/v1/health/health

# Test context propagation
curl http://localhost:8082/api/v1/runners \
  -H "MCP-Session-ID: test-session-123"
```

## Client Integration Guide

When building clients to work with the MCP server:

1. **Start with the manifest**: Always fetch the manifest to discover available tools
   ```python
   manifest = requests.get("http://localhost:8082/api/v1/manifest").json()
   ```

2. **Maintain context**: Pass the same session ID across requests
   ```python
   headers = {"MCP-Session-ID": "my-session-id"}
   response = requests.get("http://localhost:8082/api/v1/runners", headers=headers)
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