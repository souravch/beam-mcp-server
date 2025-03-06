# Apache Beam MCP Server API Documentation

## Table of Contents
- [Apache Beam MCP Server API Documentation](#apache-beam-mcp-server-api-documentation)
  - [Table of Contents](#table-of-contents)
  - [Introduction](#introduction)
  - [General API Conventions](#general-api-conventions)
    - [Response Format](#response-format)
    - [Error Handling](#error-handling)
    - [Field Inclusion Behavior](#field-inclusion-behavior)
    - [Authentication](#authentication)
  - [Resources API](#resources-api)
    - [Endpoints](#endpoints)
    - [Resource Object](#resource-object)
  - [Tools API](#tools-api)
    - [Endpoints](#endpoints-1)
    - [Tool Object](#tool-object)
  - [Contexts API](#contexts-api)
    - [Endpoints](#endpoints-2)
    - [Context Object](#context-object)
  - [Migration Guide](#migration-guide)
    - [Changes in v1.0.0](#changes-in-v100)

## Introduction

The Apache Beam MCP Server provides a RESTful API for managing Beam pipelines, resources, tools, and execution contexts. This document describes the API endpoints, request/response formats, and usage patterns.

## General API Conventions

### Response Format

All API responses follow a standard format:

```json
{
  "success": true|false,  // Whether the operation succeeded
  "message": "Human-readable message",
  "data": { ... }  // Resource/Tool/Context object, always present
}
```

### Error Handling

When an error occurs, the API always returns:
1. An appropriate HTTP status code (2xx, 4xx, or 5xx)
2. A JSON response with `success: false`
3. A human-readable error message
4. A dummy object in the `data` field (never null)

Example error response:

```json
{
  "success": false,
  "message": "Resource with ID 'nonexistent-id' not found",
  "data": {
    "mcp_resource_id": "error",
    "mcp_resource_type": "resource",
    "name": "Error",
    "description": "Error response",
    "resource_type": "custom",
    "location": "null://error",
    "status": "error",
    "metadata": {}
  }
}
```

### Field Inclusion Behavior

API responses may exclude fields that:
- Have null values
- Have unset values (defaults)

Clients should not rely on all fields being present in every response. Always check for the existence of fields before accessing them.

### Authentication

Authentication is handled through standard HTTP authentication mechanisms. See the deployment documentation for details.

## Resources API

The Resources API allows management of datasets, files, ML models, and other assets used in data pipelines.

### Endpoints

- `GET /api/v1/resources/` - List all resources
- `GET /api/v1/resources/{resource_id}` - Get a specific resource
- `POST /api/v1/resources/` - Create a new resource
- `PUT /api/v1/resources/{resource_id}` - Update a resource
- `DELETE /api/v1/resources/{resource_id}` - Delete a resource

### Resource Object

A Resource object has the following structure:

```json
{
  "mcp_resource_id": "dataset-123",         // Primary identifier
  "mcp_resource_type": "resource",
  "mcp_created_at": "2023-08-15T10:30:00Z",
  "mcp_updated_at": "2023-08-15T10:30:00Z",
  "mcp_state": "ACTIVE",
  "mcp_generation": 1,
  "mcp_labels": {},
  "mcp_annotations": {},
  "name": "Example Dataset",
  "description": "An example dataset for testing",
  "resource_type": "dataset",
  "location": "gs://beam-examples/datasets/example.csv",
  "format": "csv",
  "status": "available",
  "size_bytes": 102400,
  "schema": {
    "fields": [
      {"name": "id", "type": "integer", "description": "Unique identifier"},
      {"name": "name", "type": "string", "description": "Name field"},
      {"name": "value", "type": "float", "description": "Value field"}
    ]
  },
  "metadata": {
    "rows": 1000,
    "created_by": "system"
  }
}
```

## Tools API

The Tools API allows management of AI models, transformation functions, and data processors used in data pipelines.

### Endpoints

- `GET /api/v1/tools/` - List all tools
- `GET /api/v1/tools/{tool_id}` - Get a specific tool
- `POST /api/v1/tools/` - Create a new tool
- `PUT /api/v1/tools/{tool_id}` - Update a tool
- `DELETE /api/v1/tools/{tool_id}` - Delete a tool
- `POST /api/v1/tools/{tool_id}/invoke` - Invoke a tool

### Tool Object

A Tool object has the following structure:

```json
{
  "mcp_resource_id": "text-tokenizer",      // Primary identifier
  "mcp_resource_type": "tool",
  "mcp_created_at": "2023-08-15T10:30:00Z",
  "mcp_updated_at": "2023-08-15T10:30:00Z",
  "mcp_state": "ACTIVE",
  "mcp_generation": 1,
  "mcp_labels": {},
  "mcp_annotations": {},
  "name": "Text Tokenizer",
  "description": "Tokenizes text and returns tokens",
  "tool_type": "processor",                 // Note: 'tool_type', not 'type'
  "version": "1.0.0",
  "status": "active",
  "parameters": {
    "text": {"type": "string", "description": "Text to tokenize"}
  },
  "capabilities": ["tokenization", "text-processing"],
  "metadata": {"tags": ["text", "nlp"]}
}
```

## Contexts API

The Contexts API allows management of execution contexts used to run Beam pipelines.

### Endpoints

- `GET /api/v1/contexts/` - List all contexts
- `GET /api/v1/contexts/{context_id}` - Get a specific context
- `POST /api/v1/contexts/` - Create a new context
- `PUT /api/v1/contexts/{context_id}` - Update a context
- `DELETE /api/v1/contexts/{context_id}` - Delete a context

### Context Object

A Context object has the following structure:

```json
{
  "mcp_resource_id": "dataflow-default",    // Primary identifier
  "mcp_resource_type": "context",
  "mcp_created_at": "2023-08-15T10:30:00Z",
  "mcp_updated_at": "2023-08-15T10:30:00Z",
  "mcp_state": "ACTIVE",
  "mcp_generation": 1,
  "mcp_labels": {},
  "mcp_annotations": {},
  "name": "Dataflow Production",
  "description": "Production execution context for Dataflow jobs",
  "context_type": "dataflow",
  "status": "active",
  "parameters": {
    "region": "us-central1",
    "project": "my-beam-project",
    "temp_location": "gs://my-bucket/temp"
  },
  "resources": {
    "cpu": "2",
    "memory": "4GB"
  },
  "metadata": {
    "environment": "production"
  }
}
```

## Migration Guide

### Changes in v1.0.0

1. **Error Response Structure**
   - Error responses now always include a dummy object in the `data` field (never null)
   - Error dummy objects have `mcp_resource_id` = "error" and `status` = "error"
   - Always check the `success` flag to determine if an operation succeeded

2. **Resource Identifiers**
   - Resources are now identified by `mcp_resource_id` instead of `id`
   - Replace all references to `id` with `mcp_resource_id` in client code

3. **Field Names**
   - The Tool type field is now `tool_type` instead of `type`
   - Update all Tool creation/update operations to use `tool_type`

4. **Field Inclusion**
   - API responses may now exclude unset or null fields
   - Always check for field existence before accessing
   - Update client code to handle missing fields gracefully 