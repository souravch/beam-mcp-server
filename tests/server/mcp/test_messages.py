import pytest
import json
from datetime import datetime, UTC
from typing import Dict, Any, List, Optional

from server.mcp.messages import (
    MessageType,
    MessageDirection,
    MessageCategory,
    ErrorCode,
    JsonRpcError,
    JsonRpcRequest,
    JsonRpcResponse,
    JsonRpcBatchRequest,
    JsonRpcBatchResponse,
    MessageMetadata,
    MCPMessage,
    parse_jsonrpc_message,
    create_jsonrpc_error_response,
    create_jsonrpc_success_response
)


# Test message models
def test_jsonrpc_request():
    """Test JSON-RPC request model."""
    # Test valid request
    request = JsonRpcRequest(method="test_method", params={"param1": "value1"}, id="123")
    assert request.jsonrpc == "2.0"
    assert request.method == "test_method"
    assert request.params == {"param1": "value1"}
    assert request.id == "123"
    assert not request.is_notification
    
    # Test notification (no id)
    notification = JsonRpcRequest(method="test_notification")
    assert notification.jsonrpc == "2.0"
    assert notification.method == "test_notification"
    assert notification.params is None
    assert notification.id is None
    assert notification.is_notification
    
    # Test invalid jsonrpc version
    with pytest.raises(ValueError):
        JsonRpcRequest(jsonrpc="1.0", method="test_method")


def test_jsonrpc_response():
    """Test JSON-RPC response model."""
    # Test success response
    success = JsonRpcResponse(id="123", result={"status": "ok"})
    assert success.jsonrpc == "2.0"
    assert success.id == "123"
    assert success.result == {"status": "ok"}
    assert success.error is None
    
    # Test error response
    error = JsonRpcResponse(
        id="123",
        error=JsonRpcError(
            code=ErrorCode.INTERNAL_ERROR,
            message="Test error"
        )
    )
    assert error.jsonrpc == "2.0"
    assert error.id == "123"
    assert error.result is None
    assert error.error.code == ErrorCode.INTERNAL_ERROR
    assert error.error.message == "Test error"
    
    # Test invalid jsonrpc version
    with pytest.raises(ValueError):
        JsonRpcResponse(jsonrpc="1.0", id="123", result={})
    
    # Test missing result and error
    with pytest.raises(ValueError):
        JsonRpcResponse(id="123")
    
    # Test both result and error
    with pytest.raises(ValueError):
        JsonRpcResponse(
            id="123",
            result={"status": "ok"},
            error=JsonRpcError(code=ErrorCode.INTERNAL_ERROR, message="Test error")
        )


def test_jsonrpc_batch():
    """Test JSON-RPC batch request and response."""
    # Test batch request
    batch_request = JsonRpcBatchRequest(
        requests=[
            JsonRpcRequest(method="method1", id="1"),
            JsonRpcRequest(method="method2", id="2")
        ]
    )
    assert len(batch_request.requests) == 2
    assert batch_request.requests[0].method == "method1"
    assert batch_request.requests[1].method == "method2"
    
    # Test empty batch request
    with pytest.raises(ValueError):
        JsonRpcBatchRequest(requests=[])
    
    # Test batch response
    batch_response = JsonRpcBatchResponse(
        responses=[
            JsonRpcResponse(id="1", result={"status": "ok1"}),
            JsonRpcResponse(id="2", result={"status": "ok2"})
        ]
    )
    assert len(batch_response.responses) == 2
    assert batch_response.responses[0].result == {"status": "ok1"}
    assert batch_response.responses[1].result == {"status": "ok2"}


def test_message_metadata():
    """Test message metadata."""
    metadata = MessageMetadata(
        type=MessageType.REQUEST,
        direction=MessageDirection.CLIENT_TO_SERVER,
        category=MessageCategory.JSONRPC,
        connection_id="test-connection",
        request_id="test-request"
    )
    assert metadata.type == MessageType.REQUEST
    assert metadata.direction == MessageDirection.CLIENT_TO_SERVER
    assert metadata.category == MessageCategory.JSONRPC
    assert metadata.connection_id == "test-connection"
    assert metadata.request_id == "test-request"
    assert isinstance(metadata.timestamp, datetime)


def test_mcp_message():
    """Test MCP message."""
    request = JsonRpcRequest(method="test_method", id="123")
    metadata = MessageMetadata(
        type=MessageType.REQUEST,
        direction=MessageDirection.CLIENT_TO_SERVER,
        category=MessageCategory.JSONRPC
    )
    message = MCPMessage(metadata=metadata, payload=request)
    
    assert message.metadata == metadata
    assert message.payload == request


# Test message parsing and creation
def test_parse_jsonrpc_message_single():
    """Test parsing a single JSON-RPC message."""
    # Valid request
    json_str = '{"jsonrpc": "2.0", "method": "test_method", "params": {"param1": "value1"}, "id": "123"}'
    result = parse_jsonrpc_message(json_str)
    assert isinstance(result, JsonRpcRequest)
    assert result.method == "test_method"
    assert result.params == {"param1": "value1"}
    assert result.id == "123"
    
    # Valid notification
    json_str = '{"jsonrpc": "2.0", "method": "test_notification"}'
    result = parse_jsonrpc_message(json_str)
    assert isinstance(result, JsonRpcRequest)
    assert result.method == "test_notification"
    assert result.id is None
    
    # Invalid JSON
    json_str = '{"jsonrpc": "2.0", "method": "test_method", "params": {'
    result = parse_jsonrpc_message(json_str)
    assert isinstance(result, JsonRpcError)
    assert result.code == ErrorCode.PARSE_ERROR
    
    # Invalid request (missing method)
    json_str = '{"jsonrpc": "2.0", "params": {}, "id": "123"}'
    result = parse_jsonrpc_message(json_str)
    assert isinstance(result, JsonRpcError)
    assert result.code == ErrorCode.INVALID_REQUEST


def test_parse_jsonrpc_message_batch():
    """Test parsing a batch JSON-RPC message."""
    # Valid batch
    json_str = '[{"jsonrpc": "2.0", "method": "method1", "id": "1"}, {"jsonrpc": "2.0", "method": "method2", "id": "2"}]'
    result = parse_jsonrpc_message(json_str)
    assert isinstance(result, JsonRpcBatchRequest)
    assert len(result.requests) == 2
    assert result.requests[0].method == "method1"
    assert result.requests[1].method == "method2"
    
    # Invalid batch (empty)
    json_str = '[]'
    result = parse_jsonrpc_message(json_str)
    assert isinstance(result, JsonRpcError)
    assert result.code == ErrorCode.INVALID_REQUEST
    
    # Invalid batch (invalid request)
    json_str = '[{"jsonrpc": "2.0", "method": "method1", "id": "1"}, {"jsonrpc": "2.0", "id": "2"}]'
    result = parse_jsonrpc_message(json_str)
    assert isinstance(result, JsonRpcError)
    assert result.code == ErrorCode.INVALID_REQUEST


def test_create_jsonrpc_responses():
    """Test creating JSON-RPC responses."""
    # Success response
    success = create_jsonrpc_success_response("123", {"status": "ok"})
    assert success.jsonrpc == "2.0"
    assert success.id == "123"
    assert success.result == {"status": "ok"}
    assert success.error is None
    
    # Error response
    error = create_jsonrpc_error_response(
        "123",
        JsonRpcError(code=ErrorCode.INTERNAL_ERROR, message="Test error")
    )
    assert error.jsonrpc == "2.0"
    assert error.id == "123"
    assert error.result is None
    assert error.error.code == ErrorCode.INTERNAL_ERROR
    assert error.error.message == "Test error" 