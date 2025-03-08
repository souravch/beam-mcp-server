"""
Structured message types and validation for MCP protocol.

This module provides typed message models and validation for MCP protocol messages,
ensuring that all messages conform to the expected structure and content.
"""

from enum import Enum
from typing import Dict, List, Any, Optional, Union, Literal, TypeVar, Generic
from pydantic import BaseModel, Field, validator, root_validator, model_validator
import json
import logging
from datetime import datetime, UTC

from .errors import invalid_request_error, parse_error, method_not_found_error


logger = logging.getLogger(__name__)


# JSON-RPC Constants
JSONRPC_VERSION = "2.0"


class MessageType(str, Enum):
    """Types of MCP messages."""
    REQUEST = "request"
    RESPONSE = "response"
    NOTIFICATION = "notification"
    ERROR = "error"


class MessageDirection(str, Enum):
    """Direction of MCP messages."""
    CLIENT_TO_SERVER = "client_to_server"
    SERVER_TO_CLIENT = "server_to_client"


class MessageCategory(str, Enum):
    """Categories of MCP messages."""
    LIFECYCLE = "lifecycle"
    JSONRPC = "jsonrpc"
    RESOURCE = "resource"
    TOOL = "tool"
    CONTEXT = "context"
    JOB = "job"
    CUSTOM = "custom"


class ErrorCode(int, Enum):
    """Standard JSON-RPC error codes and MCP-specific error codes."""
    # Standard JSON-RPC error codes (-32768 to -32000)
    PARSE_ERROR = -32700
    INVALID_REQUEST = -32600
    METHOD_NOT_FOUND = -32601
    INVALID_PARAMS = -32602
    INTERNAL_ERROR = -32603
    
    # MCP-specific error codes
    PROTOCOL_VERSION_MISMATCH = -32000
    INCOMPATIBLE_CAPABILITIES = -32001
    CONNECTION_EXPIRED = -32002
    CONNECTION_NOT_FOUND = -32003
    CONNECTION_ALREADY_EXISTS = -32004
    INVALID_CONNECTION_STATE = -32005
    FEATURE_NOT_ENABLED = -32006
    
    # Transport-specific error codes
    TRANSPORT_ERROR = -32100
    
    # Validation error codes
    VALIDATION_ERROR = -32200
    
    # Resource-specific error codes
    RESOURCE_NOT_FOUND = -32300
    RESOURCE_ALREADY_EXISTS = -32301
    RESOURCE_ACCESS_DENIED = -32302
    
    # Tool-specific error codes
    TOOL_NOT_FOUND = -32400
    TOOL_EXECUTION_ERROR = -32401
    
    # Job-specific error codes
    JOB_NOT_FOUND = -32500
    JOB_ALREADY_EXISTS = -32501
    JOB_EXECUTION_ERROR = -32502


class JsonRpcError(BaseModel):
    """JSON-RPC error object."""
    code: int
    message: str
    data: Optional[Any] = None


class JsonRpcRequest(BaseModel):
    """JSON-RPC request object."""
    jsonrpc: str = JSONRPC_VERSION
    id: Optional[Union[str, int]] = None
    method: str
    params: Optional[Dict[str, Any]] = None
    
    @validator("jsonrpc")
    def validate_jsonrpc_version(cls, v):
        """Validate JSON-RPC version."""
        if v != JSONRPC_VERSION:
            raise ValueError(f"Invalid JSON-RPC version: {v}")
        return v
    
    @property
    def is_notification(self) -> bool:
        """Check if the request is a notification (no id)."""
        return self.id is None


class JsonRpcResponse(BaseModel):
    """JSON-RPC response object."""
    jsonrpc: str = JSONRPC_VERSION
    id: Optional[Union[str, int]] = None
    result: Optional[Any] = None
    error: Optional[JsonRpcError] = None
    
    @validator("jsonrpc")
    def validate_jsonrpc_version(cls, v):
        """Validate JSON-RPC version."""
        if v != JSONRPC_VERSION:
            raise ValueError(f"Invalid JSON-RPC version: {v}")
        return v
    
    @model_validator(mode='after')
    def validate_result_or_error(self) -> 'JsonRpcResponse':
        """Validate that either result or error is present, but not both."""
        if self.result is not None and self.error is not None:
            raise ValueError("Response cannot contain both result and error")
        
        if self.result is None and self.error is None:
            raise ValueError("Response must contain either result or error")
            
        return self


class JsonRpcBatchRequest(BaseModel):
    """JSON-RPC batch request."""
    requests: List[JsonRpcRequest]
    
    @validator("requests")
    def validate_non_empty(cls, v):
        """Validate that batch is not empty."""
        if not v:
            raise ValueError("Batch request cannot be empty")
        return v


class JsonRpcBatchResponse(BaseModel):
    """JSON-RPC batch response."""
    responses: List[JsonRpcResponse]


class MessageMetadata(BaseModel):
    """Metadata for MCP messages."""
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))
    type: MessageType
    direction: MessageDirection
    category: MessageCategory
    connection_id: Optional[str] = None
    request_id: Optional[str] = None
    trace_id: Optional[str] = None


class MCPMessage(BaseModel):
    """Base class for all MCP messages."""
    metadata: MessageMetadata
    payload: Union[JsonRpcRequest, JsonRpcResponse, JsonRpcBatchRequest, JsonRpcBatchResponse]


# Type for message handlers
T = TypeVar("T")
HandlerResult = Union[T, JsonRpcError]
MessageHandler = callable


def parse_jsonrpc_message(data: Union[str, bytes, Dict[str, Any]]) -> Union[JsonRpcRequest, JsonRpcBatchRequest, JsonRpcError]:
    """
    Parse a JSON-RPC message from various input formats.
    
    Args:
        data: Input data (string, bytes, or dict)
        
    Returns:
        Parsed JSON-RPC request or batch request, or error if parsing fails
    """
    # Convert data to dict if necessary
    if isinstance(data, (str, bytes)):
        try:
            if isinstance(data, bytes):
                data = data.decode("utf-8")
            data_dict = json.loads(data)
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON: {e}")
            return parse_error(str(e))
    else:
        data_dict = data
    
    # Check if it's a batch request
    if isinstance(data_dict, list):
        try:
            requests = []
            for item in data_dict:
                req = JsonRpcRequest(**item)
                requests.append(req)
            return JsonRpcBatchRequest(requests=requests)
        except Exception as e:
            logger.error(f"Failed to parse batch request: {e}")
            return invalid_request_error(str(e))
    
    # Single request
    try:
        return JsonRpcRequest(**data_dict)
    except Exception as e:
        logger.error(f"Failed to parse request: {e}")
        return invalid_request_error(str(e))


def create_jsonrpc_error_response(id: Optional[Union[str, int]], error: JsonRpcError) -> JsonRpcResponse:
    """
    Create a JSON-RPC error response.
    
    Args:
        id: Request ID (can be None for notifications)
        error: Error object
        
    Returns:
        JSON-RPC response with error
    """
    return JsonRpcResponse(id=id, error=error)


def create_jsonrpc_success_response(id: Union[str, int], result: Any) -> JsonRpcResponse:
    """
    Create a JSON-RPC success response.
    
    Args:
        id: Request ID
        result: Result object
        
    Returns:
        JSON-RPC response with result
    """
    return JsonRpcResponse(id=id, result=result)


def format_error_response(error: JsonRpcError) -> Dict[str, Any]:
    """
    Format an error as a JSON-RPC error response.
    
    Args:
        error: Error object
        
    Returns:
        Formatted error response
    """
    return {
        "jsonrpc": JSONRPC_VERSION,
        "id": None,
        "error": {
            "code": error.code,
            "message": error.message,
            "data": error.data
        }
    } 