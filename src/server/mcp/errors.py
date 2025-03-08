"""
Error handling for MCP protocol.

This module provides error classes and factory functions for creating
standardized error responses according to the MCP protocol.
"""

from enum import IntEnum
from typing import Dict, Optional, Any, List, Union
from fastapi import HTTPException, status
from pydantic import BaseModel


class MCPErrorCode(IntEnum):
    """Standard MCP error codes."""
    # Standard JSON-RPC error codes
    PARSE_ERROR = -32700
    INVALID_REQUEST = -32600
    METHOD_NOT_FOUND = -32601
    INVALID_PARAMS = -32602
    INTERNAL_ERROR = -32603
    
    # MCP-specific error codes
    PROTOCOL_VERSION_MISMATCH = -32000
    INCOMPATIBLE_CAPABILITIES = -32001
    INVALID_STATE = -32002
    CONNECTION_EXPIRED = -32003
    AUTHENTICATION_FAILED = -32004
    AUTHORIZATION_FAILED = -32005


class MCPErrorResponse(BaseModel):
    """Standard MCP error response format."""
    code: int
    message: str
    data: Optional[Dict[str, Any]] = None


class MCPError(Exception):
    """Base class for MCP protocol errors."""
    
    def __init__(
        self, 
        code: int, 
        message: str, 
        data: Optional[Any] = None,
        http_status_code: int = status.HTTP_400_BAD_REQUEST
    ):
        self.code = code
        self.message = message
        self.data = data
        self.http_status_code = http_status_code
        super().__init__(message)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert error to dictionary."""
        result = {
            "code": self.code,
            "message": self.message
        }
        if self.data is not None:
            result["data"] = self.data
        return result
    
    def to_http_exception(self) -> HTTPException:
        """Convert error to FastAPI HTTPException."""
        return HTTPException(
            status_code=self.http_status_code,
            detail=self.to_dict()
        )


# Standard JSON-RPC errors (-32768 to -32000)
def parse_error(data: Optional[Any] = None) -> MCPError:
    """Create a parse error."""
    return MCPError(
        code=-32700,
        message="Parse error",
        data=data,
        http_status_code=status.HTTP_400_BAD_REQUEST
    )


def invalid_request_error(data: Optional[Any] = None) -> MCPError:
    """Create an invalid request error."""
    return MCPError(
        code=-32600,
        message="Invalid request",
        data=data,
        http_status_code=status.HTTP_400_BAD_REQUEST
    )


def method_not_found_error(method: str, data: Optional[Any] = None) -> MCPError:
    """Create a method not found error."""
    return MCPError(
        code=-32601,
        message=f"Method not found: {method}",
        data=data,
        http_status_code=status.HTTP_404_NOT_FOUND
    )


def invalid_params_error(data: Optional[Any] = None) -> MCPError:
    """Create an invalid params error."""
    return MCPError(
        code=-32602,
        message="Invalid params",
        data=data,
        http_status_code=status.HTTP_400_BAD_REQUEST
    )


def internal_error(data: Optional[Any] = None) -> MCPError:
    """Create an internal error."""
    return MCPError(
        code=-32603,
        message="Internal error",
        data=data,
        http_status_code=status.HTTP_500_INTERNAL_SERVER_ERROR
    )


# MCP-specific errors
def protocol_version_mismatch(
    client_version: str, 
    server_version: str, 
    data: Optional[Any] = None
) -> MCPError:
    """Create a protocol version mismatch error."""
    return MCPError(
        code=-32000,
        message=f"Protocol version mismatch: client={client_version}, server={server_version}",
        data=data,
        http_status_code=status.HTTP_400_BAD_REQUEST
    )


def incompatible_capabilities(data: Optional[Any] = None) -> MCPError:
    """Create an incompatible capabilities error."""
    return MCPError(
        code=-32001,
        message="Incompatible capabilities",
        data=data,
        http_status_code=status.HTTP_400_BAD_REQUEST
    )


def connection_expired(connection_id: str, data: Optional[Any] = None) -> MCPError:
    """Create a connection expired error."""
    return MCPError(
        code=-32002,
        message=f"Connection expired: {connection_id}",
        data=data,
        http_status_code=status.HTTP_401_UNAUTHORIZED
    )


def connection_not_found(connection_id: str, data: Optional[Any] = None) -> MCPError:
    """Create a connection not found error."""
    return MCPError(
        code=-32003,
        message=f"Connection not found: {connection_id}",
        data=data,
        http_status_code=status.HTTP_404_NOT_FOUND
    )


def connection_already_exists(connection_id: str, data: Optional[Any] = None) -> MCPError:
    """Create a connection already exists error."""
    return MCPError(
        code=-32004,
        message=f"Connection already exists: {connection_id}",
        data=data,
        http_status_code=status.HTTP_409_CONFLICT
    )


def invalid_connection_state(
    connection_id: str, 
    current_state: str, 
    expected_states: List[str], 
    data: Optional[Any] = None
) -> MCPError:
    """Create an invalid connection state error."""
    return MCPError(
        code=-32005,
        message=f"Invalid connection state: {connection_id} is in state {current_state}, expected one of {expected_states}",
        data=data,
        http_status_code=status.HTTP_400_BAD_REQUEST
    )


def feature_not_enabled(feature_name: str, data: Optional[Any] = None) -> MCPError:
    """Create a feature not enabled error."""
    return MCPError(
        code=-32006,
        message=f"Feature not enabled: {feature_name}",
        data=data,
        http_status_code=status.HTTP_501_NOT_IMPLEMENTED
    )


# Transport-specific errors
def transport_error(message: str, data: Optional[Any] = None) -> MCPError:
    """Create a transport error."""
    return MCPError(
        code=-32100,
        message=f"Transport error: {message}",
        data=data,
        http_status_code=status.HTTP_500_INTERNAL_SERVER_ERROR
    )


# Validation errors
def validation_error(message: str, data: Optional[Any] = None) -> MCPError:
    """Create a validation error."""
    return MCPError(
        code=-32200,
        message=f"Validation error: {message}",
        data=data,
        http_status_code=status.HTTP_400_BAD_REQUEST
    )


# Resource-specific errors
def resource_not_found(resource_id: str, data: Optional[Any] = None) -> MCPError:
    """Create a resource not found error."""
    return MCPError(
        code=-32300,
        message=f"Resource not found: {resource_id}",
        data=data,
        http_status_code=status.HTTP_404_NOT_FOUND
    )


def resource_already_exists(resource_id: str, data: Optional[Any] = None) -> MCPError:
    """Create a resource already exists error."""
    return MCPError(
        code=-32301,
        message=f"Resource already exists: {resource_id}",
        data=data,
        http_status_code=status.HTTP_409_CONFLICT
    )


def resource_access_denied(resource_id: str, data: Optional[Any] = None) -> MCPError:
    """Create a resource access denied error."""
    return MCPError(
        code=-32302,
        message=f"Resource access denied: {resource_id}",
        data=data,
        http_status_code=status.HTTP_403_FORBIDDEN
    )


# Tool-specific errors
def tool_not_found(tool_id: str, data: Optional[Any] = None) -> MCPError:
    """Create a tool not found error."""
    return MCPError(
        code=-32400,
        message=f"Tool not found: {tool_id}",
        data=data,
        http_status_code=status.HTTP_404_NOT_FOUND
    )


def tool_execution_error(tool_id: str, message: str, data: Optional[Any] = None) -> MCPError:
    """Create a tool execution error."""
    return MCPError(
        code=-32401,
        message=f"Tool execution error: {tool_id} - {message}",
        data=data,
        http_status_code=status.HTTP_500_INTERNAL_SERVER_ERROR
    )


# Job-specific errors
def job_not_found(job_id: str, data: Optional[Any] = None) -> MCPError:
    """Create a job not found error."""
    return MCPError(
        code=-32500,
        message=f"Job not found: {job_id}",
        data=data,
        http_status_code=status.HTTP_404_NOT_FOUND
    )


def job_already_exists(job_id: str, data: Optional[Any] = None) -> MCPError:
    """Create a job already exists error."""
    return MCPError(
        code=-32501,
        message=f"Job already exists: {job_id}",
        data=data,
        http_status_code=status.HTTP_409_CONFLICT
    )


def job_execution_error(job_id: str, message: str, data: Optional[Any] = None) -> MCPError:
    """Create a job execution error."""
    return MCPError(
        code=-32502,
        message=f"Job execution error: {job_id} - {message}",
        data=data,
        http_status_code=status.HTTP_500_INTERNAL_SERVER_ERROR
    ) 