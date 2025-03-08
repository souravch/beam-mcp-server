"""
HTTP transport implementation for MCP protocol.

This module provides HTTP transport for JSON-RPC communication and
SSE (Server-Sent Events) for server-to-client notifications.
"""

import json
import logging
import asyncio
import traceback
from typing import Dict, List, Any, Optional, Callable, Awaitable, Union, Set
from fastapi import Request, Response
from sse_starlette.sse import EventSourceResponse
import uuid

from .errors import MCPError, transport_error, method_not_found_error, internal_error, invalid_params_error
from .models import ConnectionState, ConnectionLifecycleState
from .messages import (
    JsonRpcRequest, 
    JsonRpcResponse, 
    JsonRpcBatchRequest, 
    JsonRpcBatchResponse,
    MessageType,
    MessageDirection,
    MessageCategory,
    MessageMetadata,
    MCPMessage,
    parse_jsonrpc_message,
    create_jsonrpc_error_response,
    create_jsonrpc_success_response,
    HandlerResult,
    JsonRpcError
)


logger = logging.getLogger(__name__)


# Type definitions
RequestHandler = Callable[[JsonRpcRequest, ConnectionState], Awaitable[HandlerResult[Any]]]
NotificationHandler = Callable[[str, Dict[str, Any], ConnectionState], Awaitable[None]]


class HTTPTransport:
    """HTTP transport implementation for MCP protocol."""
    
    def __init__(self):
        """Initialize HTTP transport."""
        self._request_handlers: Dict[str, RequestHandler] = {}
        self._sse_connections: Dict[str, asyncio.Queue] = {}
        self._connection_state_getter: Optional[Callable[[str], Optional[ConnectionState]]] = None
    
    def set_connection_state_getter(self, getter: Callable[[str], Optional[ConnectionState]]):
        """
        Set the function to get connection state.
        
        Args:
            getter: Function that takes connection ID and returns connection state
        """
        self._connection_state_getter = getter
    
    def register_request_handler(self, method: str, handler: RequestHandler):
        """
        Register a handler for JSON-RPC requests.
        
        Args:
            method: JSON-RPC method name
            handler: Handler function for the method
        """
        self._request_handlers[method] = handler
    
    def register_request_handlers(self, handlers: Dict[str, RequestHandler]):
        """
        Register multiple request handlers.
        
        Args:
            handlers: Dictionary of method names to handler functions
        """
        self._request_handlers.update(handlers)
    
    async def handle_jsonrpc_request(self, request: Request, connection: ConnectionState) -> Response:
        """
        Handle JSON-RPC request over HTTP.
        
        Args:
            request: FastAPI request object
            connection: MCP connection state
            
        Returns:
            JSON-RPC response
        """
        try:
            # Parse request body
            body = await request.body()
            
            # Log request
            logger.debug(f"Received JSON-RPC request: {body.decode('utf-8')}")
            
            # Parse JSON-RPC message
            jsonrpc_message = parse_jsonrpc_message(body)
            
            # Check if message is an error
            if isinstance(jsonrpc_message, JsonRpcError):
                return Response(
                    content=json.dumps(create_jsonrpc_error_response(None, jsonrpc_message).model_dump()),
                    media_type="application/json"
                )
            
            # Handle batch request
            if isinstance(jsonrpc_message, JsonRpcBatchRequest):
                # Process each request in batch
                responses = []
                for req in jsonrpc_message.requests:
                    response = await self._handle_single_request(req, connection)
                    if response is not None:  # Only include responses for non-notifications
                        responses.append(response)
                
                # Return batch response
                return Response(
                    content=json.dumps(JsonRpcBatchResponse(responses=responses).model_dump()),
                    media_type="application/json"
                )
            
            # Handle single request
            response = await self._handle_single_request(jsonrpc_message, connection)
            
            # Return response (or empty for notifications)
            if response is None:
                return Response(status_code=204)
            else:
                return Response(
                    content=json.dumps(response.model_dump()),
                    media_type="application/json"
                )
        
        except Exception as e:
            # Log error
            logger.error(f"Error handling JSON-RPC request: {e}")
            logger.error(traceback.format_exc())
            
            # Return error response
            error = internal_error(str(e))
            return Response(
                content=json.dumps(create_jsonrpc_error_response(None, error).model_dump()),
                media_type="application/json",
                status_code=500
            )
    
    async def _handle_single_request(
        self, 
        request: JsonRpcRequest, 
        connection: ConnectionState
    ) -> Optional[JsonRpcResponse]:
        """
        Handle a single JSON-RPC request.
        
        Args:
            request: JSON-RPC request
            connection: MCP connection state
            
        Returns:
            JSON-RPC response, or None for notifications
        """
        method = request.method
        params = request.params or {}
        id = request.id
        
        try:
            # Find handler for method
            handler = self._request_handlers.get(method)
            if handler is None:
                if id is not None:
                    # Return error for non-notification requests
                    return create_jsonrpc_error_response(
                        id=id,
                        error=method_not_found_error(method).to_dict()
                    )
                else:
                    # Ignore notifications for unknown methods
                    logger.warning(f"Ignoring notification for unknown method: {method}")
                    return None
            
            # Create metadata for tracking
            metadata = MessageMetadata(
                type=MessageType.REQUEST if id is not None else MessageType.NOTIFICATION,
                direction=MessageDirection.CLIENT_TO_SERVER,
                category=MessageCategory.JSONRPC,
                connection_id=connection.id,
                request_id=str(id) if id is not None else None
            )
            
            # Create MCP message
            message = MCPMessage(metadata=metadata, payload=request)
            
            # Log message
            logger.debug(f"Processing message: {message.model_dump()}")
            
            # Call handler
            result = await handler(request, connection)
            
            # Return response for non-notification requests
            if id is not None:
                if isinstance(result, dict) and "code" in result and "message" in result:
                    # Error result
                    return create_jsonrpc_error_response(id=id, error=result)
                else:
                    # Success result
                    return create_jsonrpc_success_response(id=id, result=result)
            
            return None
        
        except Exception as e:
            # Log error
            logger.error(f"Error handling request for method {method}: {e}")
            logger.error(traceback.format_exc())
            
            # Return error response for non-notification requests
            if id is not None:
                return create_jsonrpc_error_response(
                    id=id,
                    error=internal_error(str(e)).to_dict()
                )
            
            return None
    
    async def handle_sse_connection(self, connection_id: str) -> EventSourceResponse:
        """
        Handle SSE connection for notifications.
        
        Args:
            connection_id: MCP connection ID
            
        Returns:
            SSE response for notifications
        """
        # Create queue for notifications
        queue = asyncio.Queue()
        
        # Register connection
        self._sse_connections[connection_id] = queue
        
        # Define event generator
        async def event_generator():
            try:
                # Send initial connection event
                yield {
                    "event": "connected",
                    "data": json.dumps({
                        "connection_id": connection_id,
                        "message": "Connected to MCP server"
                    })
                }
                
                # Wait for events from queue
                while True:
                    # Get event from queue
                    event = await queue.get()
                    
                    # Check if event is a close signal
                    if event is None:
                        break
                    
                    # Yield event
                    yield event
                    
                    # Mark task as done
                    queue.task_done()
            
            except asyncio.CancelledError:
                logger.debug(f"SSE connection cancelled for {connection_id}")
            
            finally:
                # Clean up connection
                if connection_id in self._sse_connections:
                    del self._sse_connections[connection_id]
                logger.debug(f"SSE connection closed for {connection_id}")
        
        # Return SSE response
        return EventSourceResponse(event_generator())
    
    async def send_notification(
        self, 
        connection_id: str, 
        method: str, 
        params: Optional[Dict[str, Any]] = None,
        category: MessageCategory = MessageCategory.JSONRPC
    ) -> bool:
        """
        Send notification to client.
        
        Args:
            connection_id: MCP connection ID
            method: Notification method name
            params: Notification parameters
            category: Message category
            
        Returns:
            True if notification was sent, False otherwise
        """
        # Check if connection is active
        queue = self._sse_connections.get(connection_id)
        if queue is None:
            logger.warning(f"Cannot send notification: no SSE connection for {connection_id}")
            return False
        
        # Check connection state if available
        if self._connection_state_getter is not None:
            connection = self._connection_state_getter(connection_id)
            if connection is None or connection.state != ConnectionLifecycleState.ACTIVE:
                logger.warning(f"Cannot send notification: connection {connection_id} is not active")
                return False
        
        try:
            # Create metadata for tracking
            metadata = MessageMetadata(
                type=MessageType.NOTIFICATION,
                direction=MessageDirection.SERVER_TO_CLIENT,
                category=category,
                connection_id=connection_id,
                request_id=str(uuid.uuid4())
            )
            
            # Create notification request
            notification = JsonRpcRequest(
                method=method,
                params=params
            )
            
            # Create MCP message
            message = MCPMessage(metadata=metadata, payload=notification)
            
            # Create event
            event = {
                "event": "notification",
                "data": json.dumps(notification.model_dump())
            }
            
            # Put event in queue
            await queue.put(event)
            
            # Log notification
            logger.debug(f"Sent notification to {connection_id}: {method}")
            
            return True
        
        except Exception as e:
            # Log error
            logger.error(f"Error sending notification to {connection_id}: {e}")
            return False
    
    async def broadcast_notification(
        self, 
        connection_ids: List[str], 
        method: str, 
        params: Optional[Dict[str, Any]] = None,
        category: MessageCategory = MessageCategory.JSONRPC
    ) -> Dict[str, bool]:
        """
        Broadcast notification to multiple clients.
        
        Args:
            connection_ids: List of MCP connection IDs
            method: Notification method name
            params: Notification parameters
            category: Message category
            
        Returns:
            Dictionary mapping connection IDs to success status
        """
        # Send notification to each connection
        results = {}
        for connection_id in connection_ids:
            results[connection_id] = await self.send_notification(
                connection_id, 
                method, 
                params,
                category
            )
        
        return results
    
    async def close_sse_connection(self, connection_id: str):
        """
        Close SSE connection for a client.
        
        Args:
            connection_id: MCP connection ID
        """
        # Check if connection exists
        queue = self._sse_connections.get(connection_id)
        if queue is None:
            logger.warning(f"Cannot close SSE connection: no connection for {connection_id}")
            return
        
        # Send None to signal closing
        await queue.put(None)
        
        # Remove connection
        if connection_id in self._sse_connections:
            del self._sse_connections[connection_id]
    
    def get_active_connections(self) -> Set[str]:
        """
        Get set of active connection IDs.
        
        Returns:
            Set of active connection IDs
        """
        return set(self._sse_connections.keys()) 