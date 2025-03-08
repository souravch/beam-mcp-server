import json
import uuid
import redis
import logging
from typing import Dict, Optional, Any, List, Tuple
from datetime import datetime, timedelta, UTC
from fastapi import Request, Response

from .models import (
    ConnectionState, 
    ConnectionLifecycleState,
    ClientInfo,
    InitializeRequest
)
from .errors import (
    MCPError,
    invalid_connection_state,
    connection_expired
)


logger = logging.getLogger(__name__)


class ConnectionManager:
    """
    Manages MCP connection lifecycle and state.
    Uses Redis for connection state persistence.
    """
    
    # Redis key prefix for connection state
    KEY_PREFIX = "mcp:connection:"
    
    def __init__(
        self,
        redis_url: str,
        connection_expiry_seconds: int = 1800
    ):
        """
        Initialize the connection manager.
        
        Args:
            redis_url: Redis URL for connection state storage
            connection_expiry_seconds: Connection expiry time in seconds
        """
        self.redis_url = redis_url
        self.redis = redis.from_url(redis_url)
        self.CONNECTION_EXPIRY_SECONDS = connection_expiry_seconds
        self._transport: Optional[HTTPTransport] = None
        
    def _get_connection_key(self, connection_id: str) -> str:
        """Get the Redis key for a connection."""
        return f"{self.KEY_PREFIX}{connection_id}"
    
    def create_connection(self, initialize_request: InitializeRequest) -> ConnectionState:
        """
        Create a new connection from an initialize request.
        
        Args:
            initialize_request: The initialize request from the client
            
        Returns:
            The new connection state
        """
        connection_id = str(uuid.uuid4())
        
        connection = ConnectionState(
            id=connection_id,
            client_info=initialize_request.client,
            client_capabilities=initialize_request.capabilities,
            state=ConnectionLifecycleState.INITIALIZING,
            created_at=datetime.now(UTC),
            last_activity=datetime.now(UTC)
        )
        
        # Store the connection state in Redis
        self._save_connection(connection)
        
        logger.info(
            f"Created connection {connection_id} for client {initialize_request.client.name} "
            f"v{initialize_request.client.version}"
        )
        
        return connection
    
    def _save_connection(self, connection: ConnectionState) -> None:
        """
        Save a connection state to Redis.
        
        Args:
            connection: The connection state to save
        """
        key = self._get_connection_key(connection.id)
        data = connection.to_dict()
        
        # Convert to JSON
        json_data = json.dumps(data)
        
        # Save to Redis with expiration
        self.redis.setex(
            key, 
            self.CONNECTION_EXPIRY_SECONDS,
            json_data
        )
    
    def get_connection(self, connection_id: str) -> Optional[ConnectionState]:
        """
        Get a connection state by ID.
        
        Args:
            connection_id: The connection ID
            
        Returns:
            The connection state, or None if not found
        """
        key = self._get_connection_key(connection_id)
        data = self.redis.get(key)
        
        if not data:
            return None
        
        try:
            # Parse JSON and convert to ConnectionState
            connection_data = json.loads(data)
            return ConnectionState.from_dict(connection_data)
        except Exception as e:
            logger.error(f"Error parsing connection state for {connection_id}: {e}")
            return None
    
    def update_connection(
        self, 
        connection_id: str, 
        updates: Dict[str, Any],
        expected_states: Optional[List[ConnectionLifecycleState]] = None
    ) -> ConnectionState:
        """
        Update a connection state.
        
        Args:
            connection_id: The connection ID
            updates: Dictionary of fields to update
            expected_states: Optional list of expected states for validation
            
        Returns:
            The updated connection state
            
        Raises:
            MCPError: If the connection is not found or in an unexpected state
        """
        connection = self.get_connection(connection_id)
        
        if not connection:
            raise connection_expired()
        
        # Validate the connection state if expected states were provided
        if expected_states and connection.state not in expected_states:
            raise invalid_connection_state(
                current=connection.state,
                expected=[state.value for state in expected_states]
            )
        
        # Apply updates
        for key, value in updates.items():
            setattr(connection, key, value)
        
        # Always update last_activity
        connection.last_activity = datetime.now(UTC)
        
        # Save the updated connection
        self._save_connection(connection)
        
        return connection
    
    def mark_connection_active(self, connection_id: str) -> ConnectionState:
        """
        Mark a connection as active.
        
        Args:
            connection_id: The connection ID
            
        Returns:
            The updated connection state
            
        Raises:
            MCPError: If the connection is not found or not in INITIALIZING state
        """
        return self.update_connection(
            connection_id=connection_id,
            updates={"state": ConnectionLifecycleState.ACTIVE},
            expected_states=[ConnectionLifecycleState.INITIALIZING]
        )
    
    def mark_connection_terminating(self, connection_id: str) -> ConnectionState:
        """
        Mark a connection as terminating (shutdown requested).
        
        Args:
            connection_id: The connection ID
            
        Returns:
            The updated connection state
            
        Raises:
            MCPError: If the connection is not found or not in ACTIVE state
        """
        return self.update_connection(
            connection_id=connection_id,
            updates={"state": ConnectionLifecycleState.TERMINATING},
            expected_states=[ConnectionLifecycleState.ACTIVE]
        )
    
    def mark_connection_terminated(self, connection_id: str) -> None:
        """
        Mark a connection as terminated (exit received).
        
        Args:
            connection_id: The connection ID
            
        Raises:
            MCPError: If the connection is not found or not in TERMINATING state
        """
        connection = self.update_connection(
            connection_id=connection_id,
            updates={"state": ConnectionLifecycleState.TERMINATED},
            expected_states=[ConnectionLifecycleState.TERMINATING]
        )
        
        # Delete the connection state after a short delay
        # We keep it briefly to allow final messages to be processed
        key = self._get_connection_key(connection_id)
        self.redis.expire(key, 30)  # 30 seconds
    
    def touch_connection(self, connection_id: str) -> bool:
        """
        Update the last activity timestamp for a connection.
        
        Args:
            connection_id: The connection ID
            
        Returns:
            True if the connection was found and updated, False otherwise
        """
        connection = self.get_connection(connection_id)
        
        if not connection:
            return False
        
        # Update last_activity
        connection.last_activity = datetime.now(UTC)
        
        # Save the updated connection
        self._save_connection(connection)
        
        return True
    
    def extract_connection_id(self, request: Request) -> Optional[str]:
        """
        Extract the connection ID from request headers or cookies.
        
        Args:
            request: FastAPI request object
            
        Returns:
            The connection ID, or None if not found
        """
        # First try to get from header
        connection_id = request.headers.get("X-MCP-Connection-ID")
        
        # Then try to get from cookie
        if not connection_id:
            connection_id = request.cookies.get("mcp_connection_id")
        
        return connection_id
    
    def set_connection_cookie(self, response: Response, connection_id: str) -> None:
        """
        Set the connection ID cookie in the response.
        
        Args:
            response: FastAPI response object
            connection_id: The connection ID
        """
        response.set_cookie(
            key="mcp_connection_id",
            value=connection_id,
            httponly=True,
            max_age=self.CONNECTION_EXPIRY_SECONDS,
            samesite="strict",
            secure=True,  # Requires HTTPS
        )
    
    def cleanup_expired_connections(self) -> int:
        """
        Clean up expired connections from Redis.
        
        Returns:
            Number of connections cleaned up
        """
        # Redis TTL mechanism handles this automatically
        # This method is provided for future extensions
        return 0

    def set_transport(self, transport: HTTPTransport) -> None:
        """
        Set the transport for sending notifications.
        
        Args:
            transport: Transport implementation
        """
        self._transport = transport
        
        # Set connection state getter in transport
        self._transport.set_connection_state_getter(self.get_connection)

    async def send_notification(
        self,
        connection_id: str,
        method: str,
        params: Optional[Dict[str, Any]] = None,
        category: Optional[str] = None
    ) -> bool:
        """
        Send a notification to a connection.
        
        Args:
            connection_id: Connection ID
            method: Notification method
            params: Notification parameters
            category: Message category
            
        Returns:
            True if notification was sent, False otherwise
        """
        if not self._transport:
            logger.warning("Cannot send notification: transport not set")
            return False
        
        # Convert category string to MessageCategory enum if provided
        message_category = None
        if category:
            try:
                from .messages import MessageCategory
                message_category = MessageCategory(category)
            except (ImportError, ValueError):
                logger.warning(f"Invalid message category: {category}")
        
        # Send notification via transport
        if message_category:
            return await self._transport.send_notification(
                connection_id, method, params, message_category
            )
        else:
            return await self._transport.send_notification(
                connection_id, method, params
            )

    async def broadcast_notification(
        self,
        connection_ids: List[str],
        method: str,
        params: Optional[Dict[str, Any]] = None,
        category: Optional[str] = None
    ) -> Dict[str, bool]:
        """
        Broadcast a notification to multiple connections.
        
        Args:
            connection_ids: List of connection IDs
            method: Notification method
            params: Notification parameters
            category: Message category
            
        Returns:
            Dict mapping connection IDs to success status
        """
        if not self._transport:
            logger.warning("Cannot broadcast notification: transport not set")
            return {connection_id: False for connection_id in connection_ids}
        
        # Convert category string to MessageCategory enum if provided
        message_category = None
        if category:
            try:
                from .messages import MessageCategory
                message_category = MessageCategory(category)
            except (ImportError, ValueError):
                logger.warning(f"Invalid message category: {category}")
        
        # Broadcast notification via transport
        if message_category:
            return await self._transport.broadcast_notification(
                connection_ids, method, params, message_category
            )
        else:
            return await self._transport.broadcast_notification(
                connection_ids, method, params
            )

    async def close_connection(self, connection_id: str) -> None:
        """
        Close a connection and its associated SSE channel.
        
        Args:
            connection_id: Connection ID
        """
        if not self._transport:
            logger.warning("Cannot close connection: transport not set")
            return
        
        # Close SSE connection
        await self._transport.close_sse_connection(connection_id)
        
        # Mark connection as terminated
        self.mark_connection_terminated(connection_id) 