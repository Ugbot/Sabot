"""
Common WebSocket infrastructure for exchange data collectors.

Handles:
- Connection management with reconnection
- Ping/pong (keep-alive)
- Error handling and logging
- Message parsing
"""

import asyncio
import json
import logging
import ssl
from typing import Any, Callable, Optional

import certifi
import websockets


logger = logging.getLogger(__name__)


# ============================================================================
# WebSocket Client with Reconnection
# ============================================================================

class ExchangeWebSocket:
    """
    WebSocket client for exchange data with automatic reconnection.
    
    Features:
    - Automatic reconnection with exponential backoff
    - Ping/pong handling
    - Message parsing
    - Error handling
    """
    
    def __init__(
        self,
        url: str,
        name: str = "Exchange",
        ping_interval: int = 20,
        ping_timeout: int = 60,
        max_backoff: int = 60
    ):
        self.url = url
        self.name = name
        self.ping_interval = ping_interval
        self.ping_timeout = ping_timeout
        self.max_backoff = max_backoff
        
        # SSL context
        self.ssl_ctx = ssl.create_default_context(cafile=certifi.where())
        
        # Connection state
        self.ws = None
        self.running = False
        self.backoff = 1
    
    async def connect(self) -> None:
        """Establish WebSocket connection."""
        logger.info(f"Connecting to {self.name}: {self.url[:100]}...")
        
        self.ws = await websockets.connect(
            self.url,
            ssl=self.ssl_ctx,
            ping_interval=self.ping_interval,
            ping_timeout=self.ping_timeout
        )
        
        logger.info(f"✅ Connected to {self.name}")
        self.backoff = 1  # Reset backoff on successful connection
    
    async def send(self, message: str) -> None:
        """Send message to WebSocket."""
        if self.ws:
            await self.ws.send(message)
    
    async def send_json(self, data: dict) -> None:
        """Send JSON message."""
        await self.send(json.dumps(data))
    
    async def receive(self) -> Optional[str]:
        """Receive message from WebSocket."""
        if self.ws:
            try:
                return await self.ws.recv()
            except websockets.exceptions.ConnectionClosed:
                logger.warning(f"{self.name} connection closed")
                return None
        return None
    
    async def receive_json(self) -> Optional[dict]:
        """Receive and parse JSON message."""
        frame = await self.receive()
        if frame:
            try:
                return json.loads(frame)
            except json.JSONDecodeError as e:
                logger.warning(f"JSON decode error: {e}")
                return None
        return None
    
    async def stream_messages(
        self,
        on_message: Callable[[str], Any],
        on_connect: Optional[Callable[[], Any]] = None
    ) -> None:
        """
        Stream messages with automatic reconnection.
        
        Args:
            on_message: Callback for each message (receives raw frame)
            on_connect: Optional callback after connection established
        """
        self.running = True
        
        while self.running:
            try:
                await self.connect()
                
                # Call on_connect callback
                if on_connect:
                    await on_connect()
                
                # Stream messages
                async for frame in self.ws:
                    try:
                        await on_message(frame)
                    except Exception as e:
                        logger.error(f"Error processing message: {e}")
                        continue
                
                # Connection ended normally
                logger.warning(f"{self.name} stream ended; reconnecting in {self.backoff}s")
            
            except (asyncio.CancelledError, KeyboardInterrupt):
                logger.info(f"{self.name} stream cancelled")
                break
            
            except (websockets.ConnectionClosedError, OSError) as exc:
                logger.warning(f"{self.name} connection error: {exc}; reconnecting in {self.backoff}s")
            
            except Exception as exc:
                logger.error(f"{self.name} unexpected error: {exc}; retrying in {self.backoff}s")
            
            # Exponential backoff
            if self.running:
                await asyncio.sleep(self.backoff)
                self.backoff = min(self.backoff * 2, self.max_backoff)
    
    async def close(self) -> None:
        """Close WebSocket connection."""
        self.running = False
        if self.ws:
            await self.ws.close()
            self.ws = None
            logger.info(f"Closed {self.name} connection")


# ============================================================================
# Message Router
# ============================================================================

class MessageRouter:
    """
    Routes WebSocket messages to appropriate handlers.
    
    Useful for exchanges that send different message types
    (e.g., Binance: ticker, bookTicker, trade, etc.)
    """
    
    def __init__(self):
        self.handlers = {}
    
    def register(self, message_type: str, handler: Callable[[dict], Any]) -> None:
        """Register a handler for a message type."""
        self.handlers[message_type] = handler
        logger.debug(f"Registered handler for message type: {message_type}")
    
    async def route(self, message: dict) -> None:
        """Route message to appropriate handler."""
        # Try to determine message type
        msg_type = None
        
        # Common patterns
        if "channel" in message:
            msg_type = message["channel"]
        elif "e" in message:  # Binance event type
            msg_type = message["e"]
        elif "stream" in message:  # Binance combined stream
            msg_type = message["stream"].split("@")[-1]
        elif "type" in message:
            msg_type = message["type"]
        
        # Route to handler
        if msg_type and msg_type in self.handlers:
            try:
                await self.handlers[msg_type](message)
            except Exception as e:
                logger.error(f"Handler error for {msg_type}: {e}")
        else:
            logger.debug(f"No handler for message type: {msg_type}")

