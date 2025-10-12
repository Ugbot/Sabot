"""
Common infrastructure for cryptocurrency exchange data collectors.

Provides:
- Storage backends (Kafka, Parquet, JSON Lines, SQLite)
- WebSocket client with reconnection
- Message routing
"""

from .storage import (
    KafkaStorage,
    ParquetStorage,
    JSONLinesStorage,
    SQLiteStorage,
    StorageManager,
)

from .websocket import (
    ExchangeWebSocket,
    MessageRouter,
)

__all__ = [
    # Storage
    'KafkaStorage',
    'ParquetStorage',
    'JSONLinesStorage',
    'SQLiteStorage',
    'StorageManager',
    
    # WebSocket
    'ExchangeWebSocket',
    'MessageRouter',
]

