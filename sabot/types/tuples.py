#!/usr/bin/env python3
"""
Tuple types and named tuples for Sabot.

Defines structured tuple types for Kafka/Faust-style message handling.
"""

from typing import Any, Optional, Callable, NamedTuple, Union
from .core import HeadersArg


class TP(NamedTuple):
    """Topic partition tuple (topic, partition)."""
    topic: str
    partition: int

    def __str__(self) -> str:
        return f"{self.topic}:{self.partition}"


class Message(NamedTuple):
    """Kafka/Faust-style message tuple."""
    topic: str
    partition: int
    offset: int
    key: Optional[bytes]
    value: Optional[bytes]
    timestamp: float
    headers: Optional[list]

    @property
    def tp(self) -> TP:
        """Get topic partition."""
        return TP(self.topic, self.partition)


class RecordMetadata(NamedTuple):
    """Record metadata after sending."""
    topic: str
    partition: int
    offset: int
    timestamp: float


class FutureMessage(NamedTuple):
    """Future message for async operations."""
    message: Message
    metadata: Optional[RecordMetadata] = None
    exception: Optional[Exception] = None


class PendingMessage(NamedTuple):
    """Pending message in channel."""
    message: Message
    future: Optional[Any] = None  # Future for async operations


# Callback type for message sending
MessageSentCallback = Callable[[FutureMessage], None]

# Convenience type aliases
TopicPartition = TP
MessageTuple = Message
MetadataTuple = RecordMetadata
PendingMessageTuple = PendingMessage
