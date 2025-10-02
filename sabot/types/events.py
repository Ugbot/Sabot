#!/usr/bin/env python3
"""
Event types and definitions for Sabot channels.

Defines event structures for channel communication.
"""

from typing import Generic, TypeVar, Any, Optional, Dict, Union
from dataclasses import dataclass
from .core import HeadersArg

T = TypeVar('T')


@dataclass
class EventT(Generic[T]):
    """
    Event type for channel communication.

    Represents a single event/message in a channel stream.
    """

    # Event payload
    value: T

    # Event metadata
    key: Optional[Any] = None
    headers: Optional[HeadersArg] = None
    timestamp: Optional[float] = None
    partition: Optional[int] = None
    offset: Optional[int] = None

    # Channel information
    channel: Optional[str] = None
    topic: Optional[str] = None

    def __post_init__(self):
        """Initialize event with defaults."""
        if self.timestamp is None:
            import time
            self.timestamp = time.time()

        if self.headers is None:
            self.headers = HeadersArg()

    def add_header(self, key: str, value: Union[str, int, float, bool]) -> None:
        """Add a header to the event."""
        if self.headers is None:
            self.headers = HeadersArg()
        self.headers.add(key, value)

    def get_header(self, key: str, default: Any = None) -> Any:
        """Get a header value."""
        if self.headers is None:
            return default
        return self.headers.get(key, default)

    def copy(self) -> 'EventT[T]':
        """Create a copy of the event."""
        return EventT(
            value=self.value,
            key=self.key,
            headers=self.headers.copy() if self.headers else None,
            timestamp=self.timestamp,
            partition=self.partition,
            offset=self.offset,
            channel=self.channel,
            topic=self.topic
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert event to dictionary representation."""
        return {
            "value": self.value,
            "key": self.key,
            "headers": dict(self.headers.items()) if self.headers else {},
            "timestamp": self.timestamp,
            "partition": self.partition,
            "offset": self.offset,
            "channel": self.channel,
            "topic": self.topic
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'EventT[T]':
        """Create event from dictionary representation."""
        headers = HeadersArg()
        for k, v in data.get("headers", {}).items():
            headers.add(k, v)

        return cls(
            value=data["value"],
            key=data.get("key"),
            headers=headers,
            timestamp=data.get("timestamp"),
            partition=data.get("partition"),
            offset=data.get("offset"),
            channel=data.get("channel"),
            topic=data.get("topic")
        )


# Convenience type aliases
Event = EventT
MessageEvent = EventT[Dict[str, Any]]
DataEvent = EventT[Any]
