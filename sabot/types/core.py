#!/usr/bin/env python3
"""
Core types and type variables for Sabot.

Defines fundamental type annotations and generic types used throughout Sabot.
"""

from typing import TypeVar, Dict, Any, Optional, Union, List, Tuple
from dataclasses import dataclass

# Generic type variables for key-value operations
K = TypeVar('K')  # Key type
V = TypeVar('V')  # Value type

# Generic type variables for stream processing
T = TypeVar('T')  # Generic item type
R = TypeVar('R')  # Result type

# Headers type for message metadata
HeadersType = Dict[str, Union[str, int, float, bool]]


@dataclass
class HeadersArg:
    """
    Headers argument for message metadata.

    Provides a structured way to handle message headers across different
    channel types and serialization formats.
    """

    headers: HeadersType = None

    def __post_init__(self):
        if self.headers is None:
            self.headers = {}

    def add(self, key: str, value: Union[str, int, float, bool]) -> None:
        """Add a header."""
        self.headers[key] = value

    def get(self, key: str, default: Any = None) -> Any:
        """Get a header value."""
        return self.headers.get(key, default)

    def remove(self, key: str) -> None:
        """Remove a header."""
        if key in self.headers:
            del self.headers[key]

    def clear(self) -> None:
        """Clear all headers."""
        self.headers.clear()

    def copy(self) -> 'HeadersArg':
        """Create a copy of headers."""
        return HeadersArg(headers=self.headers.copy())

    def __getitem__(self, key: str) -> Any:
        return self.headers[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self.headers[key] = value

    def __contains__(self, key: str) -> bool:
        return key in self.headers

    def __len__(self) -> int:
        return len(self.headers)

    def __iter__(self):
        return iter(self.headers)

    def items(self):
        """Iterate over header items."""
        return self.headers.items()

    def keys(self):
        """Get header keys."""
        return self.headers.keys()

    def values(self):
        """Get header values."""
        return self.headers.values()


# Open headers for channel communication (extended headers)
OpenHeadersArg = HeadersArg  # For now, same as HeadersArg

# Header preparation utility
def prepare_headers(headers: Optional[HeadersArg]) -> OpenHeadersArg:
    """
    Prepare headers for channel communication.

    Args:
        headers: Input headers

    Returns:
        Prepared open headers
    """
    if headers is None:
        return OpenHeadersArg()
    elif isinstance(headers, HeadersArg):
        return OpenHeadersArg(headers=headers.headers.copy())
    else:
        # Convert from dict or other format
        new_headers = OpenHeadersArg()
        if isinstance(headers, dict):
            for k, v in headers.items():
                new_headers.add(k, v)
        return new_headers

# Common type aliases
MessageHeaders = HeadersArg
MetadataDict = Dict[str, Any]
ConfigDict = Dict[str, Any]

# Stream processing types
RecordBatch = Any  # Placeholder for Arrow RecordBatch
StreamData = Union[Dict[str, Any], List[Dict[str, Any]], RecordBatch]

# Channel types
ChannelKey = Union[str, int]
ChannelValue = Any
ChannelMessage = Tuple[ChannelKey, ChannelValue, Optional[HeadersArg]]

# Store types
StoreKey = Union[str, bytes, int]
StoreValue = Any

# Processing result types
ProcessingResult = Union[V, List[V], Dict[str, Any]]
AggregationResult = Dict[str, Union[int, float, str]]

# Error types
ErrorInfo = Dict[str, Any]
ExceptionContext = Dict[str, Any]
