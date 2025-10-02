# -*- coding: utf-8 -*-
"""Type definitions for Sabot."""

from .channels import ChannelT
from .core import HeadersArg, K, V, OpenHeadersArg, prepare_headers
from .codecs import CodecArg
from .tuples import TP, FutureMessage, Message, MessageSentCallback, RecordMetadata, PendingMessage
from .events import EventT
from .models import ModelArg, SchemaT

# Utility functions
def _PendingMessage_to_Message(pending: 'PendingMessage') -> 'Message':
    """Extract Message from PendingMessage."""
    return pending.message

# Import from sabot_types for compatibility
try:
    from ..sabot_types import AppT, AgentT, TopicT, StreamT, TableT, AgentFun, SinkT, RecordBatch, Schema, Table
    SABOT_TYPES_AVAILABLE = True
except ImportError:
    SABOT_TYPES_AVAILABLE = False
    # Define minimal types for when sabot_types is not available
    from typing import Any, Protocol, AsyncGenerator, TypeVar, Generic
    T = TypeVar('T')

    class AppT(Protocol): pass
    class AgentT(Protocol): pass
    TopicT = Any

    # Define StreamT as a generic type when sabot_types is not available
    class StreamT(Generic[T]):
        """Minimal StreamT implementation."""
        pass

    TableT = Any
    AgentFun = Any
    SinkT = Any
    RecordBatch = Any
    Schema = Any
    Table = Any

__all__ = [
    "ChannelT", "AppT", "AgentT", "TopicT", "StreamT", "TableT",
    "AgentFun", "SinkT", "RecordBatch", "Schema", "Table",
    "HeadersArg", "K", "V", "CodecArg", "EventT", "ModelArg", "OpenHeadersArg", "SchemaT",
    "TP", "FutureMessage", "Message", "MessageSentCallback", "RecordMetadata", "PendingMessage",
    "prepare_headers"
]
