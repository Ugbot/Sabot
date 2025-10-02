"""
Sabot High-Level Python API

Provides user-friendly interfaces for stream processing while maintaining
Flink-level performance through zero-copy Arrow operations underneath.

Example:
    from sabot.api import Stream, tumbling

    # Create stream
    stream = Stream.from_kafka(topic="events", schema=schema)

    # Transform data
    stream.map(lambda batch: transform(batch))
    stream.filter(lambda batch: batch.column('value') > 100)

    # Window and aggregate
    stream.window(tumbling(seconds=60))
          .aggregate(sum='amount', count='*')

    # Output
    stream.to_flight("grpc://localhost:8815")
"""

from .stream import Stream, OutputStream
from .window import tumbling, sliding, session
from .state import State, ValueState, ListState

__all__ = [
    'Stream',
    'OutputStream',
    'tumbling',
    'sliding',
    'session',
    'State',
    'ValueState',
    'ListState',
]
