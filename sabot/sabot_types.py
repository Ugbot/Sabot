# -*- coding: utf-8 -*-
"""Type definitions for Sabot - Core streaming primitives inspired by Ray."""

from typing import (
    Any, AsyncGenerator, AsyncIterable, Awaitable, Callable, Dict,
    List, Optional, Set, Tuple, TypeVar, Union, TYPE_CHECKING
)
from dataclasses import dataclass
from enum import Enum

if TYPE_CHECKING:
    import pyarrow as pa
else:
    pa = None

# Core type variables (Ray-inspired)
K = TypeVar('K')  # Key type
V = TypeVar('V')  # Value type
T = TypeVar('T')  # Generic type

# Ray-inspired core abstractions:
# - Agents (like Ray Actors): Stateful processing units
# - Streams (like Ray Tasks): Stateless data flows
# - Tables (like Ray Objects): Immutable state stores

# Arrow types
RecordBatch = 'pa.RecordBatch'
Table = 'pa.Table'
Schema = 'pa.Schema'
Array = 'pa.Array'
ChunkedArray = 'pa.ChunkedArray'

# Stream processing types
Message = Tuple[K, V]
TopicPartition = Tuple[str, int]

# Agent types
AgentFun = Callable[[AsyncIterable[RecordBatch]], AsyncGenerator[Any, None]]
SinkT = Union[
    'AgentT',         # Another agent
    str,              # Topic name
    Callable[[Any], Awaitable[None]],  # Async callable
    Callable[[Any], None],             # Sync callable
]

# Window types
WindowT = Union[
    'TumblingWindow',
    'SlidingWindow',
    'SessionWindow',
    'HoppingWindow'
]

# SQL types
SQLQuery = str

# Protocol definitions
class AgentT:
    """Agent protocol."""
    name: str
    concurrency: int
    supervisor_strategy: Any

    def add_sink(self, sink: SinkT) -> None: ...
    async def __aenter__(self) -> 'AgentT': ...
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None: ...


# Agent execution types
class RestartPolicy(Enum):
    """Agent restart policies."""
    PERMANENT = "permanent"    # Always restart
    TRANSIENT = "transient"    # Restart only if abnormal exit
    TEMPORARY = "temporary"    # Never restart


@dataclass
class AgentSpec:
    """Specification for agent deployment and execution."""
    name: str
    fun: AgentFun
    stream: Optional[Any] = None  # Stream to process
    concurrency: int = 1
    max_restarts: int = 3
    restart_window: float = 60.0  # seconds
    health_check_interval: float = 10.0  # seconds
    memory_limit_mb: Optional[int] = None
    cpu_limit_percent: Optional[float] = None
    restart_policy: RestartPolicy = RestartPolicy.PERMANENT

class AppT:
    """Application protocol."""
    id: str
    broker: str

    def topic(self, name: str, **kwargs) -> 'TopicT': ...
    def agent(self, topic: 'TopicT', **kwargs) -> AgentT: ...
    def table(self, name: str, **kwargs) -> 'TableT': ...
    def stream(self, topic: 'TopicT', **kwargs) -> 'StreamT': ...

class TopicT:
    """Topic protocol."""
    name: str
    partitions: int
    replication_factor: int

    async def send(self, value: Any, key: Any = None, **kwargs) -> None: ...

class StreamT:
    """Stream protocol."""
    topic: TopicT

    def __aiter__(self) -> AsyncGenerator[RecordBatch, None]: ...

class TableT:
    """Table protocol."""
    name: str

    def __getitem__(self, key: Any) -> Any: ...
    def __setitem__(self, key: Any, value: Any) -> None: ...
    def get(self, key: Any, default: Any = None) -> Any: ...

class WindowT:
    """Window protocol."""
    window_type: str
    size: Any
    slide: Optional[Any]

    def apply(self, stream: StreamT) -> StreamT: ...

# Exception types
class SabotException(Exception):
    """Base Sabot exception."""
    pass

class ArrowException(SabotException):
    """Arrow-related exceptions."""
    pass

class AgentException(SabotException):
    """Agent-related exceptions."""
    pass

class StreamException(SabotException):
    """Stream processing exceptions."""
    pass
