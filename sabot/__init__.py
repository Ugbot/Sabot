# -*- coding: utf-8 -*-
"""Sabot - Streaming framework with columnar processing and Faust-like API."""

import logging

# Core application
try:
    from .app import App, RAFTStream
except ImportError as e:
    logger = logging.getLogger(__name__)
    logger.warning(f"App import failed: {e} - using stubs")
    App = None
    RAFTStream = None
from .composable_launcher import ComposableLauncher, create_composable_launcher, launch_sabot
from .distributed_coordinator import (
    DistributedCoordinator, SabotWorkerNode,
    create_distributed_coordinator, create_worker_node,
    submit_distributed_job, get_distributed_job_result
)

# Types
from .sabot_types import (
    AgentT, AppT, StreamT, TopicT, RecordBatch, Schema, Table,
    AgentFun, SinkT, WindowT, SQLQuery
)

# Checkpoint coordination (optional - requires tonbo)
try:
    from .checkpoint import Barrier, BarrierTracker, Coordinator, CheckpointStorage, RecoveryManager
    CHECKPOINT_AVAILABLE = True
except ImportError:
    CHECKPOINT_AVAILABLE = False
    Barrier = None
    BarrierTracker = None
    Coordinator = None
    CheckpointStorage = None
    RecoveryManager = None

# State management (old imports - maintain backward compatibility)
try:
    from .stores import (
        StoreBackendConfig as BackendConfig,  # Renamed for backward compat
        MemoryBackend,
        UltraFastMemoryBackend as OptimizedMemoryBackend,  # Renamed for backward compat
        MarbleDBStoreBackend,  # Primary backend (3.30x faster reads than RocksDB)
        RocksDBBackend,        # Fallback
    )
except ImportError:
    # New unified state system doesn't have these yet
    BackendConfig = None
    MemoryBackend = None
    OptimizedMemoryBackend = None
    MarbleDBStoreBackend = None
    RocksDBBackend = None

try:
    from .api.state import (
        ValueState,
        MapState,
        ListState,
        ReducingState,
    )
except ImportError:
    ValueState = None
    MapState = None
    ListState = None
    ReducingState = None

try:
    from .stores.base import StoreTransaction, MemoryTransaction
except ImportError:
    StoreTransaction = None
    MemoryTransaction = None

try:
    from sabot._cython.state.aggregating_state import AggregatingState
except ImportError:
    AggregatingState = None

# Time management
from .time import WatermarkTracker, Timers, EventTime, TimeService

# Redis client (optional)
try:
    from .redis import RedisClient, AsyncRedis, StreamManager
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    RedisClient = None
    AsyncRedis = None
    StreamManager = None

# CyArrow - Sabot's Cython-accelerated Arrow wrapper (preferred over pyarrow)
from . import cyarrow

# High-level API (new userspace API)
from .api import Stream, OutputStream, tumbling, sliding, session
from .api import ValueState as APIValueState, ListState as APIListState

# Unified engine (architecture unification)
from .engine import Sabot, create_engine

__version__ = "0.1.0"
__all__ = [
    # Unified Engine (NEW)
    "Sabot",
    "create_engine",
    
    # Core
    "App",
    "RAFTStream",
    "create_app",

    # Distributed
    "ComposableLauncher",
    "create_composable_launcher",
    "launch_sabot",
    "DistributedCoordinator",
    "SabotWorkerNode",
    "create_distributed_coordinator",
    "create_worker_node",
    "submit_distributed_job",
    "get_distributed_job_result",

    # Types
    "AgentT",
    "AppT",
    "StreamT",
    "TopicT",
    "RecordBatch",
    "Schema",
    "Table",
    "AgentFun",
    "SinkT",
    "WindowT",
    "SQLQuery",

    # Checkpoint
    "CHECKPOINT_AVAILABLE",
    "Barrier",
    "BarrierTracker",
    "Coordinator",
    "CheckpointStorage",
    "RecoveryManager",

    # State
    "BackendConfig",
    "MemoryBackend",
    "OptimizedMemoryBackend",
    "MarbleDBStoreBackend",  # Primary backend
    "RocksDBBackend",        # Fallback
    "ValueState",
    "MapState",
    "ListState",
    "ReducingState",
    "AggregatingState",
    "StoreTransaction",
    "MemoryTransaction",

    # Time
    "WatermarkTracker",
    "Timers",
    "EventTime",
    "TimeService",

    # Redis
    "RedisClient",
    "AsyncRedis",
    "StreamManager",
    "REDIS_AVAILABLE",

    # CyArrow (Cython-accelerated Arrow wrapper)
    "cyarrow",

    # High-level API
    "Stream",
    "OutputStream",
    "tumbling",
    "sliding",
    "session",
    "APIValueState",
    "APIListState",
]

def create_app(
    id: str,
    *,
    broker: str = None,  # Made optional - can work without external broker
    value_serializer: str = "arrow",
    key_serializer: str = "raw",
    enable_gpu: bool = False,
    gpu_device: int = 0,
    redis_host: str = "localhost",
    redis_port: int = 6379,
    enable_distributed_state: bool = True,
    database_url: str = "postgresql://localhost/sabot",
    **kwargs
) -> AppT:
    """Create a new Sabot application.

    Args:
        id: Unique application identifier
        broker: Message broker URL (Kafka, Redpanda, etc.) - optional for local development
        value_serializer: Default value serializer ('arrow', 'json', 'avro', etc.)
        key_serializer: Default key serializer
        enable_gpu: Enable GPU acceleration with RAFT
        gpu_device: GPU device ID to use
        redis_host: Redis host for distributed state
        redis_port: Redis port for distributed state
        enable_distributed_state: Enable CyRedis distributed state management
        database_url: Database URL for durable agent execution
        **kwargs: Additional application configuration

    Returns:
        Configured Sabot application

    Example:
        >>> # Basic usage
        >>> app = sabot.create_app('my-app')
        >>>
        >>> # With GPU acceleration
        >>> app = sabot.create_app('ml-app', enable_gpu=True, gpu_device=0)
        >>>
        >>> # With Kafka broker
        >>> app = sabot.create_app('stream-app', broker='kafka://localhost:9092')
        >>>
        >>> # GPU-accelerated ML pipeline
        >>> raft = app.raft_stream('ml-pipeline')
        >>> cluster_processor = raft.kmeans_cluster(n_clusters=8)
    """
    return App(
        id=id,
        broker=broker,
        value_serializer=value_serializer,
        key_serializer=key_serializer,
        enable_gpu=enable_gpu,
        gpu_device=gpu_device,
        redis_host=redis_host,
        redis_port=redis_port,
        enable_distributed_state=enable_distributed_state,
        database_url=database_url,
        **kwargs
    )


# ============================================================================
# Extend PyArrow Compute with Additional Functions
# ============================================================================

# Add modulo function to pyarrow.compute namespace
try:
    # Use Sabot's vendored Arrow (includes custom kernels)
    pc = cyarrow.compute
    from sabot._cython.arrow.compute import modulo
    pc.modulo = modulo
except ImportError:
    pass  # Arrow not available, skip extension
