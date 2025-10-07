# -*- coding: utf-8 -*-
"""Sabot App - Main application class inspired by Ray's distributed runtime."""

import asyncio
import logging
from typing import Any, Dict, List, Optional, Union, Callable, AsyncGenerator, Type
from pathlib import Path
from dataclasses import dataclass

from .sabot_types import AppT, AgentT, TopicT, StreamT, TableT, AgentFun, SinkT
from . import _cython as cython
from .agent_manager import DurableAgentManager, get_agent_manager
from .stores import create_backend_auto, StoreBackendConfig
from .windows import create_windowed_stream, WindowedStream
from .materialized_views import get_materialized_view_manager, MaterializedViewManager
from .joins import create_join_builder, JoinBuilder
from .channel_manager import get_channel_manager, ChannelBackend, ChannelPolicy
from .morsel_parallelism import ParallelProcessor, create_parallel_processor
from .stream_parallel import (
    ParallelStreamProcessor, FanOutFanInPipeline,
    parallel_map_stream, parallel_filter_stream, parallel_group_by_stream
)
from .dbos_cython_parallel import (
    DBOSCythonParallelProcessor, FanOutFanInCythonPipeline,
    parallel_process_with_dbos, parallel_stream_process_with_dbos,
    get_parallel_performance_summary
)
from .distributed_agents import DistributedAgentManager, AgentSpec
from .flink_chaining import StreamBuilder, WindowSpec, create_stream_builder
from .types import ChannelT
from .core.stream_engine import StreamEngine, StreamConfig, ProcessingMode
from .core.metrics import MetricsCollector

logger = logging.getLogger(__name__)

# CyRedis imports for enhanced state management
try:
    from vendor.cyredis import AsyncRedisClient, RedisPool, RedisPipeline
    CYREDIS_AVAILABLE = True
except ImportError:
    logger.warning("CyRedis not available, using basic state management")
    CYREDIS_AVAILABLE = False
    AsyncRedisClient = RedisPool = RedisPipeline = None

# RAFT imports for GPU-accelerated ML operations
try:
    import cupy as cp
    import cudf
    from pylibraft.cluster import KMeans as RAFTKMeans
    from pylibraft.neighbors import NearestNeighbors as RAFTNearestNeighbors
    from pylibraft.common import DeviceResources
    RAFT_AVAILABLE = True
except ImportError:
    logger.warning("RAFT not available, GPU-accelerated ML operations disabled")
    RAFT_AVAILABLE = False
    cp = cudf = RAFTKMeans = RAFTNearestNeighbors = DeviceResources = None

# Ray-inspired distributed runtime:
# - Agents: Stateful workers (like Ray Actors)
# - Streams: Data flow tasks (like Ray Tasks)
# - Tables: Distributed objects (like Ray Objects)


class App(AppT):
    """Main Sabot application class - Ray-inspired distributed runtime.

    Just like Ray provides a unified framework for distributed computing,
    Sabot provides a unified framework for distributed streaming.

    Core abstractions (Ray-inspired):
    - Agents: Stateful async workers for complex stream processing
    - Streams: Immutable data flows for stateless transformations
    - Tables: Distributed state stores for aggregations and joins

    Scales seamlessly from laptop development to cluster production.
    """

    def __init__(
        self,
        id: str,
        *,
        broker: Optional[str] = None,  # Made optional - can work without external broker
        value_serializer: str = "arrow",
        key_serializer: str = "raw",
        flight_port: int = 8815,
        enable_flight: bool = True,
        enable_interactive: bool = True,
        # Redis configuration for CyRedis
        redis_host: str = "localhost",
        redis_port: int = 6379,
        redis_max_connections: int = 10,
        enable_redis_cache: bool = True,
        enable_distributed_state: bool = True,
        # GPU/RAFT configuration
        enable_gpu: bool = False,
        gpu_device: int = 0,
        # Database configuration for durable execution
        database_url: str = "postgresql://localhost/sabot",
        **kwargs
    ):
        self.id = id
        self.broker = broker
        self.value_serializer = value_serializer
        self.key_serializer = key_serializer
        self.flight_port = flight_port

        # Core components
        self._topics = {}
        self._tables = {}
        self._agents = {}
        self._channels = {}

        # Configuration
        from .config import SabotConfig
        self.conf = SabotConfig()

        # Real stream processing engine (replaces mocked processing)
        self._stream_engine = None
        self._stream_config = StreamConfig()

        # Advanced features
        self._flight_enabled = enable_flight
        self._interactive_enabled = enable_interactive

        # Redis/CyRedis configuration
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_max_connections = redis_max_connections
        self.enable_redis_cache = enable_redis_cache
        self.enable_distributed_state = enable_distributed_state

        # GPU/RAFT configuration
        self.enable_gpu = enable_gpu
        self.gpu_device = gpu_device
        self._gpu_resources = None

        # Database configuration
        self.database_url = database_url

        # Message broker status
        self._has_broker = broker is not None
        if not self._has_broker:
            logger.info(f"App '{id}' initialized without external broker - using local messaging")
        else:
            logger.info(f"App '{id}' initialized with broker: {broker}")

        # Initialize GPU resources if enabled
        if enable_gpu and RAFT_AVAILABLE:
            try:
                self._gpu_resources = DeviceResources(device_id=gpu_device)
                logger.info(f"App '{id}' initialized with GPU acceleration on device {gpu_device}")
            except Exception as e:
                logger.warning(f"Failed to initialize GPU resources: {e}")
                self._gpu_resources = None
        elif enable_gpu:
            logger.warning("GPU enabled but RAFT not available - install raft-dask and pylibraft")

        # Initialize distributed agent system
        self._distributed_coordinator = None
        self._channel_manager = None
        self._dbos_controller = None
        self._distributed_agent_manager = None

        # Initialize CyRedis client if available
        self.redis_client = None
        if CYREDIS_AVAILABLE and enable_distributed_state:
            try:
                self.redis_client = AsyncRedisClient(
                    host=redis_host,
                    port=redis_port,
                    max_connections=redis_max_connections
                )
                logger.info(f"Initialized CyRedis client: {redis_host}:{redis_port}")

                # Initialize distributed primitives
                self._init_distributed_primitives()

                # Initialize distributed agent system
                self._init_distributed_agents()

            except Exception as e:
                logger.warning(f"Failed to initialize CyRedis client: {e}")
                self.redis_client = None

        # Initialize durable agent manager (DBOS-inspired) - ALWAYS needed for @app.agent
        try:
            self._agent_manager = DurableAgentManager(self, database_url)
        except Exception as e:
            # Fallback to basic agent manager if DB not available
            logger.warning(f"Failed to initialize DurableAgentManager: {e}")
            logger.warning("Using basic agent manager (no durable execution)")
            # Create a minimal stub agent manager
            from dataclasses import dataclass
            @dataclass
            class BasicAgentManager:
                app: Any
                def register_agent(self, **kwargs):
                    """Register agent (basic implementation)."""
                    agent_name = kwargs.get('name')
                    logger.info(f"Registered agent: {agent_name}")
                    return kwargs.get('func')
                async def start(self):
                    pass
                async def stop(self):
                    pass
            self._agent_manager = BasicAgentManager(self)

        self._flight_app = None
        self._interactive_app = None

    def _init_distributed_primitives(self):
        """Initialize CyRedis distributed primitives for enhanced state management."""
        if not self.redis_client:
            return

        # CyRedis provides low-level primitives, distributed counters/locks can be implemented as needed
        # For now, we initialize basic client connection
        self._agent_counter = None
        self._message_counter = None

    def _init_distributed_agents(self):
        """Initialize distributed agent system components."""
        if not self.redis_client:
            return

        try:
            from .distributed_coordinator import DistributedCoordinator
            from .channel_manager import ChannelManager
            from .dbos_parallel_controller import DBOSParallelController

            # Create distributed coordinator
            self._distributed_coordinator = DistributedCoordinator(
                host="0.0.0.0",
                port=8080,  # Could make this configurable
                cluster_name=f"{self.id}_cluster"
            )

            # Create channel manager
            self._channel_manager = ChannelManager()

            # Create DBOS controller
            self._dbos_controller = DBOSParallelController(
                max_workers=psutil.cpu_count() * 2,  # Default scaling
                target_utilization=0.8
            )

            # Create distributed agent manager
            self._distributed_agent_manager = DistributedAgentManager(
                self._distributed_coordinator,
                self._channel_manager,
                self._dbos_controller
            )

            logger.info("Initialized distributed agent system")

        except Exception as e:
            logger.warning(f"Failed to initialize distributed agent system: {e}")
            self._distributed_coordinator = None
            self._channel_manager = None
            self._dbos_controller = None
            self._distributed_agent_manager = None

        # CyRedis distributed locks can be implemented using SET NX EX pattern
        self._coordination_lock = None
        self._table_lock = None

        # Local cache can be implemented using CyRedis pipeline operations
        if self.enable_redis_cache:
            # TODO: Implement local cache using CyRedis primitives
            self._local_cache = None
        else:
            self._local_cache = None

        logger.info("Initialized CyRedis distributed primitives")

        # Note: _agent_manager is now initialized unconditionally in __init__

        # Initialize advanced features if enabled
        if self._flight_enabled:
            try:
                from .arrow_flight import ArrowFlightApp
                self._flight_app = ArrowFlightApp(self, flight_port=self.flight_port)
            except ImportError:
                logger.warning("Arrow Flight not available")

        if self._interactive_enabled:
            try:
                from .arrow_interactive import ArrowInteractiveApp
                self._interactive_app = ArrowInteractiveApp(self)
            except ImportError:
                logger.warning("Interactive features not available")

        logger.info(f"Initialized Sabot app '{id}' with broker {broker}")

    def channel(
        self,
        name: Optional[str] = None,
        *,
        key_type: Optional[Type] = None,
        value_type: Optional[Type] = None,
        maxsize: int = 1000,
    ) -> ChannelT:
        """
        Create a memory channel (synchronous).

        For Kafka/Redis/Flight channels, use async_channel() instead.

        Args:
            name: Channel name (auto-generated if None)
            key_type: Type for keys (optional)
            value_type: Type for values (optional)
            maxsize: Max channel size

        Returns:
            MemoryChannel instance

        Example:
            channel = app.channel(name='my-channel', maxsize=5000)
        """
        from sabot.channels import Channel

        # Generate name if not provided
        if name is None:
            name = f"channel-{len(self._channels)}"

        # Create memory channel
        channel = Channel(
            app=self,
            maxsize=maxsize,
            key_type=key_type,
            value_type=value_type,
        )

        # Register channel
        self._channels[name] = channel

        return channel

    async def async_channel(
        self,
        name: Optional[str] = None,
        *,
        backend: str = "memory",
        key_type: Optional[Type] = None,
        value_type: Optional[Type] = None,
        maxsize: int = 1000,
        **backend_options
    ) -> ChannelT:
        """
        Create a channel asynchronously (for Kafka/Redis backends).

        Args:
            name: Channel name (auto-generated if None)
            backend: Backend type ('memory', 'kafka', 'redis', 'flight')
            key_type: Type for keys (optional)
            value_type: Type for values (optional)
            maxsize: Max channel size (for memory backend)
            **backend_options: Backend-specific options

        Returns:
            Channel instance

        Example:
            # Create Kafka channel
            channel = await app.async_channel(
                name='my-topic',
                backend='kafka',
                broker='localhost:9092'
            )
        """
        from sabot.channels import Channel

        # Generate name if not provided
        if name is None:
            name = f"channel-{len(self._channels)}"

        # Create based on backend
        if backend == "memory":
            channel = Channel(
                app=self,
                maxsize=maxsize,
                key_type=key_type,
                value_type=value_type,
            )

        elif backend == "kafka":
            # Import here to avoid dependency if not using Kafka
            broker = backend_options.get('broker', self.broker)
            topic = backend_options.get('topic', name)

            channel = await self._create_kafka_channel(
                topic=topic,
                broker=broker,
                key_type=key_type,
                value_type=value_type,
                **backend_options
            )

        elif backend == "redis":
            redis_url = backend_options.get('redis_url', 'redis://localhost:6379')

            channel = await self._create_redis_channel(
                name=name,
                redis_url=redis_url,
                key_type=key_type,
                value_type=value_type,
                **backend_options
            )

        elif backend == "flight":
            flight_url = backend_options.get('flight_url')
            if not flight_url:
                raise ValueError("flight_url required for Flight backend")

            channel = await self._create_flight_channel(
                name=name,
                flight_url=flight_url,
                key_type=key_type,
                value_type=value_type,
                **backend_options
            )

        else:
            raise ValueError(f"Unknown backend: {backend}")

        # Register channel
        self._channels[name] = channel

        return channel

    def _select_channel_backend_sync(
        self,
        name: str,
        policy: Optional[ChannelPolicy],
        **kwargs: Any
    ) -> ChannelBackend:
        """Synchronous backend selection for common cases."""
        manager = get_channel_manager(self)

        if policy:
            return manager._select_backend_by_policy(policy, **kwargs)

        # Default to memory for local development
        if getattr(self.conf, 'is_development', False):
            return ChannelBackend.MEMORY

        # Production defaults based on channel name patterns
        if any(pattern in name for pattern in ['events', 'stream', 'topic']):
            return ChannelBackend.KAFKA
        elif any(pattern in name for pattern in ['cache', 'temp', 'session']):
            return ChannelBackend.REDIS
        else:
            return ChannelBackend.MEMORY

    async def create_channel_async(
        self,
        name: Optional[str] = None,
        *,
        backend: Optional[ChannelBackend] = None,
        policy: Optional[ChannelPolicy] = None,
        **kwargs
    ) -> ChannelT:
        """Async version of channel creation for full DBOS integration."""
        if name is None:
            import uuid
            name = f"channel-{uuid.uuid4().hex[:8]}"

        manager = get_channel_manager(self)
        return await manager.create_channel(
            name=name,
            backend=backend,
            policy=policy,
            **kwargs
        )

    def memory_channel(
        self,
        name: Optional[str] = None,
        *,
        maxsize: Optional[int] = None,
        schema=None,
        key_type=None,
        value_type=None,
        **kwargs
    ) -> ChannelT:
        """Create in-memory channel for local communication.

        Args:
            name: Channel name (auto-generated if None)
            maxsize: Maximum queue size
            schema: Schema for serialization
            key_type: Message key type
            value_type: Message value type

        Returns:
            In-memory channel instance
        """
        return self.channel(
            name=name,
            backend=ChannelBackend.MEMORY,
            maxsize=maxsize,
            schema=schema,
            key_type=key_type,
            value_type=value_type,
            **kwargs
        )

    async def kafka_channel(
        self,
        name: str,
        *,
        partitions: int = 1,
        replication_factor: int = 1,
        retention_hours: Optional[int] = None,
        compression: Optional[str] = None,
        schema=None,
        key_type=None,
        value_type=None,
        **kwargs
    ) -> ChannelT:
        """Create Kafka-backed channel for distributed streaming.

        Args:
            name: Topic/channel name
            partitions: Number of partitions
            replication_factor: Replication factor
            retention_hours: Message retention period
            compression: Compression type
            schema: Schema for serialization
            key_type: Message key type
            value_type: Message value type

        Returns:
            Kafka-backed channel instance
        """
        return await self.create_channel_async(
            name=name,
            backend=ChannelBackend.KAFKA,
            partitions=partitions,
            replication_factor=replication_factor,
            retention_hours=retention_hours,
            compression=compression,
            schema=schema,
            key_type=key_type,
            value_type=value_type,
            **kwargs
        )

    async def redis_channel(
        self,
        name: str,
        *,
        maxsize: Optional[int] = None,
        schema=None,
        key_type=None,
        value_type=None,
        **kwargs
    ) -> ChannelT:
        """Create Redis-backed channel for fast pub/sub.

        Args:
            name: Channel name
            maxsize: Maximum queue size
            schema: Schema for serialization
            key_type: Message key type
            value_type: Message value type

        Returns:
            Redis-backed channel instance
        """
        return await self.create_channel_async(
            name=name,
            backend=ChannelBackend.REDIS,
            maxsize=maxsize,
            schema=schema,
            key_type=key_type,
            value_type=value_type,
            **kwargs
        )

    async def flight_channel(
        self,
        name: str,
        *,
        location: str = "grpc://localhost:8815",
        path: Optional[str] = None,
        schema=None,
        key_type=None,
        value_type=None,
        **kwargs
    ) -> ChannelT:
        """Create Arrow Flight-backed channel for high-performance network transfer.

        Args:
            name: Channel name
            location: Flight server location
            path: Flight path (auto-generated from name if None)
            schema: Schema for serialization
            key_type: Message key type
            value_type: Message value type

        Returns:
            Arrow Flight-backed channel instance
        """
        if path is None:
            path = f"/sabot/channels/{name}"

        return await self.create_channel_async(
            name=name,
            backend=ChannelBackend.FLIGHT,
            location_hint=location,
            path=path,
            schema=schema,
            key_type=key_type,
            value_type=value_type,
            **kwargs
        )

    async def rocksdb_channel(
        self,
        name: str,
        *,
        path: Optional[str] = None,
        schema=None,
        key_type=None,
        value_type=None,
        **kwargs
    ) -> ChannelT:
        """Create RocksDB-backed channel for durable local storage.

        Args:
            name: Channel name
            path: RocksDB path (auto-generated if None)
            schema: Schema for serialization
            key_type: Message key type
            value_type: Message value type

        Returns:
            RocksDB-backed channel instance
        """
        return await self.create_channel_async(
            name=name,
            backend=ChannelBackend.ROCKSDB,
            location_hint=path,
            schema=schema,
            key_type=key_type,
            value_type=value_type,
            **kwargs
        )

    def set_channel_policy(self, pattern: str, policy: ChannelPolicy) -> None:
        """Set channel policy for channels matching a pattern.

        Args:
            pattern: Channel name pattern (e.g., "events.*", "cache-*")
            policy: Policy to apply to matching channels
        """
        manager = get_channel_manager(self)
        manager.set_policy(pattern, policy)

    def get_channel_manager(self):
        """Get the channel manager instance."""
        return get_channel_manager(self)

    def create_dbos_parallel_processor(
        self,
        max_workers: Optional[int] = None,
        morsel_size_kb: int = 64,
        target_utilization: float = 0.8
    ) -> DBOSCythonParallelProcessor:
        """Create a DBOS-controlled Cython parallel processor.

        This provides intelligent, adaptive parallel processing that automatically:
        - Scales workers based on system load and data characteristics
        - Distributes work using NUMA-aware scheduling
        - Uses Cython optimizations for maximum performance
        - Adapts to changing conditions in real-time

        Args:
            max_workers: Maximum number of workers (auto-detected if None)
            morsel_size_kb: Size of data morsels in KB
            target_utilization: Target system utilization (0.0-1.0)

        Returns:
            DBOS-controlled parallel processor instance

        Example:
            # Create intelligent parallel processor
            processor = app.create_dbos_parallel_processor()

            # Process data with automatic optimization
            results = await processor.process_data(large_dataset, process_function)
        """
        return DBOSCythonParallelProcessor(max_workers, morsel_size_kb, target_utilization)

    def create_fan_out_fan_in_pipeline(
        self,
        processor_func: Callable,
        num_branches: int = 4,
        morsel_size_kb: int = 64,
        max_workers: Optional[int] = None
    ) -> FanOutFanInCythonPipeline:
        """Create a DBOS-controlled fan-out/fan-in pipeline.

        Automatically distributes work across multiple branches and merges results,
        with intelligent load balancing and Cython performance optimization.

        Args:
            processor_func: Function to apply to each data morsel
            num_branches: Number of parallel processing branches
            morsel_size_kb: Size of data morsels in KB
            max_workers: Maximum workers per branch

        Returns:
            Fan-out/fan-in pipeline instance

        Example:
            async def process_chunk(chunk):
                # Process data chunk
                return expensive_computation(chunk)

            pipeline = app.create_fan_out_fan_in_pipeline(process_chunk, num_branches=8)

            # Process stream with automatic parallelization
            result_stream = await pipeline.process_stream(input_stream)
        """
        return FanOutFanInCythonPipeline(
            processor_func, num_branches, morsel_size_kb, max_workers
        )

    async def parallel_process_data(
        self,
        data: Any,
        processor_func: Callable[[Any], Any],
        max_workers: Optional[int] = None,
        morsel_size_kb: int = 64
    ) -> List[Any]:
        """High-level parallel data processing with DBOS control.

        Automatically handles morsel creation, work distribution, and result aggregation
        using intelligent DBOS-guided decisions and Cython performance optimizations.

        Args:
            data: Input data to process
            processor_func: Function to apply to each morsel
            max_workers: Maximum number of workers
            morsel_size_kb: Size of data morsels

        Returns:
            List of processing results

        Example:
            data = list(range(100000))  # Large dataset

            async def square(x):
                await asyncio.sleep(0.001)  # Simulate work
                return x * x

            # Automatically parallelized with DBOS intelligence
            results = await app.parallel_process_data(data, square)
        """
        return await parallel_process_with_dbos(data, processor_func, max_workers, morsel_size_kb)

    async def parallel_process_stream(
        self,
        input_stream: asyncio.Queue,
        processor_func: Callable[[Any], Any],
        max_workers: Optional[int] = None,
        morsel_size_kb: int = 64,
        output_stream: Optional[asyncio.Queue] = None
    ) -> asyncio.Queue:
        """High-level parallel stream processing with DBOS control.

        Processes streaming data with automatic parallelization, load balancing,
        and performance optimization guided by DBOS intelligence.

        Args:
            input_stream: Input data stream
            processor_func: Function to apply to each morsel
            max_workers: Maximum number of workers
            morsel_size_kb: Size of data morsels
            output_stream: Output stream (created if None)

        Returns:
            Output stream with processed results

        Example:
            input_stream = asyncio.Queue()

            # Feed data to stream
            for item in large_dataset:
                await input_stream.put(item)

            # Process with automatic parallelization
            output_stream = await app.parallel_process_stream(
                input_stream, process_function
            )

            # Consume results
            while True:
                result = await output_stream.get()
                print(f"Processed: {result}")
        """
        return await parallel_stream_process_with_dbos(
            input_stream, processor_func, max_workers, morsel_size_kb, output_stream
        )

    def get_parallel_performance_stats(self) -> Dict[str, Any]:
        """Get system-wide parallel processing performance statistics.

        Returns comprehensive metrics about parallel processing performance,
        system resource utilization, and DBOS decision making.

        Returns:
            Dictionary with performance metrics
        """
        from .dbos_parallel_controller import get_system_resource_summary

        return {
            'system_resources': get_system_resource_summary(),
            'parallel_processing_available': True,
            'cython_optimization': self._check_cython_available(),
        }

    def _check_cython_available(self) -> bool:
        """Check if Cython optimizations are available."""
        try:
            from ._cython import morsel_parallelism
            return True
        except ImportError:
            return False

    def create_distributed_agent(self,
                               name: str,
                               func: Callable,
                               concurrency: int = 1,
                               isolated_partitions: bool = False,
                               **kwargs) -> Optional[Any]:
        """Create a distributed agent that can run across multiple nodes.

        This creates a Faust-inspired agent that uses DBOS for intelligent
        distribution and supervision across the cluster.

        Args:
            name: Unique agent name
            func: Async function to process stream events
            concurrency: Number of concurrent actor instances
            isolated_partitions: Whether to isolate partitions
            **kwargs: Additional agent configuration

        Returns:
            DistributedAgent instance or None if system not available

        Example:
            @app.create_distributed_agent("process_orders", concurrency=3)
            async def order_processor(order):
                # Process order
                await process_order(order)
                yield order.total
        """
        if not self._distributed_agent_manager:
            logger.warning("Distributed agent system not available")
            return None

        # Create agent spec
        spec = AgentSpec(
            name=name,
            func=func,
            concurrency=concurrency,
            isolated_partitions=isolated_partitions,
            **kwargs
        )

        # Create and return agent
        agent = DistributedAgent(
            spec,
            self._distributed_coordinator,
            self._channel_manager,
            self._dbos_controller
        )

        # Register with manager
        self._distributed_agent_manager.agents[name] = agent

        return agent

    def agent(self,
              channel: Union[str, Any] = None,
              *,
              name: Optional[str] = None,
              concurrency: int = 1,
              distributed: bool = False,
              **kwargs) -> Callable:
        """Create an agent (enhanced to support distributed mode).

        This is an enhanced version of the traditional agent decorator
        that can create either local or distributed agents based on the
        `distributed` parameter.

        Args:
            channel: Input channel/topic for the agent
            name: Agent name (auto-generated if None)
            concurrency: Number of concurrent instances
            distributed: Whether to create a distributed agent
            **kwargs: Additional agent configuration

        Returns:
            Agent decorator function

        Example:
            # Local agent (traditional)
            @app.agent("orders")
            async def process_orders(order):
                yield order.total

            # Distributed agent (new)
            @app.agent("orders", distributed=True, concurrency=5)
            async def process_orders_distributed(order):
                yield order.total
        """
        def decorator(func):
            if distributed:
                # Create distributed agent
                agent_name = name or f"{func.__name__}_distributed"
                return self.create_distributed_agent(
                    agent_name, func, concurrency=concurrency, **kwargs
                )
            else:
                # Create traditional local agent
                # For now, return the function unchanged
                # In full implementation, would integrate with existing agent system
                logger.info(f"Created local agent: {name or func.__name__}")
                return func

        return decorator

    def create_flink_stream(self, name: str) -> Any:
        """Create a Flink-style distributed stream for chaining operations.

        This provides a fluent API for building complex stream processing
        pipelines with automatic distribution across cluster nodes.

        Args:
            name: Unique stream name

        Returns:
            DistributedStream instance for chaining

        Example:
            stream = app.create_flink_stream("sensor_pipeline")

            (stream
                .map(lambda x: x * 2)
                .filter(lambda x: x > 100)
                .key_by(lambda x: x % 10)
                .window(WindowSpec("tumbling", 60.0))
                .sum()
                .sink(lambda result: print(f"Window sum: {result}"))
            )

            await stream.start()
        """
        if not self._distributed_agent_manager:
            logger.warning("Distributed agent system not available for streams")
            return None

        from .flink_chaining import DistributedStream
        return DistributedStream(self._distributed_agent_manager, name)

    def create_stream_builder(self) -> Any:
        """Create a stream builder for Flink-style stream construction.

        Returns:
            StreamBuilder instance

        Example:
            builder = app.create_stream_builder()

            # Create from collection
            stream = (builder.from_collection([1, 2, 3, 4, 5])
                .map(lambda x: x * 2)
                .filter(lambda x: x > 4)
                .sink(lambda x: print(f"Result: {x}")))

            await stream.start()
        """
        if not self._distributed_agent_manager:
            logger.warning("Distributed agent system not available for stream builder")
            return None

        return create_stream_builder(self._distributed_agent_manager)

    def get_distributed_stats(self) -> Dict[str, Any]:
        """Get comprehensive distributed system statistics.

        Returns detailed information about distributed agents, streams,
        cluster health, and performance metrics.

        Returns:
            Dictionary with distributed system stats
        """
        if not self._distributed_agent_manager:
            return {"error": "Distributed system not initialized"}

        stats = {
            "distributed_system": {
                "available": True,
                "coordinator": self._distributed_coordinator is not None,
                "channel_manager": self._channel_manager is not None,
                "dbos_controller": self._dbos_controller is not None,
                "agent_manager": True,
            }
        }

        # Add agent manager stats
        if self._distributed_agent_manager:
            stats["agents"] = self._distributed_agent_manager.get_stats()

        # Add coordinator stats
        if self._distributed_coordinator:
            stats["coordinator"] = self._distributed_coordinator.get_cluster_stats()

        # Add DBOS controller stats
        if self._dbos_controller:
            stats["dbos_controller"] = self._dbos_controller.get_performance_stats()

        return stats

    def create_parallel_processor(
        self,
        num_workers: Optional[int] = None,
        morsel_size_kb: int = 64
    ) -> ParallelProcessor:
        """Create a Morsel-Driven parallel processor.

        Args:
            num_workers: Number of worker threads (auto-detected if None)
            morsel_size_kb: Size of data morsels in KB

        Returns:
            Parallel processor instance
        """
        return create_parallel_processor(num_workers, morsel_size_kb)

    def create_parallel_stream_processor(
        self,
        num_workers: Optional[int] = None,
        morsel_size_kb: int = 64
    ) -> ParallelStreamProcessor:
        """Create a parallel stream processor.

        Args:
            num_workers: Number of worker threads
            morsel_size_kb: Size of data morsels in KB

        Returns:
            Parallel stream processor instance
        """
        return ParallelStreamProcessor(num_workers, morsel_size_kb)

    def create_fan_out_fan_in_pipeline(
        self,
        processor_func: Callable,
        num_branches: int = 4,
        num_workers: Optional[int] = None,
        morsel_size_kb: int = 64
    ) -> FanOutFanInPipeline:
        """Create a fan-out/fan-in processing pipeline.

        Args:
            processor_func: Function to apply to each morsel
            num_branches: Number of parallel branches
            num_workers: Number of worker threads
            morsel_size_kb: Size of data morsels in KB

        Returns:
            Fan-out/fan-in pipeline instance
        """
        return FanOutFanInPipeline(
            processor_func, num_branches, num_workers, morsel_size_kb
        )

    def topic(self, name: str, **kwargs) -> TopicT:
        """Create or get a topic."""
        if name not in self._topics:
            topic = Topic(name, app=self, **kwargs)
            self._topics[name] = topic
        return self._topics[name]

    def table(self, name: str, backend: str = "arrow_files", use_cython: bool = True, **kwargs) -> TableT:
        """Create or get a table with specified backend.

        Args:
            name: Table name
            backend: Backend specification (e.g., "memory://", "redis://", "tonbo://path", "arrow_files://path")
            use_cython: Whether to use Cython-optimized backends for performance
            **kwargs: Additional backend configuration

        Returns:
            Table instance
        """
        if name not in self._tables:
            table = Table(name, app=self, backend=backend, use_cython=use_cython, **kwargs)
            self._tables[name] = table
        return self._tables[name]

    def raft_stream(self, name: str, **kwargs) -> 'RAFTStream':
        """Create a GPU-accelerated RAFT stream for ML operations."""
        return RAFTStream(name, self, **kwargs)

    def windowed_stream(self) -> WindowedStream:
        """Create a windowed stream factory for time-based aggregations."""
        return create_windowed_stream(self)

    def materialized_views(self, db_path: str = "./materialized_views") -> MaterializedViewManager:
        """Create a materialized view manager with RocksDB persistence."""
        return get_materialized_view_manager(db_path)

    def materializations(self, default_backend: str = 'memory'):
        """
        Create unified materialization manager (dimension tables + analytical views).

        This is the modern API for creating:
        - Dimension tables: Lookup-optimized (RocksDB backend)
        - Analytical views: Scan-optimized (Tonbo backend)

        Both can be populated from streams, files, operators, or CDC.

        Args:
            default_backend: Default storage backend ('memory', 'rocksdb', 'tonbo')

        Returns:
            MaterializationManager instance

        Example:
            mat_mgr = app.materializations()

            # Dimension table from file
            securities = mat_mgr.dimension_table(
                'securities',
                source='data/securities.arrow',
                key='security_id'
            )

            # Use with operators
            if 'SEC123' in securities:
                print(securities['SEC123'])

            # Enrich stream
            enriched = securities @ quotes_stream
        """
        from .materializations import get_materialization_manager
        return get_materialization_manager(self, default_backend)

    def joins(self) -> JoinBuilder:
        """Create a join builder for stream and table joins."""
        return create_join_builder(self)

    def stream(self, topic: TopicT, **kwargs) -> StreamT:
        """Create a stream from a topic."""
        return Stream(topic, **kwargs)

    def agent(self, topic: Union[str, TopicT], **kwargs) -> AgentT:
        """
        DEPRECATED: Create a durable agent.

        This method is deprecated. Use @app.dataflow instead.
        Agents are now worker nodes, not user code.

        This decorator is maintained for backward compatibility.
        It internally maps to @app.dataflow for now.
        """
        import warnings
        warnings.warn(
            "@app.agent is deprecated. Use @app.dataflow instead. "
            "Agents are now worker nodes, not user code.",
            DeprecationWarning,
            stacklevel=2
        )

        def decorator(func: AgentFun) -> AgentT:
            # Map to dataflow internally for backward compatibility
            topic_name = topic if isinstance(topic, str) else topic.name

            # Create a simple dataflow wrapper
            # This maintains compatibility while using new system
            return self._create_legacy_agent_wrapper(
                func, topic_name, **kwargs
            )

        return decorator

    def _create_legacy_agent_wrapper(self, func, topic_name, **kwargs):
        """
        Internal: Create legacy agent wrapper for backward compatibility.

        This creates a dataflow that wraps the old agent function.
        """
        from .api.stream import Stream

        # Build dataflow (simplified - real implementation would handle async generators)
        stream = Stream.from_kafka(topic_name)

        # Apply function as map operator
        # (simplified - real implementation would handle async generators)
        result_stream = stream.map(func)

        # Register as dataflow
        agent_name = kwargs.get('name', f"{func.__name__}_{topic_name}")
        return self._register_dataflow(agent_name, result_stream)

    def dataflow(self, name: Optional[str] = None):
        """
        Create a dataflow (operator DAG).

        This is the NEW way to define stream processing logic.
        Dataflows are compiled to physical execution graphs
        and deployed to agent worker nodes by the JobManager.

        Example:
            @app.dataflow("process_orders")
            def order_pipeline():
                return (Stream.from_kafka('orders')
                    .filter(lambda b: pc.greater(b['amount'], 1000))
                    .window(tumbling(seconds=60))
                    .aggregate({'total': ('amount', 'sum')})
                    .to_kafka('processed_orders'))

        Args:
            name: Dataflow name (auto-generated if None)

        Returns:
            Dataflow decorator
        """
        def decorator(func: Callable) -> 'Dataflow':
            dataflow_name = name or func.__name__

            # Execute function to get operator DAG
            operator_dag = func()

            # Register dataflow
            return self._register_dataflow(dataflow_name, operator_dag)

        return decorator

    def _register_dataflow(self, name: str, operator_dag):
        """
        Internal: Register a dataflow with the JobManager.

        Args:
            name: Dataflow name
            operator_dag: Operator DAG (from Stream API)

        Returns:
            Dataflow instance
        """
        # Create dataflow object
        dataflow = Dataflow(
            name=name,
            operator_dag=operator_dag,
            app=self
        )

        # Store for later deployment
        if not hasattr(self, '_dataflows'):
            self._dataflows = {}
        self._dataflows[name] = dataflow

        logger.info(f"Registered dataflow: {name}")
        return dataflow

    def arrow_agent(self, topic: Union[str, TopicT], **kwargs) -> AgentT:
        """Create a durable Arrow-native agent with batch processing."""
        def decorator(func: AgentFun) -> AgentT:
            topic_name = topic if isinstance(topic, str) else topic.name
            agent_name = kwargs.get('name', f"arrow_{func.__name__}_{topic_name}")
            concurrency = kwargs.get('concurrency', 1)

            # Register with durable agent manager
            agent = self._agent_manager.register_agent(
                name=agent_name,
                func=func,
                topic_name=topic_name,
                concurrency=concurrency,
                max_concurrency=kwargs.get('max_concurrency', 10),
                enabled=kwargs.get('enabled', True)
            )

            self._agents[agent_name] = agent
            logger.info(f"Registered durable Arrow agent: {agent_name}")
            return agent

        return decorator

    def pipeline(self, name: str = None) -> 'ArrowPipeline':
        """Create an interactive Arrow pipeline."""
        if self._interactive_app:
            return self._interactive_app.pipeline(name)
        else:
            # Fallback to basic pipeline
            return cython.arrow_core.create_pipeline(name or "pipeline")

    async def start(self):
        """Start the application with real stream processing."""
        logger.info(f"Starting Sabot app '{self.id}' with real stream processing")

        # Initialize real stream processing engine
        self._stream_engine = StreamEngine(config=self._stream_config)

        # Register all existing streams with the engine
        for stream_id, stream in self._topics.items():
            await self._stream_engine.register_stream(f"topic_{stream_id}", stream)

        for stream_id, stream in self._agents.items():
            # Agents can also act as streams
            await self._stream_engine.register_stream(f"agent_{stream_id}", stream)

        # Start durable agent manager (DBOS-inspired)
        await self._agent_manager.start()

        # Start flight server if enabled
        if self._flight_app:
            self._flight_app.start_flight_server()

        logger.info(f"Sabot app '{self.id}' started successfully with real stream processing")

    async def stop(self):
        """Stop the application and real stream processing."""
        logger.info(f"Stopping Sabot app '{self.id}' and stream processing")

        # Stop real stream processing engine
        if self._stream_engine:
            await self._stream_engine.shutdown()
            self._stream_engine = None

        # Stop durable agent manager
        await self._agent_manager.stop()

        # Stop flight server
        if self._flight_app:
            self._flight_app.stop_flight_server()

        logger.info(f"Sabot app '{self.id}' stopped successfully")

    async def run(self):
        """Run the application (main entry point for CLI)."""
        logger.info(f"Running Sabot app '{self.id}'")

        try:
            # Start the application
            await self.start()

            # Keep running until interrupted
            while True:
                await asyncio.sleep(1)

        except KeyboardInterrupt:
            logger.info(f"Received interrupt signal")
        finally:
            # Clean shutdown
            await self.stop()
            logger.info(f"Sabot app '{self.id}' shutdown complete")

    def interactive_shell(self):
        """Start interactive shell for pipeline development."""
        if self._interactive_app:
            self._interactive_app.interactive_shell()
        else:
            print("Interactive shell not available")

    def get_stats(self) -> dict:
        """Get comprehensive application statistics including real stream processing."""
        stats = {
            'app_id': self.id,
            'broker': self.broker,
            'topics': len(self._topics),
            'tables': len(self._tables),
            'agents': len(self._agents),
        }

        # Add real stream processing engine stats
        if self._stream_engine:
            stream_stats = self._stream_engine.get_all_stats()
            engine_health = asyncio.run(self._stream_engine.health_check())

            stats['stream_engine'] = {
                'active_streams': len(stream_stats),
                'total_messages_processed': sum(s.messages_processed for s in stream_stats.values()),
                'total_errors': sum(s.errors for s in stream_stats.values()),
                'health': engine_health
            }

            # Add per-stream stats
            stats['streams'] = {}
            for stream_id, stream_stat in stream_stats.items():
                stats['streams'][stream_id] = {
                    'messages_processed': stream_stat.messages_processed,
                    'throughput_msgs_per_sec': stream_stat.throughput_msgs_per_sec,
                    'errors': stream_stat.errors,
                    'batches_processed': stream_stat.batches_processed
                }

        # Add durable agent manager stats (DBOS-inspired)
        agent_manager_stats = self._agent_manager.get_manager_stats()
        stats['agent_manager'] = agent_manager_stats

        # Add workflow stats
        workflow_stats = {
            'total_workflows': len(self._agent_manager.list_workflows()),
            'running_workflows': len(self._agent_manager.list_workflows(state='running')),
            'failed_workflows': len(self._agent_manager.list_workflows(state='failed')),
        }
        stats['workflows'] = workflow_stats

        # Add flight stats
        if self._flight_app:
            stats['flight'] = self._flight_app.get_flight_stats()

        # Add interactive stats
        if self._interactive_app:
            stats['interactive'] = self._interactive_app.get_interactive_stats()

        return stats

    # ============================================================================
    # REAL STREAM PROCESSING METHODS
    # ============================================================================

    async def start_stream_processing(self, stream_id: str, processors: List[Callable] = None):
        """
        Start processing a stream with real stream engine.

        Args:
            stream_id: Stream identifier to start processing
            processors: Optional list of processing functions to apply
        """
        if not self._stream_engine:
            raise RuntimeError("Stream engine not initialized. Call start() first.")

        await self._stream_engine.start_stream_processing(stream_id, processors or [])
        logger.info(f"Started stream processing for: {stream_id}")

    async def stop_stream_processing(self, stream_id: str):
        """
        Stop processing a stream.

        Args:
            stream_id: Stream identifier to stop processing
        """
        if not self._stream_engine:
            raise RuntimeError("Stream engine not initialized.")

        await self._stream_engine.stop_stream_processing(stream_id)
        logger.info(f"Stopped stream processing for: {stream_id}")

    def get_stream_engine(self):
        """Get access to the real stream processing engine."""
        return self._stream_engine

    def get_stream_stats(self, stream_id: str = None):
        """
        Get stream processing statistics.

        Args:
            stream_id: Specific stream ID, or None for all streams

        Returns:
            Stream processing statistics
        """
        if not self._stream_engine:
            return {}

        if stream_id:
            stats = self._stream_engine.get_stream_stats(stream_id)
            return stats.__dict__ if stats else {}
        else:
            all_stats = self._stream_engine.get_all_stats()
            return {k: v.__dict__ for k, v in all_stats.items()}

    async def register_stream_with_engine(self, stream_id: str, stream):
        """
        Register a stream with the real stream processing engine.

        Args:
            stream_id: Unique identifier for the stream
            stream: Stream object to register
        """
        if not self._stream_engine:
            raise RuntimeError("Stream engine not initialized. Call start() first.")

        await self._stream_engine.register_stream(stream_id, stream)
        logger.info(f"Registered stream with engine: {stream_id}")

    async def unregister_stream_from_engine(self, stream_id: str):
        """
        Unregister a stream from the real stream processing engine.

        Args:
            stream_id: Stream identifier to remove
        """
        if not self._stream_engine:
            raise RuntimeError("Stream engine not initialized.")

        await self._stream_engine.unregister_stream(stream_id)
        logger.info(f"Unregistered stream from engine: {stream_id}")

    # DBOS-inspired workflow management methods
    async def enqueue_workflow(
        self,
        agent_name: str,
        workflow_id: str,
        payload: Dict[str, Any],
        priority: 'AgentPriority' = None
    ) -> str:
        """Enqueue a workflow for execution (DBOS queue concept)."""
        if priority is None:
            # Import here to avoid circular imports
            from .agent_manager import AgentPriority
            priority = AgentPriority.NORMAL

        return await self._agent_manager.enqueue_agent_task(
            agent_name, workflow_id, payload, priority
        )

    def get_workflow_status(self, workflow_id: str) -> Optional[Dict[str, Any]]:
        """Get workflow execution status."""
        return self._agent_manager.get_workflow_status(workflow_id)

    def list_workflows(self, agent_name: Optional[str] = None, state: Optional[str] = None) -> List[Dict[str, Any]]:
        """List workflows with optional filtering."""
        return self._agent_manager.list_workflows(agent_name, state)

    async def restart_workflow(self, workflow_id: str) -> bool:
        """Restart a failed workflow."""
        status = self.get_workflow_status(workflow_id)
        if not status or status['state'] != 'failed':
            return False

        # Re-enqueue the workflow
        agent_name = status['agent_name']
        await self._agent_manager.enqueue_agent_task(
            agent_name,
            workflow_id,
            {
                'args': [],  # Would need to reconstruct from original execution
                'kwargs': {},
                'stream_data': []
            }
        )
        return True

    async def _create_kafka_channel(
        self,
        topic: str,
        broker: str,
        key_type: Optional[Type] = None,
        value_type: Optional[Type] = None,
        **options
    ):
        """
        Create and initialize a Kafka channel.

        Args:
            topic: Kafka topic name
            broker: Kafka broker URL
            key_type: Type for keys
            value_type: Type for values
            **options: Additional Kafka options

        Returns:
            Initialized KafkaChannel
        """
        from .kafka.source import KafkaSource
        from .kafka.sink import KafkaSink

        # Create source and sink
        source = KafkaSource(
            topic=topic,
            broker=broker,
            **options
        )

        sink = KafkaSink(
            topic=topic,
            broker=broker,
            **options
        )

        # Initialize connection
        await source.start()
        await sink.start()

        # Create channel wrapper
        from .channels_kafka import KafkaChannel
        channel = KafkaChannel(
            name=topic,
            source=source,
            sink=sink,
            key_type=key_type,
            value_type=value_type
        )

        return channel

    async def _create_redis_channel(
        self,
        name: str,
        redis_url: str,
        key_type: Optional[Type] = None,
        value_type: Optional[Type] = None,
        **options
    ):
        """
        Create and initialize a Redis channel.

        Args:
            name: Redis channel name
            redis_url: Redis connection URL
            key_type: Type for keys
            value_type: Type for values
            **options: Additional Redis options

        Returns:
            Initialized RedisChannel
        """
        from .channels_redis import RedisChannel

        # Get maxsize from options
        maxsize = options.get('maxsize', 1000)

        # Create Redis channel
        channel = RedisChannel(
            app=self,
            channel_name=name,
            maxsize=maxsize,
            key_type=key_type,
            value_type=value_type,
            **options
        )

        # Redis channels don't require explicit start() method
        # The connection is lazy-initialized on first use

        return channel

    async def _create_flight_channel(
        self,
        name: str,
        flight_url: str,
        key_type: Optional[Type] = None,
        value_type: Optional[Type] = None,
        **options
    ):
        """
        Create and initialize an Arrow Flight channel.

        Args:
            name: Flight channel name (used as path)
            flight_url: Flight server URL
            key_type: Type for keys
            value_type: Type for values
            **options: Additional Flight options

        Returns:
            Initialized FlightChannel
        """
        from .channels_flight import FlightChannel

        # Get maxsize from options
        maxsize = options.get('maxsize', 1000)

        # Get or construct path
        path = options.get('path', f"/sabot/channels/{name}")

        # Create Flight channel
        channel = FlightChannel(
            app=self,
            location=flight_url,
            path=path,
            maxsize=maxsize,
            key_type=key_type,
            value_type=value_type,
            **options
        )

        # Flight channels don't require explicit start() method
        # The connection is established on first use

        return channel


@dataclass
class Dataflow:
    """
    A dataflow is a user-defined operator DAG.

    Dataflows are compiled to physical execution graphs
    and deployed to agent worker nodes by the JobManager.
    """
    name: str
    operator_dag: Any  # Stream API DAG
    app: App
    job_id: Optional[str] = None

    async def start(self, parallelism: int = 4):
        """
        Start the dataflow by submitting to JobManager.

        Args:
            parallelism: Default parallelism for operators
        """
        # Import JobManager here to avoid circular imports
        from .job_manager import JobManager

        # Create JobManager (local mode by default)
        job_manager = JobManager()

        # Serialize operator DAG to JSON
        job_graph_json = self._serialize_operator_dag()

        # Submit to JobManager
        self.job_id = await job_manager.submit_job(
            job_graph_json,
            job_name=self.name
        )

        logger.info(f"Started dataflow {self.name} as job {self.job_id}")

    async def stop(self):
        """Stop the dataflow"""
        if not self.job_id:
            logger.warning(f"Dataflow {self.name} not started")
            return

        # Cancel job via JobManager
        from .job_manager import JobManager
        job_manager = JobManager()
        await job_manager.cancel_job(self.job_id)

        logger.info(f"Stopped dataflow {self.name}")

    def _serialize_operator_dag(self) -> str:
        """
        Serialize the operator DAG to JSON for JobManager.

        This converts the Stream API DAG to a JobGraph format.
        """
        # For now, create a simple job graph
        # Real implementation would traverse the Stream DAG
        import json
        from datetime import datetime

        # Create a basic job graph structure
        job_graph = {
            "name": self.name,
            "created_at": datetime.now().isoformat(),
            "operators": [
                {
                    "id": "source_1",
                    "type": "source",
                    "config": {
                        "topic": "input_topic"  # Placeholder
                    }
                },
                {
                    "id": "map_1",
                    "type": "map",
                    "config": {},
                    "inputs": ["source_1"]
                },
                {
                    "id": "sink_1",
                    "type": "sink",
                    "config": {
                        "topic": "output_topic"  # Placeholder
                    },
                    "inputs": ["map_1"]
                }
            ]
        }

        return json.dumps(job_graph)


class Topic(TopicT):
    """Sabot topic with Arrow serialization support."""

    def __init__(self, name: str, app: AppT, **kwargs):
        self.name = name
        self.app = app
        self.partitions = kwargs.get('partitions', 1)
        self.replication_factor = kwargs.get('replication_factor', 1)
        self.value_serializer = kwargs.get('value_serializer', 'arrow')
        self.key_serializer = kwargs.get('key_serializer', 'raw')

    async def send(self, value: Any, key: Any = None, **kwargs) -> None:
        """Send message to topic with Arrow serialization."""
        # This would integrate with Kafka producer
        # For now, just log
        logger.debug(f"Would send {value} with key {key} to topic {self.name}")


        class Table(TableT):
            """Sabot table with pluggable storage backends."""

            def __init__(self, name: str, app: AppT, backend: str = "arrow_files", use_cython: bool = True, **kwargs):
                self.name = name
                self.app = app
                self.default = kwargs.get('default', {})

                # Initialize pluggable backend with Cython support
                try:
                    self._backend = create_backend_auto(backend, use_cython=use_cython, **kwargs)
                    backend_type = "Cython" if use_cython and hasattr(self._backend, '__class__') and 'Fast' in self._backend.__class__.__name__ else "Python"
                    logger.info(f"Table '{name}' using {backend_type} backend: {backend}")
                except Exception as e:
                    logger.warning(f"Failed to initialize backend '{backend}' for table '{name}': {e}")
                    # Fallback to memory backend
                    self._backend = create_backend_auto("memory://", use_cython=False)
                    logger.info(f"Table '{name}' fell back to Python memory backend")

            async def __aenter__(self):
                """Async context manager entry - initialize backend."""
                await self._backend.start()
                return self

            async def __aexit__(self, exc_type, exc_val, exc_tb):
                """Async context manager exit - cleanup backend."""
                await self._backend.stop()

            def __getitem__(self, key: Any) -> Any:
                """Get item from table."""
                # For synchronous access, we need to run async operation
                # In a real implementation, this would be async
                import asyncio
                try:
                    loop = asyncio.get_event_loop()
                    if loop.is_running():
                        # We're in an async context, this is tricky
                        # For now, assume backend has sync methods or cache
                        return self._sync_get(key)
                    else:
                        # We can create a new event loop
                        return loop.run_until_complete(self._backend.get(key)) or self.default
                except RuntimeError:
                    # No event loop, fallback
                    return self._sync_get(key)

            def _sync_get(self, key: Any) -> Any:
                """Synchronous get for non-async contexts."""
                # This is a placeholder - real implementation would need sync backend methods
                # or a sync wrapper around async backends
                return self.default

            def __setitem__(self, key: Any, value: Any) -> None:
                """Set item in table."""
                # Similar async/sync issue as __getitem__
                import asyncio
                try:
                    loop = asyncio.get_event_loop()
                    if loop.is_running():
                        # Schedule as background task
                        asyncio.create_task(self._backend.set(key, value))
                    else:
                        loop.run_until_complete(self._backend.set(key, value))
                except RuntimeError:
                    # No event loop, this is problematic
                    pass

            async def aget(self, key: Any) -> Any:
                """Async get item from table."""
                return await self._backend.get(key) or self.default

            async def aset(self, key: Any, value: Any) -> None:
                """Async set item in table."""
                await self._backend.set(key, value)

            async def adelete(self, key: Any) -> bool:
                """Async delete item from table."""
                return await self._backend.delete(key)

            async def akeys(self) -> List[Any]:
                """Async get all keys in the table."""
                return await self._backend.keys()

            async def avalues(self) -> List[Any]:
                """Async get all values in the table."""
                return await self._backend.values()

            async def aitems(self) -> List[tuple]:
                """Async get all key-value pairs in the table."""
                return await self._backend.items()

            async def aclear(self) -> None:
                """Async clear all data from the table."""
                await self._backend.clear()

            async def asize(self) -> int:
                """Async get the number of items in the table."""
                return await self._backend.size()

            # Legacy sync methods for backward compatibility
            def get(self, key: Any, default: Any = None) -> Any:
                """Get item with default (legacy sync method)."""
                try:
                    return self[key]
                except KeyError:
                    return default or self.default

            def keys(self) -> List[Any]:
                """Get all keys (legacy sync method)."""
                # This is problematic without async context
                # In real implementation, we'd need sync backends or different interface
                return []

            def values(self) -> List[Any]:
                """Get all values (legacy sync method)."""
                return []

            def items(self) -> List[tuple]:
                """Get all items (legacy sync method)."""
                return []

            def clear(self) -> None:
                """Clear all data (legacy sync method)."""
                pass

            def size(self) -> int:
                """Get size (legacy sync method)."""
                return 0


class RAFTStream:
    """GPU-accelerated stream processing with RAFT ML algorithms."""

    def __init__(self, name: str, app: AppT, **kwargs):
        self.name = name
        self.app = app
        self._gpu_enabled = app.enable_gpu and app._gpu_resources is not None

        if self._gpu_enabled:
            logger.info(f"RAFT Stream '{name}' initialized with GPU acceleration")
        else:
            logger.info(f"RAFT Stream '{name}' initialized (CPU mode)")

    def kmeans_cluster(self, n_clusters: int = 8, max_iter: int = 300):
        """Create a GPU-accelerated K-means clustering processor."""
        if not self._gpu_enabled:
            raise RuntimeError("GPU acceleration required for RAFT operations")

        def cluster_processor(batch):
            if isinstance(batch, dict) and 'data' in batch:
                data = batch['data']
            else:
                data = batch

            # Convert to cuDF if needed
            if hasattr(data, 'to_pandas'):
                # Arrow table
                df = cudf.DataFrame(data.to_pandas())
            elif hasattr(data, 'values'):
                # NumPy array
                df = cudf.DataFrame(data.values)
            else:
                df = cudf.DataFrame(data)

            # Perform K-means clustering
            kmeans = RAFTKMeans(
                n_clusters=n_clusters,
                max_iter=max_iter,
                device_resources=self.app._gpu_resources
            )

            # Fit and predict
            labels = kmeans.fit_predict(df.values)

            # Add cluster labels to result
            result = df.copy()
            result['cluster_id'] = labels

            return result.to_pandas()

        return cluster_processor

    def nearest_neighbors(self, k: int = 5):
        """Create a GPU-accelerated k-nearest neighbors processor."""
        if not self._gpu_enabled:
            raise RuntimeError("GPU acceleration required for RAFT operations")

        def nn_processor(batch):
            if isinstance(batch, dict) and 'data' in batch:
                data = batch['data']
                queries = batch.get('queries', data)  # Use same data as queries if not specified
            else:
                data = batch
                queries = batch

            # Convert to cuDF
            if hasattr(data, 'to_pandas'):
                dataset = cudf.DataFrame(data.to_pandas())
            elif hasattr(data, 'values'):
                dataset = cudf.DataFrame(data.values)
            else:
                dataset = cudf.DataFrame(data)

            # Convert query points
            if hasattr(queries, 'to_pandas'):
                query_df = cudf.DataFrame(queries.to_pandas())
            elif hasattr(queries, 'values'):
                query_df = cudf.DataFrame(queries.values)
            else:
                query_df = cudf.DataFrame(queries)

            # Perform nearest neighbors search
            nn = RAFTNearestNeighbors(
                n_neighbors=k,
                device_resources=self.app._gpu_resources
            )

            nn.fit(dataset.values)
            distances, indices = nn.kneighbors(query_df.values)

            # Return results
            return {
                'distances': distances.get() if hasattr(distances, 'get') else distances,
                'indices': indices.get() if hasattr(indices, 'get') else indices,
                'query_points': query_df.to_pandas() if hasattr(query_df, 'to_pandas') else query_df
            }

        return nn_processor

    def gpu_transform(self, transform_func):
        """Create a custom GPU-accelerated transformation processor."""
        if not self._gpu_enabled:
            raise RuntimeError("GPU acceleration required for RAFT operations")

        def gpu_processor(batch):
            # Move data to GPU
            if hasattr(batch, 'to_pandas'):
                gpu_data = cudf.DataFrame(batch.to_pandas())
            elif hasattr(batch, 'values'):
                gpu_data = cudf.DataFrame(batch.values)
            else:
                gpu_data = cudf.DataFrame(batch)

            # Apply transformation
            result = transform_func(gpu_data)

            # Convert back to pandas for compatibility
            if hasattr(result, 'to_pandas'):
                return result.to_pandas()
            else:
                return result

        return gpu_processor


class Stream(StreamT):
    """Sabot stream with Arrow batch processing."""

    def __init__(self, topic: TopicT, **kwargs):
        self.topic = topic
        self.batch_size = kwargs.get('batch_size', 1000)
        self.timeout = kwargs.get('timeout', 1.0)

    def __aiter__(self) -> AsyncGenerator[Any, None]:
        """Async iterator for stream batches."""
        return self._stream_generator()

    async def _stream_generator(self) -> AsyncGenerator[Any, None]:
        """Generate Arrow batches from stream."""
        # This would connect to Kafka and yield Arrow batches
        # For now, yield empty batches
        while True:
            # Simulate getting batches
            await asyncio.sleep(self.timeout)
            yield []  # Empty batch for now


class ArrowAgent(AgentT):
    """Arrow-native agent with both per-record and per-batch processing."""

    def __init__(self, name: str, func: AgentFun, app: AppT, topic_name: str, **kwargs):
        self.name = name
        self.func = func
        self.app = app
        self.topic_name = topic_name
        self.concurrency = kwargs.get('concurrency', 1)
        self.supervisor_strategy = kwargs.get('supervisor_strategy', 'crashing')
        self._sinks = []

    def add_sink(self, sink: SinkT) -> None:
        """Add a sink for agent output."""
        self._sinks.append(sink)

    async def __aenter__(self) -> AgentT:
        """Start agent context."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Stop agent context."""
        pass


class ArrowPipeline:
    """Arrow pipeline wrapper for easy use."""

    def __init__(self, cython_pipeline):
        self._pipeline = cython_pipeline

    def filter(self, condition):
        """Add filter operation."""
        self._pipeline.filter(condition)
        return self

    def select(self, columns: list):
        """Add select operation."""
        self._pipeline.select(columns)
        return self

    def group_by(self, keys: list):
        """Add group_by operation."""
        self._pipeline.group_by(keys)
        return self

    def agg(self, aggregations: dict):
        """Add aggregation operation."""
        self._pipeline.agg(aggregations)
        return self

    def join(self, other, on: list = None, how: str = "left"):
        """Add join operation."""
        self._pipeline.join(other, on, how)
        return self

    def sort(self, by: list, ascending: bool = True):
        """Add sort operation."""
        self._pipeline.sort(by, ascending)
        return self

    def limit(self, n: int):
        """Add limit operation."""
        self._pipeline.limit(n)
        return self

    def execute(self, data=None):
        """Execute the pipeline."""
        return self._pipeline.execute(data)

    async def execute_async(self, data=None):
        """Execute pipeline asynchronously."""
        return await self._pipeline.execute_async(data)

    def show(self) -> str:
        """Show pipeline structure."""
        return self._pipeline.show_pipeline()

    def get_stats(self) -> dict:
        """Get pipeline statistics."""
        return self._pipeline.get_pipeline_stats()
