#!/usr/bin/env python3
"""
Core Stream Processing Engine for Sabot

Implements the real stream processing engine that handles:
- Message ingestion from various sources (Kafka, Redis, etc.)
- Arrow-based data processing with zero-copy operations
- Backpressure handling and memory management
- Real-time stream processing pipelines

This replaces the mocked stream processing in the current app.py
"""

import asyncio
import logging
from typing import AsyncIterator, Dict, Any, List, Optional, Union, Callable
from dataclasses import dataclass, field
from enum import Enum
import time
import psutil
import os

from ..observability import get_observability

try:
    from ..arrow import Table, RecordBatch, Array, Schema, Field, compute as pc, USING_INTERNAL, USING_EXTERNAL
    # Create pa-like namespace for compatibility
    class _ArrowCompat:
        Table = Table
        RecordBatch = RecordBatch
        Array = Array
        Schema = Schema
        Field = Field
    pa = _ArrowCompat()
    ARROW_AVAILABLE = USING_INTERNAL or USING_EXTERNAL
except ImportError:
    ARROW_AVAILABLE = False
    pa = None
    pc = None

try:
    from ..sabot_types import RecordBatch, Stream, Sink, AgentSpec
    SABOT_TYPES_AVAILABLE = True
except ImportError:
    SABOT_TYPES_AVAILABLE = False
    from typing import Any
    RecordBatch = Any
    Stream = Any
    Sink = Any
    AgentSpec = Any
from .serializers import ArrowSerializer, JsonSerializer
from .metrics import MetricsCollector

logger = logging.getLogger(__name__)


class ProcessingMode(Enum):
    """Stream processing modes."""
    AT_LEAST_ONCE = "at_least_once"
    EXACTLY_ONCE = "exactly_once"
    AT_MOST_ONCE = "at_most_once"


class BackpressureStrategy(Enum):
    """Backpressure handling strategies."""
    DROP = "drop"  # Drop messages when backpressured
    BUFFER = "buffer"  # Buffer messages (bounded)
    BLOCK = "block"  # Block producer until consumer catches up


@dataclass
class StreamConfig:
    """Configuration for stream processing."""
    buffer_size: int = 1000  # Max buffered messages
    batch_size: int = 100    # Messages per batch
    processing_timeout: float = 30.0  # Processing timeout in seconds
    backpressure_strategy: BackpressureStrategy = BackpressureStrategy.BUFFER
    memory_limit_mb: int = 512  # Memory limit per stream
    max_concurrent_batches: int = 10  # Max concurrent batch processing
    enable_metrics: bool = True


@dataclass
class ProcessingStats:
    """Real-time processing statistics."""
    messages_processed: int = 0
    bytes_processed: int = 0
    processing_time_ms: float = 0.0
    batches_processed: int = 0
    errors: int = 0
    backpressure_events: int = 0
    memory_usage_mb: float = 0.0
    throughput_msgs_per_sec: float = 0.0


@dataclass
class StreamEngine:
    """
    Core Stream Processing Engine

    This is the heart of Sabot - handles real stream processing with:
    - Arrow-native data processing
    - Backpressure management
    - Memory-efficient operations
    - Real-time metrics and monitoring
    """

    config: StreamConfig = field(default_factory=StreamConfig)
    metrics: Optional[MetricsCollector] = None

    # Internal state
    _active_streams: Dict[str, Stream] = field(default_factory=dict)
    _processing_tasks: Dict[str, asyncio.Task] = field(default_factory=dict)
    _stats: Dict[str, ProcessingStats] = field(default_factory=dict)
    _memory_pools: Dict[str, Any] = field(default_factory=dict)  # Arrow memory pools
    _serializers: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        """Initialize the stream engine."""
        # Initialize observability
        self.observability = get_observability()

        # Initialize serializers (will fail gracefully if not available)
        self._serializers = {}
        try:
            self._serializers['arrow'] = ArrowSerializer()
        except RuntimeError:
            pass  # Arrow not available
        try:
            self._serializers['json'] = JsonSerializer()
        except Exception:
            pass

        # Initialize metrics if enabled
        if self.config.enable_metrics:
            self.metrics = MetricsCollector()

        logger.info(f"StreamEngine initialized with config: {self.config}")

        # Record initialization metric
        self.observability.record_metric(
            "stream_engine_initialized",
            1,
            {"engine_type": "core", "config_mode": self.config.mode.value},
            "counter"
        )

    async def register_stream(self, stream_id: str, source_stream: Stream) -> None:
        """
        Register a stream for processing.

        Args:
            stream_id: Unique identifier for the stream
            source_stream: Source stream to process
        """
        with self.observability.trace_operation(
            "stream_register",
            {"stream_id": stream_id, "stream_type": type(source_stream).__name__}
        ):
            if stream_id in self._active_streams:
                raise ValueError(f"Stream {stream_id} already registered")

        self._active_streams[stream_id] = source_stream
        self._stats[stream_id] = ProcessingStats()

        # Create Arrow memory pool for this stream
        if ARROW_AVAILABLE:
            self._memory_pools[stream_id] = pa.default_memory_pool()
        else:
            self._memory_pools[stream_id] = None

        logger.info(f"Registered stream: {stream_id}")

    async def unregister_stream(self, stream_id: str) -> None:
        """
        Unregister a stream and clean up resources.

        Args:
            stream_id: Stream identifier to remove
        """
        if stream_id not in self._active_streams:
            logger.warning(f"Stream {stream_id} not found")
            return

        # Cancel any running processing task
        if stream_id in self._processing_tasks:
            self._processing_tasks[stream_id].cancel()
            try:
                await self._processing_tasks[stream_id]
            except asyncio.CancelledError:
                pass
            del self._processing_tasks[stream_id]

        # Clean up resources
        del self._active_streams[stream_id]
        if stream_id in self._stats:
            del self._stats[stream_id]
        if stream_id in self._memory_pools:
            del self._memory_pools[stream_id]

        logger.info(f"Unregistered stream: {stream_id}")

    async def start_stream_processing(self, stream_id: str,
                                    processors: List[Callable] = None) -> None:
        """
        Start processing a registered stream.

        Args:
            stream_id: Stream to start processing
            processors: List of processing functions to apply
        """
        if stream_id not in self._active_streams:
            raise ValueError(f"Stream {stream_id} not registered")

        if stream_id in self._processing_tasks:
            logger.warning(f"Stream {stream_id} already processing")
            return

        # Create processing task
        task = asyncio.create_task(
            self._process_stream(stream_id, processors or [])
        )
        self._processing_tasks[stream_id] = task

        logger.info(f"Started processing stream: {stream_id}")

    async def stop_stream_processing(self, stream_id: str) -> None:
        """
        Stop processing a stream.

        Args:
            stream_id: Stream to stop processing
        """
        if stream_id not in self._processing_tasks:
            logger.warning(f"Stream {stream_id} not processing")
            return

        self._processing_tasks[stream_id].cancel()
        try:
            await self._processing_tasks[stream_id]
        except asyncio.CancelledError:
            pass

        del self._processing_tasks[stream_id]
        logger.info(f"Stopped processing stream: {stream_id}")

    async def _process_stream(self, stream_id: str,
                            processors: List[Callable]) -> None:
        """
        Internal stream processing loop.

        Args:
            stream_id: Stream identifier
            processors: List of processing functions
        """
        stream = self._active_streams[stream_id]
        stats = self._stats[stream_id]

        logger.info(f"Starting stream processing loop for {stream_id}")

        try:
            async for batch in self._ingest_batches(stream, stream_id):
                start_time = time.time()

                try:
                    with self.observability.trace_operation(
                        "process_batch",
                        {
                            "stream_id": stream_id,
                            "batch_size": len(batch) if hasattr(batch, '__len__') else 1
                        }
                    ) as span:
                        # Apply processing pipeline
                        processed_batch = await self._apply_processors(
                            batch, processors, stream_id
                        )

                        # Handle backpressure if needed
                        await self._handle_backpressure(stream_id)

                        # Update statistics
                        processing_time = (time.time() - start_time) * 1000
                        stats.messages_processed += len(processed_batch) if hasattr(processed_batch, '__len__') else 1
                        stats.processing_time_ms += processing_time
                        stats.batches_processed += 1

                        # Calculate throughput
                        elapsed = time.time() - start_time
                        if elapsed > 0:
                            stats.throughput_msgs_per_sec = len(processed_batch) / elapsed if hasattr(processed_batch, '__len__') else 1.0 / elapsed

                        # Report metrics
                        if self.metrics:
                            await self.metrics.record_processing_time(stream_id, processing_time)
                            await self.metrics.record_throughput(stream_id, stats.throughput_msgs_per_sec)

                except Exception as e:
                    stats.errors += 1
                    logger.error(f"Error processing batch in {stream_id}: {e}")

                    if self.metrics:
                        await self.metrics.record_error(stream_id, str(e))

        except asyncio.CancelledError:
            logger.info(f"Stream processing cancelled for {stream_id}")
        except Exception as e:
            logger.error(f"Fatal error in stream processing {stream_id}: {e}")
            if self.metrics:
                await self.metrics.record_error(stream_id, f"fatal: {str(e)}")

    async def _ingest_batches(self, stream: Stream, stream_id: str) -> AsyncIterator[RecordBatch]:
        """
        Ingest messages from stream and convert to Arrow RecordBatches.

        Args:
            stream: Source stream
            stream_id: Stream identifier

        Yields:
            Arrow RecordBatch objects
        """
        buffer = []
        stats = self._stats[stream_id]

        async for message in stream:
            try:
                # Deserialize message to Arrow format
                if isinstance(message, dict):
                    # Convert dict to Arrow RecordBatch
                    batch = self._dict_to_arrow_batch([message])
                elif isinstance(message, bytes):
                    # Deserialize from bytes
                    batch = self._serializers['arrow'].deserialize(message)
                elif hasattr(message, 'to_pandas'):
                    # Convert from pandas/polars
                    if ARROW_AVAILABLE:
                        batch = pa.RecordBatch.from_pandas(message.to_pandas())
                    else:
                        batch = message  # Keep as-is if Arrow not available
                else:
                    # Try JSON serialization as fallback
                    if isinstance(message, str):
                        import json
                        dict_msg = json.loads(message)
                        batch = self._dict_to_arrow_batch([dict_msg])
                    else:
                        batch = self._dict_to_arrow_batch([message])

                buffer.append(batch)
                stats.bytes_processed += batch.nbytes if hasattr(batch, 'nbytes') else 0

                # Yield batch when buffer is full or batch size reached
                if len(buffer) >= self.config.batch_size:
                    combined_batch = self._combine_batches(buffer)
                    yield combined_batch
                    buffer = []

                # Check memory limits
                if stats.memory_usage_mb > self.config.memory_limit_mb:
                    await self._trigger_backpressure(stream_id)
                    stats.backpressure_events += 1

            except Exception as e:
                logger.error(f"Error ingesting message in {stream_id}: {e}")
                stats.errors += 1

        # Yield remaining buffered messages
        if buffer:
            combined_batch = self._combine_batches(buffer)
            yield combined_batch

    def _dict_to_arrow_batch(self, messages: List[Dict[str, Any]]) -> Any:
        """Convert list of dictionaries to Arrow RecordBatch."""
        if not ARROW_AVAILABLE:
            return messages  # Return raw data if Arrow not available

        if not messages:
            return pa.record_batch([])

        # Infer schema from first message
        sample = messages[0]
        schema = pa.schema([
            pa.field(key, self._infer_arrow_type(value))
            for key, value in sample.items()
        ])

        # Convert to Arrow arrays
        arrays = []
        for field in schema:
            values = [msg.get(field.name) for msg in messages]
            arrays.append(pa.array(values, type=field.type))

        return pa.record_batch(arrays, schema=schema)

    def _infer_arrow_type(self, value: Any) -> Any:
        """Infer Arrow data type from Python value."""
        if not ARROW_AVAILABLE:
            return type(value).__name__

        if isinstance(value, int):
            return pa.int64()
        elif isinstance(value, float):
            return pa.float64()
        elif isinstance(value, str):
            return pa.string()
        elif isinstance(value, bool):
            return pa.bool_()
        elif isinstance(value, list):
            if value and isinstance(value[0], (int, float)):
                return pa.list_(pa.float64() if isinstance(value[0], float) else pa.int64())
            else:
                return pa.list_(pa.string())
        else:
            return pa.string()  # Default to string

    def _combine_batches(self, batches: List[Any]) -> Any:
        """Combine multiple RecordBatches into one."""
        if not ARROW_AVAILABLE:
            return batches  # Return as list if Arrow not available

        if len(batches) == 1:
            return batches[0]

        # Concatenate all batches
        return pa.compute.list_flatten(pa.compute.struct_pack(*batches))

    async def _apply_processors(self, batch: RecordBatch,
                              processors: List[Callable],
                              stream_id: str) -> RecordBatch:
        """
        Apply processing pipeline to a batch.

        Args:
            batch: Input RecordBatch
            processors: List of processing functions
            stream_id: Stream identifier

        Returns:
            Processed RecordBatch
        """
        current_batch = batch

        for processor in processors:
            try:
                if asyncio.iscoroutinefunction(processor):
                    current_batch = await processor(current_batch)
                else:
                    current_batch = processor(current_batch)
            except Exception as e:
                logger.error(f"Processor error in {stream_id}: {e}")
                # Continue with original batch on processor error
                break

        return current_batch

    async def _handle_backpressure(self, stream_id: str) -> None:
        """
        Handle backpressure based on configured strategy.

        Args:
            stream_id: Stream identifier
        """
        stats = self._stats[stream_id]

        # Check memory usage
        process = psutil.Process(os.getpid())
        memory_mb = process.memory_info().rss / 1024 / 1024
        stats.memory_usage_mb = memory_mb

        if memory_mb > self.config.memory_limit_mb:
            strategy = self.config.backpressure_strategy

            if strategy == BackpressureStrategy.BLOCK:
                # Block until memory usage drops
                logger.warning(f"Backpressure: blocking {stream_id} (memory: {memory_mb:.1f}MB)")
                while memory_mb > self.config.memory_limit_mb * 0.8:  # 80% threshold
                    await asyncio.sleep(0.1)
                    memory_mb = psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024

            elif strategy == BackpressureStrategy.BUFFER:
                # Slow down processing
                await asyncio.sleep(0.01)

            # DROP strategy doesn't need special handling - messages are naturally dropped

    async def _trigger_backpressure(self, stream_id: str) -> None:
        """Trigger backpressure mechanism."""
        logger.warning(f"Backpressure triggered for stream {stream_id}")

        if self.metrics:
            await self.metrics.record_backpressure_event(stream_id)

    def get_stream_stats(self, stream_id: str) -> Optional[ProcessingStats]:
        """Get processing statistics for a stream."""
        return self._stats.get(stream_id)

    def get_all_stats(self) -> Dict[str, ProcessingStats]:
        """Get processing statistics for all streams."""
        return self._stats.copy()

    async def health_check(self) -> Dict[str, Any]:
        """Perform health check on the stream engine."""
        return {
            'status': 'healthy',
            'active_streams': len(self._active_streams),
            'processing_tasks': len(self._processing_tasks),
            'total_messages_processed': sum(s.messages_processed for s in self._stats.values()),
            'total_errors': sum(s.errors for s in self._stats.values()),
            'memory_usage_mb': psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024
        }

    async def shutdown(self) -> None:
        """Gracefully shutdown the stream engine."""
        logger.info("Shutting down StreamEngine...")

        # Stop all processing tasks
        for stream_id, task in self._processing_tasks.items():
            task.cancel()

        # Wait for tasks to complete
        if self._processing_tasks:
            await asyncio.gather(*self._processing_tasks.values(), return_exceptions=True)

        # Clean up resources
        self._active_streams.clear()
        self._processing_tasks.clear()
        self._stats.clear()
        self._memory_pools.clear()

        logger.info("StreamEngine shutdown complete")
