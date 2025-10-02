#!/usr/bin/env python3
"""
Unified State Store Manager for Sabot.

Provides a high-level interface for state management across multiple backends:
- Automatic backend selection and failover
- Unified API for all state operations
- Checkpointing and recovery
- Performance monitoring
- Multi-tenant isolation
"""

import asyncio
import time
from typing import Any, Dict, List, Optional, Union, Callable, AsyncIterator
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path

from .base import StoreBackend, StoreBackendConfig
from .memory import MemoryBackend
from .rocksdb import RocksDBBackend
from .redis import RedisBackend
from .tonbo import TonboBackend
from .kafka import KafkaBackend
from .checkpoint import CheckpointManager, CheckpointConfig


class BackendType(Enum):
    """Available backend types."""
    TONBO = "tonbo"        # Preferred: High-performance Arrow-based LSM store
    KAFKA = "kafka"        # Optional: Distributed messaging for bridging jobs/systems
    MEMORY = "memory"      # Fallback: In-memory for development
    ROCKSDB = "rocksdb"    # Alternative: Embedded key-value store
    REDIS = "redis"        # Alternative: Distributed cache


class StateIsolation(Enum):
    """State isolation levels."""
    SHARED = "shared"      # All tables share the same backend
    TABLE = "table"        # Each table has its own backend instance
    NAMESPACE = "namespace"  # Tables share backends but are namespaced


@dataclass
class StateStoreConfig:
    """Configuration for the state store manager."""
    backend_type: BackendType = BackendType.TONBO  # Tonbo is now the preferred backend
    isolation_level: StateIsolation = StateIsolation.SHARED

    # Backend-specific configs
    memory_config: StoreBackendConfig = field(default_factory=lambda: StoreBackendConfig(
        backend_type="memory"
    ))

    rocksdb_config: StoreBackendConfig = field(default_factory=lambda: StoreBackendConfig(
        backend_type="rocksdb",
        path=Path("./sabot_state/rocksdb")
    ))

    redis_config: StoreBackendConfig = field(default_factory=lambda: StoreBackendConfig(
        backend_type="redis",
        options={
            'host': 'localhost',
            'port': 6379,
            'namespace': 'sabot',
            'serializer': 'json'
        }
    ))

    tonbo_config: StoreBackendConfig = field(default_factory=lambda: StoreBackendConfig(
        backend_type="tonbo",
        path=Path("./sabot_state/tonbo")
    ))

    kafka_config: StoreBackendConfig = field(default_factory=lambda: StoreBackendConfig(
        backend_type="kafka",
        options={
            'bootstrap_servers': 'localhost:9092',
            'group_id': 'sabot-consumer',
            'auto_offset_reset': 'earliest',
            'enable_auto_commit': False,
            'topic_prefix': 'sabot.',
            'replication_factor': 1,
            'num_partitions': 1
        }
    ))

    # Checkpointing config
    checkpoint_config: CheckpointConfig = field(default_factory=lambda: CheckpointConfig(
        enabled=True,
        interval_seconds=300.0,
        max_checkpoints=10
    ))

    # Performance monitoring
    enable_metrics: bool = True
    enable_monitoring: bool = True


@dataclass
class StateMetrics:
    """Metrics for state operations."""
    operations_total: int = 0
    operations_by_type: Dict[str, int] = field(default_factory=dict)
    latency_by_type: Dict[str, float] = field(default_factory=dict)
    errors_total: int = 0
    errors_by_type: Dict[str, int] = field(default_factory=dict)
    cache_hit_ratio: float = 0.0
    memory_usage_bytes: int = 0
    last_operation_time: float = 0.0


class StateTable:
    """
    A state table that provides typed key-value storage.

    Tables are the primary interface for state management in Sabot.
    They provide type-safe operations and automatic serialization.
    """

    def __init__(self, name: str, backend: StoreBackend, enable_metrics: bool = True):
        self.name = name
        self.backend = backend
        self.enable_metrics = enable_metrics

        # Metrics
        self.metrics = StateMetrics() if enable_metrics else None

        # Operation tracking
        self._lock = asyncio.Lock()

    async def _record_operation(self, operation: str, start_time: float, success: bool = True) -> None:
        """Record operation metrics."""
        if not self.metrics:
            return

        duration = time.time() - start_time
        self.metrics.operations_total += 1
        self.metrics.operations_by_type[operation] = self.metrics.operations_by_type.get(operation, 0) + 1
        self.metrics.last_operation_time = time.time()

        if operation not in self.metrics.latency_by_type:
            self.metrics.latency_by_type[operation] = duration
        else:
            # Exponential moving average
            alpha = 0.1
            self.metrics.latency_by_type[operation] = (
                alpha * duration + (1 - alpha) * self.metrics.latency_by_type[operation]
            )

        if not success:
            self.metrics.errors_total += 1
            self.metrics.errors_by_type[operation] = self.metrics.errors_by_type.get(operation, 0) + 1

    async def get(self, key: Any) -> Optional[Any]:
        """Get a value from the table."""
        start_time = time.time()
        try:
            async with self._lock:
                value = await self.backend.get(key)
                await self._record_operation("get", start_time, True)
                return value
        except Exception as e:
            await self._record_operation("get", start_time, False)
            raise e

    async def set(self, key: Any, value: Any) -> None:
        """Set a value in the table."""
        start_time = time.time()
        try:
            async with self._lock:
                await self.backend.set(key, value)
                await self._record_operation("set", start_time, True)
        except Exception as e:
            await self._record_operation("set", start_time, False)
            raise e

    async def delete(self, key: Any) -> bool:
        """Delete a key from the table."""
        start_time = time.time()
        try:
            async with self._lock:
                result = await self.backend.delete(key)
                await self._record_operation("delete", start_time, True)
                return result
        except Exception as e:
            await self._record_operation("delete", start_time, False)
            raise e

    async def exists(self, key: Any) -> bool:
        """Check if a key exists."""
        start_time = time.time()
        try:
            async with self._lock:
                result = await self.backend.exists(key)
                await self._record_operation("exists", start_time, True)
                return result
        except Exception as e:
            await self._record_operation("exists", start_time, False)
            raise e

    async def keys(self, prefix: Optional[str] = None) -> List[Any]:
        """Get all keys, optionally filtered by prefix."""
        start_time = time.time()
        try:
            async with self._lock:
                keys = await self.backend.keys(prefix)
                await self._record_operation("keys", start_time, True)
                return keys
        except Exception as e:
            await self._record_operation("keys", start_time, False)
            raise e

    async def values(self) -> List[Any]:
        """Get all values."""
        start_time = time.time()
        try:
            async with self._lock:
                values = await self.backend.values()
                await self._record_operation("values", start_time, True)
                return values
        except Exception as e:
            await self._record_operation("values", start_time, False)
            raise e

    async def items(self, prefix: Optional[str] = None) -> List[tuple]:
        """Get all key-value pairs."""
        start_time = time.time()
        try:
            async with self._lock:
                items = await self.backend.items(prefix)
                await self._record_operation("items", start_time, True)
                return items
        except Exception as e:
            await self._record_operation("items", start_time, False)
            raise e

    async def clear(self) -> None:
        """Clear all data in the table."""
        start_time = time.time()
        try:
            async with self._lock:
                await self.backend.clear()
                await self._record_operation("clear", start_time, True)
        except Exception as e:
            await self._record_operation("clear", start_time, False)
            raise e

    async def size(self) -> int:
        """Get the number of items in the table."""
        start_time = time.time()
        try:
            async with self._lock:
                size = await self.backend.size()
                await self._record_operation("size", start_time, True)
                return size
        except Exception as e:
            await self._record_operation("size", start_time, False)
            raise e

    async def scan(
        self,
        start_key: Optional[Any] = None,
        end_key: Optional[Any] = None,
        prefix: Optional[str] = None,
        limit: Optional[int] = None
    ) -> List[tuple]:
        """Scan key-value pairs with range and limit."""
        start_time = time.time()
        try:
            async with self._lock:
                items = await self.backend.scan(start_key, end_key, prefix, limit)
                await self._record_operation("scan", start_time, True)
                return items
        except Exception as e:
            await self._record_operation("scan", start_time, False)
            raise e

    async def increment(self, key: Any, amount: Union[int, float] = 1) -> Union[int, float]:
        """Atomically increment a numeric value."""
        start_time = time.time()
        try:
            async with self._lock:
                current = await self.backend.get(key) or 0
                if not isinstance(current, (int, float)):
                    raise TypeError(f"Cannot increment non-numeric value: {type(current)}")

                new_value = current + amount
                await self.backend.set(key, new_value)
                await self._record_operation("increment", start_time, True)
                return new_value
        except Exception as e:
            await self._record_operation("increment", start_time, False)
            raise e

    def get_metrics(self) -> Optional[StateMetrics]:
        """Get performance metrics for this table."""
        return self.metrics

    async def health_check(self) -> Dict[str, Any]:
        """Perform a health check on the table."""
        try:
            # Basic operations test
            test_key = "__health_check__"
            test_value = {"timestamp": time.time(), "status": "ok"}

            await self.set(test_key, test_value)
            retrieved = await self.get(test_key)
            await self.delete(test_key)

            if retrieved != test_value:
                return {"status": "error", "message": "Data integrity check failed"}

            return {
                "status": "healthy",
                "backend_type": self.backend.__class__.__name__,
                "size": await self.size()
            }

        except Exception as e:
            return {"status": "error", "message": str(e)}


class StateStoreManager:
    """
    Unified State Store Manager for Sabot.

    This is the main entry point for state management in Sabot applications.
    It provides a high-level API for creating and managing state tables.
    """

    def __init__(self, config: StateStoreConfig):
        self.config = config
        self.backends: Dict[str, StoreBackend] = {}
        self.tables: Dict[str, StateTable] = {}
        self.checkpoint_managers: Dict[str, CheckpointManager] = {}
        self.is_running = False

        # Shared backend for isolation level SHARED
        self.shared_backend: Optional[StoreBackend] = None

        # Metrics
        self.global_metrics = StateMetrics() if config.enable_metrics else None

    async def start(self) -> None:
        """Initialize the state store manager."""
        if self.is_running:
            return

        try:
            # Initialize shared backend if needed
            if self.config.isolation_level == StateIsolation.SHARED:
                self.shared_backend = await self._create_backend("shared")
                await self.shared_backend.start()

                # Initialize checkpointing for shared backend
                if self.config.checkpoint_config.enabled:
                    self.checkpoint_managers["shared"] = CheckpointManager(
                        self.shared_backend, self.config.checkpoint_config
                    )
                    await self.checkpoint_managers["shared"].auto_recover_on_startup()

            self.is_running = True
            print("✅ State Store Manager started")

        except Exception as e:
            print(f"❌ Failed to start State Store Manager: {e}")
            raise

    async def stop(self) -> None:
        """Shut down the state store manager."""
        if not self.is_running:
            return

        # Stop checkpoint managers
        for cp_manager in self.checkpoint_managers.values():
            await cp_manager.stop_auto_checkpointing()

        # Stop all backends
        for backend in self.backends.values():
            try:
                await backend.stop()
            except Exception as e:
                print(f"Error stopping backend: {e}")

        if self.shared_backend:
            try:
                await self.shared_backend.stop()
            except Exception as e:
                print(f"Error stopping shared backend: {e}")

        self.is_running = False
        print("🛑 State Store Manager stopped")

    def _get_backend_key(self, table_name: str) -> str:
        """Get the backend key for a table based on isolation level."""
        if self.config.isolation_level == StateIsolation.SHARED:
            return "shared"
        elif self.config.isolation_level == StateIsolation.TABLE:
            return table_name
        elif self.config.isolation_level == StateIsolation.NAMESPACE:
            # Group tables by namespace (e.g., "user", "session", "cache")
            return table_name.split('_')[0] if '_' in table_name else table_name
        else:
            return "shared"

    async def _create_backend(self, backend_key: str) -> StoreBackend:
        """Create a backend instance."""
        if backend_key in self.backends:
            return self.backends[backend_key]

        # Create backend based on type
        if self.config.backend_type == BackendType.MEMORY:
            config = self.config.memory_config
            backend = MemoryBackend(config)

        elif self.config.backend_type == BackendType.ROCKSDB:
            config = self.config.rocksdb_config.copy()
            # Add table-specific path for TABLE isolation
            if self.config.isolation_level == StateIsolation.TABLE:
                config.path = config.path / backend_key
            backend = RocksDBBackend(config)

        elif self.config.backend_type == BackendType.REDIS:
            config = self.config.redis_config.copy()
            # Add table-specific namespace for TABLE isolation
            if self.config.isolation_level == StateIsolation.TABLE:
                config.options = config.options.copy()
                config.options['namespace'] = f"{config.options.get('namespace', 'sabot')}:{backend_key}"
            backend = RedisBackend(config)

        elif self.config.backend_type == BackendType.TONBO:
            config = self.config.tonbo_config.copy()
            # Add table-specific path for TABLE isolation
            if self.config.isolation_level == StateIsolation.TABLE:
                config.path = config.path / backend_key
            backend = TonboBackend(config)

        elif self.config.backend_type == BackendType.KAFKA:
            config = self.config.kafka_config.copy()
            # Add table-specific topic prefix for TABLE isolation
            if self.config.isolation_level == StateIsolation.TABLE:
                config.options = config.options.copy()
                config.options['topic_prefix'] = f"{config.options.get('topic_prefix', 'sabot.')}{backend_key}."
            backend = KafkaBackend(config)

        else:
            raise ValueError(f"Unsupported backend type: {self.config.backend_type}")

        await backend.start()
        self.backends[backend_key] = backend

        return backend

    async def set_table_data(self, table_name: str, arrow_table: object) -> None:
        """
        Store Arrow table data using Tonbo's Arrow integration.

        Args:
            table_name: Name of the table
            arrow_table: PyArrow Table to store
        """
        backend = await self._get_backend(table_name)
        if hasattr(backend, 'arrow_insert_batch'):
            await backend.arrow_insert_batch(arrow_table)
        else:
            # Fallback: convert to individual records
            for batch in arrow_table.to_batches():
                for row in batch.to_pylist():
                    key = row.get('key', row.get('id', f"{table_name}_{id(row)}"))
                    await backend.set(key, row)

    async def scan_as_arrow_table(self, table_name: str, start_key: str = None,
                                end_key: str = None, limit: int = 10000) -> Optional[object]:
        """
        Scan table data and return as Arrow table.

        Args:
            table_name: Name of the table to scan
            start_key: Start key for range scan
            end_key: End key for range scan
            limit: Maximum rows to return

        Returns:
            PyArrow Table or None
        """
        backend = await self._get_backend(table_name)
        if hasattr(backend, 'arrow_scan_to_table'):
            return await backend.arrow_scan_to_table(start_key, end_key, limit)
        return None

    async def arrow_filter(self, table_name: str, condition: object) -> Optional[object]:
        """
        Filter table data using Arrow compute expressions.

        Args:
            table_name: Name of the table
            condition: PyArrow compute condition

        Returns:
            Filtered PyArrow Table
        """
        backend = await self._get_backend(table_name)
        if hasattr(backend, 'arrow_filter'):
            return await backend.arrow_filter(condition)
        return None

    async def arrow_aggregate(self, table_name: str, group_by: List[str],
                            aggregations: Dict[str, str]) -> Optional[object]:
        """
        Aggregate table data using Arrow compute.

        Args:
            table_name: Name of the table
            group_by: Columns to group by
            aggregations: Aggregation functions

        Returns:
            Aggregated PyArrow Table
        """
        backend = await self._get_backend(table_name)
        if hasattr(backend, 'arrow_aggregate'):
            return await backend.arrow_aggregate(group_by, aggregations)
        return None

    async def arrow_join(self, table_name: str, other_table: object,
                        join_keys: List[str] = None, join_type: str = "inner") -> Optional[object]:
        """
        Join table with another Arrow table.

        Args:
            table_name: Name of the first table
            other_table: PyArrow Table to join with
            join_keys: Keys to join on
            join_type: Type of join

        Returns:
            Joined PyArrow Table
        """
        backend = await self._get_backend(table_name)
        if hasattr(backend, 'arrow_join'):
            return await backend.arrow_join(other_table, join_keys, join_type)
        return None

    async def arrow_streaming_aggregate(self, table_name: str, aggregation_type: str,
                                      group_by_cols: List[str], **params) -> AsyncIterator[object]:
        """
        Perform streaming aggregation on table data.

        Args:
            table_name: Name of the table
            aggregation_type: Type of aggregation
            group_by_cols: Columns to group by
            **params: Additional parameters

        Yields:
            PyArrow Tables with results
        """
        backend = await self._get_backend(table_name)
        if hasattr(backend, 'arrow_streaming_aggregate'):
            async for result in backend.arrow_streaming_aggregate(aggregation_type, group_by_cols, **params):
                yield result

    async def arrow_export_dataset(self, table_name: str, output_path: str,
                                 partitioning: Dict = None) -> bool:
        """
        Export table as Arrow Dataset.

        Args:
            table_name: Name of the table
            output_path: Path to export to
            partitioning: Partitioning configuration

        Returns:
            True if successful
        """
        backend = await self._get_backend(table_name)
        if hasattr(backend, 'arrow_export_dataset'):
            return await backend.arrow_export_dataset(output_path, partitioning)
        return False

    # Kafka-specific methods for bridging jobs/systems

    async def send_to_topic(self, topic: str, key: Any, value: Any, headers: Dict = None) -> None:
        """
        Send message directly to a Kafka topic.

        Args:
            topic: Kafka topic name
            key: Message key
            value: Message value
            headers: Optional message headers
        """
        # Use the first backend if it's Kafka, otherwise create a Kafka backend
        for backend in self.backends.values():
            if hasattr(backend, 'send_to_topic'):
                await backend.send_to_topic(topic, key, value, headers)
                return

        # Create a temporary Kafka backend for this operation
        kafka_config = self.config.kafka_config.copy()
        kafka_backend = KafkaBackend(kafka_config)
        await kafka_backend.start()
        try:
            await kafka_backend.send_to_topic(topic, key, value, headers)
        finally:
            await kafka_backend.stop()

    async def consume_topic(self, topic: str, timeout_ms: int = 1000) -> AsyncIterator[tuple]:
        """
        Consume messages from a Kafka topic.

        Args:
            topic: Kafka topic name
            timeout_ms: Consumer timeout

        Yields:
            (key, value, metadata) tuples
        """
        # Use the first backend if it's Kafka, otherwise create a Kafka backend
        for backend in self.backends.values():
            if hasattr(backend, 'consume_topic'):
                async for item in backend.consume_topic(topic, timeout_ms):
                    yield item
                return

        # Create a temporary Kafka backend for this operation
        kafka_config = self.config.kafka_config.copy()
        kafka_backend = KafkaBackend(kafka_config)
        await kafka_backend.start()
        try:
            async for item in kafka_backend.consume_topic(topic, timeout_ms):
                yield item
        finally:
            await kafka_backend.stop()

    async def bridge_topics(self, source_topic: str, target_topic: str,
                           transform_func: Callable = None) -> None:
        """
        Bridge messages between Kafka topics with optional transformation.

        This enables job-to-job communication and data flow orchestration.

        Args:
            source_topic: Topic to consume from
            target_topic: Topic to send to
            transform_func: Optional function to transform messages
        """
        async for key, value, metadata in self.consume_topic(source_topic):
            # Apply transformation if provided
            if transform_func:
                try:
                    key, value = transform_func(key, value, metadata)
                except Exception as e:
                    self.logger.error(f"Transformation failed: {e}")
                    continue

            # Send to target topic
            await self.send_to_topic(target_topic, key, value, metadata.get('headers'))

    async def create_topic_bridge(self, source_topic: str, target_topic: str,
                                transform_func: Callable = None) -> asyncio.Task:
        """
        Create a persistent topic bridge as a background task.

        Args:
            source_topic: Source topic
            target_topic: Target topic
            transform_func: Optional transformation function

        Returns:
            Background task handle
        """
        task = asyncio.create_task(
            self.bridge_topics(source_topic, target_topic, transform_func)
        )
        return task

    async def create_table(self, name: str) -> StateTable:
        """
        Create a new state table.

        Args:
            name: Table name

        Returns:
            StateTable instance
        """
        if name in self.tables:
            return self.tables[name]

        # Get or create backend
        backend_key = self._get_backend_key(name)
        if self.config.isolation_level == StateIsolation.SHARED:
            backend = self.shared_backend
        else:
            backend = await self._create_backend(backend_key)

        # Create table
        table = StateTable(name, backend, self.config.enable_metrics)
        self.tables[name] = table

        # Set up checkpointing for this backend if not already done
        if backend_key not in self.checkpoint_managers and self.config.checkpoint_config.enabled:
            if self.config.backend_type in [BackendType.ROCKSDB, BackendType.REDIS]:
                # Only enable checkpointing for persistent backends
                cp_config = self.config.checkpoint_config
                if self.config.isolation_level == StateIsolation.TABLE:
                    # Use table-specific checkpoint directory
                    cp_config = CheckpointConfig(
                        **self.config.checkpoint_config.__dict__,
                        checkpoint_dir=self.config.checkpoint_config.checkpoint_dir / name
                    )

                self.checkpoint_managers[backend_key] = CheckpointManager(backend, cp_config)

                # Start auto-checkpointing
                await self.checkpoint_managers[backend_key].start_auto_checkpointing()

        print(f"📋 Created state table '{name}' with {self.config.backend_type.value} backend")
        return table

    async def get_table(self, name: str) -> Optional[StateTable]:
        """Get an existing table."""
        return self.tables.get(name)

    async def drop_table(self, name: str) -> bool:
        """
        Drop a table.

        Returns:
            True if table was dropped
        """
        if name not in self.tables:
            return False

        table = self.tables[name]
        backend_key = self._get_backend_key(name)

        # Clear table data
        try:
            await table.clear()
        except Exception as e:
            print(f"Warning: Failed to clear table {name}: {e}")

        # Remove from tables
        del self.tables[name]

        # If TABLE isolation, we can stop the backend
        if self.config.isolation_level == StateIsolation.TABLE:
            if backend_key in self.backends:
                try:
                    await self.backends[backend_key].stop()
                    del self.backends[backend_key]
                except Exception as e:
                    print(f"Warning: Failed to stop backend for {name}: {e}")

            # Stop checkpointing for this table
            if backend_key in self.checkpoint_managers:
                await self.checkpoint_managers[backend_key].stop_auto_checkpointing()
                del self.checkpoint_managers[backend_key]

        print(f"🗑️ Dropped state table '{name}'")
        return True

    async def list_tables(self) -> List[str]:
        """List all table names."""
        return list(self.tables.keys())

    async def create_checkpoint(self, table_name: Optional[str] = None, force: bool = False) -> Optional[str]:
        """
        Create a checkpoint.

        Args:
            table_name: Specific table to checkpoint, or all tables if None
            force: Force checkpoint creation

        Returns:
            Checkpoint ID if created
        """
        if not self.config.checkpoint_config.enabled:
            return None

        if table_name:
            # Checkpoint specific table
            backend_key = self._get_backend_key(table_name)
            if backend_key in self.checkpoint_managers:
                return await self.checkpoint_managers[backend_key].create_checkpoint(force)
        else:
            # Checkpoint all backends
            checkpoint_ids = []
            for cp_manager in self.checkpoint_managers.values():
                cp_id = await cp_manager.create_checkpoint(force)
                if cp_id:
                    checkpoint_ids.append(cp_id)

            return checkpoint_ids[0] if checkpoint_ids else None

        return None

    async def restore_checkpoint(self, checkpoint_id: str, table_name: Optional[str] = None) -> bool:
        """
        Restore from a checkpoint.

        Args:
            checkpoint_id: Checkpoint to restore
            table_name: Specific table to restore, or all if None

        Returns:
            True if restoration succeeded
        """
        if table_name:
            backend_key = self._get_backend_key(table_name)
            if backend_key in self.checkpoint_managers:
                return await self.checkpoint_managers[backend_key].restore_checkpoint(checkpoint_id)
        else:
            # Restore all backends
            success = True
            for cp_manager in self.checkpoint_managers.values():
                if not await cp_manager.restore_checkpoint(checkpoint_id):
                    success = False
            return success

        return False

    def get_metrics(self) -> Dict[str, Any]:
        """Get global metrics."""
        if not self.global_metrics:
            return {}

        # Aggregate metrics from all tables
        table_metrics = {}
        for name, table in self.tables.items():
            if table.metrics:
                table_metrics[name] = {
                    'operations_total': table.metrics.operations_total,
                    'errors_total': table.metrics.errors_total,
                    'avg_latency': sum(table.metrics.latency_by_type.values()) / len(table.metrics.latency_by_type) if table.metrics.latency_by_type else 0
                }

        return {
            'tables': table_metrics,
            'backends': list(self.backends.keys()),
            'checkpoint_managers': list(self.checkpoint_managers.keys()),
            'isolation_level': self.config.isolation_level.value,
            'backend_type': self.config.backend_type.value
        }

    async def health_check(self) -> Dict[str, Any]:
        """Perform a comprehensive health check."""
        results = {
            'overall_status': 'healthy',
            'tables': {},
            'backends': {},
            'checkpoints': {}
        }

        # Check tables
        for name, table in self.tables.items():
            table_health = await table.health_check()
            results['tables'][name] = table_health
            if table_health['status'] != 'healthy':
                results['overall_status'] = 'degraded'

        # Check backends
        for name, backend in self.backends.items():
            try:
                # Basic backend health check
                size = await backend.size()
                results['backends'][name] = {
                    'status': 'healthy',
                    'size': size,
                    'type': backend.__class__.__name__
                }
            except Exception as e:
                results['backends'][name] = {
                    'status': 'error',
                    'error': str(e)
                }
                results['overall_status'] = 'degraded'

        # Check checkpoints
        for name, cp_manager in self.checkpoint_managers.items():
            stats = await cp_manager.get_checkpoint_stats()
            results['checkpoints'][name] = {
                'status': 'healthy' if stats['valid_checkpoints'] > 0 else 'warning',
                'stats': stats
            }

        return results

    async def optimize(self) -> None:
        """Optimize all backends."""
        for backend in self.backends.values():
            try:
                if hasattr(backend, 'compact'):
                    await backend.compact()
                if hasattr(backend, 'optimize'):
                    await backend.optimize()
            except Exception as e:
                print(f"Failed to optimize backend {backend.__class__.__name__}: {e}")

        print("🔧 State store optimization completed")
