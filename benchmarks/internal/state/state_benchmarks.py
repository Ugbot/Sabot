#!/usr/bin/env python3
"""
State Management Benchmarks for Sabot

Benchmarks various state operations including:
- Key-value store performance
- State persistence and recovery
- Concurrent state access
- State size scaling
- Backend comparison
"""

import asyncio
import time
import random
import psutil
from typing import Dict, Any, List
from dataclasses import dataclass

from .runner import BenchmarkRunner, BenchmarkConfig, benchmark


@dataclass
class StateBenchmarkConfig:
    """Configuration for state benchmarks."""
    operation_count: int = 10000
    key_space_size: int = 1000
    value_size_bytes: int = 256
    read_write_ratio: float = 0.8  # 80% reads, 20% writes
    concurrency: int = 1
    batch_size: int = 100
    backend_type: str = "memory"  # memory, redis, rocksdb


class StateBenchmark:
    """Benchmark suite for state management operations."""

    def __init__(self):
        self.runner = BenchmarkRunner()

    @benchmark(BenchmarkConfig(
        name="state_kv_operations",
        description="Benchmark basic key-value operations",
        iterations=3,
        warmup_iterations=1
    ))
    async def benchmark_kv_operations(self) -> Dict[str, Any]:
        """Benchmark basic key-value operations."""
        config = StateBenchmarkConfig(
            operation_count=50000,
            key_space_size=5000,
            value_size_bytes=128,
            read_write_ratio=0.7
        )

        return await self._run_state_benchmark(config)

    @benchmark(BenchmarkConfig(
        name="state_concurrent_access",
        description="Benchmark concurrent state access",
        iterations=3,
        warmup_iterations=1,
        concurrency=4
    ))
    async def benchmark_concurrent_access(self) -> Dict[str, Any]:
        """Benchmark concurrent state access."""
        config = StateBenchmarkConfig(
            operation_count=25000,
            key_space_size=2000,
            value_size_bytes=256,
            read_write_ratio=0.5,
            concurrency=8
        )

        return await self._run_state_benchmark(config)

    @benchmark(BenchmarkConfig(
        name="state_batch_operations",
        description="Benchmark batch state operations",
        iterations=3,
        warmup_iterations=1
    ))
    async def benchmark_batch_operations(self) -> Dict[str, Any]:
        """Benchmark batch state operations."""
        config = StateBenchmarkConfig(
            operation_count=10000,
            key_space_size=1000,
            value_size_bytes=512,
            batch_size=500
        )

        return await self._run_batch_state_benchmark(config)

    @benchmark(BenchmarkConfig(
        name="state_large_dataset",
        description="Benchmark state operations with large dataset",
        iterations=2,
        warmup_iterations=1
    ))
    async def benchmark_large_dataset(self) -> Dict[str, Any]:
        """Benchmark with large state dataset."""
        config = StateBenchmarkConfig(
            operation_count=100000,
            key_space_size=50000,  # Large key space
            value_size_bytes=64,
            read_write_ratio=0.9  # Mostly reads
        )

        return await self._run_state_benchmark(config)

    async def _run_state_benchmark(self, config: StateBenchmarkConfig) -> Dict[str, Any]:
        """Run a state benchmark with given configuration."""
        # Initialize mock state store (simulate actual state backend)
        state_store = self._create_mock_state_store(config.backend_type)

        # Pre-populate state
        await self._populate_state_store(state_store, config.key_space_size, config.value_size_bytes)

        # Generate operation sequence
        operations = self._generate_operations(config.operation_count, config.read_write_ratio)

        start_time = time.time()
        read_count = 0
        write_count = 0
        error_count = 0

        # Execute operations
        if config.concurrency == 1:
            # Single-threaded execution
            for op in operations:
                try:
                    if op['type'] == 'read':
                        await state_store.get(op['key'])
                        read_count += 1
                    else:  # write
                        await state_store.set(op['key'], op['value'])
                        write_count += 1
                except Exception:
                    error_count += 1
        else:
            # Concurrent execution
            semaphore = asyncio.Semaphore(config.concurrency)

            async def execute_operation(op):
                async with semaphore:
                    try:
                        if op['type'] == 'read':
                            await state_store.get(op['key'])
                            return 'read'
                        else:  # write
                            await state_store.set(op['key'], op['value'])
                            return 'write'
                    except Exception:
                        return 'error'

            tasks = [execute_operation(op) for op in operations]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            read_count = sum(1 for r in results if r == 'read')
            write_count = sum(1 for r in results if r == 'write')
            error_count = sum(1 for r in results if r == 'error')

        end_time = time.time()
        total_time = end_time - start_time

        # Calculate metrics
        total_operations = read_count + write_count
        throughput = total_operations / total_time if total_time > 0 else 0

        return {
            'total_operations': total_operations,
            'read_operations': read_count,
            'write_operations': write_count,
            'error_operations': error_count,
            'success_rate': total_operations / config.operation_count,
            'total_time_seconds': total_time,
            'throughput_ops_per_sec': throughput,
            'avg_latency_ms': (total_time / total_operations * 1000) if total_operations > 0 else 0,
            'concurrency': config.concurrency,
            'backend_type': config.backend_type,
            'key_space_size': config.key_space_size,
            'read_write_ratio': config.read_write_ratio
        }

    async def _run_batch_state_benchmark(self, config: StateBenchmarkConfig) -> Dict[str, Any]:
        """Run batch state operations benchmark."""
        state_store = self._create_mock_state_store(config.backend_type)

        # Generate batch operations
        batches = []
        operations_per_batch = config.batch_size

        for i in range(0, config.operation_count, operations_per_batch):
            batch = []
            for j in range(min(operations_per_batch, config.operation_count - i)):
                key = f"batch_key_{i + j}"
                value = 'x' * config.value_size_bytes
                batch.append(('set', key, value))
            batches.append(batch)

        start_time = time.time()
        total_operations = 0
        error_count = 0

        # Execute batches
        for batch in batches:
            try:
                # Simulate batch operation
                for op_type, key, value in batch:
                    if op_type == 'set':
                        await state_store.set(key, value)
                    total_operations += 1

                # Simulate batch commit delay
                await asyncio.sleep(0.001)

            except Exception:
                error_count += len(batch)

        end_time = time.time()
        total_time = end_time - start_time

        return {
            'total_operations': total_operations,
            'batches_processed': len(batches),
            'batch_size': config.batch_size,
            'error_operations': error_count,
            'total_time_seconds': total_time,
            'throughput_ops_per_sec': total_operations / total_time if total_time > 0 else 0,
            'throughput_batches_per_sec': len(batches) / total_time if total_time > 0 else 0,
            'backend_type': config.backend_type
        }

    def _create_mock_state_store(self, backend_type: str) -> 'MockStateStore':
        """Create a mock state store for benchmarking."""
        return MockStateStore(backend_type)

    async def _populate_state_store(self, store: 'MockStateStore',
                                  key_count: int, value_size: int) -> None:
        """Populate state store with initial data."""
        for i in range(key_count):
            key = f"initial_key_{i}"
            value = 'x' * value_size
            await store.set(key, value)

    def _generate_operations(self, count: int, read_write_ratio: float) -> List[Dict[str, Any]]:
        """Generate sequence of read/write operations."""
        operations = []

        for i in range(count):
            if random.random() < read_write_ratio:
                # Read operation
                key = f"key_{random.randint(0, 999)}"  # Access existing keys
                operations.append({
                    'type': 'read',
                    'key': key
                })
            else:
                # Write operation
                key = f"key_{random.randint(0, 1999)}"  # May create new keys
                value = 'x' * 128  # Fixed value size for writes
                operations.append({
                    'type': 'write',
                    'key': key,
                    'value': value
                })

        return operations

    async def run_all_benchmarks(self) -> List[Dict[str, Any]]:
        """Run all state benchmarks."""
        benchmarks = [
            self.benchmark_kv_operations,
            self.benchmark_concurrent_access,
            self.benchmark_batch_operations,
            self.benchmark_large_dataset
        ]

        return await self.runner.run_comprehensive_suite(benchmarks)


class MockStateStore:
    """Mock state store for benchmarking."""

    def __init__(self, backend_type: str):
        self.backend_type = backend_type
        self.data = {}
        self.access_times = []  # Track access latencies

    async def get(self, key: str) -> Any:
        """Get value by key."""
        start_time = time.time()

        # Simulate backend-specific latency
        if self.backend_type == 'redis':
            await asyncio.sleep(0.0001)  # 100μs for Redis
        elif self.backend_type == 'rocksdb':
            await asyncio.sleep(0.0002)  # 200μs for RocksDB
        else:
            await asyncio.sleep(0.00005)  # 50μs for memory

        value = self.data.get(key)

        end_time = time.time()
        self.access_times.append(end_time - start_time)

        return value

    async def set(self, key: str, value: Any) -> None:
        """Set value by key."""
        start_time = time.time()

        # Simulate backend-specific latency
        if self.backend_type == 'redis':
            await asyncio.sleep(0.00015)  # 150μs for Redis
        elif self.backend_type == 'rocksdb':
            await asyncio.sleep(0.0003)  # 300μs for RocksDB
        else:
            await asyncio.sleep(0.00008)  # 80μs for memory

        self.data[key] = value

        end_time = time.time()
        self.access_times.append(end_time - start_time)

    async def delete(self, key: str) -> bool:
        """Delete key."""
        if key in self.data:
            del self.data[key]
            return True
        return False

    async def exists(self, key: str) -> bool:
        """Check if key exists."""
        return key in self.data

    async def scan(self, prefix: str = "", limit: int = 100) -> List[str]:
        """Scan keys with optional prefix."""
        keys = [k for k in self.data.keys() if k.startswith(prefix)]
        return keys[:limit]

    def get_stats(self) -> Dict[str, Any]:
        """Get store statistics."""
        return {
            'total_keys': len(self.data),
            'avg_access_time': sum(self.access_times) / len(self.access_times) if self.access_times else 0,
            'backend_type': self.backend_type
        }
