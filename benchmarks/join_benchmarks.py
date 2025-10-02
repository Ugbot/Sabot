#!/usr/bin/env python3
"""
Join Operation Benchmarks for Sabot

Benchmarks various join operations including:
- Stream-Stream joins
- Stream-Table joins
- Temporal joins
- Join performance scaling
- Memory usage during joins
"""

import asyncio
import time
import random
from typing import Dict, Any, List
from dataclasses import dataclass

from .runner import BenchmarkRunner, BenchmarkConfig, benchmark


@dataclass
class JoinBenchmarkConfig:
    """Configuration for join benchmarks."""
    left_stream_size: int = 10000
    right_stream_size: int = 10000
    join_type: str = "inner"  # inner, left, right, full_outer
    join_condition: str = "equality"  # equality, range, temporal
    window_size_seconds: float = 60.0
    time_skew_seconds: float = 5.0


class JoinBenchmark:
    """Benchmark suite for join operations."""

    def __init__(self):
        self.runner = BenchmarkRunner()

    @benchmark(BenchmarkConfig(
        name="join_stream_stream_equality",
        description="Benchmark Stream-Stream equality joins",
        iterations=3,
        warmup_iterations=1
    ))
    async def benchmark_stream_stream_equality_join(self) -> Dict[str, Any]:
        """Benchmark Stream-Stream equality joins."""
        config = JoinBenchmarkConfig(
            left_stream_size=5000,
            right_stream_size=5000,
            join_type="inner",
            join_condition="equality",
            window_size_seconds=30.0
        )

        return await self._run_join_benchmark(config)

    @benchmark(BenchmarkConfig(
        name="join_stream_stream_temporal",
        description="Benchmark Stream-Stream temporal joins",
        iterations=3,
        warmup_iterations=1
    ))
    async def benchmark_stream_stream_temporal_join(self) -> Dict[str, Any]:
        """Benchmark Stream-Stream temporal joins."""
        config = JoinBenchmarkConfig(
            left_stream_size=5000,
            right_stream_size=5000,
            join_type="inner",
            join_condition="temporal",
            window_size_seconds=60.0,
            time_skew_seconds=10.0
        )

        return await self._run_join_benchmark(config)

    @benchmark(BenchmarkConfig(
        name="join_stream_table_lookup",
        description="Benchmark Stream-Table lookup joins",
        iterations=3,
        warmup_iterations=1
    ))
    async def benchmark_stream_table_join(self) -> Dict[str, Any]:
        """Benchmark Stream-Table lookup joins."""
        config = JoinBenchmarkConfig(
            left_stream_size=10000,
            right_stream_size=1000,  # Smaller table for lookup
            join_type="left",
            join_condition="equality"
        )

        return await self._run_join_benchmark(config)

    @benchmark(BenchmarkConfig(
        name="join_complex_windowed",
        description="Benchmark complex windowed joins",
        iterations=2,
        warmup_iterations=1
    ))
    async def benchmark_complex_windowed_join(self) -> Dict[str, Any]:
        """Benchmark complex windowed joins with large datasets."""
        config = JoinBenchmarkConfig(
            left_stream_size=25000,
            right_stream_size=25000,
            join_type="inner",
            join_condition="range",
            window_size_seconds=300.0,  # 5 minute window
            time_skew_seconds=30.0
        )

        return await self._run_join_benchmark(config)

    async def _run_join_benchmark(self, config: JoinBenchmarkConfig) -> Dict[str, Any]:
        """Run a join benchmark with given configuration."""
        # Generate test data
        left_stream = self._generate_join_stream(config.left_stream_size, "left")
        right_stream = self._generate_join_stream(config.right_stream_size, "right")

        # Setup join state
        join_state = self._initialize_join_state(config)

        start_time = time.time()
        joined_records = 0
        processed_records = 0

        # Process streams and perform joins
        if config.join_condition == "equality":
            joined_records = await self._perform_equality_join(
                left_stream, right_stream, join_state, config
            )
        elif config.join_condition == "temporal":
            joined_records = await self._perform_temporal_join(
                left_stream, right_stream, join_state, config
            )
        elif config.join_condition == "range":
            joined_records = await self._perform_range_join(
                left_stream, right_stream, join_state, config
            )

        processed_records = len(left_stream) + len(right_stream)

        end_time = time.time()
        total_time = end_time - start_time

        return {
            'left_stream_size': len(left_stream),
            'right_stream_size': len(right_stream),
            'joined_records': joined_records,
            'processed_records': processed_records,
            'join_efficiency': joined_records / max(1, processed_records),
            'total_time_seconds': total_time,
            'throughput_records_per_sec': processed_records / total_time if total_time > 0 else 0,
            'join_type': config.join_type,
            'join_condition': config.join_condition,
            'window_size_seconds': config.window_size_seconds
        }

    def _generate_join_stream(self, size: int, stream_name: str) -> List[Dict[str, Any]]:
        """Generate a stream of records for join benchmarking."""
        records = []

        for i in range(size):
            # Generate records with join keys
            record = {
                'id': f"{stream_name}_{i}",
                'timestamp': time.time() + random.uniform(-300, 300),  # ±5 minutes variance
                'join_key': f"key_{random.randint(1, min(1000, size // 10))}",  # Some keys will match
                'value': random.randint(1, 1000),
                'category': random.choice(['A', 'B', 'C', 'D']),
                'stream': stream_name
            }

            # Add stream-specific fields
            if stream_name == "left":
                record['left_specific'] = f"left_data_{i}"
            else:
                record['right_specific'] = f"right_data_{i}"

            records.append(record)

        # Sort by timestamp for temporal joins
        records.sort(key=lambda r: r['timestamp'])

        return records

    def _initialize_join_state(self, config: JoinBenchmarkConfig) -> Dict[str, Any]:
        """Initialize join state based on configuration."""
        state = {
            'join_type': config.join_type,
            'window_size': config.window_size_seconds,
            'time_skew': config.time_skew_seconds,
            'left_buffer': [],
            'right_buffer': [],
            'join_index': {}  # For equality joins
        }

        return state

    async def _perform_equality_join(self, left_stream: List[Dict[str, Any]],
                                   right_stream: List[Dict[str, Any]],
                                   state: Dict[str, Any],
                                   config: JoinBenchmarkConfig) -> int:
        """Perform equality join between streams."""
        joined_count = 0

        # Build index for efficient lookup (simulating table in stream-table join)
        if config.right_stream_size < config.left_stream_size:
            # Right stream is smaller, index it
            for record in right_stream:
                key = record['join_key']
                if key not in state['join_index']:
                    state['join_index'][key] = []
                state['join_index'][key].append(record)
        else:
            # Left stream is smaller, index it
            for record in left_stream:
                key = record['join_key']
                if key not in state['join_index']:
                    state['join_index'][key] = []
                state['join_index'][key].append(record)

        # Perform join
        search_stream = right_stream if config.right_stream_size >= config.left_stream_size else left_stream

        for record in search_stream:
            key = record['join_key']
            matches = state['join_index'].get(key, [])

            for match in matches:
                # Simulate join output
                joined_record = {
                    'left_id': match['id'] if match['stream'] == 'left' else record['id'],
                    'right_id': record['id'] if record['stream'] == 'right' else match['id'],
                    'join_key': key,
                    'left_value': match['value'] if match['stream'] == 'left' else record['value'],
                    'right_value': record['value'] if record['stream'] == 'right' else match['value'],
                    'joined_at': time.time()
                }
                joined_count += 1

                # Simulate processing delay
                await asyncio.sleep(0.00001)  # 10 microseconds per join

        return joined_count

    async def _perform_temporal_join(self, left_stream: List[Dict[str, Any]],
                                   right_stream: List[Dict[str, Any]],
                                   state: Dict[str, Any],
                                   config: JoinBenchmarkConfig) -> int:
        """Perform temporal join between streams."""
        joined_count = 0

        # Convert to time-ordered streams
        all_records = []
        all_records.extend(('left', i, r) for i, r in enumerate(left_stream))
        all_records.extend(('right', i, r) for i, r in enumerate(right_stream))

        # Sort by timestamp
        all_records.sort(key=lambda x: x[2]['timestamp'])

        # Sliding window temporal join
        window_size = config.window_size_seconds

        # Maintain sliding windows for each stream
        left_window = []
        right_window = []

        for stream_name, index, record in all_records:
            current_time = record['timestamp']

            # Add to appropriate window
            if stream_name == 'left':
                left_window.append((index, record))
            else:
                right_window.append((index, record))

            # Remove expired records
            left_window = [(i, r) for i, r in left_window if current_time - r['timestamp'] <= window_size]
            right_window = [(i, r) for i, r in right_window if current_time - r['timestamp'] <= window_size]

            # Perform joins within time window
            if stream_name == 'left':
                # Join this left record with all right records in window
                for right_idx, right_record in right_window:
                    time_diff = abs(record['timestamp'] - right_record['timestamp'])
                    if time_diff <= config.time_skew_seconds:
                        # Time-based join condition met
                        joined_record = {
                            'left_id': record['id'],
                            'right_id': right_record['id'],
                            'time_diff_seconds': time_diff,
                            'left_timestamp': record['timestamp'],
                            'right_timestamp': right_record['timestamp'],
                            'joined_at': time.time()
                        }
                        joined_count += 1
                        await asyncio.sleep(0.00001)  # Simulate processing
            else:
                # Join this right record with all left records in window
                for left_idx, left_record in left_window:
                    time_diff = abs(record['timestamp'] - left_record['timestamp'])
                    if time_diff <= config.time_skew_seconds:
                        joined_record = {
                            'left_id': left_record['id'],
                            'right_id': record['id'],
                            'time_diff_seconds': time_diff,
                            'left_timestamp': left_record['timestamp'],
                            'right_timestamp': record['timestamp'],
                            'joined_at': time.time()
                        }
                        joined_count += 1
                        await asyncio.sleep(0.00001)  # Simulate processing

        return joined_count

    async def _perform_range_join(self, left_stream: List[Dict[str, Any]],
                                right_stream: List[Dict[str, Any]],
                                state: Dict[str, Any],
                                config: JoinBenchmarkConfig) -> int:
        """Perform range-based join between streams."""
        joined_count = 0

        # For range joins, we join based on value ranges
        # This simulates things like "find all orders within 10% of this price"

        # Build spatial index for efficient range queries
        value_index = {}
        for record in right_stream:
            value = record['value']
            bucket = value // 100  # Bucket by hundreds
            if bucket not in value_index:
                value_index[bucket] = []
            value_index[bucket].append(record)

        # Perform range joins
        for left_record in left_stream:
            left_value = left_record['value']
            range_min = left_value * 0.9  # ±10% range
            range_max = left_value * 1.1

            # Find buckets that could contain matches
            min_bucket = int(range_min // 100)
            max_bucket = int(range_max // 100)

            potential_matches = []
            for bucket in range(min_bucket, max_bucket + 1):
                potential_matches.extend(value_index.get(bucket, []))

            # Check each potential match
            for right_record in potential_matches:
                right_value = right_record['value']
                if range_min <= right_value <= range_max:
                    # Range condition met
                    joined_record = {
                        'left_id': left_record['id'],
                        'right_id': right_record['id'],
                        'left_value': left_value,
                        'right_value': right_value,
                        'value_ratio': right_value / left_value if left_value != 0 else 0,
                        'joined_at': time.time()
                    }
                    joined_count += 1
                    await asyncio.sleep(0.00001)  # Simulate processing

        return joined_count

    async def run_all_benchmarks(self) -> List[Dict[str, Any]]:
        """Run all join benchmarks."""
        benchmarks = [
            self.benchmark_stream_stream_equality_join,
            self.benchmark_stream_stream_temporal_join,
            self.benchmark_stream_table_join,
            self.benchmark_complex_windowed_join
        ]

        return await self.runner.run_comprehensive_suite(benchmarks)
