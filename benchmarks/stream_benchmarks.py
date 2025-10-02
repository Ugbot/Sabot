#!/usr/bin/env python3
"""
Stream Processing Benchmarks for Sabot

Benchmarks various stream processing scenarios including:
- Message throughput
- Processing latency
- Memory usage
- CPU utilization
- Scalability with concurrency
"""

import asyncio
import time
import random
import psutil
from typing import Dict, Any, List
from dataclasses import dataclass

from .runner import BenchmarkRunner, BenchmarkConfig, benchmark


@dataclass
class StreamBenchmarkConfig:
    """Configuration for stream processing benchmarks."""
    message_count: int = 10000
    message_size_bytes: int = 1024
    batch_size: int = 100
    concurrency: int = 1
    processing_complexity: str = "simple"  # simple, medium, complex


class StreamProcessingBenchmark:
    """Benchmark suite for stream processing performance."""

    def __init__(self):
        self.runner = BenchmarkRunner()

    @benchmark(BenchmarkConfig(
        name="stream_throughput_simple",
        description="Measure throughput for simple stream processing",
        iterations=3,
        warmup_iterations=1,
        concurrency=1
    ))
    async def benchmark_simple_throughput(self) -> Dict[str, Any]:
        """Benchmark simple message processing throughput."""
        config = StreamBenchmarkConfig(
            message_count=50000,
            message_size_bytes=512,
            processing_complexity="simple"
        )

        return await self._run_stream_benchmark(config)

    @benchmark(BenchmarkConfig(
        name="stream_throughput_complex",
        description="Measure throughput for complex stream processing",
        iterations=3,
        warmup_iterations=1,
        concurrency=1
    ))
    async def benchmark_complex_throughput(self) -> Dict[str, Any]:
        """Benchmark complex message processing throughput."""
        config = StreamBenchmarkConfig(
            message_count=10000,
            message_size_bytes=2048,
            processing_complexity="complex"
        )

        return await self._run_stream_benchmark(config)

    @benchmark(BenchmarkConfig(
        name="stream_concurrency_scaling",
        description="Measure performance scaling with concurrency",
        iterations=3,
        warmup_iterations=1,
        concurrency=4
    ))
    async def benchmark_concurrency_scaling(self) -> Dict[str, Any]:
        """Benchmark how performance scales with concurrency."""
        config = StreamBenchmarkConfig(
            message_count=25000,
            message_size_bytes=1024,
            concurrency=4,
            processing_complexity="medium"
        )

        return await self._run_stream_benchmark(config)

    @benchmark(BenchmarkConfig(
        name="stream_memory_efficiency",
        description="Measure memory usage during stream processing",
        iterations=2,
        warmup_iterations=1,
        concurrency=1
    ))
    async def benchmark_memory_usage(self) -> Dict[str, Any]:
        """Benchmark memory efficiency of stream processing."""
        config = StreamBenchmarkConfig(
            message_count=100000,
            message_size_bytes=256,
            processing_complexity="simple"
        )

        # Track memory before
        process = psutil.Process()
        memory_before = process.memory_info().rss

        result = await self._run_stream_benchmark(config)

        # Track memory after
        memory_after = process.memory_info().rss
        memory_delta = memory_after - memory_before

        result['memory_before_mb'] = memory_before / (1024 * 1024)
        result['memory_after_mb'] = memory_after / (1024 * 1024)
        result['memory_delta_mb'] = memory_delta / (1024 * 1024)

        return result

    async def _run_stream_benchmark(self, config: StreamBenchmarkConfig) -> Dict[str, Any]:
        """Run a stream processing benchmark with given configuration."""
        # Generate test messages
        messages = self._generate_test_messages(config.message_count, config.message_size_bytes)

        start_time = time.time()
        processed_count = 0
        error_count = 0

        # Process messages
        if config.concurrency == 1:
            # Single-threaded processing
            for message in messages:
                try:
                    await self._process_message(message, config.processing_complexity)
                    processed_count += 1
                except Exception:
                    error_count += 1
        else:
            # Concurrent processing
            semaphore = asyncio.Semaphore(config.concurrency)

            async def process_with_semaphore(msg):
                async with semaphore:
                    try:
                        await self._process_message(msg, config.processing_complexity)
                        return True
                    except Exception:
                        return False

            tasks = [process_with_semaphore(msg) for msg in messages]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            processed_count = sum(1 for r in results if r is True)
            error_count = sum(1 for r in results if r is False)

        end_time = time.time()

        total_time = end_time - start_time
        throughput = processed_count / total_time if total_time > 0 else 0

        return {
            'total_messages': len(messages),
            'processed_messages': processed_count,
            'error_messages': error_count,
            'total_time_seconds': total_time,
            'throughput_msg_per_sec': throughput,
            'avg_latency_ms': (total_time / processed_count * 1000) if processed_count > 0 else 0,
            'concurrency': config.concurrency,
            'message_size_bytes': config.message_size_bytes,
            'processing_complexity': config.processing_complexity
        }

    def _generate_test_messages(self, count: int, size_bytes: int) -> List[Dict[str, Any]]:
        """Generate test messages for benchmarking."""
        messages = []

        for i in range(count):
            # Generate realistic message content
            message = {
                'id': f"msg_{i}",
                'timestamp': time.time(),
                'user_id': f"user_{random.randint(1, 1000)}",
                'event_type': random.choice(['click', 'view', 'purchase', 'search']),
                'data': 'x' * size_bytes,  # Payload of specified size
                'metadata': {
                    'source': 'benchmark',
                    'version': '1.0',
                    'priority': random.randint(1, 5)
                }
            }
            messages.append(message)

        return messages

    async def _process_message(self, message: Dict[str, Any], complexity: str) -> None:
        """Process a single message with specified complexity."""
        if complexity == "simple":
            # Simple processing - just validate and transform
            if 'id' in message and 'timestamp' in message:
                message['processed'] = True
                message['processed_at'] = time.time()
            else:
                raise ValueError("Invalid message format")

        elif complexity == "medium":
            # Medium complexity - some computation and validation
            await asyncio.sleep(0.001)  # Simulate I/O delay

            # Validate message structure
            required_fields = ['id', 'timestamp', 'user_id', 'event_type']
            for field in required_fields:
                if field not in message:
                    raise ValueError(f"Missing required field: {field}")

            # Perform some computation
            user_hash = hash(message['user_id']) % 1000
            event_score = len(message.get('data', '')) + message.get('metadata', {}).get('priority', 1)

            message['processed'] = True
            message['user_hash'] = user_hash
            message['event_score'] = event_score
            message['processed_at'] = time.time()

        elif complexity == "complex":
            # Complex processing - multiple async operations, data transformation
            await asyncio.sleep(0.002)  # Simulate network call

            # Complex validation
            if not self._validate_complex_message(message):
                raise ValueError("Complex validation failed")

            # Simulate data enrichment
            await self._enrich_message_data(message)

            # Simulate state lookup
            user_state = await self._lookup_user_state(message['user_id'])

            # Complex computation
            risk_score = self._calculate_risk_score(message, user_state)
            recommendation = self._generate_recommendation(message, risk_score)

            message['processed'] = True
            message['user_state'] = user_state
            message['risk_score'] = risk_score
            message['recommendation'] = recommendation
            message['processed_at'] = time.time()

    def _validate_complex_message(self, message: Dict[str, Any]) -> bool:
        """Complex message validation."""
        # Simulate complex validation logic
        return (
            'id' in message and
            'timestamp' in message and
            'user_id' in message and
            isinstance(message.get('metadata', {}), dict) and
            len(message.get('data', '')) > 0
        )

    async def _enrich_message_data(self, message: Dict[str, Any]) -> None:
        """Simulate data enrichment from external service."""
        await asyncio.sleep(0.001)  # Simulate API call

        # Add enriched data
        message['enriched'] = {
            'geolocation': 'US',
            'device_type': 'mobile',
            'session_count': random.randint(1, 100)
        }

    async def _lookup_user_state(self, user_id: str) -> Dict[str, Any]:
        """Simulate user state lookup."""
        await asyncio.sleep(0.0005)  # Simulate database lookup

        return {
            'login_count': random.randint(1, 1000),
            'last_login': time.time() - random.randint(3600, 86400 * 30),
            'account_status': random.choice(['active', 'inactive', 'suspended']),
            'risk_level': random.choice(['low', 'medium', 'high'])
        }

    def _calculate_risk_score(self, message: Dict[str, Any], user_state: Dict[str, Any]) -> float:
        """Calculate risk score for the message."""
        base_score = 0.1

        # Risk factors
        if user_state['risk_level'] == 'high':
            base_score += 0.3
        elif user_state['risk_level'] == 'medium':
            base_score += 0.1

        if message['event_type'] == 'purchase':
            amount = len(message.get('data', ''))  # Simulate amount from data size
            if amount > 1000:  # High value purchase
                base_score += 0.2

        if user_state['login_count'] < 10:
            base_score += 0.1  # New user risk

        return min(base_score, 1.0)  # Cap at 1.0

    def _generate_recommendation(self, message: Dict[str, Any], risk_score: float) -> str:
        """Generate recommendation based on message and risk."""
        if risk_score > 0.7:
            return "BLOCK_TRANSACTION"
        elif risk_score > 0.4:
            return "REQUIRE_ADDITIONAL_VERIFICATION"
        elif message['event_type'] == 'purchase':
            return "OFFER_LOYALTY_POINTS"
        else:
            return "ALLOW_NORMAL_PROCESSING"

    async def run_all_benchmarks(self) -> List[Dict[str, Any]]:
        """Run all stream processing benchmarks."""
        benchmarks = [
            self.benchmark_simple_throughput,
            self.benchmark_complex_throughput,
            self.benchmark_concurrency_scaling,
            self.benchmark_memory_usage
        ]

        return await self.runner.run_comprehensive_suite(benchmarks)
