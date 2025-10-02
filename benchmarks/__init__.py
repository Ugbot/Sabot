# -*- coding: utf-8 -*-
"""
Sabot Performance Benchmarks

Comprehensive benchmarking suite to validate Flink-level performance targets.
Tests throughput, latency, memory usage, and scalability across all components.
"""

__all__ = [
    'BenchmarkRunner',
    'StateBenchmarks',
    'StreamBenchmarks',
    'WindowBenchmarks',
    'JoinBenchmarks',
    'CheckpointBenchmarks',
    'MemoryBenchmarks',
    'ScalabilityBenchmarks',
]

# Import benchmark modules
try:
    from .runner import BenchmarkRunner
    from .state_benchmarks import StateBenchmarks
    from .stream_benchmarks import StreamBenchmarks
    from .window_benchmarks import WindowBenchmarks
    from .join_benchmarks import JoinBenchmarks
    from .checkpoint_benchmarks import CheckpointBenchmarks
    from .memory_benchmarks import MemoryBenchmarks
    from .scalability_benchmarks import ScalabilityBenchmarks

    BENCHMARKS_AVAILABLE = True

except ImportError:
    BENCHMARKS_AVAILABLE = False

    # Fallback classes
    class BenchmarkRunner:
        """Fallback benchmark runner."""
        pass