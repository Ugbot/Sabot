# -*- coding: utf-8 -*-
"""
Benchmark Runner

Orchestrates comprehensive performance benchmarking against Flink targets.
Validates that Sabot achieves enterprise-grade streaming performance.
"""

import time
import asyncio
import statistics
import json
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class BenchmarkResult:
    """Result of a single benchmark run."""
    name: str
    category: str
    target: Optional[float] = None
    unit: str = ""
    values: List[float] = field(default_factory=list)
    mean: float = 0.0
    median: float = 0.0
    p95: float = 0.0
    p99: float = 0.0
    min_val: float = 0.0
    max_val: float = 0.0
    passed: bool = False
    error: Optional[str] = None

    def calculate_stats(self):
        """Calculate statistics from benchmark values."""
        if not self.values:
            return

        self.mean = statistics.mean(self.values)
        self.median = statistics.median(self.values)
        self.min_val = min(self.values)
        self.max_val = max(self.values)

        if len(self.values) >= 20:  # Need enough samples for percentiles
            sorted_values = sorted(self.values)
            self.p95 = sorted_values[int(len(sorted_values) * 0.95)]
            self.p99 = sorted_values[int(len(sorted_values) * 0.99)]

        # Check if target is met
        if self.target is not None:
            if self.unit in ['ns', 'μs', 'ms', 's']:
                # Lower is better for latency metrics
                self.passed = self.p95 <= self.target
            else:
                # Higher is better for throughput metrics
                self.passed = self.mean >= self.target


@dataclass
class BenchmarkSuite:
    """Collection of benchmark results."""
    name: str
    timestamp: str
    results: List[BenchmarkResult] = field(default_factory=list)
    summary: Dict[str, Any] = field(default_factory=dict)

    def add_result(self, result: BenchmarkResult):
        """Add a benchmark result."""
        result.calculate_stats()
        self.results.append(result)

    def get_summary(self) -> Dict[str, Any]:
        """Get summary of all results."""
        total_tests = len(self.results)
        passed_tests = sum(1 for r in self.results if r.passed)
        failed_tests = total_tests - passed_tests

        # Group by category
        categories = {}
        for result in self.results:
            if result.category not in categories:
                categories[result.category] = {'passed': 0, 'failed': 0, 'total': 0}
            categories[result.category]['total'] += 1
            if result.passed:
                categories[result.category]['passed'] += 1
            else:
                categories[result.category]['failed'] += 1

        return {
            'total_tests': total_tests,
            'passed_tests': passed_tests,
            'failed_tests': failed_tests,
            'pass_rate': passed_tests / total_tests if total_tests > 0 else 0,
            'categories': categories,
            'flink_parity_achieved': passed_tests >= total_tests * 0.8,  # 80% pass rate = Flink parity
        }

    def save_to_file(self, filepath: str):
        """Save benchmark results to JSON file."""
        data = {
            'suite': self.name,
            'timestamp': self.timestamp,
            'summary': self.get_summary(),
            'results': [
                {
                    'name': r.name,
                    'category': r.category,
                    'target': r.target,
                    'unit': r.unit,
                    'mean': r.mean,
                    'median': r.median,
                    'p95': r.p95,
                    'p99': r.p99,
                    'min': r.min_val,
                    'max': r.max_val,
                    'passed': r.passed,
                    'error': r.error,
                    'samples': len(r.values)
                }
                for r in self.results
            ]
        }

        Path(filepath).parent.mkdir(parents=True, exist_ok=True)
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)


class BenchmarkRunner:
    """
    Main benchmark runner for Sabot performance validation.

    Runs comprehensive benchmarks against Flink performance targets:
    - Throughput: 1M+ msg/sec per core
    - Latency: <10ms p99 for processing
    - State Access: <100ns (Arrow), <500ns (Tonbo), <1ms (RocksDB)
    - Checkpoint Time: <5s for 10GB state
    """

    def __init__(self, output_dir: str = "benchmarks/results"):
        """Initialize benchmark runner."""
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Import benchmark modules
        self._import_benchmarks()

    def _import_benchmarks(self):
        """Import available benchmark modules."""
        try:
            from . import state_benchmarks
            self.state_benchmarks = state_benchmarks.StateBenchmarks()
        except ImportError:
            self.state_benchmarks = None

        try:
            from . import stream_benchmarks
            self.stream_benchmarks = stream_benchmarks.StreamBenchmarks()
        except ImportError:
            self.stream_benchmarks = None

        try:
            from . import window_benchmarks
            self.window_benchmarks = window_benchmarks.WindowBenchmarks()
        except ImportError:
            self.window_benchmarks = None

        try:
            from . import join_benchmarks
            self.join_benchmarks = join_benchmarks.JoinBenchmarks()
        except ImportError:
            self.join_benchmarks = None

        try:
            from . import checkpoint_benchmarks
            self.checkpoint_benchmarks = checkpoint_benchmarks.CheckpointBenchmarks()
        except ImportError:
            self.checkpoint_benchmarks = None

        try:
            from . import memory_benchmarks
            self.memory_benchmarks = memory_benchmarks.MemoryBenchmarks()
        except ImportError:
            self.memory_benchmarks = None

        try:
            from . import scalability_benchmarks
            self.scalability_benchmarks = scalability_benchmarks.ScalabilityBenchmarks()
        except ImportError:
            self.scalability_benchmarks = None

    async def run_all_benchmarks(self, iterations: int = 5) -> BenchmarkSuite:
        """
        Run all available benchmarks.

        Args:
            iterations: Number of iterations per benchmark

        Returns:
            Complete benchmark suite with results
        """
        suite = BenchmarkSuite(
            name="Sabot Flink Parity Benchmarks",
            timestamp=time.strftime("%Y-%m-%d %H:%M:%S")
        )

        print("🚀 Running Sabot Performance Benchmarks")
        print("=" * 60)

        # State benchmarks
        if self.state_benchmarks:
            print("📊 Running State Benchmarks...")
            await self._run_state_benchmarks(suite, iterations)

        # Stream processing benchmarks
        if self.stream_benchmarks:
            print("🌊 Running Stream Processing Benchmarks...")
            await self._run_stream_benchmarks(suite, iterations)

        # Windowing benchmarks
        if self.window_benchmarks:
            print("🪟 Running Windowing Benchmarks...")
            await self._run_window_benchmarks(suite, iterations)

        # Join benchmarks
        if self.join_benchmarks:
            print("🔗 Running Join Benchmarks...")
            await self._run_join_benchmarks(suite, iterations)

        # Checkpoint benchmarks
        if self.checkpoint_benchmarks:
            print("💾 Running Checkpoint Benchmarks...")
            await self._run_checkpoint_benchmarks(suite, iterations)

        # Memory benchmarks
        if self.memory_benchmarks:
            print("🧠 Running Memory Benchmarks...")
            await self._run_memory_benchmarks(suite, iterations)

        # Scalability benchmarks
        if self.scalability_benchmarks:
            print("📈 Running Scalability Benchmarks...")
            await self._run_scalability_benchmarks(suite, iterations)

        # Generate summary
        summary = suite.get_summary()
        suite.summary = summary

        # Save results
        output_file = self.output_dir / f"benchmark_results_{int(time.time())}.json"
        suite.save_to_file(str(output_file))

        # Print summary
        self._print_summary(suite)

        return suite

    async def _run_state_benchmarks(self, suite: BenchmarkSuite, iterations: int):
        """Run state management benchmarks."""
        if not self.state_benchmarks:
            return

        # ValueState operations
        result = await self.state_benchmarks.benchmark_value_state(iterations)
        suite.add_result(result)

        # ListState operations
        result = await self.state_benchmarks.benchmark_list_state(iterations)
        suite.add_result(result)

        # MapState operations
        result = await self.state_benchmarks.benchmark_map_state(iterations)
        suite.add_result(result)

        # RocksDB state operations
        result = await self.state_benchmarks.benchmark_rocksdb_state(iterations)
        suite.add_result(result)

    async def _run_stream_benchmarks(self, suite: BenchmarkSuite, iterations: int):
        """Run stream processing benchmarks."""
        if not self.stream_benchmarks:
            return

        # Basic stream processing throughput
        result = await self.stream_benchmarks.benchmark_stream_throughput(iterations)
        suite.add_result(result)

        # Stream processing latency
        result = await self.stream_benchmarks.benchmark_stream_latency(iterations)
        suite.add_result(result)

        # Arrow batch processing
        result = await self.stream_benchmarks.benchmark_arrow_processing(iterations)
        suite.add_result(result)

    async def _run_window_benchmarks(self, suite: BenchmarkSuite, iterations: int):
        """Run windowing benchmarks."""
        if not self.window_benchmarks:
            return

        # Tumbling window performance
        result = await self.window_benchmarks.benchmark_tumbling_windows(iterations)
        suite.add_result(result)

        # Sliding window performance
        result = await self.window_benchmarks.benchmark_sliding_windows(iterations)
        suite.add_result(result)

        # Window aggregation performance
        result = await self.window_benchmarks.benchmark_window_aggregation(iterations)
        suite.add_result(result)

    async def _run_join_benchmarks(self, suite: BenchmarkSuite, iterations: int):
        """Run join benchmarks."""
        if not self.join_benchmarks:
            return

        # Stream-table join performance
        result = await self.join_benchmarks.benchmark_stream_table_join(iterations)
        suite.add_result(result)

        # Stream-stream join performance
        result = await self.join_benchmarks.benchmark_stream_stream_join(iterations)
        suite.add_result(result)

        # Interval join performance
        result = await self.join_benchmarks.benchmark_interval_join(iterations)
        suite.add_result(result)

    async def _run_checkpoint_benchmarks(self, suite: BenchmarkSuite, iterations: int):
        """Run checkpoint benchmarks."""
        if not self.checkpoint_benchmarks:
            return

        # Checkpoint creation time
        result = await self.checkpoint_benchmarks.benchmark_checkpoint_creation(iterations)
        suite.add_result(result)

        # Checkpoint recovery time
        result = await self.checkpoint_benchmarks.benchmark_checkpoint_recovery(iterations)
        suite.add_result(result)

        # Incremental checkpoint performance
        result = await self.checkpoint_benchmarks.benchmark_incremental_checkpoint(iterations)
        suite.add_result(result)

    async def _run_memory_benchmarks(self, suite: BenchmarkSuite, iterations: int):
        """Run memory usage benchmarks."""
        if not self.memory_benchmarks:
            return

        # Memory usage per operation
        result = await self.memory_benchmarks.benchmark_memory_per_operation(iterations)
        suite.add_result(result)

        # Peak memory usage
        result = await self.memory_benchmarks.benchmark_peak_memory_usage(iterations)
        suite.add_result(result)

    async def _run_scalability_benchmarks(self, suite: BenchmarkSuite, iterations: int):
        """Run scalability benchmarks."""
        if not self.scalability_benchmarks:
            return

        # Multi-core scalability
        result = await self.scalability_benchmarks.benchmark_multi_core_scaling(iterations)
        suite.add_result(result)

        # Memory scaling with data size
        result = await self.scalability_benchmarks.benchmark_memory_scaling(iterations)
        suite.add_result(result)

    def _print_summary(self, suite: BenchmarkSuite):
        """Print benchmark summary."""
        summary = suite.get_summary()

        print("\n" + "=" * 60)
        print("📊 BENCHMARK SUMMARY")
        print("=" * 60)

        print(f"Total Tests: {summary['total_tests']}")
        print(f"Passed: {summary['passed_tests']}")
        print(f"Failed: {summary['failed_tests']}")
        print(".1%")

        if summary['flink_parity_achieved']:
            print("🎉 FLINK PARITY ACHIEVED! 🚀")
        else:
            print("⚠️  Flink parity not yet achieved")

        print("\n📈 Category Breakdown:")
        for category, stats in summary['categories'].items():
            pass_rate = stats['passed'] / stats['total'] if stats['total'] > 0 else 0
            print(".1%")

        # Show top failures
        failed_results = [r for r in suite.results if not r.passed and r.target is not None]
        if failed_results:
            print("\n❌ Top Performance Gaps:")
            failed_results.sort(key=lambda x: x.target / x.p95 if x.p95 > 0 else float('inf'))
            for result in failed_results[:5]:
                if result.unit in ['ns', 'μs', 'ms', 's']:
                    gap = result.p95 / result.target if result.target > 0 else float('inf')
                    print(".2f")
                else:
                    gap = result.target / result.mean if result.mean > 0 else 0
                    print(".2f")
        print(f"\n💾 Results saved to: {self.output_dir}")
        print("=" * 60)


async def main():
    """Run benchmarks from command line."""
    import argparse

    parser = argparse.ArgumentParser(description="Sabot Performance Benchmarks")
    parser.add_argument("--iterations", type=int, default=5,
                       help="Number of iterations per benchmark")
    parser.add_argument("--output-dir", type=str, default="benchmarks/results",
                       help="Output directory for results")

    args = parser.parse_args()

    runner = BenchmarkRunner(args.output_dir)
    await runner.run_all_benchmarks(args.iterations)


if __name__ == "__main__":
    asyncio.run(main())