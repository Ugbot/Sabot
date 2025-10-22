#!/usr/bin/env python3
"""
Morsel-Driven Operator Benchmarks

Compares performance of:
1. Direct batch processing (baseline)
2. Morsel-driven parallel processing (1, 2, 4, 8 workers)

Operators tested:
- Map (CPU-bound transformation)
- Filter (predicate evaluation)
- Chained operations (map → filter → map)
"""

import time
import asyncio
from sabot import cyarrow as pa
from sabot.cyarrow import compute as pc
from typing import List, Dict, Any
import numpy as np

from sabot._cython.operators.transform import CythonMapOperator, CythonFilterOperator
from sabot._cython.operators.morsel_operator import MorselDrivenOperator


class BenchmarkRunner:
    """Runner for morsel benchmarks"""

    def __init__(self, batch_size: int = 100000):
        self.batch_size = batch_size
        self.results: List[Dict[str, Any]] = []

    def create_test_batch(self) -> pa.RecordBatch:
        """Create test batch with various column types"""
        return pa.RecordBatch.from_pydict({
            'id': list(range(self.batch_size)),
            'value': np.random.uniform(0, 1000, self.batch_size).tolist(),
            'category': np.random.choice(['A', 'B', 'C'], self.batch_size).tolist(),
        })

    def benchmark_direct_processing(
        self,
        operator,
        batch: pa.RecordBatch,
        iterations: int = 10
    ) -> Dict[str, Any]:
        """Benchmark direct batch processing (no morsels)"""

        times = []

        for _ in range(iterations):
            start = time.perf_counter()
            result = operator.process_batch(batch)
            elapsed = time.perf_counter() - start
            times.append(elapsed)

        return {
            'mode': 'direct',
            'workers': 1,
            'mean_time_ms': np.mean(times) * 1000,
            'std_time_ms': np.std(times) * 1000,
            'throughput_rows_per_sec': self.batch_size / np.mean(times),
        }

    async def benchmark_morsel_processing(
        self,
        operator,
        batch: pa.RecordBatch,
        num_workers: int,
        iterations: int = 10
    ) -> Dict[str, Any]:
        """Benchmark morsel-driven parallel processing"""

        # Wrap operator with morsel execution
        morsel_op = MorselDrivenOperator(
            operator,
            num_workers=num_workers,
            morsel_size_kb=64
        )

        times = []

        for _ in range(iterations):
            start = time.perf_counter()
            result = await morsel_op._async_process_with_morsels(batch)
            elapsed = time.perf_counter() - start
            times.append(elapsed)

        # Get stats
        stats = morsel_op.get_stats()

        return {
            'mode': 'morsel',
            'workers': num_workers,
            'mean_time_ms': np.mean(times) * 1000,
            'std_time_ms': np.std(times) * 1000,
            'throughput_rows_per_sec': self.batch_size / np.mean(times),
            'morsels_created': stats.get('total_morsels_created', 0),
            'morsels_per_sec': stats.get('morsels_per_second', 0),
        }

    def run_map_benchmark(self):
        """Benchmark map operator"""
        print("\n=== MAP OPERATOR BENCHMARK ===")

        # Create batch
        batch = self.create_test_batch()

        # Create map operator (CPU-intensive computation)
        def complex_transform(b):
            # Simulate expensive computation
            result = b.append_column(
                'computed',
                pc.add(
                    pc.multiply(b.column('value'), 2.5),
                    pc.power(b.column('value'), 1.5)
                )
            )
            return result

        map_op = CythonMapOperator(source=None, map_func=complex_transform)

        # Benchmark direct processing
        print("\nDirect processing (baseline):")
        direct_result = self.benchmark_direct_processing(map_op, batch)
        self.results.append({'operator': 'map', **direct_result})
        self._print_result(direct_result)

        # Benchmark morsel processing with different worker counts
        for num_workers in [2, 4, 8]:
            print(f"\nMorsel processing ({num_workers} workers):")
            morsel_result = asyncio.run(
                self.benchmark_morsel_processing(map_op, batch, num_workers)
            )
            self.results.append({'operator': 'map', **morsel_result})
            self._print_result(morsel_result)

            # Calculate speedup
            speedup = direct_result['mean_time_ms'] / morsel_result['mean_time_ms']
            print(f"  Speedup: {speedup:.2f}x")

    def run_filter_benchmark(self):
        """Benchmark filter operator"""
        print("\n=== FILTER OPERATOR BENCHMARK ===")

        # Create batch
        batch = self.create_test_batch()

        # Create filter operator
        def complex_predicate(b):
            return pc.and_(
                pc.greater(b.column('value'), 500),
                pc.equal(b.column('category'), 'A')
            )

        filter_op = CythonFilterOperator(source=None, predicate=complex_predicate)

        # Benchmark direct processing
        print("\nDirect processing (baseline):")
        direct_result = self.benchmark_direct_processing(filter_op, batch)
        self.results.append({'operator': 'filter', **direct_result})
        self._print_result(direct_result)

        # Benchmark morsel processing
        for num_workers in [2, 4, 8]:
            print(f"\nMorsel processing ({num_workers} workers):")
            morsel_result = asyncio.run(
                self.benchmark_morsel_processing(filter_op, batch, num_workers)
            )
            self.results.append({'operator': 'filter', **morsel_result})
            self._print_result(morsel_result)

            speedup = direct_result['mean_time_ms'] / morsel_result['mean_time_ms']
            print(f"  Speedup: {speedup:.2f}x")

    def run_chained_benchmark(self):
        """Benchmark chained operators"""
        print("\n=== CHAINED OPERATORS BENCHMARK ===")

        # Create batch
        batch = self.create_test_batch()

        # Create pipeline: map → filter → map
        def transform1(b):
            return b.set_column(1, 'value', pc.multiply(b.column('value'), 2))

        def predicate(b):
            return pc.greater(b.column('value'), 1000)

        def transform2(b):
            return b.append_column('log_value', pc.ln(b.column('value')))

        map1 = CythonMapOperator(source=None, map_func=transform1)
        filter1 = CythonFilterOperator(source=map1, predicate=predicate)
        map2 = CythonMapOperator(source=filter1, map_func=transform2)

        # Benchmark direct processing
        print("\nDirect processing (baseline):")
        direct_result = self.benchmark_direct_processing(map2, batch)
        self.results.append({'operator': 'chained', **direct_result})
        self._print_result(direct_result)

        # Benchmark morsel processing
        for num_workers in [2, 4, 8]:
            print(f"\nMorsel processing ({num_workers} workers):")
            morsel_result = asyncio.run(
                self.benchmark_morsel_processing(map2, batch, num_workers)
            )
            self.results.append({'operator': 'chained', **morsel_result})
            self._print_result(morsel_result)

            speedup = direct_result['mean_time_ms'] / morsel_result['mean_time_ms']
            print(f"  Speedup: {speedup:.2f}x")

    def _print_result(self, result: Dict[str, Any]):
        """Pretty print benchmark result"""
        print(f"  Mean time: {result['mean_time_ms']:.2f} ms")
        print(f"  Throughput: {result['throughput_rows_per_sec']:,.0f} rows/sec")
        if result['mode'] == 'morsel':
            print(f"  Morsels created: {result.get('morsels_created', 0)}")

    def print_summary(self):
        """Print summary of all results"""
        print("\n" + "=" * 60)
        print("BENCHMARK SUMMARY")
        print("=" * 60)

        # Group by operator
        by_operator = {}
        for result in self.results:
            op = result['operator']
            if op not in by_operator:
                by_operator[op] = []
            by_operator[op].append(result)

        # Print summary for each operator
        for op, results in by_operator.items():
            print(f"\n{op.upper()} Operator:")

            # Find baseline
            baseline = next(r for r in results if r['mode'] == 'direct')
            baseline_time = baseline['mean_time_ms']

            print(f"  Baseline time: {baseline_time:.2f} ms")

            # Print speedups
            for result in results:
                if result['mode'] == 'morsel':
                    speedup = baseline_time / result['mean_time_ms']
                    efficiency = (speedup / result['workers']) * 100
                    print(f"    {result['workers']} workers: {speedup:.2f}x speedup "
                          f"({efficiency:.2f}% efficiency)")


def main():
    """Run all benchmarks"""
    print("Morsel-Driven Operator Benchmarks")
    print("=" * 60)
    print(f"Batch size: 100,000 rows")
    print(f"Iterations: 10 per configuration")

    runner = BenchmarkRunner(batch_size=100000)

    # Run benchmarks
    runner.run_map_benchmark()
    runner.run_filter_benchmark()
    runner.run_chained_benchmark()

    # Print summary
    runner.print_summary()


if __name__ == '__main__':
    main()
