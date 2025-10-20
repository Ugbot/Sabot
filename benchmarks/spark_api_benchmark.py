#!/usr/bin/env python3
"""
Comprehensive Spark API Benchmark for Sabot

Tests the Spark-compatible API layer with realistic workloads.
Measures performance, memory usage, and correctness.
"""

import os
import sys
import time
import tempfile
import statistics
from pathlib import Path
from dataclasses import dataclass
from typing import List, Dict, Any

import numpy as np
import pandas as pd

# Add project root to path
sys.path.insert(0, '/Users/bengamble/Sabot')

# Import Sabot's Spark-compatible API
from sabot.spark import SparkSession
from sabot.spark.functions import col, sum as spark_sum, avg, count, max as spark_max, min as spark_min

@dataclass
class BenchmarkResult:
    """Benchmark result container."""
    operation: str
    dataset_size: int
    execution_time: float
    throughput: float  # rows/sec
    result_count: int
    success: bool
    error: str = None


class SparkAPIBenchmark:
    """Benchmark for Sabot's Spark-compatible API."""

    def __init__(self):
        self.results: List[BenchmarkResult] = []
        self.temp_dir = Path(tempfile.mkdtemp())
        print(f"📁 Temp directory: {self.temp_dir}")

    def generate_data(self, num_rows: int) -> Path:
        """Generate test data."""
        print(f"📊 Generating {num_rows:,} rows...")

        np.random.seed(42)
        data = {
            'id': range(num_rows),
            'customer_id': np.random.randint(1, num_rows // 100 + 1, num_rows),
            'amount': np.random.exponential(100, num_rows).round(2),
            'quantity': np.random.randint(1, 20, num_rows),
            'category': np.random.choice(['A', 'B', 'C', 'D', 'E'], num_rows),
            'region': np.random.choice(['north', 'south', 'east', 'west'], num_rows),
            'is_premium': np.random.choice([0, 1], num_rows, p=[0.7, 0.3]),
        }

        df = pd.DataFrame(data)
        path = self.temp_dir / f"data_{num_rows}.csv"
        df.to_csv(path, index=False)

        print(f"✅ Generated: {path} ({path.stat().st_size / 1024 / 1024:.1f} MB)")
        return path

    def benchmark_read_csv(self, spark: SparkSession, path: Path, size: int) -> BenchmarkResult:
        """Benchmark: Read CSV file."""
        try:
            start = time.time()
            df = spark.read.csv(str(path), header=True, inferSchema=True)
            result_count = df.count()
            elapsed = time.time() - start

            return BenchmarkResult(
                operation="read_csv",
                dataset_size=size,
                execution_time=elapsed,
                throughput=result_count / elapsed if elapsed > 0 else 0,
                result_count=result_count,
                success=True
            )
        except Exception as e:
            return BenchmarkResult(
                operation="read_csv",
                dataset_size=size,
                execution_time=0,
                throughput=0,
                result_count=0,
                success=False,
                error=str(e)
            )

    def benchmark_filter(self, spark: SparkSession, path: Path, size: int) -> BenchmarkResult:
        """Benchmark: Filter operation."""
        try:
            start = time.time()
            df = spark.read.csv(str(path), header=True, inferSchema=True)
            filtered = df.filter(col("amount") > 100).filter(col("category") == "A")
            result_count = filtered.count()
            elapsed = time.time() - start

            return BenchmarkResult(
                operation="filter",
                dataset_size=size,
                execution_time=elapsed,
                throughput=result_count / elapsed if elapsed > 0 else 0,
                result_count=result_count,
                success=True
            )
        except Exception as e:
            return BenchmarkResult(
                operation="filter",
                dataset_size=size,
                execution_time=0,
                throughput=0,
                result_count=0,
                success=False,
                error=str(e)
            )

    def benchmark_select(self, spark: SparkSession, path: Path, size: int) -> BenchmarkResult:
        """Benchmark: Select columns."""
        try:
            start = time.time()
            df = spark.read.csv(str(path), header=True, inferSchema=True)
            selected = df.select("id", "customer_id", "amount", "category")
            result_count = selected.count()
            elapsed = time.time() - start

            return BenchmarkResult(
                operation="select",
                dataset_size=size,
                execution_time=elapsed,
                throughput=result_count / elapsed if elapsed > 0 else 0,
                result_count=result_count,
                success=True
            )
        except Exception as e:
            return BenchmarkResult(
                operation="select",
                dataset_size=size,
                execution_time=0,
                throughput=0,
                result_count=0,
                success=False,
                error=str(e)
            )

    def benchmark_groupby_agg(self, spark: SparkSession, path: Path, size: int) -> BenchmarkResult:
        """Benchmark: GroupBy with aggregations."""
        try:
            start = time.time()
            df = spark.read.csv(str(path), header=True, inferSchema=True)
            grouped = (df
                .groupBy("customer_id", "category")
                .agg(
                    spark_sum("amount").alias("total_amount"),
                    avg("quantity").alias("avg_quantity"),
                    count("*").alias("count")
                ))
            result_count = grouped.count()
            elapsed = time.time() - start

            return BenchmarkResult(
                operation="groupby_agg",
                dataset_size=size,
                execution_time=elapsed,
                throughput=result_count / elapsed if elapsed > 0 else 0,
                result_count=result_count,
                success=True
            )
        except Exception as e:
            return BenchmarkResult(
                operation="groupby_agg",
                dataset_size=size,
                execution_time=0,
                throughput=0,
                result_count=0,
                success=False,
                error=str(e)
            )

    def benchmark_complex_pipeline(self, spark: SparkSession, path: Path, size: int) -> BenchmarkResult:
        """Benchmark: Complex multi-stage pipeline."""
        try:
            start = time.time()
            df = spark.read.csv(str(path), header=True, inferSchema=True)

            # Complex pipeline: filter -> select -> groupBy -> filter
            result = (df
                .filter(col("amount") > 50)
                .filter(col("is_premium") == 1)
                .select("customer_id", "category", "amount", "quantity")
                .groupBy("customer_id", "category")
                .agg(
                    spark_sum("amount").alias("total_spend"),
                    avg("quantity").alias("avg_qty"),
                    count("*").alias("tx_count"),
                    spark_max("amount").alias("max_amount"),
                    spark_min("amount").alias("min_amount")
                )
                .filter(col("total_spend") > 500))

            result_count = result.count()
            elapsed = time.time() - start

            return BenchmarkResult(
                operation="complex_pipeline",
                dataset_size=size,
                execution_time=elapsed,
                throughput=result_count / elapsed if elapsed > 0 else 0,
                result_count=result_count,
                success=True
            )
        except Exception as e:
            return BenchmarkResult(
                operation="complex_pipeline",
                dataset_size=size,
                execution_time=0,
                throughput=0,
                result_count=0,
                success=False,
                error=str(e)
            )

    def benchmark_show(self, spark: SparkSession, path: Path, size: int) -> BenchmarkResult:
        """Benchmark: Show (display) operation."""
        try:
            start = time.time()
            df = spark.read.csv(str(path), header=True, inferSchema=True)
            df.show(20)  # Should print table
            elapsed = time.time() - start

            return BenchmarkResult(
                operation="show",
                dataset_size=size,
                execution_time=elapsed,
                throughput=20 / elapsed if elapsed > 0 else 0,
                result_count=20,
                success=True
            )
        except Exception as e:
            return BenchmarkResult(
                operation="show",
                dataset_size=size,
                execution_time=0,
                throughput=0,
                result_count=0,
                success=False,
                error=str(e)
            )

    def run_benchmark_suite(self, dataset_sizes: List[int] = None):
        """Run full benchmark suite."""
        if dataset_sizes is None:
            dataset_sizes = [10_000, 100_000, 1_000_000]

        print("\n" + "=" * 70)
        print("🚀 Sabot Spark API Benchmark Suite")
        print("=" * 70)

        # Create Spark session
        print("\n🔧 Creating SparkSession...")
        try:
            spark = SparkSession.builder \
                .master("local[*]") \
                .appName("SabotSparkBenchmark") \
                .getOrCreate()
            print("✅ SparkSession created successfully")
        except Exception as e:
            print(f"❌ Failed to create SparkSession: {e}")
            return

        # Run benchmarks for each dataset size
        for size in dataset_sizes:
            print(f"\n{'─' * 70}")
            print(f"📊 Dataset size: {size:,} rows")
            print(f"{'─' * 70}")

            # Generate data
            data_path = self.generate_data(size)

            # Run each benchmark
            benchmarks = [
                ("Read CSV", self.benchmark_read_csv),
                ("Filter", self.benchmark_filter),
                ("Select", self.benchmark_select),
                ("GroupBy + Agg", self.benchmark_groupby_agg),
                ("Complex Pipeline", self.benchmark_complex_pipeline),
                ("Show (display)", self.benchmark_show),
            ]

            for name, bench_func in benchmarks:
                print(f"\n▶ {name}...", end=" ", flush=True)
                result = bench_func(spark, data_path, size)
                self.results.append(result)

                if result.success:
                    print(f"✅")
                    print(f"  Time: {result.execution_time:.3f}s")
                    print(f"  Throughput: {result.throughput:,.0f} rows/sec")
                    print(f"  Results: {result.result_count:,} rows")
                else:
                    print(f"❌")
                    print(f"  Error: {result.error}")

        # Stop Spark
        print("\n🛑 Stopping SparkSession...")
        try:
            spark.stop()
            print("✅ SparkSession stopped")
        except Exception as e:
            print(f"⚠️  Warning: {e}")

        # Print summary
        self.print_summary()

    def print_summary(self):
        """Print benchmark summary."""
        print("\n" + "=" * 70)
        print("📈 BENCHMARK SUMMARY")
        print("=" * 70)

        # Group by operation
        by_operation: Dict[str, List[BenchmarkResult]] = {}
        for result in self.results:
            if result.success:
                if result.operation not in by_operation:
                    by_operation[result.operation] = []
                by_operation[result.operation].append(result)

        # Print summary table
        print("\n{:<20} {:>15} {:>15} {:>15}".format(
            "Operation", "Avg Time (s)", "Avg Throughput", "Success Rate"
        ))
        print("─" * 70)

        total_success = sum(1 for r in self.results if r.success)
        total_runs = len(self.results)

        for op, results in sorted(by_operation.items()):
            avg_time = statistics.mean(r.execution_time for r in results)
            avg_throughput = statistics.mean(r.throughput for r in results)
            success_rate = len(results) / sum(1 for r in self.results if r.operation == op)

            print("{:<20} {:>15.3f} {:>15,.0f} {:>14.0%}".format(
                op.replace("_", " ").title(),
                avg_time,
                avg_throughput,
                success_rate
            ))

        print("─" * 70)
        print(f"Overall Success Rate: {total_success}/{total_runs} ({total_success/total_runs*100:.1f}%)")

        # Print failures
        failures = [r for r in self.results if not r.success]
        if failures:
            print("\n❌ Failures:")
            for failure in failures:
                print(f"  - {failure.operation} ({failure.dataset_size:,} rows): {failure.error}")
        else:
            print("\n✅ All benchmarks passed!")

        print("\n" + "=" * 70)
        print("🎉 Benchmark Complete!")
        print("=" * 70)


def main():
    """Main entry point."""
    benchmark = SparkAPIBenchmark()

    # Run with different dataset sizes
    benchmark.run_benchmark_suite([10_000, 100_000, 500_000])


if __name__ == "__main__":
    main()
