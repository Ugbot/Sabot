#!/usr/bin/env python3
"""
Comprehensive Sabot vs PySpark Performance Benchmark

Focus: Zero-copy operations and direct performance comparison.
Uses properly typed in-memory data to avoid I/O overhead.
"""

import sys
import time
import statistics
from typing import List, Dict
from dataclasses import dataclass

import numpy as np
import pyarrow as pa

sys.path.insert(0, '/Users/bengamble/Sabot')

# Optional PySpark import
try:
    from pyspark.sql import SparkSession as PySparkSession
    from pyspark.sql.functions import col, sum as pyspark_sum, avg as pyspark_avg, count as pyspark_count
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False
    print("⚠️  PySpark not available - install with: uv pip install pyspark")

# Sabot Spark API
from sabot.spark import SparkSession as SabotSparkSession
from sabot.spark.functions import col as sabot_col, sum as sabot_sum, avg as sabot_avg, count as sabot_count


@dataclass
class BenchmarkResult:
    """Benchmark result."""
    system: str
    operation: str
    dataset_size: int
    execution_time: float
    throughput: float
    success: bool
    error: str = None


def generate_arrow_table(num_rows: int) -> pa.Table:
    """Generate properly-typed Arrow table."""
    np.random.seed(42)

    data = {
        'id': pa.array(range(num_rows), type=pa.int64()),
        'customer_id': pa.array(np.random.randint(1, num_rows // 100 + 1, num_rows), type=pa.int64()),
        'amount': pa.array(np.random.exponential(100, num_rows).round(2), type=pa.float64()),
        'quantity': pa.array(np.random.randint(1, 20, num_rows), type=pa.int32()),
        'category': pa.array(np.random.choice(['A', 'B', 'C', 'D', 'E'], num_rows), type=pa.string()),
        'region': pa.array(np.random.choice(['north', 'south', 'east', 'west'], num_rows), type=pa.string()),
        'is_premium': pa.array(np.random.choice([0, 1], num_rows, p=[0.7, 0.3]), type=pa.int8()),
    }

    return pa.table(data)


def benchmark_pyspark(table: pa.Table, num_rows: int) -> List[BenchmarkResult]:
    """Run PySpark benchmarks."""
    results = []

    if not PYSPARK_AVAILABLE:
        return results

    print("\n🔥 PySpark Benchmarks")
    print("─" * 60)

    # Create Spark session
    spark = PySparkSession.builder \
        .appName("PySparkBenchmark") \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()

    # Convert Arrow to Spark DataFrame
    df = spark.createDataFrame(table.to_pandas())

    # Benchmark 1: Filter
    try:
        start = time.time()
        filtered = df.filter((col("amount") > 100) & (col("category") == "A"))
        result_count = filtered.count()
        elapsed = time.time() - start

        results.append(BenchmarkResult(
            system="PySpark",
            operation="filter",
            dataset_size=num_rows,
            execution_time=elapsed,
            throughput=result_count / elapsed if elapsed > 0 else 0,
            success=True
        ))
        print(f"✅ Filter: {elapsed:.3f}s ({result_count:,} rows, {result_count/elapsed:,.0f} rows/sec)")
    except Exception as e:
        print(f"❌ Filter failed: {e}")
        results.append(BenchmarkResult("PySpark", "filter", num_rows, 0, 0, False, str(e)))

    # Benchmark 2: GroupBy + Agg
    try:
        start = time.time()
        grouped = df.groupBy("customer_id", "category").agg(
            pyspark_sum("amount").alias("total_amount"),
            pyspark_avg("quantity").alias("avg_quantity"),
            pyspark_count("*").alias("count")
        )
        result_count = grouped.count()
        elapsed = time.time() - start

        results.append(BenchmarkResult(
            system="PySpark",
            operation="groupby_agg",
            dataset_size=num_rows,
            execution_time=elapsed,
            throughput=result_count / elapsed if elapsed > 0 else 0,
            success=True
        ))
        print(f"✅ GroupBy+Agg: {elapsed:.3f}s ({result_count:,} groups, {result_count/elapsed:,.0f} groups/sec)")
    except Exception as e:
        print(f"❌ GroupBy+Agg failed: {e}")
        results.append(BenchmarkResult("PySpark", "groupby_agg", num_rows, 0, 0, False, str(e)))

    # Benchmark 3: Select (projection)
    try:
        start = time.time()
        selected = df.select("id", "customer_id", "amount", "category")
        result_count = selected.count()
        elapsed = time.time() - start

        results.append(BenchmarkResult(
            system="PySpark",
            operation="select",
            dataset_size=num_rows,
            execution_time=elapsed,
            throughput=result_count / elapsed if elapsed > 0 else 0,
            success=True
        ))
        print(f"✅ Select: {elapsed:.3f}s ({result_count:,} rows, {result_count/elapsed:,.0f} rows/sec)")
    except Exception as e:
        print(f"❌ Select failed: {e}")
        results.append(BenchmarkResult("PySpark", "select", num_rows, 0, 0, False, str(e)))

    # Benchmark 4: Complex pipeline
    try:
        start = time.time()
        result = (df
            .filter(col("amount") > 50)
            .filter(col("is_premium") == 1)
            .select("customer_id", "category", "amount", "quantity")
            .groupBy("customer_id", "category")
            .agg(
                pyspark_sum("amount").alias("total_spend"),
                pyspark_avg("quantity").alias("avg_qty"),
                pyspark_count("*").alias("tx_count")
            )
            .filter(col("total_spend") > 500))
        result_count = result.count()
        elapsed = time.time() - start

        results.append(BenchmarkResult(
            system="PySpark",
            operation="complex_pipeline",
            dataset_size=num_rows,
            execution_time=elapsed,
            throughput=result_count / elapsed if elapsed > 0 else 0,
            success=True
        ))
        print(f"✅ Complex Pipeline: {elapsed:.3f}s ({result_count:,} results, {result_count/elapsed:,.0f} results/sec)")
    except Exception as e:
        print(f"❌ Complex Pipeline failed: {e}")
        results.append(BenchmarkResult("PySpark", "complex_pipeline", num_rows, 0, 0, False, str(e)))

    spark.stop()
    return results


def benchmark_sabot(table: pa.Table, num_rows: int) -> List[BenchmarkResult]:
    """Run Sabot Spark API benchmarks."""
    results = []

    print("\n⚡ Sabot Benchmarks")
    print("─" * 60)

    # Create Spark session
    spark = SabotSparkSession.builder \
        .appName("SabotBenchmark") \
        .master("local[*]") \
        .getOrCreate()

    # Convert Arrow to Sabot DataFrame
    df = spark.createDataFrame(table)

    # Benchmark 1: Filter
    try:
        start = time.time()
        filtered = df.filter((sabot_col("amount") > 50) & (sabot_col("category") == "A"))
        result_count = filtered.count()
        elapsed = time.time() - start

        results.append(BenchmarkResult(
            system="Sabot",
            operation="filter",
            dataset_size=num_rows,
            execution_time=elapsed,
            throughput=result_count / elapsed if elapsed > 0 else 0,
            success=True
        ))
        print(f"✅ Filter: {elapsed:.3f}s ({result_count:,} rows, {result_count/elapsed:,.0f} rows/sec)")
    except Exception as e:
        print(f"❌ Filter failed: {e}")
        results.append(BenchmarkResult("Sabot", "filter", num_rows, 0, 0, False, str(e)))

    # Benchmark 2: Select (projection - zero-copy!)
    try:
        start = time.time()
        selected = df.select("id", "customer_id", "amount", "category")
        result_count = selected.count()
        elapsed = time.time() - start

        results.append(BenchmarkResult(
            system="Sabot",
            operation="select",
            dataset_size=num_rows,
            execution_time=elapsed,
            throughput=result_count / elapsed if elapsed > 0 else 0,
            success=True
        ))
        print(f"✅ Select: {elapsed:.3f}s ({result_count:,} rows, {result_count/elapsed:,.0f} rows/sec)")
    except Exception as e:
        print(f"❌ Select failed: {e}")
        results.append(BenchmarkResult("Sabot", "select", num_rows, 0, 0, False, str(e)))

    # Note: GroupBy disabled for now due to type inference issue
    # Will work once we fix the aggregation operator

    spark.stop()
    return results


def print_comparison(results: List[BenchmarkResult]):
    """Print performance comparison."""
    print("\n" + "=" * 70)
    print("📊 PERFORMANCE COMPARISON")
    print("=" * 70)

    # Group by operation
    by_operation = {}
    for r in results:
        if r.success:
            if r.operation not in by_operation:
                by_operation[r.operation] = {}
            by_operation[r.operation][r.system] = r

    # Compare each operation
    for operation, systems in sorted(by_operation.items()):
        pyspark = systems.get('PySpark')
        sabot = systems.get('Sabot')

        print(f"\n{operation.upper().replace('_', ' ')}")
        print("─" * 70)

        if pyspark:
            print(f"  PySpark: {pyspark.execution_time:.3f}s | {pyspark.throughput:,.0f} rows/sec")
        if sabot:
            print(f"  Sabot:   {sabot.execution_time:.3f}s | {sabot.throughput:,.0f} rows/sec")

        if pyspark and sabot and pyspark.execution_time > 0 and sabot.execution_time > 0:
            speedup = pyspark.execution_time / sabot.execution_time
            if speedup > 1:
                print(f"  🚀 Sabot is {speedup:.1f}x FASTER")
            elif speedup < 1:
                print(f"  ⚠️  Sabot is {1/speedup:.1f}x slower")
            else:
                print(f"  ✅ Comparable performance")


def main():
    """Run comprehensive benchmark."""
    print("=" * 70)
    print("🏁 Sabot vs PySpark Performance Benchmark")
    print("=" * 70)
    print("\nFocus: Zero-copy operations, properly typed data")
    print("Data: In-memory Arrow tables (no I/O overhead)")

    # Test dataset sizes
    dataset_sizes = [100_000, 500_000, 1_000_000]

    all_results = []

    for size in dataset_sizes:
        print(f"\n{'='*70}")
        print(f"📊 Dataset Size: {size:,} rows")
        print(f"{'='*70}")

        # Generate data
        print(f"Generating {size:,} rows...")
        table = generate_arrow_table(size)
        print(f"✅ Generated {table.num_rows:,} rows ({table.nbytes / 1024 / 1024:.1f} MB)")

        # Run benchmarks
        if PYSPARK_AVAILABLE:
            pyspark_results = benchmark_pyspark(table, size)
            all_results.extend(pyspark_results)

        sabot_results = benchmark_sabot(table, size)
        all_results.extend(sabot_results)

        # Print comparison for this dataset size
        print_comparison([r for r in all_results if r.dataset_size == size])

    # Print final summary
    print("\n" + "=" * 70)
    print("🏆 FINAL SUMMARY")
    print("=" * 70)
    print_comparison(all_results)

    # Calculate averages
    print("\n" + "=" * 70)
    print("📈 AVERAGE PERFORMANCE")
    print("=" * 70)

    sabot_times = {}
    pyspark_times = {}

    for r in all_results:
        if not r.success:
            continue
        if r.system == 'Sabot':
            if r.operation not in sabot_times:
                sabot_times[r.operation] = []
            sabot_times[r.operation].append(r.execution_time)
        elif r.system == 'PySpark':
            if r.operation not in pyspark_times:
                pyspark_times[r.operation] = []
            pyspark_times[r.operation].append(r.execution_time)

    for operation in sorted(set(sabot_times.keys()) | set(pyspark_times.keys())):
        sabot_avg = statistics.mean(sabot_times.get(operation, [0]))
        pyspark_avg = statistics.mean(pyspark_times.get(operation, [0]))

        print(f"\n{operation.replace('_', ' ').title()}:")
        if pyspark_avg > 0:
            print(f"  PySpark: {pyspark_avg:.3f}s (average)")
        if sabot_avg > 0:
            print(f"  Sabot:   {sabot_avg:.3f}s (average)")

        if pyspark_avg > 0 and sabot_avg > 0:
            speedup = pyspark_avg / sabot_avg
            print(f"  Speedup: {speedup:.2f}x")

    print("\n" + "=" * 70)
    print("✅ Benchmark Complete!")
    print("=" * 70)


if __name__ == "__main__":
    main()
