#!/usr/bin/env python3
"""
Test PySpark shim with optimizations enabled

Compares:
1. PySpark shim WITHOUT date optimization
2. PySpark shim WITH date optimization (new)
"""

import time
import sys
from pathlib import Path

# Add benchmark path
sys.path.append(str(Path(__file__).parent / "polars-benchmark"))

from sabot.spark import SparkSession

def test_without_optimization():
    """Test PySpark shim with optimize_dates=False"""
    print("="*70)
    print("TEST 1: PySpark Shim WITHOUT Date Optimization")
    print("="*70)
    print()
    
    spark = SparkSession.builder.appName("test").getOrCreate()
    
    data_path = "benchmarks/polars-benchmark/data/tables/scale-0.1/lineitem.parquet"
    
    start = time.time()
    
    # Read without optimization
    df = spark.read.parquet(data_path, optimize_dates=False)
    
    # TPC-H Q1-style query
    result = (df
        .filter(df.l_shipdate <= "1998-09-02")
        .groupBy("l_returnflag", "l_linestatus")
        .agg({
            "l_quantity": "sum",
            "l_extendedprice": "sum",
            "l_discount": "mean"
        })
    )
    
    count = result.count()
    elapsed = time.time() - start
    
    print(f"Time: {elapsed:.3f}s")
    print(f"Rows: {count}")
    print(f"Throughput: {600_572/elapsed/1_000_000:.2f}M rows/sec")
    print()
    
    return elapsed


def test_with_optimization():
    """Test PySpark shim with optimize_dates=True (default)"""
    print("="*70)
    print("TEST 2: PySpark Shim WITH Date Optimization")
    print("="*70)
    print()
    
    spark = SparkSession.builder.appName("test").getOrCreate()
    
    data_path = "benchmarks/polars-benchmark/data/tables/scale-0.1/lineitem.parquet"
    
    start = time.time()
    
    # Read with optimization (default)
    df = spark.read.parquet(data_path, optimize_dates=True)
    
    # TPC-H Q1-style query
    result = (df
        .filter(df.l_shipdate <= "1998-09-02")
        .groupBy("l_returnflag", "l_linestatus")
        .agg({
            "l_quantity": "sum",
            "l_extendedprice": "sum",
            "l_discount": "mean"
        })
    )
    
    count = result.count()
    elapsed = time.time() - start
    
    print(f"Time: {elapsed:.3f}s")
    print(f"Rows: {count}")
    print(f"Throughput: {600_572/elapsed/1_000_000:.2f}M rows/sec")
    print()
    
    return elapsed


if __name__ == "__main__":
    print()
    print("╔" + "="*68 + "╗")
    print("║" + " "*15 + "PYSPARK SHIM OPTIMIZATION TEST" + " "*23 + "║")
    print("╚" + "="*68 + "╝")
    print()
    
    time_without = test_without_optimization()
    time_with = test_with_optimization()
    
    print("="*70)
    print("COMPARISON")
    print("="*70)
    print()
    print(f"Without optimization: {time_without:.3f}s")
    print(f"With optimization:    {time_with:.3f}s")
    print(f"Speedup:              {time_without/time_with:.2f}x")
    print()
    print("Expected speedup: 1.5-2x from date conversion optimization")
    print()
