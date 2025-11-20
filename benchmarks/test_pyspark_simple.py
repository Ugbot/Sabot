#!/usr/bin/env python3
"""
Simple PySpark shim benchmark with CyArrow optimization
"""

import time
from sabot.spark import SparkSession

print()
print("="*70)
print("PYSPARK SHIM BENCHMARK (CyArrow + Date Optimization)")
print("="*70)
print()

spark = SparkSession.builder.appName("test").getOrCreate()

data_path = "benchmarks/polars-benchmark/data/tables/scale-0.1/lineitem.parquet"

print("Reading Parquet with automatic date optimization...")
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

print()
print(f"Time:       {elapsed:.3f}s")
print(f"Groups:     {count}")
print(f"Throughput: {600_572/elapsed/1_000_000:.2f}M rows/sec")
print()
print("="*70)
print()
