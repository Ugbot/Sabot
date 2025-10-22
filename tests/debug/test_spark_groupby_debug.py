#!/usr/bin/env python3
"""Debug GroupBy issue."""

import sys
sys.path.insert(0, '/Users/bengamble/Sabot')

import traceback
import pandas as pd
import tempfile
from pathlib import Path

from sabot.spark import SparkSession
from sabot.spark.functions import col, sum as spark_sum, avg, count

# Generate test data
df_pandas = pd.DataFrame({
    'customer_id': [1, 1, 2, 2, 3],
    'category': ['A', 'B', 'A', 'B', 'A'],
    'amount': [100, 200, 150, 250, 300]
})

temp_dir = Path(tempfile.mkdtemp())
csv_path = temp_dir / "test.csv"
df_pandas.to_csv(csv_path, index=False)

print(f"Test data: {csv_path}")
print(df_pandas)

# Create Spark session
spark = SparkSession.builder.master("local[*]").getOrCreate()

# Read data
df = spark.read.csv(str(csv_path), header=True, inferSchema=True)
print(f"\n✅ Read DataFrame")

# Try GroupBy
print("\n▶ Attempting GroupBy...")
try:
    grouped = df.groupBy("customer_id", "category")
    print(f"✅ GroupBy created: {type(grouped)}")
    print(f"  Grouped stream: {type(grouped._grouped_stream)}")
    print(f"  Group keys: {grouped._group_keys}")
except Exception as e:
    print(f"❌ GroupBy failed: {e}")
    traceback.print_exc()
    sys.exit(1)

# Try simple aggregation with dict
print("\n▶ Attempting dict-style agg...")
try:
    result = grouped.agg(amount="sum")
    print(f"✅ Dict agg succeeded")
    print(f"  Result: {result.count()} rows")
except Exception as e:
    print(f"❌ Dict agg failed: {e}")
    traceback.print_exc()

# Try aggregation with functions
print("\n▶ Attempting function-style agg...")
try:
    agg_expr = spark_sum("amount").alias("total_amount")
    print(f"  Expression type: {type(agg_expr)}")
    print(f"  Expression._expr: {agg_expr._expr}")

    result = grouped.agg(agg_expr)
    print(f"✅ Function agg succeeded")
    print(f"  Result: {result.count()} rows")
except Exception as e:
    print(f"❌ Function agg failed: {e}")
    traceback.print_exc()

# Try multiple aggregations
print("\n▶ Attempting multi-agg...")
try:
    result = grouped.agg(
        spark_sum("amount").alias("total_amount"),
        avg("amount").alias("avg_amount"),
        count("*").alias("count")
    )
    print(f"✅ Multi-agg succeeded")
    print(f"  Result: {result.count()} rows")
except Exception as e:
    print(f"❌ Multi-agg failed: {e}")
    traceback.print_exc()

spark.stop()
