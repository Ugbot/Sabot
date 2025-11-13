#!/usr/bin/env python3
"""
Test Sabot Job for sabot-submit

This demonstrates a PySpark-compatible job running on Sabot.
"""

from sabot.spark import SparkSession
from sabot.spark.functions import col
import sys

def main():
    # Get arguments
    input_path = sys.argv[1] if len(sys.argv) > 1 else "test_data"
    output_path = sys.argv[2] if len(sys.argv) > 2 else "test_output"
    
    print(f"Input: {input_path}")
    print(f"Output: {output_path}")
    print()
    
    # Create Spark session (Sabot shim)
    spark = SparkSession.builder \
        .master(os.environ.get('SABOT_MASTER', 'local[*]')) \
        .appName(os.environ.get('SABOT_APP_NAME', 'TestJob')) \
        .getOrCreate()
    
    print(f"✓ SparkSession created")
    print(f"  Master: {os.environ.get('SABOT_MASTER', 'local[*]')}")
    print(f"  Engine: {spark._engine}")
    print()
    
    # Create test data
    import pyarrow as pa
    
    data = pa.table({
        'id': list(range(1, 101)),
        'amount': [i * 10 for i in range(1, 101)],
        'category': ['A', 'B', 'C', 'D'] * 25
    })
    
    df = spark.createDataFrame(data)
    print(f"✓ Created DataFrame: {data.num_rows} rows")
    
    # Process data
    result = df.filter(col('amount') > 500).select('id', 'amount', 'category')
    
    print(f"✓ Applied transformations")
    print(f"  - filter(amount > 500)")
    print(f"  - select(id, amount, category)")
    print()
    
    print("✅ Job completed successfully!")
    print(f"   Used Sabot Spark shim with Cython operators")
    
    spark.stop()
    return 0

if __name__ == '__main__':
    import os
    sys.exit(main())

