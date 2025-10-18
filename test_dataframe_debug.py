#!/usr/bin/env python3
"""
Debug DataFrame operations
"""

import pandas as pd
import tempfile
import os

def test_dataframe_debug():
    """Test DataFrame operations step by step."""
    
    # Create test data
    data = {
        'id': [1, 2, 3, 4, 5],
        'amount': [100, 200, 300, 400, 500],
        'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve']
    }
    df = pd.DataFrame(data)
    
    # Save to CSV
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        df.to_csv(f.name, index=False)
        csv_path = f.name
    
    print(f"Created test CSV: {csv_path}")
    print(f"Data:\n{df}")
    
    try:
        # Test Sabot SparkSession
        from sabot.spark import SparkSession
        
        print("\n=== Testing Sabot SparkSession ===")
        spark = SparkSession.builder.master("local[*]").getOrCreate()
        print(f"SparkSession created: {spark}")
        print(f"Engine: {spark._engine}")
        print(f"Stream API: {spark._engine.stream}")
        
        # Test DataFrameReader
        print("\n=== Testing DataFrameReader ===")
        reader = spark.read
        print(f"Reader: {reader}")
        print(f"Reader session: {reader._session}")
        
        # Test CSV reading
        print("\n=== Testing CSV Reading ===")
        df_sabot = reader.csv(csv_path, header=True, inferSchema=True)
        print(f"DataFrame created: {df_sabot}")
        print(f"DataFrame stream: {df_sabot._stream}")
        print(f"DataFrame session: {df_sabot._session}")
        
        # Test count
        print("\n=== Testing Count ===")
        count = df_sabot.count()
        print(f"Count result: {count}")
        
        # Test collect
        print("\n=== Testing Collect ===")
        rows = df_sabot.collect()
        print(f"Collect result: {rows}")
        
        # Test show
        print("\n=== Testing Show ===")
        df_sabot.show()
        
        spark.stop()
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        # Cleanup
        if os.path.exists(csv_path):
            os.unlink(csv_path)

if __name__ == "__main__":
    test_dataframe_debug()
