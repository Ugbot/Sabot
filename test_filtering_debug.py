#!/usr/bin/env python3
"""
Debug filtering operations
"""

import pandas as pd
import tempfile
import os

def test_filtering_debug():
    """Test filtering operations step by step."""
    
    # Create test data
    data = {
        'id': [1, 2, 3, 4, 5],
        'value': [100, 200, 300, 400, 500],
        'category': ['A', 'B', 'A', 'B', 'A']
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
        from sabot.spark.functions import col
        
        print("\n=== Testing Sabot SparkSession ===")
        spark = SparkSession.builder.master("local[*]").getOrCreate()
        
        # Test DataFrameReader
        print("\n=== Testing DataFrameReader ===")
        reader = spark.read
        
        # Test CSV reading
        print("\n=== Testing CSV Reading ===")
        df_sabot = reader.csv(csv_path, header=True, inferSchema=True)
        print(f"DataFrame created: {df_sabot}")
        
        # Test count
        print("\n=== Testing Count ===")
        count = df_sabot.count()
        print(f"Count result: {count}")
        
        # Test filtering
        print("\n=== Testing Filtering ===")
        print("Creating filter condition...")
        filter_condition = col("value") > 300
        print(f"Filter condition: {filter_condition}")
        print(f"Filter condition type: {type(filter_condition)}")
        
        print("Applying filter...")
        filtered_df = df_sabot.filter(filter_condition)
        print(f"Filtered DataFrame: {filtered_df}")
        
        print("Counting filtered results...")
        filtered_count = filtered_df.count()
        print(f"Filtered count result: {filtered_count}")
        
        # Test collect
        print("\n=== Testing Filtered Collect ===")
        filtered_rows = filtered_df.collect()
        print(f"Filtered collect result: {filtered_rows}")
        
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
    test_filtering_debug()
