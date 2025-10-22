#!/usr/bin/env python3
"""
Debug Column operations
"""

import pandas as pd
import tempfile
import os

def test_column_debug():
    """Test Column operations step by step."""
    
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
        
        # Test Column operations
        print("\n=== Testing Column Operations ===")
        
        # Get first batch to test Column operations
        batches = list(df_sabot._stream)
        print(f"Number of batches: {len(batches)}")
        
        if batches:
            batch = batches[0]
            print(f"Batch: {batch}")
            print(f"Batch columns: {batch.schema.names}")
            
            # Test basic column access
            print("\n--- Testing basic column access ---")
            value_col = batch.column('value')
            print(f"Value column: {value_col}")
            print(f"Value column type: {type(value_col)}")
            print(f"Value column length: {len(value_col)}")
            print(f"Value column values: {value_col.to_pylist()}")
            
            # Test Column expression
            print("\n--- Testing Column expression ---")
            col_expr = col("value")
            print(f"Column expression: {col_expr}")
            print(f"Column expression type: {type(col_expr)}")
            
            # Test _get_array
            print("\n--- Testing _get_array ---")
            array_result = col_expr._get_array(batch)
            print(f"Array result: {array_result}")
            print(f"Array result type: {type(array_result)}")
            print(f"Array result length: {len(array_result)}")
            print(f"Array result values: {array_result.to_pylist()}")
            
            # Test comparison
            print("\n--- Testing comparison ---")
            comparison = col("value") > 300
            print(f"Comparison: {comparison}")
            print(f"Comparison type: {type(comparison)}")
            
            # Test comparison evaluation
            print("\n--- Testing comparison evaluation ---")
            comparison_result = comparison._get_array(batch)
            print(f"Comparison result: {comparison_result}")
            print(f"Comparison result type: {type(comparison_result)}")
            print(f"Comparison result length: {len(comparison_result)}")
            print(f"Comparison result values: {comparison_result.to_pylist()}")
            
            # Test filtering with Arrow compute
            print("\n--- Testing filtering with Arrow compute ---")
            from sabot import cyarrow as ca
            mask = ca.compute.greater(value_col, 300)
            print(f"Mask: {mask}")
            print(f"Mask type: {type(mask)}")
            print(f"Mask values: {mask.to_pylist()}")
            
            filtered_batch = batch.filter(mask)
            print(f"Filtered batch: {filtered_batch}")
            print(f"Filtered batch rows: {filtered_batch.num_rows}")
            if filtered_batch.num_rows > 0:
                print(f"Filtered batch values: {filtered_batch.column('value').to_pylist()}")
        
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
    test_column_debug()
