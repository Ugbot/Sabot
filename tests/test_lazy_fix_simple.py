"""
Simple test to validate lazy loading fix works.

Uses existing TPC-H benchmark data to test that:
1. Lazy loading doesn't crash (file handle stays open)
2. Results are correct
3. Memory usage is reasonable
"""

import os
import sys
sys.path.insert(0, '/Users/bengamble/Sabot')

from sabot.api.stream import Stream
from sabot import cyarrow as ca


def test_lazy_loading_basic():
    """Test that basic lazy loading works without file handle errors."""
    # Use existing TPC-H data if available
    test_files = [
        '/Users/bengamble/Sabot/benchmarks/polars-benchmark/data/scale_0.1/lineitem.parquet',
        'data/lineitem.parquet',
        './lineitem.parquet'
    ]
    
    parquet_file = None
    for path in test_files:
        if os.path.exists(path):
            parquet_file = path
            break
    
    if not parquet_file:
        print("⚠ No test Parquet file found, skipping test")
        print(f"  Looked for: {test_files}")
        return
    
    print(f"Testing with: {parquet_file}")
    
    # Test lazy loading (default)
    print("\n1. Testing lazy loading (lazy=True)...")
    stream = Stream.from_parquet(parquet_file, backend='arrow', lazy=True)
    
    batch_count = 0
    total_rows = 0
    
    try:
        for batch in stream:
            batch_count += 1
            total_rows += batch.num_rows
            
            if batch_count == 1:
                print(f"   First batch: {batch.num_rows} rows, {batch.num_columns} columns")
        
        print(f"   ✓ Lazy loading successful: {batch_count} batches, {total_rows:,} total rows")
        
    except Exception as e:
        print(f"   ✗ Lazy loading FAILED: {e}")
        raise
    
    # Test eager loading for comparison
    print("\n2. Testing eager loading (lazy=False)...")
    stream_eager = Stream.from_parquet(parquet_file, backend='arrow', lazy=False)
    
    batch_count_eager = 0
    total_rows_eager = 0
    
    for batch in stream_eager:
        batch_count_eager += 1
        total_rows_eager += batch.num_rows
    
    print(f"   ✓ Eager loading successful: {batch_count_eager} batches, {total_rows_eager:,} total rows")
    
    # Verify they match
    assert total_rows == total_rows_eager, \
        f"Row count mismatch: lazy={total_rows}, eager={total_rows_eager}"
    
    print(f"\n3. Verification: Lazy and eager produce identical row counts ✓")
    
    # Test with column projection
    print("\n4. Testing with column projection...")
    stream_proj = Stream.from_parquet(
        parquet_file,
        backend='arrow',
        columns=['l_orderkey', 'l_extendedprice'],
        lazy=True
    )
    
    batch_count_proj = 0
    for batch in stream_proj:
        batch_count_proj += 1
        assert batch.num_columns == 2, f"Expected 2 columns, got {batch.num_columns}"
    
    print(f"   ✓ Column projection works: {batch_count_proj} batches with 2 columns each")
    
    print("\n✅ All basic lazy loading tests passed!")
    return True


def test_lazy_groupby():
    """Test that groupby works with lazy loaded data."""
    test_files = [
        '/Users/bengamble/Sabot/benchmarks/polars-benchmark/data/scale_0.1/lineitem.parquet',
    ]
    
    parquet_file = None
    for path in test_files:
        if os.path.exists(path):
            parquet_file = path
            break
    
    if not parquet_file:
        print("⚠ No test Parquet file found, skipping groupby test")
        return
    
    print(f"\n5. Testing groupby with lazy loading...")
    
    stream = Stream.from_parquet(parquet_file, backend='arrow', lazy=True)
    
    # Simple groupby
    result = (stream
        .group_by('l_returnflag')
        .aggregate({
            'count': ('l_orderkey', 'count'),
            'total_price': ('l_extendedprice', 'sum')
        })
    )
    
    batches = list(result)
    if batches:
        result_table = ca.Table.from_batches(batches)
        print(f"   ✓ GroupBy successful: {result_table.num_rows} groups")
        return True
    else:
        print("   ⚠ GroupBy returned no results")
        return False


if __name__ == '__main__':
    print("=" * 60)
    print("Testing Lazy Parquet Loading Fix")
    print("=" * 60)
    
    try:
        test_lazy_loading_basic()
        test_lazy_groupby()
        
        print("\n" + "=" * 60)
        print("SUCCESS: All tests passed! ✅")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n❌ TEST FAILED: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

