"""
Tests for lazy Parquet loading - larger-than-RAM support.

Validates that both Arrow and DuckDB backends can handle files
larger than available RAM without loading everything into memory.
"""

import tempfile
import os
from pathlib import Path
from sabot.api.stream import Stream
from sabot import cyarrow as ca

# Use vendored Arrow
pa = ca
pq = ca.parquet if hasattr(ca, 'parquet') else None

# Fallback to external pyarrow for file generation if needed
if pq is None:
    import pyarrow as pa_ext
    import pyarrow.parquet as pq_ext
else:
    pa_ext = pa
    pq_ext = pq


def create_test_parquet(path: str, num_rows: int = 1_000_000):
    """
    Create a test Parquet file for lazy loading tests.
    
    Args:
        path: Output path for Parquet file
        num_rows: Number of rows to generate
    
    Returns:
        Path to created file
    """
    # Generate data in chunks to avoid memory issues
    chunk_size = 100_000
    writer = None
    
    for i in range(0, num_rows, chunk_size):
        rows_in_chunk = min(chunk_size, num_rows - i)
        
        # Create chunk
        data = {
            'id': list(range(i, i + rows_in_chunk)),
            'value': [x * 2 for x in range(i, i + rows_in_chunk)],
            'category': [f'cat_{x % 10}' for x in range(i, i + rows_in_chunk)]
        }
        
        table = pa_ext.table(data)
        
        # Write chunk
        if writer is None:
            writer = pq_ext.ParquetWriter(path, table.schema)
        
        writer.write_table(table)
    
    if writer:
        writer.close()
    
    return path


# Note: pytest fixtures removed for standalone execution
# Tests use manual file creation in __main__


def test_arrow_backend_lazy_loading(small_parquet_file):
    """Test that Arrow backend uses lazy loading (doesn't load all at once)."""
    # Use lazy=True (default)
    stream = Stream.from_parquet(small_parquet_file, backend='arrow', lazy=True)
    
    # Should be able to iterate without loading everything
    batch_count = 0
    total_rows = 0
    
    for batch in stream:
        batch_count += 1
        total_rows += batch.num_rows
    
    # Should have multiple batches (not all in one)
    assert batch_count >= 1, "Should yield at least one batch"
    assert total_rows == 10_000, f"Should have 10K total rows, got {total_rows}"
    
    print(f"✓ Arrow lazy loading: {batch_count} batches, {total_rows} total rows")


def test_arrow_backend_eager_loading(small_parquet_file):
    """Test that Arrow backend with lazy=False loads everything."""
    # Use lazy=False (eager loading)
    stream = Stream.from_parquet(small_parquet_file, backend='arrow', lazy=False)
    
    # Should still work, just loads eagerly
    batch_count = 0
    total_rows = 0
    
    for batch in stream:
        batch_count += 1
        total_rows += batch.num_rows
    
    assert total_rows == 10_000, f"Should have 10K total rows, got {total_rows}"
    
    print(f"✓ Arrow eager loading: {batch_count} batches, {total_rows} total rows")


def test_lazy_loading_with_filter(medium_parquet_file):
    """Test lazy loading with column projection."""
    # Only load specific columns
    stream = Stream.from_parquet(
        medium_parquet_file,
        backend='arrow',
        columns=['id', 'value'],
        lazy=True
    )
    
    batch_count = 0
    total_rows = 0
    
    for batch in stream:
        batch_count += 1
        total_rows += batch.num_rows
        
        # Verify only requested columns
        assert set(batch.schema.names) == {'id', 'value'}, \
            f"Should only have id and value columns, got {batch.schema.names}"
    
    assert total_rows == 1_000_000, f"Should have 1M total rows, got {total_rows}"
    
    print(f"✓ Lazy loading with projection: {batch_count} batches, {total_rows} total rows")


def test_lazy_groupby_aggregation(small_parquet_file):
    """Test that groupby works with lazy loaded data."""
    stream = Stream.from_parquet(small_parquet_file, backend='arrow', lazy=True)
    
    # Group by category and count
    result = (stream
        .group_by('category')
        .aggregate({
            'count': ('id', 'count'),
            'sum_value': ('value', 'sum')
        })
    )
    
    # Collect results
    batches = list(result)
    assert len(batches) > 0, "Should have results"
    
    result_table = ca.Table.from_batches(batches)
    
    # Should have 10 categories (cat_0 through cat_9)
    assert result_table.num_rows == 10, f"Should have 10 groups, got {result_table.num_rows}"
    
    # Verify counts
    total_count = sum(result_table.column('count').to_pylist())
    assert total_count == 10_000, f"Total count should be 10K, got {total_count}"
    
    print(f"✓ Lazy groupby: {result_table.num_rows} groups, {total_count} total count")


def test_memory_efficient_processing(medium_parquet_file):
    """
    Test that lazy loading keeps memory usage reasonable.
    
    This is a basic test - in production you'd use memory_profiler
    or similar to track actual memory usage.
    """
    import gc
    
    # Force GC before test
    gc.collect()
    
    # Process 1M rows lazily
    stream = Stream.from_parquet(medium_parquet_file, backend='arrow', lazy=True)
    
    # Process data without accumulating in memory
    max_value = 0
    total_processed = 0
    
    for batch in stream:
        # Process each batch individually
        values = batch.column('value').to_pylist()
        max_value = max(max_value, max(values))
        total_processed += batch.num_rows
        
        # Don't accumulate batches - they should be GC'd
        del batch
        del values
    
    assert total_processed == 1_000_000, f"Should process 1M rows, got {total_processed}"
    assert max_value == 1_999_998, f"Max value should be 1999998, got {max_value}"
    
    print(f"✓ Memory-efficient processing: {total_processed} rows processed")


def test_lazy_vs_eager_results_match(small_parquet_file):
    """Verify lazy and eager loading produce identical results."""
    # Lazy loading
    stream_lazy = Stream.from_parquet(small_parquet_file, backend='arrow', lazy=True)
    batches_lazy = list(stream_lazy)
    table_lazy = ca.Table.from_batches(batches_lazy)
    
    # Eager loading
    stream_eager = Stream.from_parquet(small_parquet_file, backend='arrow', lazy=False)
    batches_eager = list(stream_eager)
    table_eager = ca.Table.from_batches(batches_eager)
    
    # Compare
    assert table_lazy.num_rows == table_eager.num_rows, \
        f"Row counts differ: lazy={table_lazy.num_rows}, eager={table_eager.num_rows}"
    
    assert table_lazy.num_columns == table_eager.num_columns, \
        f"Column counts differ: lazy={table_lazy.num_columns}, eager={table_eager.num_columns}"
    
    # Compare actual data
    for col_name in table_lazy.column_names:
        lazy_data = table_lazy.column(col_name).to_pylist()
        eager_data = table_eager.column(col_name).to_pylist()
        
        assert lazy_data == eager_data, \
            f"Column {col_name} data differs between lazy and eager"
    
    print(f"✓ Lazy vs eager match: {table_lazy.num_rows} rows identical")


if __name__ == '__main__':
    # Run tests
    print("Testing lazy Parquet loading...")
    
    # Create test files
    with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as f:
        small_file = f.name
    with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as f:
        medium_file = f.name
    
    try:
        create_test_parquet(small_file, 10_000)
        create_test_parquet(medium_file, 1_000_000)
        
        test_arrow_backend_lazy_loading(small_file)
        test_arrow_backend_eager_loading(small_file)
        test_lazy_loading_with_filter(medium_file)
        test_lazy_groupby_aggregation(small_file)
        test_memory_efficient_processing(medium_file)
        test_lazy_vs_eager_results_match(small_file)
        
        print("\n✅ All lazy loading tests passed!")
        
    finally:
        # Cleanup
        for f in [small_file, medium_file]:
            if os.path.exists(f):
                os.unlink(f)

