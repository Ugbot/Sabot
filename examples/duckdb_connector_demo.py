#!/usr/bin/env python3
"""
DuckDB Connector Demo for Sabot

Demonstrates high-performance data ingestion and export using DuckDB connectors
with automatic filter/projection pushdown and zero-copy Arrow streaming.

Features:
- Parquet, CSV, JSON, Postgres, Delta Lake sources
- Zero-copy Arrow streaming via vendored Arrow C++
- Automatic filter and projection pushdown
- Stream API integration
- URI-based smart connectors
"""

import asyncio
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from sabot.api.stream import Stream
from sabot import cyarrow as ca


# ============================================================================
# Example 1: Parquet Source with Pushdown
# ============================================================================

async def example1_parquet_pushdown():
    """
    Read Parquet file with filter and projection pushdown.

    This demonstrates automatic pushdown optimization where filters and
    column selections are pushed to DuckDB's Parquet reader for maximum performance.
    """
    print("\n" + "="*80)
    print("Example 1: Parquet Source with Pushdown")
    print("="*80)

    # Create test data
    print("\n1. Creating test Parquet file...")
    test_data = [
        {'id': i, 'amount': i * 100, 'category': 'A' if i % 2 == 0 else 'B', 'date': '2025-01-01'}
        for i in range(1000)
    ]

    # Write to Parquet
    table = ca.Table.from_pylist(test_data)
    output_path = '/tmp/test_transactions.parquet'
    ca.parquet.write_table(table, output_path)
    print(f"   Created: {output_path}")

    # Read with pushdown
    print("\n2. Reading with filter and projection pushdown...")
    stream = Stream.from_parquet(
        output_path,
        filters={'amount': '> 50000'},  # Pushed to Parquet reader
        columns=['id', 'amount', 'date']  # Only read these columns
    )

    # Process stream
    print("\n3. Processing stream...")
    total_rows = 0
    async for batch in stream:
        total_rows += batch.num_rows
        if total_rows <= 10:
            # Show first few rows
            print(f"   Batch: {batch.num_rows} rows, columns: {batch.schema.names}")

    print(f"\n   Total rows (after filter): {total_rows}")
    print("   ✅ Pushdown optimization applied")


# ============================================================================
# Example 2: Stream API Integration
# ============================================================================

async def example2_stream_api():
    """
    Use Stream API methods for reading and writing.

    Demonstrates the high-level Stream API with automatic connector selection.
    """
    print("\n" + "="*80)
    print("Example 2: Stream API Integration")
    print("="*80)

    # Create test data
    print("\n1. Creating test data...")
    test_data = [
        {'id': i, 'price': i * 10.5, 'volume': i * 100}
        for i in range(100)
    ]

    # Write as Parquet using Stream API
    input_path = '/tmp/test_data.parquet'
    stream = Stream.from_dicts(test_data)

    print(f"\n2. Writing to Parquet: {input_path}")
    await stream.to_parquet(input_path, mode='overwrite')
    print("   ✅ Written successfully")

    # Read using Stream API
    print(f"\n3. Reading from Parquet with transformation...")
    result = (
        Stream.from_parquet(input_path, columns=['id', 'price', 'volume'])
        .filter(lambda b: ca.compute.greater(b.column('price'), ca.scalar(500)))
        .map(lambda b: b.append_column(
            'total_value',
            ca.compute.multiply(b.column('price'), b.column('volume'))
        ))
    )

    # Collect results
    total_rows = 0
    async for batch in result:
        total_rows += batch.num_rows
        if total_rows <= 5:
            print(f"   Batch: {batch.num_rows} rows, schema: {batch.schema.names}")

    print(f"\n   Total rows (after filter): {total_rows}")
    print("   ✅ Stream API pipeline executed")


# ============================================================================
# Example 3: SQL Query Source
# ============================================================================

async def example3_sql_query():
    """
    Read from arbitrary SQL query with DuckDB.

    This shows how to use DuckDB's full SQL capabilities as a data source.
    """
    print("\n" + "="*80)
    print("Example 3: SQL Query Source")
    print("="*80)

    # Create test data
    print("\n1. Creating test Parquet files...")
    data1 = [{'id': i, 'value': i * 10} for i in range(100)]
    data2 = [{'id': i, 'name': f'Item_{i}'} for i in range(100)]

    table1 = ca.Table.from_pylist(data1)
    table2 = ca.Table.from_pylist(data2)

    ca.parquet.write_table(table1, '/tmp/test_values.parquet')
    ca.parquet.write_table(table2, '/tmp/test_names.parquet')
    print("   Created: /tmp/test_values.parquet")
    print("   Created: /tmp/test_names.parquet")

    # Read using SQL join
    print("\n2. Reading with SQL join...")
    sql = """
    SELECT v.id, v.value, n.name
    FROM read_parquet('/tmp/test_values.parquet') v
    JOIN read_parquet('/tmp/test_names.parquet') n
    ON v.id = n.id
    WHERE v.value > 500
    """

    stream = Stream.from_sql(
        sql,
        columns=['id', 'value', 'name']  # Projection pushdown
    )

    # Process results
    print("\n3. Processing join results...")
    total_rows = 0
    async for batch in stream:
        total_rows += batch.num_rows
        if total_rows <= 5:
            print(f"   Batch: {batch.num_rows} rows, schema: {batch.schema.names}")

    print(f"\n   Total rows (after join and filter): {total_rows}")
    print("   ✅ SQL query executed with pushdown")


# ============================================================================
# Example 4: Format-Specific Helpers
# ============================================================================

async def example4_format_helpers():
    """
    Use format-specific helper functions.

    Demonstrates convenience wrappers for common data formats.
    """
    print("\n" + "="*80)
    print("Example 4: Format-Specific Helpers")
    print("="*80)

    from sabot.connectors import ParquetSource, CSVSink

    # Create test data
    print("\n1. Creating test data...")
    test_data = [
        {'id': i, 'name': f'User_{i}', 'score': i * 10}
        for i in range(50)
    ]
    table = ca.Table.from_pylist(test_data)
    ca.parquet.write_table(table, '/tmp/test_input.parquet')

    # Read using ParquetSource
    print("\n2. Reading with ParquetSource...")
    source = ParquetSource(
        '/tmp/test_input.parquet',
        filters={'score': '> 200'},
        columns=['id', 'name', 'score']
    )

    batches = []
    async for batch in source.stream_batches():
        batches.append(batch)
        print(f"   Read batch: {batch.num_rows} rows")

    # Write using CSVSink
    print("\n3. Writing with CSVSink...")
    sink = CSVSink('/tmp/test_output.csv', mode='overwrite')

    for batch in batches:
        await sink.write_batch(batch)

    await sink.close()
    print(f"   Written to: /tmp/test_output.csv")
    print("   ✅ Format helpers executed successfully")


# ============================================================================
# Example 5: URI-Based Smart Connectors
# ============================================================================

async def example5_uri_connectors():
    """
    Use URI-based smart connectors for automatic format detection.

    This demonstrates the most convenient API where the connector is
    automatically selected based on the URI scheme and file extension.
    """
    print("\n" + "="*80)
    print("Example 5: URI-Based Smart Connectors")
    print("="*80)

    from sabot.connectors import from_uri, to_uri

    # Create test data
    print("\n1. Creating test data...")
    test_data = [
        {'id': i, 'amount': i * 100, 'category': 'A' if i % 2 == 0 else 'B'}
        for i in range(100)
    ]
    table = ca.Table.from_pylist(test_data)
    ca.parquet.write_table(table, '/tmp/smart_input.parquet')

    # Read using URI (auto-detects Parquet)
    print("\n2. Reading with from_uri (auto-detects Parquet)...")
    source = from_uri(
        'file:///tmp/smart_input.parquet',
        filters={'amount': '> 5000'},
        columns=['id', 'amount']
    )

    batches = []
    async for batch in source.stream_batches():
        batches.append(batch)
        print(f"   Read batch: {batch.num_rows} rows")

    # Write using URI (auto-detects CSV from extension)
    print("\n3. Writing with to_uri (auto-detects CSV)...")
    sink = to_uri('file:///tmp/smart_output.csv', mode='overwrite')

    for batch in batches:
        await sink.write_batch(batch)

    await sink.close()
    print(f"   Written to: /tmp/smart_output.csv")
    print("   ✅ URI-based connectors executed successfully")


# ============================================================================
# Example 6: Performance Benchmark
# ============================================================================

async def example6_performance():
    """
    Benchmark DuckDB connector performance with large dataset.

    Measures throughput for reading and processing with pushdown.
    """
    print("\n" + "="*80)
    print("Example 6: Performance Benchmark")
    print("="*80)

    import time

    # Create large test dataset
    print("\n1. Creating large test dataset (100K rows)...")
    test_data = [
        {
            'id': i,
            'amount': i * 10.5,
            'category': f'Cat_{i % 10}',
            'date': f'2025-01-{(i % 28) + 1:02d}'
        }
        for i in range(100_000)
    ]

    table = ca.Table.from_pylist(test_data)
    test_path = '/tmp/large_test.parquet'
    ca.parquet.write_table(table, test_path)
    print(f"   Created: {test_path} ({len(test_data):,} rows)")

    # Benchmark: Read with pushdown
    print("\n2. Benchmarking read with pushdown...")
    start = time.perf_counter()

    stream = Stream.from_parquet(
        test_path,
        filters={'amount': '> 50000'},
        columns=['id', 'amount', 'date']
    )

    total_rows = 0
    batch_count = 0
    async for batch in stream:
        total_rows += batch.num_rows
        batch_count += 1

    elapsed = time.perf_counter() - start
    throughput = total_rows / elapsed / 1000  # K rows/sec

    print(f"\n   Results:")
    print(f"   - Total rows: {total_rows:,}")
    print(f"   - Batches: {batch_count}")
    print(f"   - Time: {elapsed*1000:.2f}ms")
    print(f"   - Throughput: {throughput:.1f}K rows/sec")
    print("   ✅ Benchmark complete")


# ============================================================================
# Main
# ============================================================================

async def main():
    """Run all examples."""
    print("\n" + "="*80)
    print("DuckDB Connector Demo for Sabot")
    print("="*80)
    print("\nDemonstrating high-performance data connectors with:")
    print("  • Zero-copy Arrow streaming via vendored Arrow C++")
    print("  • Automatic filter/projection pushdown")
    print("  • Stream API integration")
    print("  • Multiple data formats (Parquet, CSV, SQL, etc.)")
    print("  • URI-based smart connectors")

    try:
        # Run examples
        await example1_parquet_pushdown()
        await example2_stream_api()
        await example3_sql_query()
        await example4_format_helpers()
        await example5_uri_connectors()
        await example6_performance()

        print("\n" + "="*80)
        print("✅ All examples completed successfully!")
        print("="*80)

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    asyncio.run(main())
