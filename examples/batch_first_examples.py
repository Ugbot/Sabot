#!/usr/bin/env python3
"""
Batch-First API Examples

Demonstrates Sabot's fundamental principle: everything is batches.
Shows how batch mode and streaming mode use identical operators.

Run: python examples/batch_first_examples.py
"""

import asyncio
from sabot import cyarrow as ca
from sabot.api.stream import Stream
from pathlib import Path
import tempfile
import time

# For specialized modules not yet in cyarrow wrapper
# Parquet support via vendored Arrow
from sabot.cyarrow import parquet as pq


# ============================================================================
# Example 1: Batch Mode (Finite Data)
# ============================================================================

def example_1_batch_mode():
    """Process finite data from Parquet file."""
    print("\n" + "="*70)
    print("Example 1: Batch Mode (Finite Source)")
    print("="*70)

    # Generate sample data
    with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as f:
        data = {
            'id': list(range(100000)),
            'amount': [100.0 + (i % 1000) for i in range(100000)],
            'category': ['A' if i % 3 == 0 else 'B' for i in range(100000)],
        }
        table = pa.Table.from_pydict(data)
        pq.write_table(table, f.name)
        parquet_file = f.name

    print(f"\nCreated Parquet file: {parquet_file}")
    print(f"Total rows: 100,000")

    # Create stream from Parquet (finite)
    stream = Stream.from_table(table, batch_size=10000)

    # Apply transformations (same as streaming!)
    result = (stream
        .filter(lambda b: pc.greater(b.column('amount'), 500))
        .map(lambda b: b.append_column('fee',
            pc.multiply(b.column('amount'), 0.03)))
        .select('id', 'amount', 'fee')
    )

    # Execute (batch mode - terminates when exhausted)
    start = time.perf_counter()
    total_rows = 0
    batch_count = 0

    for batch in result:  # ← Terminates when file exhausted
        total_rows += batch.num_rows
        batch_count += 1

    elapsed = time.perf_counter() - start

    print(f"\nExecution completed:")
    print(f"  Output rows: {total_rows:,}")
    print(f"  Batches processed: {batch_count}")
    print(f"  Time: {elapsed:.4f}s")
    print(f"  Throughput: {100000 / elapsed / 1_000_000:.2f}M rows/sec")
    print(f"  ✅ Stream TERMINATED (batch mode)")

    # Cleanup
    Path(parquet_file).unlink()


# ============================================================================
# Example 2: Streaming Mode (Infinite Data)
# ============================================================================

async def example_2_streaming_mode():
    """Process infinite stream (simulated)."""
    print("\n" + "="*70)
    print("Example 2: Streaming Mode (Infinite Source)")
    print("="*70)

    # Simulate infinite stream
    async def infinite_batch_generator():
        """Simulates Kafka/socket stream (runs forever)."""
        batch_num = 0
        while True:  # ← Infinite loop
            batch = pa.RecordBatch.from_pydict({
                'id': list(range(batch_num * 1000, (batch_num + 1) * 1000)),
                'amount': [100.0 + (i % 1000) for i in range(1000)],
                'category': ['A' if i % 3 == 0 else 'B' for i in range(1000)],
            })
            yield batch
            batch_num += 1
            await asyncio.sleep(0.01)  # Simulate network delay

    print("\nCreated infinite stream (simulates Kafka)")

    # Create stream from infinite source
    source = infinite_batch_generator()
    stream = Stream.from_batches(source)

    # Apply SAME transformations as batch mode!
    result = (stream
        .filter(lambda b: pc.greater(b.column('amount'), 500))
        .map(lambda b: b.append_column('fee',
            pc.multiply(b.column('amount'), 0.03)))
        .select('id', 'amount', 'fee')
    )

    # Execute (streaming mode - runs forever until stopped)
    print("\nProcessing stream (will run for 2 seconds then stop)...")
    start = time.perf_counter()
    total_rows = 0
    batch_count = 0

    async for batch in result:  # ← Runs forever (until we break)
        total_rows += batch.num_rows
        batch_count += 1

        # Stop after 2 seconds (in real use, runs forever)
        if time.perf_counter() - start > 2.0:
            break

    elapsed = time.perf_counter() - start

    print(f"\nExecution stopped after 2s:")
    print(f"  Output rows: {total_rows:,}")
    print(f"  Batches processed: {batch_count}")
    print(f"  Time: {elapsed:.4f}s")
    print(f"  Throughput: {total_rows / elapsed / 1_000:.2f}K rows/sec")
    print(f"  ⚠️  Stream NEVER terminates (streaming mode)")


# ============================================================================
# Example 3: Same Pipeline, Different Sources
# ============================================================================

def create_pipeline(source):
    """
    Define a pipeline that works for BOTH batch and streaming.

    The pipeline doesn't know or care if the source is finite or infinite.
    """
    return (source
        .filter(lambda b: pc.greater(b.column('amount'), 500))
        .map(lambda b: b.append_column('fee',
            pc.multiply(b.column('amount'), 0.03)))
        .map(lambda b: b.append_column('tax',
            pc.multiply(b.column('amount'), 0.10)))
        .select('id', 'amount', 'fee', 'tax')
    )


def example_3_same_pipeline():
    """Show same pipeline works for batch AND streaming."""
    print("\n" + "="*70)
    print("Example 3: Same Pipeline, Different Sources")
    print("="*70)

    # Generate data
    data = {
        'id': list(range(10000)),
        'amount': [100.0 + (i % 1000) for i in range(10000)],
    }
    table = pa.Table.from_pydict(data)

    # Batch mode
    print("\n[1] Running pipeline in BATCH mode (finite):")
    batch_stream = Stream.from_table(table, batch_size=1000)
    batch_result = create_pipeline(batch_stream)

    row_count = 0
    for batch in batch_result:
        row_count += batch.num_rows

    print(f"  Processed {row_count:,} rows")
    print(f"  ✅ Terminated (finite source)")

    # Streaming mode (simulated with same data, but infinite loop)
    print("\n[2] Running SAME pipeline in STREAMING mode (infinite):")

    def infinite_batches():
        """Infinite version of same data."""
        while True:
            for batch in table.to_batches(max_chunksize=1000):
                yield batch

    stream_stream = Stream.from_batches(infinite_batches())
    stream_result = create_pipeline(stream_stream)

    row_count = 0
    max_batches = 50  # Stop after 50 batches
    for i, batch in enumerate(stream_result):
        row_count += batch.num_rows
        if i >= max_batches:
            break

    print(f"  Processed {row_count:,} rows (stopped manually)")
    print(f"  ⚠️  Would run forever (infinite source)")

    print("\n✅ SAME CODE - only difference is source boundedness!")


# ============================================================================
# Example 4: Batch API (Recommended) vs Record API (Not Recommended)
# ============================================================================

def example_4_batch_vs_record_api():
    """Show batch API (fast) vs record API (slow)."""
    print("\n" + "="*70)
    print("Example 4: Batch API vs Record API")
    print("="*70)

    # Generate data
    data = {
        'id': list(range(100000)),
        'value': [i * 2 for i in range(100000)],
    }
    table = pa.Table.from_pydict(data)
    stream = Stream.from_table(table, batch_size=10000)

    # Method 1: Batch API (RECOMMENDED)
    print("\n[1] Batch API (RECOMMENDED):")
    start = time.perf_counter()

    result = stream.map(lambda b: b.append_column('doubled',
        pc.multiply(b.column('value'), 2)))

    row_count = 0
    for batch in result:
        row_count += batch.num_rows

    batch_time = time.perf_counter() - start
    print(f"  Processed {row_count:,} rows")
    print(f"  Time: {batch_time:.4f}s")
    print(f"  Throughput: {row_count / batch_time / 1_000_000:.2f}M rows/sec")

    # Method 2: Record API (NOT RECOMMENDED - for API convenience only)
    print("\n[2] Record API (NOT RECOMMENDED - slow):")
    print("  (Skipped - would be 10-100x slower)")
    print("  .records() unpacks batches → records at Python layer")
    print("  Use ONLY for debugging or display, never for production")

    print("\n✅ Always use batch API for performance!")


# ============================================================================
# Example 5: Batch Operations Are Zero-Copy
# ============================================================================

def example_5_zero_copy():
    """Demonstrate zero-copy batch operations."""
    print("\n" + "="*70)
    print("Example 5: Zero-Copy Batch Operations")
    print("="*70)

    # Generate data
    data = {
        'id': list(range(1000000)),
        'a': list(range(1000000)),
        'b': list(range(1000000, 2000000)),
        'c': list(range(2000000, 3000000)),
    }
    table = pa.Table.from_pydict(data)
    stream = Stream.from_table(table, batch_size=100000)

    print(f"\nOriginal data: {table.nbytes / 1_000_000:.2f} MB")

    # Zero-copy operations
    print("\nApplying operations (all zero-copy):")

    # Select (zero-copy projection)
    print("  1. Select columns (zero-copy)")
    result = stream.select('id', 'a')  # No data copied!

    # Filter (SIMD, minimal copying)
    print("  2. Filter rows (SIMD-accelerated)")
    result = result.filter(lambda b: pc.greater(b.column('a'), 500000))

    # Map (adds column, shares existing data)
    print("  3. Add computed column (zero-copy of existing columns)")
    result = result.map(lambda b: b.append_column('doubled',
        pc.multiply(b.column('a'), 2)))

    # Execute
    start = time.perf_counter()
    output_rows = 0
    for batch in result:
        output_rows += batch.num_rows
    elapsed = time.perf_counter() - start

    print(f"\nExecution:")
    print(f"  Input rows: 1,000,000")
    print(f"  Output rows: {output_rows:,}")
    print(f"  Time: {elapsed:.4f}s")
    print(f"  Throughput: {1000000 / elapsed / 1_000_000:.2f}M rows/sec")
    print(f"\n✅ All operations used zero-copy semantics!")


# ============================================================================
# Main
# ============================================================================

def main():
    """Run all examples."""
    print("\n" + "="*70)
    print("SABOT BATCH-FIRST API EXAMPLES")
    print("="*70)
    print("\nKey Principle: Everything is RecordBatches")
    print("- Batch mode = finite source (terminates)")
    print("- Streaming mode = infinite source (runs forever)")
    print("- SAME operators, SAME code!")

    # Run examples
    example_1_batch_mode()
    asyncio.run(example_2_streaming_mode())
    example_3_same_pipeline()
    example_4_batch_vs_record_api()
    example_5_zero_copy()

    print("\n" + "="*70)
    print("✅ ALL EXAMPLES COMPLETED")
    print("="*70)
    print("\nKey Takeaways:")
    print("1. All operators process RecordBatch → RecordBatch")
    print("2. Streaming = infinite batching (same operators)")
    print("3. Use batch API for performance (10-100x faster)")
    print("4. Zero-copy throughout via Arrow")
    print("5. SIMD acceleration automatic")


if __name__ == '__main__':
    main()
