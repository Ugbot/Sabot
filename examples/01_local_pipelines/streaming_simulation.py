#!/usr/bin/env python3
"""
Streaming Simulation Example - Process Events in Batches
=========================================================

**What this demonstrates:**
- Simulated streaming with a generator
- Processing events in micro-batches
- Batch-at-a-time processing (streaming = infinite batches)
- Local execution (no Kafka, no infrastructure)

**Prerequisites:** Completed 00_quickstart, batch_processing.py

**Runtime:** ~5 seconds

**Next steps:**
- See window_aggregation.py for windowing operations
- See 03_distributed_basics/ for real distributed streaming

**Pattern:**
Generator → Batches → Filter → Map → Aggregate → Print

"""

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

import pyarrow as pa
import pyarrow.compute as pc
import time
from typing import Iterator


def stream_generator(num_batches: int = 10, batch_size: int = 100) -> Iterator[pa.Table]:
    """
    Simulate a streaming source by generating batches.

    In real streaming, this would be:
    - Kafka consumer
    - Socket reader
    - File watcher
    """
    print(f"\n📡 Stream Generator Started")
    print(f"   Batches: {num_batches}, Batch size: {batch_size}")
    print("=" * 50)

    for batch_num in range(num_batches):
        # Generate batch
        start_id = batch_num * batch_size
        category = ['A', 'B', 'C'][batch_num % 3]
        data = {
            'event_id': list(range(start_id, start_id + batch_size)),
            'value': [(start_id + i) * 0.5 for i in range(batch_size)],
            'category': [category] * batch_size,
            'timestamp': [time.time() + i * 0.001 for i in range(batch_size)]
        }
        batch = pa.table(data)

        print(f"Batch {batch_num + 1}/{num_batches}: Generated {batch.num_rows} events")

        yield batch

        # Simulate streaming delay
        time.sleep(0.1)


def process_streaming_batch(batch: pa.Table, batch_num: int) -> dict:
    """
    Process a single batch (filter, map, aggregate).

    This is what happens inside an operator.
    """

    # Step 1: Filter (value > 50)
    mask = pc.greater(batch['value'], 50.0)
    filtered = batch.filter(mask)

    # Step 2: Map (add doubled_value column)
    doubled = pc.multiply(filtered['value'], 2.0)
    mapped = filtered.append_column('doubled_value', doubled)

    # Step 3: Aggregate (compute statistics)
    stats = {
        'batch_num': batch_num + 1,
        'input_rows': batch.num_rows,
        'filtered_rows': filtered.num_rows,
        'avg_value': pc.mean(filtered['value']).as_py() if filtered.num_rows > 0 else 0.0,
        'sum_value': pc.sum(filtered['value']).as_py() if filtered.num_rows > 0 else 0.0,
        'category': batch['category'][0].as_py()
    }

    return stats


def main():
    print("🌊 Streaming Simulation Example")
    print("=" * 50)
    print("\nKey insight: Streaming = Processing infinite batches")
    print("Each batch is processed independently (stateless)")

    # Configuration
    num_batches = 10
    batch_size = 100

    # Process stream
    all_stats = []

    print("\n\n⚙️  Processing Stream...")
    print("=" * 50)

    for i, batch in enumerate(stream_generator(num_batches, batch_size)):
        # Process batch
        stats = process_streaming_batch(batch, i)
        all_stats.append(stats)

        # Print batch results
        print(f"\n  Batch {stats['batch_num']}:")
        print(f"    Input: {stats['input_rows']} rows")
        print(f"    Filtered: {stats['filtered_rows']} rows (value > 50)")
        print(f"    Category: {stats['category']}")
        print(f"    Avg value: {stats['avg_value']:.2f}")
        print(f"    Sum value: {stats['sum_value']:.2f}")

    # Summary
    print("\n\n📊 Stream Processing Summary")
    print("=" * 50)
    total_input = sum(s['input_rows'] for s in all_stats)
    total_filtered = sum(s['filtered_rows'] for s in all_stats)
    total_sum = sum(s['sum_value'] for s in all_stats)

    print(f"Total batches processed: {len(all_stats)}")
    print(f"Total input rows: {total_input:,}")
    print(f"Total filtered rows: {total_filtered:,}")
    print(f"Total sum (filtered): {total_sum:,.2f}")
    print(f"Average batch size: {total_input / len(all_stats):.0f} rows")

    print("\n\n✅ Streaming simulation complete!")

    print("\n\n💡 Key Takeaways:")
    print("=" * 50)
    print("1. Streaming = infinite batches")
    print("2. Each batch is processed independently")
    print("3. Operations: filter → map → aggregate")
    print("4. Real streaming uses Kafka, sockets, or files")
    print("5. Sabot handles batching automatically")

    print("\n\n🔗 Next Steps:")
    print("=" * 50)
    print("- See window_aggregation.py for stateful windowing")
    print("- See 03_distributed_basics/ for distributed streaming")
    print("- See 04_production_patterns/ for real-world patterns")


if __name__ == "__main__":
    main()
