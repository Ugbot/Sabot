#!/usr/bin/env python3
"""
Morsel-Driven Parallelism Demo

Demonstrates that morsel-driven parallelism is now enabled by default.
"""

import time
import pyarrow as pa
import pyarrow.compute as pc
from sabot.api import Stream


def demo_default_parallelism():
    """Demo that parallelism is enabled by default"""
    print("=== Default Parallelism Demo ===\n")

    # Create large batch
    print("Creating test data (100K rows)...")
    batch = pa.RecordBatch.from_pydict({
        'x': list(range(100000)),
        'y': list(range(100000, 200000))
    })

    # Define expensive transformation
    def expensive_transform(b):
        """Simulates expensive computation"""
        result = b.append_column(
            'z',
            pc.add(
                pc.multiply(b.column('x'), 2.5),
                pc.power(b.column('y'), 1.5)
            )
        )
        return result

    # Create stream - parallelism is enabled by default
    print("\n1. Processing with default parallelism:")
    stream = Stream.from_batches([batch])

    start = time.perf_counter()
    result = (stream
        .map(expensive_transform)  # ← Automatically parallel
        .filter(lambda b: pc.greater(b.column('z'), 1000000))  # ← Automatically parallel
    )

    # Process the stream
    processed_batches = list(result)
    time_default = time.perf_counter() - start

    print(f"   Time: {time_default * 1000:.2f} ms")
    print(f"   Batches processed: {len(processed_batches)}")

    if processed_batches:
        total_rows = sum(b.num_rows for b in processed_batches)
        print(f"   Total rows: {total_rows:,.0f}")

    # Compare with sequential
    print("\n2. Processing with sequential (forced):")
    start = time.perf_counter()
    sequential_result = (stream
        .sequential()  # ← Disable parallelism
        .map(expensive_transform)
        .filter(lambda b: pc.greater(b.column('z'), 1000000))
    )

    sequential_batches = list(sequential_result)
    time_sequential = time.perf_counter() - start

    print(f"   Time: {time_sequential * 1000:.2f} ms")
    speedup = time_sequential / time_default if time_default > 0 else 0
    print(f"   Speedup from parallelism: {speedup:.2f}x")


def demo_stream_api():
    """Demo using high-level Stream API with default parallelism"""
    print("\n=== Stream API Demo (Default Parallelism) ===\n")

    # Create in-memory source
    data = [
        {'transaction_id': i, 'amount': i * 10.5}
        for i in range(50000)
    ]

    print("Creating stream - parallelism enabled by default...")

    # Build stream - parallelism is automatic
    stream = (Stream.from_iterable(data)
        .map(lambda b: b.append_column(  # ← Automatically parallel
            'tax',
            pc.multiply(b.column('amount'), 0.08)
        ))
        .filter(lambda b: pc.greater(b.column('amount'), 100))  # ← Automatically parallel
    )

    print("Processing stream with automatic parallelism...")
    start = time.perf_counter()

    total_rows = 0
    for batch in stream:
        total_rows += batch.num_rows

    elapsed = time.perf_counter() - start

    print(f"\nProcessed {total_rows:,} rows in {elapsed*1000:.2f}ms")
    print(",.0f")


def demo_configuration():
    """Demo configuring parallelism settings"""
    print("\n=== Configuration Demo ===\n")

    # Create batch
    batch = pa.RecordBatch.from_pydict({
        'value': list(range(50000))
    })

    def expensive_func(b):
        return b.set_column(0, 'value', pc.power(b.column('value'), 2))

    print("Testing different parallelism configurations:\n")

    configs = [
        ("Default auto", lambda s: s),
        ("8 workers", lambda s: s.parallel(num_workers=8)),
        ("4 workers, 32KB morsels", lambda s: s.parallel(num_workers=4, morsel_size_kb=32)),
        ("Sequential", lambda s: s.sequential()),
    ]

    for name, config_func in configs:
        stream = Stream.from_batches([batch])
        configured_stream = config_func(stream.map(expensive_func))

        start = time.perf_counter()
        results = list(configured_stream)
        elapsed = time.perf_counter() - start

        print(".2f")


def main():
    """Run all demos"""
    print("Morsel-Driven Parallelism Demos (Default Enabled)")
    print("=" * 60)
    print("Parallelism is now enabled by default for all operations!")
    print("=" * 60)

    demo_default_parallelism()
    demo_stream_api()
    demo_configuration()

    print("\n" + "=" * 60)
    print("Key Takeaways:")
    print("• Morsel parallelism is enabled by default")
    print("• Small batches automatically bypass parallelism")
    print("• Large batches get automatic parallel processing")
    print("• Use .sequential() to disable when needed")
    print("• Use .parallel() to configure settings")


if __name__ == '__main__':
    main()
