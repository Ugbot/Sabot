#!/usr/bin/env python3
"""Test async iteration for operators."""

import asyncio
from sabot import cyarrow as pa
from sabot.cyarrow import compute as pc
from sabot._cython.operators import CythonMapOperator, CythonFilterOperator


async def async_batch_source():
    """Mock async source (like Kafka)."""
    for i in range(10):
        batch = pa.RecordBatch.from_pydict({
            'id': list(range(i * 100, (i + 1) * 100)),
            'value': [j * 2 for j in range(100)]
        })
        yield batch
        await asyncio.sleep(0.001)  # Simulate network delay


def sync_batch_source():
    """Mock sync source (like Parquet)."""
    for i in range(10):
        yield pa.RecordBatch.from_pydict({
            'id': list(range(i * 100, (i + 1) * 100)),
            'value': [j * 2 for j in range(100)]
        })


async def test_async_iteration():
    """Test async iteration over streaming source."""
    print("\n=== Testing Async Iteration ===")

    # Create operator with async source
    source = async_batch_source()
    operator = CythonMapOperator(
        source=source,
        map_func=lambda b: b.append_column('doubled',
            pc.multiply(b.column('value'), 2))
    )

    # Consume asynchronously
    batch_count = 0
    row_count = 0
    async for batch in operator:
        batch_count += 1
        row_count += batch.num_rows
        assert 'doubled' in batch.schema.names

    print(f"  Processed {batch_count} batches, {row_count} rows")
    assert batch_count == 10
    assert row_count == 1000


async def test_sync_source_in_async():
    """Test sync source in async context."""
    print("\n=== Testing Sync Source in Async Context ===")

    # Create operator with sync source
    source = sync_batch_source()
    operator = CythonFilterOperator(
        source=source,
        predicate=lambda b: pc.greater(b.column('value'), 100)
    )

    # Consume asynchronously
    batch_count = 0
    async for batch in operator:
        batch_count += 1
        # All values should be > 100
        assert all(v > 100 for v in batch.column('value').to_pylist())

    print(f"  Processed {batch_count} batches")
    assert batch_count > 0


def test_sync_iteration_unchanged():
    """Verify sync iteration still works."""
    print("\n=== Testing Sync Iteration (Unchanged) ===")

    source = sync_batch_source()
    operator = CythonMapOperator(
        source=source,
        map_func=lambda b: b
    )

    batch_count = 0
    for batch in operator:
        batch_count += 1

    print(f"  Processed {batch_count} batches")
    assert batch_count == 10


if __name__ == '__main__':
    # Run async tests
    asyncio.run(test_async_iteration())
    asyncio.run(test_sync_source_in_async())

    # Run sync test
    test_sync_iteration_unchanged()

    print("\n✅ All async iteration tests passed!")
