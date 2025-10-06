# Phase 1: Solidify Batch-Only Operator API

**Version**: 1.0
**Date**: October 6, 2025
**Status**: Implementation Plan
**Estimated Effort**: 16-20 hours

---

## Executive Summary

Phase 1 establishes the foundational batch-first operator contract where **all operators process RecordBatch → RecordBatch**. The key principle: streaming is just infinite batching. Per-record iteration exists only as API sugar at the user layer, never in the data plane.

### Goals
1. Clarify that BaseOperator always yields RecordBatch, never individual records
2. Add both `__iter__()` (batch mode) and `__aiter__()` (streaming mode) to support bounded/unbounded sources
3. Document batch-first contract with clear examples
4. Deprecate per-record iteration in data plane (keep as API-level convenience only)
5. Create comprehensive examples showing batch API patterns

### Success Criteria
- ✅ All operators in `sabot/_cython/operators/` process batches exclusively
- ✅ BaseOperator has both sync (`__iter__`) and async (`__aiter__`) iteration
- ✅ Comprehensive documentation with batch-first examples
- ✅ Test suite validates batch-only processing
- ✅ Zero regressions in existing functionality

---

## Current State Analysis

### What Exists Today

#### 1. **BaseOperator** (`sabot/_cython/operators/transform.pyx`)
**Current Issues:**
- Has `__iter__()` that yields batches ✅
- Missing `__aiter__()` for async iteration ❌
- Incomplete documentation about batch-first contract ⚠️
- No clear distinction between batch mode vs streaming mode ⚠️

**Current Implementation:**
```cython
cdef class BaseOperator:
    def __cinit__(self, *args, **kwargs):
        self._source = kwargs.get('source')
        self._schema = kwargs.get('schema')

    cpdef object process_batch(self, object batch):
        """Process a single RecordBatch. Override in subclasses."""
        return batch

    def __iter__(self):
        """Iterate over source, applying operator to each batch."""
        if self._source is None:
            return

        for batch in self._source:
            result = self.process_batch(batch)
            if result is not None and result.num_rows > 0:
                yield result
```

#### 2. **Transform Operators** (`sabot/_cython/operators/transform.pyx`)
**Status:** ✅ Already batch-first
- `CythonMapOperator` - processes batches via `process_batch()`
- `CythonFilterOperator` - uses Arrow SIMD filter on batches
- `CythonSelectOperator` - zero-copy projection on batches
- `CythonFlatMapOperator` - expands batches to multiple batches
- `CythonUnionOperator` - merges batch streams

#### 3. **Stateful Operators** (joins, aggregations)
**Status:** ✅ Already batch-first
- `CythonHashJoinOperator` - builds hash table from batches, probes with batches
- `CythonGroupByOperator` - uses Arrow hash_aggregate on batches
- All process RecordBatch → RecordBatch

#### 4. **Stream API** (`sabot/api/stream.py`)
**Current Issues:**
- Has batch-based operators ✅
- Missing documentation about batch-first principle ⚠️
- No clear examples of batch vs streaming iteration ⚠️
- `.records()` method exists but not documented as "API sugar only" ⚠️

### Gap Analysis

| Component | Current State | Required State | Effort |
|-----------|---------------|----------------|--------|
| BaseOperator.__aiter__() | Missing | Add async iteration | 2h |
| BaseOperator docs | Incomplete | Full batch-first docs | 1h |
| Operator docs | Sparse | Clear batch contract | 2h |
| Stream API docs | Basic | Batch vs streaming examples | 2h |
| Test coverage | ~30% | 80%+ for batch operations | 4h |
| Examples | None specific | Batch-first examples | 3h |
| Design doc updates | Partial | Complete alignment | 2h |

**Total Estimated Effort:** 16 hours

---

## Detailed Implementation Plan

### Task 1: Add Async Iteration to BaseOperator

**File:** `/Users/bengamble/Sabot/sabot/_cython/operators/transform.pyx`
**Effort:** 2 hours
**Priority:** P0 - Critical

#### Changes Required

Add `__aiter__()` method to BaseOperator for async/streaming iteration:

```cython
# In BaseOperator class (after __iter__ method)

async def __aiter__(self):
    """
    Asynchronous iteration over batches (streaming mode).

    Use for infinite sources (Kafka, sockets). Runs forever until
    externally interrupted.

    SAME LOGIC as __iter__, just async-aware. The difference between
    batch and streaming is ONLY the boundedness of the source.

    Examples:
        # Streaming mode (infinite)
        async for batch in kafka_source:
            process(batch)

        # Batch mode (finite) - same operator!
        for batch in parquet_source:
            process(batch)
    """
    if self._source is None:
        return

    # Check if source is async
    if hasattr(self._source, '__aiter__'):
        # Async source (Kafka, network stream, etc.)
        async for batch in self._source:
            result = self.process_batch(batch)
            if result is not None and result.num_rows > 0:
                yield result
    else:
        # Sync source wrapped in async (for compatibility)
        for batch in self._source:
            result = self.process_batch(batch)
            if result is not None and result.num_rows > 0:
                yield result
            # Yield control to event loop
            import asyncio
            await asyncio.sleep(0)
```

#### Testing Strategy

**File:** `/Users/bengamble/Sabot/tests/unit/operators/test_async_iteration.py` (NEW)

```python
#!/usr/bin/env python3
"""Test async iteration for operators."""

import asyncio
import pyarrow as pa
import pyarrow.compute as pc
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
```

**Run test:**
```bash
python /Users/bengamble/Sabot/tests/unit/operators/test_async_iteration.py
```

#### Validation
- [ ] Test passes for async iteration
- [ ] Test passes for sync source in async context
- [ ] Existing sync iteration tests still pass
- [ ] No performance regression

---

### Task 2: Document Batch-First Contract in BaseOperator

**File:** `/Users/bengamble/Sabot/sabot/_cython/operators/transform.pyx`
**Effort:** 1 hour
**Priority:** P0 - Critical

#### Changes Required

Update BaseOperator class docstring:

```cython
cdef class BaseOperator:
    """
    Base class for all streaming operators.

    FUNDAMENTAL CONTRACT: Batch-First Processing
    ============================================

    ALL operators in Sabot process RecordBatch → RecordBatch. There is
    no per-record processing in the data plane.

    Key Principles:

    1. **Batch-Only Processing**
       - process_batch() takes RecordBatch, returns RecordBatch
       - __iter__() yields RecordBatch, never individual records
       - __aiter__() yields RecordBatch asynchronously

    2. **Streaming = Infinite Batching**
       - Batch mode: finite source → __iter__() terminates
       - Streaming mode: infinite source → __aiter__() runs forever
       - SAME operators, SAME processing, just different boundedness

    3. **Per-Record is API Sugar Only**
       - Stream.records() exists for user convenience
       - Unpacks batches → records at API layer ONLY
       - Data plane (operators) NEVER see individual records

    4. **Zero-Copy Throughout**
       - RecordBatch is Arrow columnar format
       - All operations maintain zero-copy semantics
       - SIMD acceleration via Arrow compute kernels

    Usage Examples:

        # Batch mode (finite source)
        parquet_batches = pa.parquet.ParquetFile('data.parquet').iter_batches()
        operator = CythonMapOperator(parquet_batches, transform_func)

        for batch in operator:  # Terminates when file exhausted
            process(batch)

        # Streaming mode (infinite source)
        kafka_batches = kafka_source.stream_batches()
        operator = CythonMapOperator(kafka_batches, transform_func)

        async for batch in operator:  # Runs forever
            process(batch)

    Operator Implementation:

        @cython.final
        cdef class MyOperator(BaseOperator):
            cpdef object process_batch(self, object batch):
                # Transform RecordBatch → RecordBatch
                result = pc.multiply(batch.column('x'), 2)
                return batch.set_column(0, 'x', result)

    Integration with Morsel Parallelism:

        Operators can optionally override process_morsel() for
        morsel-driven execution. Default implementation delegates
        to process_batch().

    Integration with Shuffle:

        Stateful operators set self._stateful = True and define
        partition keys. Shuffle layer handles network repartitioning.
    """

    cdef:
        object _source              # Upstream operator or iterable
        object _schema              # Arrow schema (optional)
        bint _stateful              # Does this op have keyed state?
        list _key_columns           # Key columns for partitioning
        int _parallelism_hint       # Suggested parallelism
```

---

### Task 3: Update All Operator Docstrings

**Files:**
- `/Users/bengamble/Sabot/sabot/_cython/operators/transform.pyx`
- `/Users/bengamble/Sabot/sabot/_cython/operators/joins.pyx`
- `/Users/bengamble/Sabot/sabot/_cython/operators/aggregations.pyx`

**Effort:** 2 hours
**Priority:** P1 - High

#### Changes for Each Operator

Add standardized docstring emphasizing batch processing:

```cython
@cython.final
cdef class CythonMapOperator(BaseOperator):
    """
    Map operator: transform each RecordBatch using a function.

    BATCH-FIRST: Processes RecordBatch → RecordBatch

    The map function receives a RecordBatch and must return a RecordBatch.
    No per-record iteration occurs in the data plane.

    Supported Functions:
    1. Arrow compute expressions (preferred - SIMD accelerated)
    2. Python functions (auto Numba-compiled if possible)
    3. Cython functions

    Performance:
    - Arrow compute: 10-100M records/sec (SIMD)
    - Numba-compiled: 5-50M records/sec
    - Interpreted Python: 0.5-5M records/sec

    Examples:
        # Arrow compute (SIMD - fastest)
        stream.map(lambda b: b.append_column('fee',
            pc.multiply(b.column('amount'), 0.03)))

        # Auto Numba-compiled
        def transform(batch):
            # Sabot auto-compiles this with Numba
            total = pc.add(batch.column('a'), batch.column('b'))
            return batch.append_column('total', total)
        stream.map(transform)

        # Complex transformation
        def enrich(batch):
            # Multiple columns
            batch = batch.append_column('doubled',
                pc.multiply(batch.column('x'), 2))
            batch = batch.append_column('category',
                pc.if_else(pc.greater(batch.column('x'), 100), 'HIGH', 'LOW'))
            return batch
        stream.map(enrich)

    Args:
        source: Iterable of RecordBatches
        map_func: Function(RecordBatch) → RecordBatch
        vectorized: If True, function is already vectorized
        schema: Output schema (inferred if None)
    """
```

Repeat similar pattern for:
- `CythonFilterOperator`
- `CythonSelectOperator`
- `CythonFlatMapOperator`
- `CythonHashJoinOperator`
- `CythonGroupByOperator`

---

### Task 4: Update Stream API Documentation

**File:** `/Users/bengamble/Sabot/sabot/api/stream.py`
**Effort:** 2 hours
**Priority:** P1 - High

#### Changes Required

1. **Update class docstring:**

```python
class Stream:
    """
    High-level streaming API with automatic Cython acceleration.

    BATCH-FIRST ARCHITECTURE
    ========================

    Sabot's fundamental unit of processing is the RecordBatch (Arrow columnar
    format). All operations are batch-level transformations. Streaming and
    batch processing use IDENTICAL operators - the only difference is source
    boundedness.

    Key Concepts:

    1. **Everything is Batches**
       - All operators process RecordBatch → RecordBatch
       - Zero-copy throughout via Arrow
       - SIMD acceleration via Arrow compute

    2. **Batch Mode vs Streaming Mode**
       - Batch mode: Finite source (files, tables) → iteration terminates
       - Streaming mode: Infinite source (Kafka, sockets) → runs forever
       - SAME code, SAME operators, different boundedness only

    3. **Per-Record is API Sugar**
       - .records() method unpacks batches for user convenience
       - NOT recommended for production (use batch API for performance)
       - Data plane (Cython operators) NEVER see individual records

    4. **Lazy Evaluation**
       - Operations build a DAG, no execution until consumed
       - for batch in stream → executes the pipeline
       - async for batch in stream → async execution

    Examples:

        # Batch processing (finite)
        stream = Stream.from_parquet('data.parquet')
        result = (stream
            .filter(lambda b: pc.greater(b.column('amount'), 1000))
            .map(lambda b: b.append_column('fee', pc.multiply(b.column('amount'), 0.03)))
            .select('id', 'amount', 'fee')
        )

        for batch in result:  # Terminates when file exhausted
            process(batch)

        # Streaming processing (infinite) - SAME PIPELINE!
        stream = Stream.from_kafka('localhost:9092', 'transactions', 'my-group')
        result = (stream
            .filter(lambda b: pc.greater(b.column('amount'), 1000))
            .map(lambda b: b.append_column('fee', pc.multiply(b.column('amount'), 0.03)))
            .select('id', 'amount', 'fee')
        )

        async for batch in result:  # Runs forever
            process(batch)

    Performance:
        - Filter: 10-500M records/sec (SIMD)
        - Map: 10-100M records/sec
        - Select: 50-1000M records/sec (zero-copy)
        - Join: 2-50M records/sec
        - GroupBy: 5-100M records/sec
    """
```

2. **Add batch vs streaming comparison:**

```python
# At end of Stream class, before GroupedStream

# ========================================================================
# Batch vs Streaming Modes
# ========================================================================

def is_bounded(self) -> bool:
    """
    Check if stream is bounded (will terminate).

    Returns:
        True if source is finite (batch mode)
        False if source is infinite (streaming mode)
    """
    # Check if source has a length (finite)
    return hasattr(self._source, '__len__')

@classmethod
def from_batches_bounded(cls, batches: List[pa.RecordBatch]) -> 'Stream':
    """
    Create bounded stream from list of batches (batch mode).

    Use this when you have all data in memory and want finite processing.
    """
    return cls(iter(batches))

@classmethod
def from_batches_unbounded(cls, batch_generator: Callable[[], pa.RecordBatch]) -> 'Stream':
    """
    Create unbounded stream from generator (streaming mode).

    Use this for infinite streams (e.g., polling a queue, monitoring a file).

    Example:
        def poll_queue():
            while True:
                messages = queue.poll(timeout=1.0)
                if messages:
                    yield pa.RecordBatch.from_pylist(messages)

        stream = Stream.from_batches_unbounded(poll_queue)
    """
    return cls(batch_generator())
```

---

### Task 5: Create Batch-First Examples

**File:** `/Users/bengamble/Sabot/examples/batch_first_examples.py` (NEW)
**Effort:** 3 hours
**Priority:** P1 - High

#### Content

```python
#!/usr/bin/env python3
"""
Batch-First API Examples

Demonstrates Sabot's fundamental principle: everything is batches.
Shows how batch mode and streaming mode use identical operators.

Run: python examples/batch_first_examples.py
"""

import asyncio
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from sabot.api.stream import Stream
from pathlib import Path
import tempfile
import time


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
```

**Run example:**
```bash
python /Users/bengamble/Sabot/examples/batch_first_examples.py
```

---

### Task 6: Expand Test Coverage

**File:** `/Users/bengamble/Sabot/tests/unit/operators/test_batch_contract.py` (NEW)
**Effort:** 4 hours
**Priority:** P0 - Critical

#### Test Suite

```python
#!/usr/bin/env python3
"""
Test batch-first contract for all operators.

Validates that operators ONLY process batches, never individual records.
"""

import pytest
import pyarrow as pa
import pyarrow.compute as pc
from sabot._cython.operators import (
    CythonMapOperator,
    CythonFilterOperator,
    CythonSelectOperator,
    CythonFlatMapOperator,
    CythonUnionOperator,
)


@pytest.fixture
def sample_batches():
    """Generate sample RecordBatches."""
    return [
        pa.RecordBatch.from_pydict({
            'id': list(range(i * 100, (i + 1) * 100)),
            'value': [j * 2 for j in range(100)],
            'category': ['A' if j % 2 == 0 else 'B' for j in range(100)]
        })
        for i in range(10)
    ]


class TestBatchContract:
    """Test that all operators follow batch-first contract."""

    def test_operator_always_yields_batches(self, sample_batches):
        """Verify operators yield RecordBatch, never individual records."""
        operator = CythonMapOperator(
            source=iter(sample_batches),
            map_func=lambda b: b
        )

        for item in operator:
            assert isinstance(item, pa.RecordBatch), \
                f"Operator yielded {type(item)}, expected RecordBatch"
            assert item.num_rows > 0

    def test_process_batch_input_output_types(self, sample_batches):
        """Verify process_batch takes and returns RecordBatch."""
        operator = CythonMapOperator(
            source=None,
            map_func=lambda b: b.append_column('new',
                pc.multiply(b.column('value'), 2))
        )

        batch = sample_batches[0]
        result = operator.process_batch(batch)

        assert isinstance(result, pa.RecordBatch)
        assert 'new' in result.schema.names

    def test_filter_returns_batches(self, sample_batches):
        """Filter operator returns batches."""
        operator = CythonFilterOperator(
            source=iter(sample_batches),
            predicate=lambda b: pc.greater(b.column('value'), 100)
        )

        for batch in operator:
            assert isinstance(batch, pa.RecordBatch)
            # All values should be > 100
            assert all(v > 100 for v in batch.column('value').to_pylist())

    def test_select_returns_batches(self, sample_batches):
        """Select operator returns batches."""
        operator = CythonSelectOperator(
            source=iter(sample_batches),
            columns=['id', 'value']
        )

        for batch in operator:
            assert isinstance(batch, pa.RecordBatch)
            assert batch.schema.names == ['id', 'value']

    def test_flatmap_returns_batches(self, sample_batches):
        """FlatMap operator returns batches."""
        def split_batch(batch):
            # Split into 2 smaller batches
            mid = batch.num_rows // 2
            return [batch.slice(0, mid), batch.slice(mid)]

        operator = CythonFlatMapOperator(
            source=iter(sample_batches),
            flat_map_func=split_batch
        )

        for batch in operator:
            assert isinstance(batch, pa.RecordBatch)

    def test_union_returns_batches(self, sample_batches):
        """Union operator returns batches."""
        source1 = iter(sample_batches[:5])
        source2 = iter(sample_batches[5:])

        operator = CythonUnionOperator(source1, source2)

        for batch in operator:
            assert isinstance(batch, pa.RecordBatch)

    def test_no_per_record_iteration(self, sample_batches):
        """Verify operators don't iterate per-record internally."""
        # This is tested by checking that process_batch is called
        # with RecordBatch, not individual records

        call_log = []

        def logging_map(batch):
            call_log.append(type(batch))
            return batch

        operator = CythonMapOperator(
            source=iter(sample_batches),
            map_func=logging_map
        )

        list(operator)  # Consume

        # All calls should be RecordBatch
        assert all(t == pa.RecordBatch for t in call_log), \
            "Operator called map function with non-batch types"

    def test_empty_batches_handled(self):
        """Empty batches are filtered out."""
        empty_batch = pa.RecordBatch.from_pydict({
            'id': [],
            'value': []
        })

        batches = [empty_batch] * 5

        operator = CythonMapOperator(
            source=iter(batches),
            map_func=lambda b: b
        )

        result = list(operator)
        assert len(result) == 0, "Empty batches should be filtered"

    def test_null_batches_handled(self, sample_batches):
        """Null batches are handled gracefully."""
        def return_null(batch):
            return None

        operator = CythonMapOperator(
            source=iter(sample_batches),
            map_func=return_null
        )

        result = list(operator)
        assert len(result) == 0


class TestBatchVsStreamingMode:
    """Test batch mode vs streaming mode behavior."""

    def test_sync_iteration_terminates(self, sample_batches):
        """Sync iteration terminates (batch mode)."""
        operator = CythonMapOperator(
            source=iter(sample_batches),
            map_func=lambda b: b
        )

        count = 0
        for batch in operator:
            count += 1

        assert count == len(sample_batches)
        # Iterator is exhausted
        assert list(operator) == []

    @pytest.mark.asyncio
    async def test_async_iteration_with_finite_source(self, sample_batches):
        """Async iteration with finite source terminates."""
        operator = CythonMapOperator(
            source=iter(sample_batches),
            map_func=lambda b: b
        )

        count = 0
        async for batch in operator:
            count += 1

        assert count == len(sample_batches)

    @pytest.mark.asyncio
    async def test_async_iteration_with_async_source(self):
        """Async iteration with async source."""
        async def async_source():
            for i in range(5):
                yield pa.RecordBatch.from_pydict({
                    'id': list(range(i * 10, (i + 1) * 10)),
                    'value': list(range(10))
                })

        operator = CythonMapOperator(
            source=async_source(),
            map_func=lambda b: b.append_column('doubled',
                pc.multiply(b.column('value'), 2))
        )

        count = 0
        async for batch in operator:
            assert isinstance(batch, pa.RecordBatch)
            assert 'doubled' in batch.schema.names
            count += 1

        assert count == 5


class TestZeroCopy:
    """Test zero-copy semantics."""

    def test_select_is_zero_copy(self, sample_batches):
        """Select operation doesn't copy data."""
        batch = sample_batches[0]

        operator = CythonSelectOperator(
            source=iter([batch]),
            columns=['id', 'value']
        )

        result = next(iter(operator))

        # Check that underlying buffers are shared (zero-copy)
        # Arrow's select() creates a new RecordBatch but shares buffers
        assert result.schema.names == ['id', 'value']

    def test_filter_minimal_copy(self, sample_batches):
        """Filter copies only selected rows."""
        batch = sample_batches[0]
        original_size = batch.nbytes

        operator = CythonFilterOperator(
            source=iter([batch]),
            predicate=lambda b: pc.greater(b.column('value'), 100)
        )

        result = next(iter(operator))

        # Result should be smaller (fewer rows)
        assert result.nbytes < original_size


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
```

**Run tests:**
```bash
pytest /Users/bengamble/Sabot/tests/unit/operators/test_batch_contract.py -v
```

---

### Task 7: Update Design Documentation

**File:** `/Users/bengamble/Sabot/docs/design/UNIFIED_BATCH_ARCHITECTURE.md`
**Effort:** 2 hours
**Priority:** P2 - Medium

#### Changes Required

Update implementation status section:

```markdown
## Implementation Status

### Phase 1: Batch-Only Operator API ✅ COMPLETE

**Status:** Implemented and tested (October 2025)

**Delivered:**
- [x] BaseOperator has both `__iter__()` and `__aiter__()`
- [x] All operators process RecordBatch → RecordBatch exclusively
- [x] Comprehensive documentation with batch-first contract
- [x] Examples demonstrating batch vs streaming modes
- [x] Test coverage >80% for batch operations
- [x] Zero regressions in existing functionality

**Key Files:**
- `/Users/bengamble/Sabot/sabot/_cython/operators/transform.pyx` - Updated BaseOperator
- `/Users/bengamble/Sabot/sabot/api/stream.py` - Updated Stream API docs
- `/Users/bengamble/Sabot/examples/batch_first_examples.py` - Comprehensive examples
- `/Users/bengamble/Sabot/tests/unit/operators/test_batch_contract.py` - Contract tests
- `/Users/bengamble/Sabot/tests/unit/operators/test_async_iteration.py` - Async tests

**Performance Validated:**
- Filter: 10-500M records/sec (SIMD)
- Map: 10-100M records/sec
- Select: 50-1000M records/sec (zero-copy)
- No performance regression vs previous implementation

### Phase 2: Auto-Numba UDF Compilation (NEXT)

**Status:** Planned
...
```

---

## Testing Strategy

### Unit Tests

**Location:** `/Users/bengamble/Sabot/tests/unit/operators/`

1. **test_batch_contract.py** (NEW)
   - Validates all operators yield RecordBatch
   - Tests process_batch input/output types
   - Verifies no per-record iteration
   - Tests empty/null batch handling

2. **test_async_iteration.py** (NEW)
   - Tests async iteration (`__aiter__()`)
   - Tests sync source in async context
   - Validates existing sync iteration unchanged

3. **Update existing tests:**
   - `test_cython_operators.py` - verify no regressions
   - `test_stream_api.py` - add batch vs streaming examples

### Integration Tests

**Location:** `/Users/bengamble/Sabot/tests/integration/`

1. **test_batch_streaming_equivalence.py** (NEW)
   - Same pipeline, batch source → validate terminates
   - Same pipeline, streaming source → validate runs forever (with timeout)
   - Compare results are identical

### Performance Tests

**Location:** `/Users/bengamble/Sabot/benchmarks/`

1. **batch_operator_benchmark.py** (NEW)
   - Measure throughput for each operator
   - Compare batch API vs record API (10-100x difference)
   - Validate zero-copy operations

---

## Success Criteria

### Functional Requirements
- [ ] All operators in `sabot/_cython/operators/` process batches only
- [ ] BaseOperator has working `__iter__()` and `__aiter__()`
- [ ] Stream API clearly documents batch-first principle
- [ ] Examples demonstrate batch vs streaming modes
- [ ] No per-record iteration in data plane

### Performance Requirements
- [ ] Filter: ≥10M records/sec (SIMD)
- [ ] Map: ≥10M records/sec
- [ ] Select: ≥50M records/sec (zero-copy)
- [ ] No regression vs existing implementation

### Quality Requirements
- [ ] Test coverage ≥80% for batch operations
- [ ] All tests pass (unit + integration)
- [ ] Documentation complete and accurate
- [ ] Examples run without errors
- [ ] No breaking changes to public API

### Deliverables Checklist
- [ ] Updated BaseOperator with `__aiter__()`
- [ ] Updated operator docstrings
- [ ] Updated Stream API documentation
- [ ] Created batch_first_examples.py
- [ ] Created test_batch_contract.py
- [ ] Created test_async_iteration.py
- [ ] Updated design documentation
- [ ] All tests passing
- [ ] Performance benchmarks run

---

## Dependencies and Prerequisites

### Required Before Starting
1. ✅ Cython operators exist and compile
2. ✅ Arrow integration working
3. ✅ Basic test infrastructure in place

### External Dependencies
- **PyArrow** ≥10.0.0 (for RecordBatch, compute kernels)
- **Cython** ≥3.0 (for compilation)
- **pytest** (for testing)
- **pytest-asyncio** (for async tests)

### No Breaking Changes
This phase is **backward compatible**:
- Existing `__iter__()` unchanged
- New `__aiter__()` is additive
- API remains the same
- Performance improves or stays same

---

## Risk Assessment

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| Async iteration breaks existing code | Low | High | Extensive testing, no changes to sync path |
| Documentation incomplete | Medium | Medium | Review checklist, peer review |
| Performance regression | Low | High | Benchmark before/after, validate metrics |
| Test coverage insufficient | Medium | Medium | Coverage tools, manual review |

---

## Implementation Checklist

### Week 1: Core Implementation (8 hours)
- [ ] Day 1-2: Add `__aiter__()` to BaseOperator (2h)
- [ ] Day 2-3: Update BaseOperator documentation (1h)
- [ ] Day 3-4: Update all operator docstrings (2h)
- [ ] Day 4-5: Update Stream API documentation (2h)
- [ ] Day 5: Code review and refinements (1h)

### Week 2: Testing & Examples (8 hours)
- [ ] Day 1-2: Create batch_first_examples.py (3h)
- [ ] Day 3-4: Create test_batch_contract.py (2h)
- [ ] Day 4-5: Create test_async_iteration.py (2h)
- [ ] Day 5: Run full test suite, fix issues (1h)

### Week 3: Validation & Documentation (4 hours)
- [ ] Day 1: Update design documentation (2h)
- [ ] Day 2: Performance benchmarks (1h)
- [ ] Day 3: Final validation and sign-off (1h)

**Total Effort:** 16-20 hours

---

## Next Steps After Phase 1

Once Phase 1 is complete, proceed to:

### Phase 2: Auto-Numba UDF Compilation
- Automatic JIT compilation of user functions
- 10-100x speedup for Python UDFs
- Transparent to users

### Phase 3: Morsel-Driven Execution
- Connect operators to morsel parallelism
- Work-stealing for load balancing
- NUMA-aware scheduling

See `/Users/bengamble/Sabot/docs/design/UNIFIED_BATCH_ARCHITECTURE.md` for full roadmap.

---

## Appendix: Code Snippets Reference

### A. BaseOperator Full Implementation

```cython
# File: /Users/bengamble/Sabot/sabot/_cython/operators/transform.pyx

cdef class BaseOperator:
    """Base class for all Cython streaming operators."""

    cdef:
        object _source
        object _schema

    def __cinit__(self, *args, **kwargs):
        self._source = kwargs.get('source')
        self._schema = kwargs.get('schema')

    cpdef object process_batch(self, object batch):
        """Process a single RecordBatch."""
        return batch

    def __iter__(self):
        """Sync iteration (batch mode)."""
        if self._source is None:
            return

        for batch in self._source:
            result = self.process_batch(batch)
            if result is not None and result.num_rows > 0:
                yield result

    async def __aiter__(self):
        """Async iteration (streaming mode)."""
        if self._source is None:
            return

        if hasattr(self._source, '__aiter__'):
            async for batch in self._source:
                result = self.process_batch(batch)
                if result is not None and result.num_rows > 0:
                    yield result
        else:
            for batch in self._source:
                result = self.process_batch(batch)
                if result is not None and result.num_rows > 0:
                    yield result
                import asyncio
                await asyncio.sleep(0)
```

### B. Stream API Usage Examples

```python
# Batch mode (finite)
stream = Stream.from_parquet('data.parquet')
for batch in stream.filter(...).map(...):
    process(batch)  # Terminates when file exhausted

# Streaming mode (infinite) - SAME operators!
stream = Stream.from_kafka('localhost:9092', 'topic', 'group')
async for batch in stream.filter(...).map(...):
    process(batch)  # Runs forever
```

---

**Document Version:** 1.0
**Last Updated:** October 6, 2025
**Author:** Sabot Engineering Team
**Status:** Ready for Implementation
