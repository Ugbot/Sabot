# CyArrow - Sabot's Zero-Copy Arrow Integration

## What is CyArrow?

**CyArrow** is Sabot's custom Cython-accelerated wrapper around Apache Arrow's C++ API. It provides zero-copy columnar operations with direct buffer access for maximum performance.

### NOT the same as PyArrow

- **PyArrow**: Standard Python bindings for Apache Arrow
- **CyArrow**: Sabot's optimized Cython layer with direct C++ access

## Why CyArrow?

1. **Zero-copy operations**: Direct C++ buffer access (~5ns per element)
2. **SIMD acceleration**: Vectorized operations via Arrow compute kernels
3. **GIL-free execution**: All hot paths release Python's GIL
4. **10-100x faster**: Compared to pure Python implementations

## Performance Benchmarks

Measured on M1 Pro with 500K rows:

| Operation | CyArrow Throughput | Notes |
|-----------|-------------------|-------|
| Hash Join | 7.2M rows/sec | SIMD-accelerated hash |
| Window IDs | ~2-3ns per element | Tumbling windows |
| Filtering | 50-100x Python | Arrow compute kernels |
| Sorting | 10M+ rows/sec | Zero-copy slicing |
| Buffer Access | ~5ns per element | Direct C++ pointers |

## Usage

### Basic Import

```python
# Import CyArrow (Sabot's optimized layer)
from sabot import cyarrow
from sabot.cyarrow import (
    compute_window_ids,
    hash_join_batches,
    sort_and_take,
    ArrowComputeEngine,
    USING_ZERO_COPY
)

# Still use PyArrow for standard operations
import pyarrow as pa
import pyarrow.compute as pc
```

### Zero-Copy Operations

```python
import pyarrow as pa
from sabot.cyarrow import hash_join_batches, compute_window_ids

# Create sample data
left = pa.record_batch({
    'id': [1, 2, 3],
    'value': [10, 20, 30]
})

right = pa.record_batch({
    'id': [2, 3, 4],
    'name': ['A', 'B', 'C']
})

# Zero-copy hash join (7M+ rows/sec)
joined = hash_join_batches(left, right, 'id', 'id', 'inner')

# Zero-copy windowing (2-3ns per element)
timestamps = pa.record_batch({
    'timestamp': [1000, 2000, 3000, 4000, 5000],
    'value': [1, 2, 3, 4, 5]
})

windowed = compute_window_ids(timestamps, 'timestamp', window_size_ms=1000)
print(windowed)
# Adds 'window_id' column with tumbling window assignments
```

### High-Performance Filtering

```python
from sabot.cyarrow import ArrowComputeEngine

engine = ArrowComputeEngine()

batch = pa.record_batch({
    'price': [100.0, 200.0, 300.0, 400.0],
    'quantity': [10, 20, 30, 40]
})

# 50-100x faster than Python loops
filtered = engine.filter_batch(batch, 'price > 150.0')
```

### Zero-Copy Sorting

```python
from sabot.cyarrow import sort_and_take

batch = pa.record_batch({
    'name': ['Alice', 'Bob', 'Charlie', 'David'],
    'score': [95, 87, 92, 88]
})

# Get top 2 by score (zero-copy slice)
top_2 = sort_and_take(batch, [('score', 'descending')], limit=2)
```

## Architecture

```
┌─────────────────────────────────────┐
│  Your Application                   │
│  from sabot.cyarrow import ...      │
└─────────────────────────────────────┘
              ↓
┌─────────────────────────────────────┐
│  sabot/cyarrow.py                   │
│  Python API layer                   │
└─────────────────────────────────────┘
              ↓
┌─────────────────────────────────────┐
│  sabot/_c/arrow_core.pyx            │
│  Cython C++ bindings (zero-copy)    │
│  - compute_window_ids()             │
│  - hash_join_batches()              │
│  - sort_and_take()                  │
│  - ArrowComputeEngine               │
└─────────────────────────────────────┘
              ↓
┌─────────────────────────────────────┐
│  PyArrow C++ API                    │
│  cimport pyarrow.lib                │
│  Direct buffer access (no overhead) │
└─────────────────────────────────────┘
```

## Key Modules

### Core Operations
- `sabot/_c/arrow_core.pyx` - Core zero-copy operations
- `sabot/_cython/arrow/batch_processor.pyx` - Batch processing
- `sabot/_cython/arrow/join_processor.pyx` - Join operations
- `sabot/_cython/arrow/window_processor.pyx` - Window operations

### Python API
- `sabot/cyarrow.py` - High-level Python API
- Exports: `compute_window_ids`, `hash_join_batches`, `sort_and_take`, `ArrowComputeEngine`

## When to Use CyArrow vs PyArrow

### Use CyArrow when:
- ✅ You need maximum performance (7M+ rows/sec joins)
- ✅ Processing large datasets (100K+ rows)
- ✅ Zero-copy operations are critical
- ✅ Working within Sabot streaming pipelines

### Use PyArrow when:
- ✅ Standard Arrow functionality is sufficient
- ✅ Small datasets (< 10K rows)
- ✅ Using Arrow IPC, Parquet, Flight
- ✅ Need full Arrow ecosystem (not just compute)

## Migration from sabot.arrow

If you have existing code using `sabot.arrow`, update to `sabot.cyarrow`:

```python
# Old (deprecated)
from sabot.arrow import hash_join_batches

# New (current)
from sabot.cyarrow import hash_join_batches
```

**Note:** `sabot.arrow` has been renamed to `sabot.cyarrow` to clearly distinguish it from standard `pyarrow`.

## Checking Zero-Copy Status

```python
from sabot.cyarrow import USING_ZERO_COPY

if USING_ZERO_COPY:
    print("✅ CyArrow zero-copy enabled")
else:
    print("⚠️  Falling back to PyArrow (rebuild Cython extensions)")
```

## Building CyArrow Extensions

If `USING_ZERO_COPY` is False, rebuild the Cython extensions:

```bash
python setup.py build_ext --inplace
```

Verify `.so` files exist:
```bash
ls -la sabot/_c/*.so
ls -la sabot/_cython/arrow/*.so
```

## API Reference

### Functions

#### `compute_window_ids(batch, timestamp_column, window_size_ms)`
Compute tumbling window IDs for a RecordBatch.

**Performance**: ~2-3ns per element (SIMD)

**Args**:
- `batch`: Arrow RecordBatch
- `timestamp_column`: Name of timestamp column
- `window_size_ms`: Window size in milliseconds

**Returns**: RecordBatch with added `window_id` column

#### `hash_join_batches(left, right, left_key, right_key, join_type='inner')`
Hash join two RecordBatches.

**Performance**: O(n+m) with SIMD-accelerated hash

**Args**:
- `left`: Left RecordBatch
- `right`: Right RecordBatch
- `left_key`: Join key in left batch
- `right_key`: Join key in right batch
- `join_type`: "inner", "left outer", "right outer", "full outer"

**Returns**: Joined RecordBatch

#### `sort_and_take(batch, sort_keys, limit=-1)`
Sort RecordBatch and optionally take top N rows.

**Performance**: 10M+ rows/sec

**Args**:
- `batch`: Arrow RecordBatch
- `sort_keys`: List of (column_name, order) tuples
- `limit`: Number of rows to return (-1 for all)

**Returns**: Sorted (and sliced) RecordBatch

### Classes

#### `ArrowComputeEngine`
High-performance filtering and compute operations.

**Methods**:
- `filter_batch(batch, condition)` - Filter with Arrow compute kernels

## Examples

See working examples in:
- `examples/fintech_enrichment_demo/arrow_optimized_enrichment.py`
- `examples/fintech_enrichment_demo/operators/enrichment.py`
- `examples/fintech_enrichment_demo/operators/windowing.py`
- `examples/fintech_enrichment_demo/operators/ranking.py`

## Troubleshooting

### ImportError: cannot import name 'compute_window_ids'

CyArrow extensions not built. Build them:
```bash
python setup.py build_ext --inplace
```

### USING_ZERO_COPY is False

Cython extensions not available. Check:
1. Extensions are built: `ls sabot/_c/*.so`
2. PyArrow is installed: `pip install pyarrow`
3. Rebuild if needed: `python setup.py build_ext --inplace`

### Performance is slower than expected

1. Verify zero-copy is enabled: `print(USING_ZERO_COPY)`
2. Check you're using CyArrow, not PyArrow
3. Profile with larger datasets (10K+ rows for best performance)
4. Ensure `-O3 -march=native` compilation flags were used

## Contributing

To add new zero-copy operations:

1. Implement in `sabot/_c/arrow_core.pyx` using `cimport pyarrow.lib`
2. Export in `sabot/cyarrow.py`
3. Add tests in `tests/unit/test_arrow_integration.py`
4. Benchmark in `test_arrow_performance.py`

## License

Apache 2.0 - Same as Apache Arrow
