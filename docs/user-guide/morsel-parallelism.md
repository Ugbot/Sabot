# Morsel-Driven Parallelism

Sabot uses morsel-driven parallelism **by default** for optimal performance. This guide explains how it works and how to configure it.

## What is Morsel-Driven Parallelism?

Morsel-driven parallelism splits large batches into small, cache-friendly chunks (morsels) that are processed in parallel by workers. Workers use work-stealing to balance load dynamically.

### Benefits

- **2-4x speedup** for CPU-bound operations
- **Automatic**: Enabled by default, no code changes required
- **Cache-friendly**: Morsels fit in L2/L3 cache (64KB default)
- **Load-balanced**: Work-stealing prevents idle workers
- **Transparent**: Works with all existing operators

### How It Works

By default, all operators automatically use morsel parallelism:
- Small batches (<10K rows) bypass parallelism (no overhead)
- Large batches (>10K rows) are split into 64KB morsels
- Morsels are processed in parallel across CPU cores
- Results are reassembled into output batches

### When It Activates

Morsel parallelism activates automatically when:
- Batches are large (>10K rows)
- Operations are CPU-bound (transforms, filters, joins)
- Multiple CPU cores are available
- Parallelism is not explicitly disabled

It automatically skips parallelism when:
- Batches are small (<10K rows) - overhead not worth it
- Operations are I/O-bound - won't help performance
- Running in single-core environment
- Parallelism is explicitly disabled with `.sequential()`

## Usage

### Automatic Parallelism (Default)

```python
from sabot.api import Stream

# Create stream - parallelism is enabled by default
stream = Stream.from_batches(batches)

# All operations automatically use morsel parallelism
result = (stream
    .map(expensive_transform)    # ← Automatically parallel
    .filter(lambda b: pc.greater(b['x'], 100))  # ← Automatically parallel
    .select('id', 'x')           # ← Automatically parallel
)

# Process with automatic parallelism
for batch in result:
    process(batch)
```

### Configuration

```python
# Use default auto-detected settings (recommended)
stream.map(transform).filter(condition)

# Configure worker count
stream.parallel(num_workers=8).map(transform)

# Custom morsel size for memory-constrained environments
stream.parallel(morsel_size_kb=32).filter(condition)

# Force sequential processing when needed
stream.sequential().map(side_effect_operation)
```

### Performance Tuning

**Morsel Size**:
- Smaller (16-32KB): Better cache locality, higher overhead
- Default (64KB): Good balance for most workloads
- Larger (128-256KB): Lower overhead, may not fit in cache

**Worker Count**:
- Start with: `os.cpu_count() * 0.8`
- Increase if: CPU utilization low, operations are CPU-bound
- Decrease if: Context switching overhead, operations are I/O-bound

### Statistics

```python
# Get morsel processing statistics
stats = parallel_stream.get_stats()

print(f"Workers: {stats['num_workers']}")
print(f"Morsels created: {stats['total_morsels_created']}")
print(f"Throughput: {stats['morsels_per_second']:.0f} morsels/sec")
```

## Advanced: Custom Morsel Processing

Operators can override `process_morsel()` for custom logic:

```python
from sabot._cython.operators.base_operator cimport BaseOperator

cdef class CustomOperator(BaseOperator):
    """Custom operator with NUMA-aware morsel processing"""

    cpdef object process_morsel(self, object morsel):
        # Pin to NUMA node
        numa_node = morsel.numa_node
        pin_to_numa_node(numa_node)

        # Process batch
        result_batch = self.process_batch(morsel.data)

        # Update morsel
        morsel.data = result_batch
        morsel.mark_processed()
        return morsel
```

## Troubleshooting

**Q: Performance is slower than expected?**
A: Check if batches are small (<10K rows) - parallelism automatically disables for small batches to avoid overhead.

**Q: Not seeing speedup with more workers?**
A: Operation may be memory-bound or I/O-bound rather than CPU-bound. Use `.sequential()` to confirm.

**Q: Need deterministic ordering?**
A: Parallelism may change batch ordering. Use `.sequential()` if order matters.

**Q: High CPU usage but low throughput?**
A: Too many workers causing context switching. Reduce worker count with `.parallel(num_workers=4)`.

**Q: Operations have side effects?**
A: Parallel execution may interleave side effects. Use `.sequential()` for operations with side effects.

## See Also

- [UNIFIED_BATCH_ARCHITECTURE.md](../design/UNIFIED_BATCH_ARCHITECTURE.md) - Architecture overview
- [morsel_operator_bench.py](../../benchmarks/morsel_operator_bench.py) - Benchmarks
