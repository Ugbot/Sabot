# Sabot Optimization Implementation Plan

**Goal: Close the 4-5x gap with Polars through targeted optimizations**

---

## Current Performance Analysis

### Benchmark Results (TPC-H SF 0.1, 600K rows)

| Query | Sabot Native | Polars | Gap | Bottleneck |
|-------|--------------|--------|-----|------------|
| Q1 (GroupBy) | 1.34s | 0.33s | 4.1x | GroupBy operator |
| Q6 (Filter) | 3.15s | 0.58s | 5.4x | Stream overhead |

**Average gap: 4-5x**

---

## Optimization 1: Fix GroupBy Operator

### Current State

**Problem:**
```python
# In sabot/api/stream.py - GroupedStream.aggregate()
if AGGREGATION_OPERATORS_AVAILABLE and CythonGroupByOperator:
    operator = CythonGroupByOperator(...)  # Tries this
    return Stream(operator, None)

# Falls back to:
grouped = table.group_by(self._keys)  # Arrow's implementation
result = grouped.aggregate(agg_list)  # Generic, not optimized
```

**Issue:** CythonGroupByOperator import fails or initialization fails

### Investigation Needed

1. Check why CythonGroupByOperator fails
2. Verify Cython operator signature
3. Fix initialization or use alternative

### Solution Approach

**Option A: Fix Cython Operator**
- Debug import issue
- Fix signature mismatch
- Enable proper usage

**Option B: Optimize Arrow Fallback**
- Keep Arrow fallback
- But optimize how we call it
- Reduce overhead

**Option C: Custom Hash Aggregation**
- Implement optimized hash aggregation in Cython
- Use `ca.compute.hash_array` for hashing
- Streaming-friendly accumulation

**Recommended: Option C** - Build proper streaming hash aggregation

### Implementation

```cython
# sabot/_cython/streaming_aggregations.pyx

cdef class StreamingHashAggregator:
    """
    Streaming hash aggregation optimized for Sabot.
    
    Uses:
    - Sabot's hash_array kernel
    - Incremental aggregation (no full collection)
    - SIMD throughout
    """
    
    cdef:
        dict hash_table  # {hash: {agg_name: accumulator}}
        list group_keys
        dict aggregations
    
    cdef void process_batch(self, batch):
        """Process a batch incrementally."""
        # Hash group keys
        hash_values = compute_hash(batch, self.group_keys)
        
        # Update accumulators
        for i in range(batch.num_rows):
            hash_val = hash_values[i]
            if hash_val not in self.hash_table:
                self.hash_table[hash_val] = init_accumulators()
            
            # Update each aggregation
            for agg_name, (col, func) in self.aggregations.items():
                update_accumulator(
                    self.hash_table[hash_val][agg_name],
                    batch[col][i],
                    func
                )
    
    cdef finalize(self):
        """Finalize and return results."""
        # Build result arrays
        # Return as RecordBatch
```

**Expected gain: 2-3x on GroupBy queries**

---

## Optimization 2: Optimize Date Handling

### Current State

**Problem:**
```python
# Every batch, every filter:
.filter(lambda b: pc.less_equal(
    pc.strptime(b['l_shipdate'], '%Y-%m-%d', 'date32'),  # Parse EVERY TIME
    ca.scalar(cutoff_date)
))
```

**Cost:** String parsing for every row, every batch

### Solution

**Convert date columns once at read:**

```python
# In Stream.from_parquet()
def from_parquet(path, date_columns=None):
    table = pq.read_table(path)
    
    # Convert date columns upfront
    if date_columns:
        for col in date_columns:
            if pa.types.is_string(table[col].type):
                table = table.set_column(
                    table.schema.get_field_index(col),
                    col,
                    pc.strptime(table[col], '%Y-%m-%d', 'date32')
                )
    
    return Stream(table.to_batches())
```

**Then filters become:**
```python
.filter(lambda b: pc.less_equal(b['l_shipdate'], ca.scalar(cutoff)))
# No string parsing - just integer comparison (SIMD)
```

**Expected gain: 1.5-2x on date-heavy queries**

---

## Optimization 3: Streaming Aggregations

### Current State

**Problem:**
```python
# In GroupedStream.aggregate()
all_batches = list(self._source)  # Collect ALL batches
table = pa.Table.from_batches(all_batches)  # Build full table
grouped = table.group_by(self._keys)  # Then group
result = grouped.aggregate(agg_list)  # Then aggregate
```

**Issues:**
- Collects all data in memory
- Not streaming-friendly
- Defeats morsel parallelism

### Solution

**Incremental hash aggregation:**

```python
def streaming_aggregate(source, keys, aggregations):
    """
    Stream-based hash aggregation.
    
    Process batches incrementally:
    - Maintain hash table of partial aggregates
    - Update as batches arrive
    - Finalize at end
    """
    accumulators = {}
    
    for batch in source:
        # Hash group keys using Sabot's kernel
        from sabot.cyarrow.compute import hash_array
        
        # Get key columns
        key_arrays = [batch[k] for k in keys]
        
        # Compute hashes (SIMD)
        if len(key_arrays) == 1:
            hashes = hash_array(key_arrays[0])
        else:
            hashes = hash_array(key_arrays[0])
            for arr in key_arrays[1:]:
                from sabot.cyarrow.compute import hash_combine
                hashes = hash_combine(hashes, hash_array(arr))
        
        # Update accumulators incrementally
        for i in range(batch.num_rows):
            h = hashes[i].as_py()
            
            if h not in accumulators:
                accumulators[h] = {
                    'keys': tuple(batch[k][i].as_py() for k in keys),
                    'aggs': init_aggregators(aggregations)
                }
            
            # Update each aggregation
            for agg_name, (col, func) in aggregations.items():
                update_agg(accumulators[h]['aggs'][agg_name],
                          batch[col][i].as_py(), func)
    
    # Finalize and build result
    return build_result_batch(accumulators)
```

**Expected gain: 1.5-2x (no full collection, true streaming)**

---

## Implementation Timeline

### Day 1: Diagnosis (2 hours)

- Profile current GroupBy execution
- Measure where time is spent
- Identify exact bottlenecks

### Day 2: GroupBy Fix (6 hours)

- Implement streaming hash aggregation
- Use Sabot's hash_array kernel
- Test and validate
- Measure improvement

### Day 3: Date Optimization (4 hours)

- Add date column conversion at read
- Update filters to use date types
- Test and validate
- Measure improvement

### Day 4: Streaming Aggregations (6 hours)

- Replace collect-then-aggregate pattern
- Implement true streaming aggregation
- Test and validate
- Measure improvement

### Day 5: Re-benchmark (2 hours)

- Run TPC-H again
- Compare with before
- Validate improvements

**Total: 20 hours (~3 days)**

---

## Expected Results

### After Optimizations

**Q1 (Currently 1.34s):**
- GroupBy fix: 1.34s → 0.67s (2x)
- Date opt: 0.67s → 0.45s (1.5x)
- Streaming: 0.45s → 0.30s (1.5x)
- **Target: 0.30s vs Polars 0.33s** ✅ Competitive!

**Q6 (Currently 3.15s):**
- Date opt: 3.15s → 2.10s (1.5x)
- Streaming: 2.10s → 1.40s (1.5x)
- Filter opt: 1.40s → 0.70s (2x)
- **Target: 0.70s vs Polars 0.58s** ✅ Close!

**Combined: 4-5x improvement → Match Polars**

---

## Focus Areas

### Not About PySpark Anymore

**Stop:**
- Comparing to PySpark
- Building shim features
- API compatibility

**Start:**
- Optimizing Sabot itself
- Profiling and tuning
- Matching best-in-class (Polars/DuckDB)
- Testing at scale (distributed)

### Sabot's Real Competition

**Single machine:**
- Polars (Rust, highly optimized)
- DuckDB (C++, vectorized)

**Distributed:**
- Spark (slow but distributed)
- Sabot should be: Fast + Distributed

**Goal: Best of both worlds**

---

## Next Session Goals

1. Fix GroupBy to use proper hash aggregation
2. Optimize date type handling
3. Implement streaming aggregations
4. Re-run benchmarks
5. Get within 2x of Polars

**This is the real work - optimize Sabot** ✅
EOF
cat OPTIMIZATION_PLAN.md
