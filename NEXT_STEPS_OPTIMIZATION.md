# Next Steps: Optimize Sabot Performance

**Focus: Make Sabot competitive with Polars/DuckDB, not just beat PySpark**

---

## Current Reality

### Performance vs Best-in-Class

**TPC-H Results:**
- Polars: 0.33s (Q1), 0.58s (Q6)
- **Sabot: 1.34s (Q1), 3.15s (Q6)**
- Gap: **4-5x slower**

**This is the real benchmark - not PySpark**

---

## Root Causes Identified

### 1. CythonGroupByOperator Not Loaded

**File exists:** `sabot/_cython/operators/aggregations.pyx`  
**Has operator:** `CythonGroupByOperator` (line 40)  
**Problem:** Module import fails

**Evidence:**
```python
from sabot._cython.operators.aggregations import CythonGroupByOperator
# ModuleNotFoundError: No module named 'sabot._cython.operators.aggregations'
```

**Cause:** Cython module not compiled or not in path

**Fix:** Build the Cython module properly

**Impact:** Currently using Arrow fallback (slower, collects all data)

### 2. Full Data Collection Pattern

**Current:**
```python
all_batches = list(self._source)  # Collect everything
table = pa.Table.from_batches(all_batches)  # Build full table
grouped = table.group_by(self._keys)  # Then group
```

**Problem:** Not streaming, defeats morsel parallelism

**Should:**
- Incremental hash aggregation
- Process batch-by-batch
- True streaming

### 3. Date Parsing Every Time

**Current:**
```python
pc.strptime(b['l_shipdate'], '%Y-%m-%d', 'date32')  # Every batch, every row
```

**Problem:** Expensive string parsing

**Should:**
- Parse once at read
- Use integer comparisons
- Cache

---

## Three Concrete Fixes

### Fix 1: Build aggregations.so (2 hours)

```bash
cd /Users/bengamble/Sabot/sabot/_cython/operators

# Check if aggregations.so exists
ls -la aggregations*.so

# If not, build it
python3 setup.py build_ext --inplace

# Verify import
python3 -c "from aggregations import CythonGroupByOperator"
```

**Expected:** Enable Cython operator, 2x faster

### Fix 2: Add Date Type Conversion (2 hours)

```python
# In Stream.from_parquet(), add:
def from_parquet(cls, path, date_columns=None):
    table = pq.read_table(path)
    
    # Auto-detect or use provided date columns
    if date_columns is None:
        date_columns = [n for n in table.schema.names if 'date' in n.lower()]
    
    # Convert string dates to date32
    for col in date_columns:
        if pa.types.is_string(table[col].type):
            table = table.set_column(
                table.schema.get_field_index(col),
                col,
                pc.strptime(table[col], '%Y-%m-%d', 'date32')
            )
    
    return cls(table.to_batches())
```

**Expected:** 1.5-2x faster on date filters

### Fix 3: Streaming Hash Aggregation (4 hours)

```python
def streaming_group_by_aggregate(source, keys, aggregations):
    """
    True streaming hash aggregation.
    
    Uses:
    - Sabot's hash_array for key hashing
    - Incremental accumulators
    - No full data collection
    """
    from sabot.cyarrow.compute import hash_array, hash_combine
    
    accumulators = {}
    
    for batch in source:
        # Hash keys
        hashes = hash_group_keys(batch, keys)
        
        # Update accumulators batch-by-batch
        for i in range(batch.num_rows):
            h = hashes[i]
            if h not in accumulators:
                accumulators[h] = init_agg_state(keys, batch, i)
            
            update_aggregators(accumulators[h], batch, i, aggregations)
        
        # Can emit partial results for true streaming
        # Or accumulate and emit at end
    
    return finalize_aggregations(accumulators)
```

**Expected:** 1.5-2x faster, true streaming

---

## Implementation Order

### Day 1: Build and Test (2 hours)

1. Build aggregations.so
2. Verify CythonGroupByOperator loads
3. Test on simple query
4. Measure improvement

### Day 2: Date Optimization (2 hours)

1. Add date conversion at read
2. Update filters
3. Test on Q6
4. Measure improvement

### Day 3: Streaming Aggregation (4 hours)

1. Implement streaming hash aggregation
2. Replace collect-then-aggregate pattern
3. Test on Q1
4. Measure improvement

### Day 4: Validate (2 hours)

1. Re-run all TPC-H queries
2. Compare with Polars
3. Document improvements
4. Identify next bottlenecks

**Total: 10 hours**

---

## Expected Outcomes

### After These Fixes

**Q1:**
- Current: 1.34s
- After fixes: 0.30-0.40s
- vs Polars (0.33s): Competitive! ✅

**Q6:**
- Current: 3.15s
- After fixes: 0.60-0.80s
- vs Polars (0.58s): Close! ✅

**Overall: Match Polars on single machine**

---

## Then Focus on Sabot's Strengths

### Unique Capabilities to Benchmark

1. **Distributed execution** (Polars can't)
2. **Streaming throughput** (events/sec)
3. **Graph queries** (Cypher/SPARQL)
4. **Multi-paradigm** (SQL + DataFrame + Graph)

**Show what only Sabot can do**

---

## Summary

**Stop:**
- Focusing on PySpark
- Building more shim features

**Start:**
- Optimizing Sabot core
- Matching Polars/DuckDB
- Benchmarking distributed
- Benchmarking streaming

**This is the path to making Sabot truly competitive** ✅

**Next session: Implement these 3 fixes, get 4-5x faster**
