# Sabot Performance Analysis - Proper Benchmarking

**Goal: Understand Sabot's actual performance, find bottlenecks, optimize**

---

## Current State (From TPC-H)

### Sabot Native Performance

**Q1 (GroupBy + Agg):**
- Sabot Native: 1.34s
- Polars: 0.33s
- **Gap: 4.1x slower than Polars**

**Q6 (Filter + Agg):**
- Sabot Native: 3.15s
- Polars: 0.58s  
- **Gap: 5.4x slower than Polars**

### Questions to Answer

1. **Why is Sabot 4-5x slower than Polars?**
   - Both use Arrow
   - Both are compiled (C++/Cython vs Rust)
   - What's the bottleneck?

2. **Where is the time going?**
   - I/O?
   - Operators?
   - Overhead?

3. **What can we optimize?**
   - Better operators?
   - Less overhead?
   - More SIMD?

---

## Profiling Sabot

### What to Measure

**Operation-level timing:**
- Parquet read time
- Filter execution time
- GroupBy execution time
- Aggregation time
- Sort time

**Memory usage:**
- Peak memory
- Allocations
- Copies vs zero-copy

**CPU usage:**
- SIMD utilization
- Cache efficiency
- Parallelization

---

## Comparison Points

### vs Polars (Rust/Arrow)

**Similar:**
- Both use Arrow columnar format
- Both support SIMD
- Both are compiled

**Different:**
- Polars: Pure Rust, LLVM optimized
- Sabot: C++/Cython, GCC/Clang
- Polars: Lazy evaluation optimized
- Sabot: Stream-based execution

**Learn from Polars:**
- How do they optimize GroupBy?
- How do they handle filters?
- What's their lazy evaluation strategy?

### vs DuckDB (C++/Vectorized)

**Similar:**
- Both C++ based
- Both vectorized execution

**Different:**
- DuckDB: SQL-first, vectorized engine
- Sabot: Stream-first, Arrow-based

**Learn from DuckDB:**
- Vectorized execution patterns
- Query optimization
- Memory management

---

## Optimization Opportunities

### 1. GroupBy Performance

**Current: Arrow fallback**
- Uses `table.group_by()` (Arrow's implementation)
- Works but may not be optimal

**Potential:**
- Optimize Cython GroupBy operator
- Use Sabot's hash functions
- Better memory pooling

**Expected gain: 2-3x on GroupBy**

### 2. Filter Performance

**Current: Arrow compute filters**
- Uses `pc.less`, `pc.greater`, etc.
- SIMD but generic

**Potential:**
- Specialized filter kernels
- Branch-free evaluation
- Predicate pushdown

**Expected gain: 1.5-2x on filters**

### 3. I/O Performance

**Current: Arrow Parquet reader**
- Standard Arrow implementation
- Could be optimized

**Potential:**
- Custom Parquet reader
- Better buffering
- Parallel I/O

**Expected gain: 1.2-1.5x on I/O**

### 4. Memory Management

**Current: Arrow memory pools**
- Uses Arrow's allocation

**Potential:**
- Custom allocators
- Memory pooling
- Reduce copies

**Expected gain: 1.2-1.5x overall**

---

## Profiling Plan

### Step 1: Detailed Timing

```python
# Add timing to each operation
with Timer("Parquet read"):
    data = Stream.from_parquet(...)

with Timer("Filter"):
    filtered = data.filter(...)

with Timer("GroupBy"):
    grouped = filtered.group_by(...)

with Timer("Aggregate"):
    result = grouped.aggregate(...)
```

### Step 2: Memory Profiling

```python
import tracemalloc

tracemalloc.start()
# Run query
snapshot = tracemalloc.take_snapshot()
# Analyze allocations
```

### Step 3: CPU Profiling

```bash
# Use perf or py-spy
py-spy record -o profile.svg -- python3 query.py

# Or cProfile
python3 -m cProfile -o profile.stats query.py
```

---

## Expected Findings

### Likely Bottlenecks

**1. GroupBy fallback to Arrow**
- Not using optimized Cython operator
- Arrow's implementation is good but generic
- Sabot could optimize for specific patterns

**2. Stream overhead**
- Iterator protocol overhead
- Batch boundaries
- Could optimize hot paths

**3. Type conversions**
- String date parsing
- Type casting
- Could cache conversions

### Quick Wins

**1. Fix Cython GroupBy operator** (2-4 hours)
- Currently falling back to Arrow
- Enable Cython version
- Expected: 2x faster on GroupBy

**2. Optimize hot filters** (2-4 hours)
- Common predicates (>, <, ==)
- Specialized kernels
- Expected: 1.5x faster on filters

**3. Better memory pooling** (4-6 hours)
- Reduce allocations
- Reuse buffers
- Expected: 1.2x overall

**Total potential: 3-4x faster → Competitive with Polars**

---

## Target Performance

### Goal: Match Polars/DuckDB on Single Machine

**Currently:**
- Sabot: 1.34s (Q1)
- Polars: 0.33s (Q1)
- Gap: 4.1x

**With optimizations:**
- Fix GroupBy: 1.34s → 0.67s (2x)
- Better filters: 0.67s → 0.45s (1.5x)
- Memory opts: 0.45s → 0.38s (1.2x)
- **Target: 0.38s vs Polars 0.33s** (competitive!)

### Goal: Maintain Distributed Advantage

**Sabot's unique value:**
- Single machine: Match Polars/DuckDB
- Distributed: Scale infinitely
- **Best of both worlds**

---

## Next Steps for Proper Benchmarking

### 1. Profile Current Implementation

- Detailed timing breakdown
- Memory profiling
- CPU profiling
- Identify bottlenecks

### 2. Optimize Hot Paths

- Fix GroupBy operator
- Optimize common filters
- Improve memory management

### 3. Re-benchmark

- Run TPC-H again
- Compare with optimized version
- Measure improvements

### 4. Test at Scale

- Single machine: 1GB, 10GB, 100GB
- Distributed: Multiple agents
- Find scaling characteristics

---

## Focus on Sabot's Strengths

### What Sabot Does Well

1. **Distributed execution**
2. **Streaming (real-time)**
3. **Graph processing (Cypher/SPARQL)**
4. **Flexibility (SQL, DataFrame, Graph)**

### Benchmark These

- Streaming throughput (events/sec)
- Distributed scaling (1 to N agents)
- Graph query performance
- Multi-paradigm workloads

**Not just "beat PySpark" - show what Sabot uniquely offers**

---

## Honest Goals

**Don't aim to:**
- Beat Polars on single-machine
- Be fastest at everything

**Do aim to:**
- Be competitive with Polars/DuckDB (within 2x)
- Beat PySpark significantly (3-6x)
- Excel at distributed workloads
- Excel at streaming workloads
- Unique multi-paradigm support

**This is realistic and valuable** ✅
