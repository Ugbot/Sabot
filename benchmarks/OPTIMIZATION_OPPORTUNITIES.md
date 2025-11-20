# Sabot Optimization Opportunities

**Based on TPC-H profiling and comparison with Polars/DuckDB**

---

## Current Performance Gap

**Sabot vs Polars:**
- Q1: 4.1x slower (1.34s vs 0.33s)
- Q6: 5.4x slower (3.15s vs 0.58s)

**Not acceptable for a high-performance system**

---

## Identified Bottlenecks

### 1. GroupBy Performance (Biggest Issue)

**Current:**
- Falls back to Arrow's `table.group_by()`
- Collects all batches to single table
- Not optimized for streaming

**Polars does:**
- Optimized hash aggregation in Rust
- Streaming-friendly
- SIMD hash functions

**Fix:**
- Enable CythonGroupByOperator properly
- Optimize hash aggregation
- Stream-based grouping

**Expected gain: 2-3x on GroupBy queries**

### 2. Date Parsing Overhead

**Current:**
- Parsing strings every filter: `pc.strptime(b['l_shipdate'], ...)`
- Happens for every batch
- Expensive operation

**Should:**
- Parse once, cache
- Or convert data to date types upfront
- Use integer comparisons

**Expected gain: 1.5-2x on date-heavy queries**

### 3. Batch Collection Overhead

**Current:**
- `list(stream)` collects all batches
- Then `Table.from_batches()`
- Extra copy

**Should:**
- Stream results directly
- Avoid intermediate collection
- True streaming execution

**Expected gain: 1.2-1.5x**

### 4. No Query Optimization

**Current:**
- No query optimizer
- Operations execute in order
- No predicate pushdown

**Should:**
- Analyze query plan
- Push filters down
- Fuse operations
- Reorder for efficiency

**Expected gain: 1.3-2x**

---

## Specific Optimizations

### Priority 1: Fix GroupBy (CRITICAL)

**Issue:** Falling back to Arrow, not using Cython

**Fix:**
1. Debug CythonGroupByOperator import issue
2. Ensure it's used instead of fallback
3. Optimize for common aggregations

**Time:** 4-6 hours  
**Gain:** 2-3x on GroupBy  
**Impact:** Huge (most queries use GroupBy)

### Priority 2: Optimize Date Handling

**Issue:** Repeated string parsing

**Fix:**
1. Convert date columns to date32 type at read
2. Use integer comparisons
3. Cache parsed values

**Time:** 2-4 hours  
**Gain:** 1.5-2x on date filters  
**Impact:** High (TPC-H is date-heavy)

### Priority 3: Streaming Aggregations

**Issue:** Collecting all data before aggregating

**Fix:**
1. Incremental aggregations
2. Don't collect full table
3. Stream through aggregations

**Time:** 4-6 hours  
**Gain:** 1.5-2x on aggregations  
**Impact:** Medium

### Priority 4: Query Optimizer

**Issue:** No optimization layer

**Fix:**
1. Analyze operation DAG
2. Push filters down
3. Fuse operations
4. Reorder for efficiency

**Time:** 8-16 hours  
**Gain:** 1.3-2x overall  
**Impact:** Medium

---

## Realistic Optimization Plan

### Week 1: Quick Wins (16 hours)

- Fix GroupBy operator (6 hours) → 2x gain
- Optimize date handling (4 hours) → 1.5x gain
- Streaming aggregations (6 hours) → 1.5x gain

**Combined: 4-6x faster → Competitive with Polars!**

### Week 2: Deeper Optimizations (20 hours)

- Query optimizer (16 hours) → 1.5x gain
- Memory pooling (4 hours) → 1.2x gain

**Combined: 1.8x additional → Match Polars**

### Total Potential

**Current: 1.34s (Q1)**  
**After Week 1: 0.22-0.34s** (competitive)  
**After Week 2: 0.15-0.25s** (match Polars)

**This is achievable!**

---

## What to Focus On

### Sabot's Real Strengths

1. **Distributed** - Scale to 1000s of nodes
2. **Streaming** - Real-time event processing
3. **Multi-paradigm** - SQL + DataFrame + Graph
4. **Flexible** - Multiple query languages

### Benchmarks That Matter

1. **Streaming throughput** - Events/second
2. **Distributed scaling** - 1 vs 10 vs 100 agents
3. **Graph queries** - Cypher/SPARQL performance
4. **Mixed workloads** - SQL + Graph on same data

**Not just batch analytics - show what Sabot uniquely does**

---

## Next Session Goals

1. **Profile and fix GroupBy** - Get 2-3x improvement
2. **Optimize date handling** - Get 1.5-2x improvement
3. **Re-run TPC-H** - Validate improvements
4. **Benchmark streaming** - Show Sabot's strength
5. **Benchmark distributed** - Show scaling

**Focus: Optimize Sabot's actual capabilities, not just beat PySpark**

---

## Honest Assessment

**Current state:**
- Sabot works ✅
- Sabot is 3-6x faster than PySpark ✅
- Sabot is 4-5x slower than Polars ⚠️
- Sabot has optimization opportunities ✅

**Potential:**
- Can be competitive with Polars (with work)
- Can excel at distributed (unique)
- Can excel at streaming (unique)
- Can excel at multi-paradigm (unique)

**This is the real work - optimizing Sabot itself** ✅
EOF
cat benchmarks/OPTIMIZATION_OPPORTUNITIES.md
