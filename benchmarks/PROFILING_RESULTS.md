# Sabot Profiling Results - I/O vs Compute

**Detailed breakdown of where time is spent**

---

## Methodology

Measured each operation separately:
1. **Ingress:** Reading Parquet files
2. **Processing:** Filters, GroupBy, aggregations, sorting
3. **Egress:** Writing/collecting results

**Goal:** Understand if Sabot is I/O bound or compute bound

---

## Results

### TPC-H Q6 (Filter + Aggregate)

**Breakdown:**
- Ingress (Parquet read): [X]s ([X]%)
- Processing:
  - Date filter (strptime): [X]s ([X]%)
  - Discount filter: [X]s ([X]%)
  - Quantity filter: [X]s ([X]%)
  - Revenue compute: [X]s ([X]%)
  - Aggregation: [X]s ([X]%)
- Egress (Result collection): [X]s ([X]%)

**Total: [X]s**

**Bottleneck:** [Identified from profiling]

### TPC-H Q1 (GroupBy + Aggregate)

**Breakdown:**
- Ingress (Parquet read): [X]s ([X]%)
- Processing:
  - Date filter: [X]s ([X]%)
  - Combine batches: [X]s ([X]%)
  - GroupBy + Aggregate: [X]s ([X]%) ← Expected bottleneck
  - Sort: [X]s ([X]%)
- Egress: [X]s ([X]%)

**Total: [X]s**

**Bottleneck:** [GroupBy likely]

---

## Analysis

### Expected Findings

**If Ingress > 50%:**
- I/O bound
- Need: Better Parquet reader, parallel I/O, caching
- Polars advantage: Optimized I/O pipeline

**If GroupBy > 40%:**
- Compute bound on GroupBy
- Need: Optimized hash aggregation, streaming
- This is likely the case

**If Date parsing > 20%:**
- Type conversion overhead
- Need: Pre-convert to date types
- Quick win

---

## Optimization Priorities (Based on Profiling)

### If GroupBy is Bottleneck (Likely)

**Fix 1: Streaming Hash Aggregation**
- Don't collect all batches
- Incremental hash table updates
- Use `hash_array` kernel

**Fix 2: Enable Cython Operator**
- Build aggregations.so
- Use CythonGroupByOperator

**Expected: 2-3x improvement**

### If Date Parsing is Significant

**Fix: Type Conversion at Read**
- Convert date columns once
- Use int comparisons thereafter
- Cache parsed values

**Expected: 1.5-2x improvement**

### If I/O is Dominant

**Fix: Optimized Ingestion**
- Custom Parquet reader
- Parallel I/O
- Better buffering

**Expected: 1.2-1.5x improvement**

---

## Next Steps

1. **Run profiling** (get actual numbers)
2. **Identify #1 bottleneck**
3. **Fix that first** (biggest impact)
4. **Re-profile**
5. **Fix #2 bottleneck**
6. **Iterate**

**This is proper performance engineering** ✅

---

## Compare with Polars

**Polars is fast because:**
1. Lazy evaluation (optimizes full query)
2. Optimized I/O (parallel Parquet reading)
3. LLVM-optimized code generation
4. Streaming execution (no full collection)
5. Years of optimization

**Sabot can match by:**
1. Fix GroupBy (streaming hash agg)
2. Optimize I/O (parallel reading)
3. Enable Cython operators
4. Reduce overhead
5. Learn from profiling

**Target: Within 2x of Polars** (achievable)

---

## Focus on Sabot's Strengths After Optimization

Once competitive on single-machine:

1. **Benchmark distributed** (Polars can't)
2. **Benchmark streaming** (real-time)
3. **Benchmark graph** (Cypher/SPARQL)
4. **Show scaling** (1 to 1000 agents)

**Prove Sabot's unique value**

