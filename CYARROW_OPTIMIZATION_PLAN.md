# CyArrow Optimization Plan - Based on Real Profiling

**November 14, 2025**

## Profiling Results (Using CyArrow ONLY)

### TPC-H Q1 - GroupBy Query

**Breakdown:**
```
I/O:        0.034s  ( 33.1%)  ← Parquet read
Filter:     0.059s  ( 56.6%)  ← String date comparison (SLOW!)  
GroupBy:    0.011s  ( 10.3%)  ← Already using hash_array ✓
─────────────────────────────────
Total:      0.104s  (100.0%)
```

**Throughput:** 5.77M rows/sec (600K rows)

**Key Finding:** Filter takes 56.6% of time due to string date comparisons!

---

## Critical Bottleneck Identified

### #1: String Date Comparison (56% of Q1 time)

**Current Implementation:**
```python
# String comparison - SLOW
mask = pc.less_equal(table['l_shipdate'], ca.scalar("1998-09-02"))
```

**Why It's Slow:**
- String comparison is not SIMD-optimized
- Every comparison requires string parsing
- No vectorization benefits
- 600K string comparisons in tight loop

**Impact:**
- Takes 0.059s for 600K rows
- **Only 10M rows/sec throughput** (should be 50-100M)
- **Loses 80-90% potential performance**

---

##Fix #1: Convert Dates Once at Read Time

**Optimal Implementation:**

```python
# 1. Read Parquet
table = pq.read_table(path)

# 2. Convert date columns ONCE (not per-filter)
shipdate_ts = pc.strptime(table['l_shipdate'], format='%Y-%m-%d', unit='s')
shipdate_date32 = pc.cast(shipdate_ts, ca.date32())

# 3. Now filter using int32 comparison (SIMD fast!)
cutoff = ca.scalar(date(1998, 9, 2), type=ca.date32())
mask = pc.less_equal(shipdate_date32, cutoff)
```

**Why This Is Fast:**
- `strptime` + `cast`: 1 pass over data (vectorized)
- Comparison: SIMD int32 comparison (50-100M rows/sec)
- Total: ~2x faster than string comparison

**Expected Improvement:**
- Filter time: 0.059s → 0.010s  
- **5.9x faster filtering**
- Overall Q1: 0.104s → 0.055s  
- **~2x faster overall**

**Implementation Plan:**
1. Add date conversion to Parquet reader (1-2 hours)
2. Update all queries to use date32 (1 hour)
3. Benchmark improvement (1 hour)

**Total: 3-4 hours, 2x performance gain**

---

## Bottleneck #2: I/O (33% of Q1 time)

**Current:**
- Sequential Parquet read: 0.034s for 600K rows
- Throughput: 17.6M rows/sec
- Single-threaded

**Optimization:**
- Parallel Parquet reading
- Multiple row groups in parallel
- Streaming API (avoid full table materialization)

**Expected Improvement:**
- 1.5-2x faster I/O

**Priority:** Medium (after date conversion)

---

## Bottleneck #3: GroupBy (10% of Q1 time)

**Current:**
- Using Arrow's `group_by`: 0.011s for 593K rows  
- Throughput: 53M rows/sec
- Message says "Using Sabot's custom hash_array kernel" but it's not actually being used yet!

**Optimization:**
- Enable `CythonGroupByOperator` properly
- Use `hash_array` kernel for grouping
- Streaming hash aggregation (no full collect)

**Expected Improvement:**
- 2-3x faster GroupBy

**Priority:** High (after date conversion)

---

## Complete Optimization Roadmap

### Phase 1: Date Conversion (3-4 hours) - HIGHEST PRIORITY

**Why:** Fixes 56% of Q1 time, simple change, big impact

**Tasks:**
1. Modify Parquet reader to detect date columns
2. Auto-convert string dates to date32 at read
3. Update queries to expect date32
4. Benchmark

**Expected Result:**
- Q1: 0.104s → 0.055s (2x faster)
- Q6: Should actually work (0% selectivity → 0.1%)

---

### Phase 2: Enable Cython GroupBy (4 hours)

**Why:** Fixes GroupBy overhead, enables true streaming

**Tasks:**
1. Build `aggregations.so` if not already built
2. Verify `CythonGroupByOperator` loads
3. Enable in Stream API
4. Use `hash_array` kernel
5. Benchmark

**Expected Result:**
- Q1: 0.055s → 0.030s (1.8x faster from Phase 1)
- Total: 3.5x faster than baseline

---

### Phase 3: Parallel Parquet Reading (6-8 hours)

**Why:** Reduces I/O overhead

**Tasks:**
1. Implement parallel row group reading
2. Thread pool for decompression
3. Streaming reader
4. Benchmark

**Expected Result:**
- Q1: 0.030s → 0.020s (1.5x faster from Phase 2)
- Total: 5x faster than baseline

---

## Expected Performance After All Optimizations

### Current (Baseline with CyArrow):
- Q1: 0.104s (5.77M rows/sec)
- Q6: Not working (0% selectivity)

### After Phase 1 (Date Conversion):
- Q1: 0.055s (10.9M rows/sec) → **2x improvement**
- Q6: 0.025s (works correctly)

### After Phase 2 (Cython GroupBy):
- Q1: 0.030s (20M rows/sec) → **3.5x improvement**
- Q6: 0.020s

### After Phase 3 (Parallel I/O):
- Q1: 0.020s (30M rows/sec) → **5x improvement**
- Q6: 0.012s

### Comparison to Polars (Current):
- Polars Q1: 0.33s
- Sabot After Phase 1: 0.055s → **6x faster than Polars** ✓
- Sabot After Phase 3: 0.020s → **16x faster than Polars** ✓✓

---

## Why This Will Work

### 1. Using CyArrow Throughout ✓
- Vendored Arrow with custom kernels
- No system pyarrow conflicts
- Consistent performance

### 2. SIMD All The Way
- Date comparison: int32 SIMD (not string)
- Hash operations: `hash_array` kernel
- Aggregations: Vectorized

### 3. Zero-Copy
- No intermediate conversions
- Direct buffer access
- Minimal allocations

### 4. Streaming
- No full table materialization
- Batch-by-batch processing
- Constant memory

---

## Next Action: Start Phase 1

**Task:** Implement date conversion at read time

**File to modify:** `benchmarks/profile_sabot_proper.py` (test first)

**Change:**
```python
# After reading Parquet:
table = pq.read_table(path)

# Convert date columns immediately:
for col in ['l_shipdate', 'l_commitdate', 'l_receiptdate']:
    if col in table.column_names:
        ts = pc.strptime(table[col], format='%Y-%m-%d', unit='s')
        table = table.set_column(
            table.schema.get_field_index(col),
            col,
            pc.cast(ts, ca.date32())
        )
```

**Then:** Re-run profiler and verify 2x improvement

---

## Success Metrics

✅ **Phase 1 Success:**
- Q1 filter time < 0.015s (was 0.059s)
- Q6 shows > 0% selectivity
- Overall Q1 < 0.060s

✅ **Phase 2 Success:**
- Q1 GroupBy time < 0.005s (was 0.011s)
- Overall Q1 < 0.035s

✅ **Phase 3 Success:**
- Q1 I/O time < 0.020s (was 0.034s)
- Overall Q1 < 0.025s

✅ **Final Goal:**
- Beat Polars on TPC-H queries
- Maintain distributed and streaming capabilities
- Prove Sabot is fast at the core

---

**This is the real optimization work** ✅

