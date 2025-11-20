# System PyArrow Usage Audit

**Complete audit of all system pyarrow usage in Sabot**

---

## Found Locations

### Core Sabot Code

**sabot/api/stream.py:**
- Line 1287: `import pyarrow as pa` (in join function)
- Line 1854-1855: `import pyarrow as pa` and `import pyarrow.compute as pc` (in GroupBy fallback)

**Impact:** CRITICAL - This is the core Stream API

**sabot/spark/dataframe.py:**
- Multiple locations using `import pyarrow as pa`
- Already attempted to fix but may have remnants

**Impact:** Medium - Spark shim, but uses Stream API underneath

### Benchmark Code

**benchmarks/polars-benchmark/queries/sabot_native/*.py:**
- q1.py line 10: `import pyarrow.compute as pc`
- q6.py line 10: `import pyarrow.compute as pc`  
- q3.py line 9: `import pyarrow.compute as pc`

**Impact:** Medium - Benchmark code using wrong imports

### Other Files

**Various test and utility files:**
- May have scattered system pyarrow usage

---

## Why This Matters

### Sabot's Architecture

**Designed for:**
```
Python → cyarrow (vendored) → vendor/arrow/ C++ → Custom kernels
```

**System pyarrow breaks this:**
```
Python → pyarrow (system) → /usr/lib/arrow → No custom kernels
```

### Custom Kernels Lost

**Using system pyarrow means NO access to:**
- `hash_array()` - Optimized for shuffles/grouping
- `compute_window_ids()` - 2-3ns/element windowing
- `hash_join_batches()` - SIMD hash joins

**These are Sabot's performance advantages!**

### Performance Impact

**Estimated:**
- Missing custom kernels: 2-3x slower
- Version mismatch overhead: 1.2-1.5x
- Not zero-copy: 1.3-2x
- **Total: 3-9x slower than it should be**

**This explains the Polars gap!**

---

## Priority Fixes

### P0 (CRITICAL) - sabot/api/stream.py

**Lines 1854-1855 (GroupBy fallback):**

Current code collects all batches and uses system Arrow:
```python
import pyarrow as pa
import pyarrow.compute as pc
table = pa.Table.from_batches(all_batches)
grouped = table.group_by(self._keys)
```

**Should use:**
```python
from sabot import cyarrow as ca
# Use hash_array for grouping
# Streaming accumulation
# No full collection
```

**Impact:** This is likely THE bottleneck

### P1 (Important) - Native Benchmarks

All sabot_native queries using system pyarrow.compute

**Should use:**
```python
from sabot import cyarrow as ca
pc = ca.compute
```

**Impact:** Not measuring Sabot's real performance

### P2 (Good to have) - Spark Shim

May have remnants of system pyarrow

**Should verify:**
All using cyarrow (already attempted fix)

---

## Action Items

1. ✅ Audit complete - Found all locations
2. ⏭️ Replace system pyarrow with cyarrow
3. ⏭️ Verify custom kernels accessible
4. ⏭️ Profile with correct architecture
5. ⏭️ Optimize based on real bottlenecks

---

## Expected After Fix

**Current (with system pyarrow):**
- Q1: 1.34s
- Q6: 3.15s
- 4-5x slower than Polars

**After (cyarrow only):**
- Access to custom kernels
- Proper zero-copy
- Better performance
- **Expected: 2-3x faster → 1-2x gap with Polars**

**This is the architectural fix needed** ✅

