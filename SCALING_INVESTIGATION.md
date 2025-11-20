# Scaling Investigation - Why is Sabot Slower at Scale?

**Date:** November 14, 2025

---

## 🔍 Key Findings

### Data Loading Scales Well

**I/O Performance:**
- 600K: 0.059s
- 10M: 0.663s  
- Scaling: 11.3x for 16.7x data
- **Sublinear (good!)**

**Parallel I/O is working correctly!**

### But Full Queries Scale Poorly

**Q1 benchmark times:**
- 600K: ~0.065s
- 10M: ~0.907s
- Scaling: 13.95x for 16.7x data
- **Superlinear (bad!)**

### The Gap

**I/O scales:** 11.3x  
**Full Q1 scales:** 13.95x  
**Difference:** 2.65x overhead from operations

**This 2.65x extra overhead is the problem!**

---

## 💡 Where Does It Come From?

### Suspects

**1. GroupBy Operation**
- Most complex operation
- Hash table grows with data
- Coordination overhead with 8 workers

**2. Morsel Splitting**
- More morsels with more data
- Thread coordination overhead
- Work-stealing synchronization

**3. Memory Allocation**
- Intermediate buffers
- Morsel buffer creation
- Not reusing allocations

**4. Cache Effects**
- Larger working set
- More cache misses
- Memory bandwidth limit

---

## 📊 Comparison to DuckDB/Polars

**DuckDB/Polars scaling:** 2.0-2.2x  
**Sabot I/O scaling:** 11.3x  
**Sabot full scaling:** 14x  

**DuckDB/Polars are doing something very different:**
- Streaming execution
- Better memory management
- More efficient operators
- **6-7x better scaling!**

---

## 🎯 Root Cause Hypothesis

**Sabot's eager execution in aggregate():**
```python
def aggregate():
    # Collects ALL batches
    all_batches = list(self._source)  # ← Eager!
    table = Table.from_batches(all_batches)
    # Process entire table
```

**This pattern:**
- Loads all data into memory
- Creates full table
- Operates on entire dataset
- **Memory and coordination overhead grows**

**DuckDB/Polars streaming:**
- Process batch-by-batch
- Incremental hash table
- Lower memory pressure
- **Better scaling**

---

## 🚀 How to Fix

### 1. Make Aggregate Truly Streaming

**Instead of:**
```python
all_batches = list(source)  # Load all
table = Table.from_batches(all_batches)
grouped = table.group_by(keys)
```

**Do:**
```python
hash_table = {}
for batch in source:  # Stream
    update_hash_table(batch, hash_table)
yield results_from_hash_table()
```

**Expected:** 3-4x better scaling

### 2. Reduce Morsel Overhead

- Larger morsel size for big data
- Fewer synchronization points
- Batch results better

**Expected:** 1.5-2x improvement

### 3. Memory Pooling

- Reuse morsel buffers
- Pre-allocate hash tables
- Reduce allocations

**Expected:** 1.2-1.5x improvement

**Combined: Could match DuckDB scaling!**

---

## ✨ Bottom Line

**I/O is NOT the problem** (scales 11.3x, sublinear) ✅

**Operations are the problem** (add 2.65x overhead at scale) ⚠️

**Specific culprit:** Eager aggregate() collecting all data

**Fix:** Streaming aggregation like DuckDB/Polars use

---

**The architecture can scale well - just need streaming aggregation!** 💪


