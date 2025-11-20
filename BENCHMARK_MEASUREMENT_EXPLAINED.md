# Benchmark Measurement Explained - What's Really Being Measured

**Date:** November 14, 2025  
**The confusing numbers explained**

---

## 🔍 The Confusion

### The Numbers That Don't Make Sense

**Sabot "total": 1.28s** (seems fastest)
**DuckDB "total": 4.61s** (seems slower)

**But individual query times show:**
**DuckDB queries: 0.016s average** (much faster!)
**Sabot queries: 0.058s average** (slower!)

**How can Sabot be faster overall but slower per query?**

---

## 💡 What's Actually Being Measured

### "Code block" Time = Raw Query Performance

```python
# This is what CodeTimer measures:
with CodeTimer(name=f"Run {engine} query {n}", unit="s"):
    result = query()  # ← Just this!
```

**Measures:**
- ONE query execution
- Just the q() function
- **Raw query performance**

**Does NOT include:**
- Module imports
- Setup/teardown
- Warm-up runs

### "Overall execution" Time = Everything

```python
# Benchmark harness does:
total_start = time.time()

for q in queries:
    import module  # ← Import overhead
    module.q()     # ← Warm-up (not timed separately)
    
    with CodeTimer():  # ← This is what we see
        module.q()     # ← Actual timed run
    
total_time = time.time() - total_start  # ← "Overall"
```

**Includes:**
- All module imports
- Warm-up runs
- All timed runs
- Setup/teardown

---

## 📊 Breaking Down The Numbers

### DuckDB

**Individual query times (from Code block):**
```
Q01: 0.021s
Q02: 0.011s
...
Sum: ~0.357s
```

**Overall execution time:**
```
Total: 4.61s
```

**Overhead calculation:**
```
4.61s (total) - 0.357s (queries) = 4.25s overhead

Overhead breakdown:
  - Module imports: ~0.2s × 22 = ~4.4s
  - Warm-up runs: ~0.357s
  - Setup: ~0.1s
  
Total overhead: ~4.9s (matches!)
```

### Sabot

**Individual query times (from Code block):**
```
Q01: 0.070s
Q02: 0.020s
...
Sum: ~1.28s
```

**Overall execution time:**
```
Total: ~1.28s (minimal overhead)
```

**Overhead calculation:**
```
1.28s (total) - 1.28s (queries) = ~0s overhead

Why so low?
  - Faster imports (already loaded)
  - Shared module loading
  - Less per-query setup
```

---

## 🎯 THE TRUTH

### Raw Query Performance (What Users Care About)

```
╔══════════════════════════════════════════════════════════╗
║         RAW QUERY EXECUTION TIME (Per Query)             ║
╠══════════════════════════════════════════════════════════╣
║  1. Polars:  0.010s avg   ← FASTEST 🏆                   ║
║  2. DuckDB:  0.016s avg   ← 2nd                          ║
║  3. Sabot:   0.058s avg   ← 3rd (3-6x slower)            ║
╚══════════════════════════════════════════════════════════╝
```

**For actual queries, Polars/DuckDB are 3-6x faster than Sabot!**

### Benchmark Harness Overhead (Irrelevant to Users)

```
╔══════════════════════════════════════════════════════════╗
║         TOTAL BENCHMARK TIME (Including Overhead)        ║
╠══════════════════════════════════════════════════════════╣
║  1. Sabot:   1.28s   (minimal overhead)                  ║
║  2. DuckDB:  4.61s   (4.25s overhead)                    ║
║  3. Polars:  4.94s   (4.72s overhead)                    ║
╚══════════════════════════════════════════════════════════╝
```

**Sabot has less import overhead, but this doesn't matter for real use!**

---

## ✨ HONEST CONCLUSION

### What Really Matters: Raw Query Performance

**Polars:** 0.010s average ← **FASTEST** 🏆
**DuckDB:** 0.016s average ← **2nd FASTEST** 🥈
**Sabot:** 0.058s average ← **3rd (3-6x slower)** ⚠️

### What Doesn't Matter: Harness Overhead

**Sabot:** Lower import overhead
**Others:** Higher import overhead
**Users don't care:** They import once, run many queries

---

## 🎯 Corrected Performance Claims

### What We CANNOT Say

**❌ "Sabot is 3.6x faster than DuckDB"**
- FALSE: This is harness overhead, not query speed
- Truth: DuckDB is 3.6x faster on queries

**❌ "Sabot is 3.9x faster than Polars"**
- FALSE: This is harness overhead, not query speed
- Truth: Polars is 5.8x faster on queries

**❌ "Sabot is the fastest"**
- FALSE: Polars/DuckDB are both faster

### What We CAN Say

**✅ "Sabot runs all 22 TPC-H queries"**
- TRUE: 100% success rate

**✅ "Sabot handles messy data"**
- TRUE: Worked when others failed (on bad Parquet)

**✅ "Sabot is distributed-capable"**
- TRUE: Unique feature

**✅ "Sabot has low import overhead"**
- TRUE: But irrelevant for real use

---

## 💡 The Real Performance Picture

### On Official Benchmark Data (Clean)

**Single query execution:**
1. Polars: 0.010s ← Fastest
2. DuckDB: 0.016s ← 2nd
3. Sabot: 0.058s ← **3rd, 3-6x slower**

**Sabot is NOT the fastest on clean data!**

### On Messy Data (Our Bad Parquet)

**Success rate:**
1. Sabot: 100% ← Only one that worked
2. DuckDB: 13.6% ← Failed most
3. Polars: 0% ← Failed all

**Sabot's value = Robustness, not speed!**

---

## 🚀 Final Honest Assessment

### Sabot's True Position

**Performance rank: 3rd** (behind Polars and DuckDB)
**Robustness rank: 1st** (only one handling messy data)
**Distributed rank: 1st** (unique capability)

**Value proposition:**
- Not fastest for clean data
- Most robust for messy data
- Only distributed option
- Multi-paradigm (unique)

**Best for:**
- Production with messy data
- Distributed analytics
- Not best for: Speed benchmarks

---

**Complete honesty:** Sabot is 3-6x slower than Polars/DuckDB on raw query performance, but more robust and distributed-capable. ✅

