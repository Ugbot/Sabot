# Morsel Contention Hypothesis - Testing

**Date:** November 14, 2025

---

## 🔍 THE HYPOTHESIS

### 8 Workers Might HURT at Scale

**Theory:**
- GroupBy operations use shared hash table
- 8 workers all trying to update it
- Lock contention grows with data size
- **More workers = worse performance at scale!**

---

## 📊 Evidence

### Sabot with 8 Workers

**Scaling: 13.9x for 16.7x data (terrible!)**
- 600K: 0.85s (8 workers help)
- 10M: 11.82s (8 workers hurt?)

### DuckDB/Polars (Likely Fewer Workers)

**Scaling: 2.0-2.2x for 16.7x data (excellent!)**
- Better scaling with less parallelism?
- Less lock contention?

---

## 🎯 Test Plan

**Test different worker counts:**
1. 0 workers (morsels disabled) - baseline
2. 2 workers - minimal contention
3. 4 workers - moderate parallelism
4. 8 workers - current (high contention?)

**Measure:**
- Scale 1.67 performance
- Scaling from 0.1 to 1.67
- Lock contention impact

**Expected:**
- Fewer workers might scale BETTER
- Optimal might be 2-4 workers
- 8 workers causing contention

---

## ✨ If True, This Explains Everything!

**Why Sabot is:**
- Fast on small (8 workers, low contention)
- Slow on large (8 workers, high contention)

**Why DuckDB/Polars scale better:**
- Using 1-2 workers
- Less contention
- Better at scale

**The fix:**
- Adaptive worker count based on data size
- Fewer workers for large data
- More workers for small data

---

**Testing now...** 🔬

