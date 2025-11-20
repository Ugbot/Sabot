# Sabot Native vs Spark Shim Performance

**Why native API should be faster**

---

## Three Implementations Compared

### 1. PySpark (Baseline)
```python
spark.sql("SELECT ... FROM ... WHERE ...")
```
- SQL parsing
- JVM execution
- Slowest

### 2. Sabot Spark Shim  
```python
df.filter(...).groupBy(...).agg(...)  # Via Spark API
```
- Spark API compatibility layer
- Translates to Sabot Stream
- 3.1x faster

### 3. Sabot Native
```python
Stream.from_parquet(...)
  .filter(lambda b: pc.less(...))  # Direct cyarrow
  .group_by([...])                 # Direct Cython operator
```
- No compatibility layer
- Direct cyarrow access
- Cython operators
- Expected: 5-10x faster

---

## Why Native Should Be Faster

### Eliminates Overhead

**Spark Shim Path:**
```
DataFrame API → Column expressions → Stream API → Cython operators
```

**Native Path:**
```
Stream API → Cython operators  (skip 2 layers)
```

### Direct Cyarrow Access

**Shim:**
```python
df.filter(col('amount') > 100)  # Creates Column, wraps, unwraps
```

**Native:**
```python
stream.filter(lambda b: pc.greater(b['amount'], 100))  # Direct
```

### Better Operator Control

**Native can:**
- Choose specific Cython operators
- Inline calculations
- Fuse operations
- Use custom kernels directly

**Expected gain: 1.5-2x over shim**

---

## Expected Results

**If Sabot Spark shim is 3.1x faster:**
- Sabot Native: 5-7x faster (2x better than shim)

**If some queries are 5x faster:**
- Sabot Native: 10-15x faster

**On CSV data:**
- Could reach 200x+ faster

---

## Next: Implement Full Native Suite

Create native versions of all TPC-H queries to show maximum Sabot performance.

This will prove:
- Spark shim: 3.1x (compatibility with overhead)
- Native API: 5-10x (maximum performance)

Both beat PySpark, native shows full potential.
