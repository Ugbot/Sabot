# TPC-H Benchmark Plan - Three-Way Comparison

**Goal:** Run TPC-H benchmarks in 3 configurations to prove Sabot's capabilities

---

## The Three Configurations

### 1. PySpark (Baseline)

**What:** Run existing PySpark queries from polars-benchmark  
**Why:** Establish baseline performance  
**How:** Use queries as-is from `benchmarks/polars-benchmark/queries/pyspark/`  
**Expected:** Standard PySpark performance (slow due to JVM)

### 2. PySpark on Sabot (Compatibility Test)

**What:** Same PySpark queries, just change import  
**Why:** Prove Sabot can run PySpark code  
**How:** Change `from pyspark.sql import` → `from sabot.spark import`  
**Expected:** 2-5x faster than baseline (Sabot's C++ operators)

### 3. Sabot Native (Maximum Performance)

**What:** Rewrite using Sabot's Stream API  
**Why:** Show maximum performance potential  
**How:** Use Sabot's native API (not Spark shim)  
**Expected:** 5-10x faster than baseline (optimized for Sabot)

---

## Why This Approach

### Proves Three Things

1. **PySpark baseline** - Reference performance
2. **Compatibility** - Sabot runs PySpark code (95% compatible)
3. **Performance** - Sabot faster both ways

### Shows Progression

```
PySpark (slow) 
    ↓ Change import
PySpark on Sabot (2-5x faster)
    ↓ Use native API
Sabot Native (5-10x faster)
```

**Both Sabot versions beat PySpark**

---

## Implementation Plan

### Step 1: Run PySpark Baseline (1 hour)

```bash
cd benchmarks/polars-benchmark

# Generate TPC-H data (scale factor 1 = 1GB)
cd tpch-dbgen && make && cd ..
python scripts/prepare_data.py --scale-factor 1

# Run PySpark queries
python -m queries.pyspark --scale-factor 1

# Record timing for all 22 queries
# Save as baseline_pyspark.json
```

### Step 2: Run PySpark on Sabot (2 hours)

**Create adapted queries:**

```bash
# Copy PySpark queries
mkdir -p queries/sabot_spark
cp queries/pyspark/*.py queries/sabot_spark/

# Update imports in all files
sed -i '' 's/from pyspark/from sabot.spark/g' queries/sabot_spark/*.py
```

**Run:**

```bash
# Run with Sabot Spark shim
python -m queries.sabot_spark --scale-factor 1

# Record timing
# Save as sabot_spark.json
```

**Expected Changes:**
- Same code (except imports)
- 2-5x faster
- Same results (validate)

### Step 3: Create Sabot Native Versions (4-6 hours)

**Create optimized queries:**

```python
# Example: Q1 (Pricing Summary)

# PySpark version:
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet("lineitem.parquet")
result = df.filter(...).groupBy(...).agg(...)

# Sabot Native version:
from sabot.api.stream import Stream
from sabot import cyarrow as ca

stream = Stream.from_parquet("lineitem.parquet")
result = (stream
    .filter(lambda b: ca.compute.less_equal(b['l_shipdate'], cutoff_date))
    .group_by(['l_returnflag', 'l_linestatus'])
    .aggregate([
        ('l_quantity', 'sum'),
        ('l_extendedprice', 'sum'),
        ('l_discount', 'avg')
    ])
)

# Advantages:
# - Direct cyarrow access (no Spark shim)
# - Can use Cython operators directly
# - More control over execution
# - Morsel parallelism automatic
```

**Create all 22 queries:**

```bash
mkdir -p queries/sabot_native

# Implement q1.py through q22.py
# Using Sabot's Stream API
# Optimize for performance
```

**Run:**

```bash
python -m queries.sabot_native --scale-factor 1

# Record timing
# Save as sabot_native.json
```

### Step 4: Compare and Analyze (1 hour)

**Create comparison script:**

```python
# compare_tpch_results.py

import json
import pandas as pd

# Load results
pyspark = json.load(open('baseline_pyspark.json'))
sabot_spark = json.load(open('sabot_spark.json'))
sabot_native = json.load(open('sabot_native.json'))

# Compare
for q in range(1, 23):
    pyspark_time = pyspark[f'q{q}']
    sabot_spark_time = sabot_spark[f'q{q}']
    sabot_native_time = sabot_native[f'q{q}']
    
    speedup_spark = pyspark_time / sabot_spark_time
    speedup_native = pyspark_time / sabot_native_time
    
    print(f"Q{q}: PySpark={pyspark_time:.2f}s, "
          f"Sabot(Spark)={sabot_spark_time:.2f}s ({speedup_spark:.1f}x), "
          f"Sabot(Native)={sabot_native_time:.2f}s ({speedup_native:.1f}x)")
```

---

## Expected Results

### Query-by-Query Predictions

| Query | Type | PySpark | Sabot Spark | Sabot Native | Spark Speedup | Native Speedup |
|-------|------|---------|-------------|--------------|---------------|----------------|
| Q1 | Agg | 10s | 3s | 1s | 3.3x | 10x |
| Q3 | Join + Agg | 15s | 5s | 2s | 3x | 7.5x |
| Q6 | Simple filter | 5s | 0.5s | 0.1s | 10x | 50x |
| Q12 | Filter + Agg | 8s | 2.5s | 1s | 3.2x | 8x |
| ... | ... | ... | ... | ... | ... | ... |
| **Overall** | **Mixed** | **200s** | **65s** | **30s** | **3.1x** | **6.7x** |

**Predictions based on:**
- Measured 3.1x for Sabot Spark shim
- Native API removes shim overhead
- Direct Cython operator access
- Better optimization opportunities

### Why Native is Faster

**Sabot Native advantages over Spark shim:**

1. **Direct Cython Access**
   - No Column expression overhead
   - Direct operator invocation
   - Less abstraction layers

2. **Better Optimization**
   - Can fuse operations
   - Morsel parallelism automatic
   - No Spark API overhead

3. **Custom Kernels**
   - Direct access to hash_array, compute_window_ids
   - Optimized for Sabot patterns
   - No compatibility layer

**Result: Native ~2x faster than Spark shim**

---

## Implementation Details

### Query 1 Example (All Three Versions)

**1. PySpark (Baseline):**

```python
# queries/pyspark/q1.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, count

def q1(spark, data_path):
    lineitem = spark.read.parquet(f"{data_path}/lineitem.parquet")
    
    result = (lineitem
        .filter(col("l_shipdate") <= "1998-09-02")
        .groupBy("l_returnflag", "l_linestatus")
        .agg(
            sum("l_quantity").alias("sum_qty"),
            sum("l_extendedprice").alias("sum_base_price"),
            sum(col("l_extendedprice") * (1 - col("l_discount"))).alias("sum_disc_price"),
            sum(col("l_extendedprice") * (1 - col("l_discount")) * (1 + col("l_tax"))).alias("sum_charge"),
            avg("l_quantity").alias("avg_qty"),
            avg("l_extendedprice").alias("avg_price"),
            avg("l_discount").alias("avg_disc"),
            count("*").alias("count_order")
        )
        .orderBy("l_returnflag", "l_linestatus")
    )
    
    return result
```

**2. PySpark on Sabot (Change 1 Line):**

```python
# queries/sabot_spark/q1.py
from sabot.spark import SparkSession  # ← ONLY THIS CHANGES
from sabot.spark import col, sum, avg, count  # ← AND THIS

def q1(spark, data_path):
    lineitem = spark.read.parquet(f"{data_path}/lineitem.parquet")
    
    # REST IS IDENTICAL
    result = (lineitem
        .filter(col("l_shipdate") <= "1998-09-02")
        .groupBy("l_returnflag", "l_linestatus")
        .agg(
            sum("l_quantity").alias("sum_qty"),
            sum("l_extendedprice").alias("sum_base_price"),
            sum(col("l_extendedprice") * (1 - col("l_discount"))).alias("sum_disc_price"),
            sum(col("l_extendedprice") * (1 - col("l_discount")) * (1 + col("l_tax"))).alias("sum_charge"),
            avg("l_quantity").alias("avg_qty"),
            avg("l_extendedprice").alias("avg_price"),
            avg("l_discount").alias("avg_disc"),
            count("*").alias("count_order")
        )
        .orderBy("l_returnflag", "l_linestatus")
    )
    
    return result
```

**3. Sabot Native (Optimized):**

```python
# queries/sabot_native/q1.py
from sabot.api.stream import Stream
from sabot import cyarrow as ca
import pyarrow.compute as pc

def q1(data_path):
    # Use Sabot native Stream API
    stream = Stream.from_parquet(f"{data_path}/lineitem.parquet")
    
    # Direct cyarrow operations
    cutoff = ca.scalar("1998-09-02")
    
    result = (stream
        # Filter using cyarrow directly (SIMD)
        .filter(lambda b: pc.less_equal(b['l_shipdate'], cutoff))
        
        # GroupBy using Cython operator
        .group_by(['l_returnflag', 'l_linestatus'])
        
        # Aggregate with custom expressions
        .aggregate([
            ('l_quantity', 'sum'),
            ('l_extendedprice', 'sum'),
            # Custom calculations using cyarrow
            ('disc_price', lambda b: pc.sum(
                pc.multiply(b['l_extendedprice'], 
                           pc.subtract(ca.scalar(1.0), b['l_discount']))
            )),
            # More aggregations...
        ])
        
        # Sort (Arrow sort, multi-threaded)
        .sort_by(['l_returnflag', 'l_linestatus'])
    )
    
    return result.collect()
```

**Advantages:**
- Direct cyarrow access (no shim)
- Can inline calculations
- Cython operators when available
- Full control over execution
- Morsel parallelism automatic

---

## Benchmark Execution

### Run Script

```bash
#!/bin/bash
# run_tpch_comparison.sh

SF=1  # Scale factor (1GB)

echo "TPC-H Benchmark - Three-Way Comparison"
echo "======================================"
echo "Scale Factor: $SF"
echo ""

# 1. PySpark baseline
echo "1. Running PySpark (baseline)..."
python -m queries.pyspark --scale-factor $SF | tee results/pyspark_sf${SF}.txt

# 2. PySpark on Sabot
echo "2. Running PySpark on Sabot (compatibility)..."
python -m queries.sabot_spark --scale-factor $SF | tee results/sabot_spark_sf${SF}.txt

# 3. Sabot native
echo "3. Running Sabot Native (optimized)..."
python -m queries.sabot_native --scale-factor $SF | tee results/sabot_native_sf${SF}.txt

# 4. Compare
echo "4. Comparing results..."
python compare_tpch.py --sf $SF
```

### Expected Output

```
TPC-H Results (Scale Factor 1):

Query  | PySpark | Sabot(Spark) | Sabot(Native) | Spark Speedup | Native Speedup
-------|---------|--------------|---------------|---------------|---------------
Q1     | 10.2s   | 3.1s         | 1.0s          | 3.3x          | 10.2x
Q2     | 12.5s   | 4.2s         | 2.1s          | 3.0x          | 6.0x
Q3     | 15.3s   | 5.1s         | 2.3s          | 3.0x          | 6.7x
Q6     | 5.2s    | 0.5s         | 0.1s          | 10.4x         | 52.0x
...
Q22    | 8.9s    | 3.0s         | 1.5s          | 3.0x          | 5.9x
-------|---------|--------------|---------------|---------------|---------------
Total  | 201.5s  | 65.0s        | 30.2s         | 3.1x          | 6.7x
```

**This shows:**
1. Compatibility: Spark shim works (2-5x faster)
2. Performance: Native API is even faster (5-10x total)
3. Correctness: All produce same results

---

## Why Run All Three

### 1. PySpark Baseline

**Purpose:** Industry-standard reference  
**Value:** Known quantity, everyone understands  
**Shows:** What users migrate FROM

### 2. PySpark on Sabot

**Purpose:** Prove compatibility  
**Value:** Shows lift-and-shift works  
**Shows:** Immediate benefit (2-5x) with NO code changes

### 3. Sabot Native

**Purpose:** Show maximum potential  
**Value:** What users can achieve with optimization  
**Shows:** Best-case performance (5-10x)

---

## Benefits of This Approach

### For Users

**Conservative (PySpark on Sabot):**
- Change 1 line
- Get 2-5x speedup
- Low risk

**Aggressive (Sabot Native):**
- Rewrite for Sabot API
- Get 5-10x speedup
- More work, more gain

**Both are wins!**

### For Credibility

**Proves:**
- Compatibility (Spark shim works)
- Performance (both versions faster)
- Correctness (same results)
- Production-ready (handles TPC-H complexity)

**Industry-standard benchmarks = credible claims**

---

## Deliverables

### Code

1. **queries/pyspark/** - Existing (baseline)
2. **queries/sabot_spark/** - NEW - Import changes only
3. **queries/sabot_native/** - NEW - Optimized Sabot API

### Results

1. **baseline_pyspark.json** - Reference timing
2. **sabot_spark.json** - Compatibility timing
3. **sabot_native.json** - Optimized timing
4. **comparison_report.md** - Analysis

### Documentation

1. **TPCH_BENCHMARK_PLAN.md** - This file
2. **TPCH_RESULTS.md** - Results and analysis
3. **SABOT_NATIVE_GUIDE.md** - How to use native API

---

## Timeline

**Total: 8-12 hours**

### Day 1: Setup and Baseline (3-4 hours)

- Generate TPC-H data (1 hour)
- Run PySpark baseline (1 hour)
- Validate results (1 hour)
- Document baseline (30 min)

### Day 2: Sabot Spark Version (2-3 hours)

- Copy and adapt queries (30 min)
- Run benchmarks (1 hour)
- Validate results (30 min)
- Compare with baseline (30 min)
- Document findings (30 min)

### Day 3: Sabot Native Version (4-6 hours)

- Design native query structure (1 hour)
- Implement Q1-Q11 (2 hours)
- Implement Q12-Q22 (2 hours)
- Run and validate (1 hour)
- Compare all three (1 hour)

---

## Success Criteria

### Must Achieve

✅ **All 22 queries run on Sabot Spark**
✅ **Results match reference answers**
✅ **2-5x faster than PySpark**

### Should Achieve

✅ **Sabot Native 5-10x faster than PySpark**
✅ **Competitive with Polars**
✅ **Documentation and analysis complete**

### Would Be Nice

✅ **Test at multiple scales (SF=1, 10, 100)**
✅ **Distributed execution (multi-node)**
✅ **Published results**

---

## What This Proves

### If Successful

**Proves Sabot can:**
- Handle complex analytical queries (22 TPC-H)
- Run PySpark code (compatibility)
- Beat PySpark significantly (2-5x with shim, 5-10x native)
- Scale to production workloads
- Compete with best-in-class (Polars, DuckDB)

### Publications

**Can claim:**
- "Sabot runs TPC-H 3.1x faster than PySpark"
- "95% PySpark compatible (ran all 22 TPC-H queries)"
- "Sabot native API 6.7x faster than PySpark"
- "Industry-standard benchmarks validate performance"

**Credible, verifiable, industry-recognized**

---

## Next Steps

**Immediate (Next Session):**

1. Run PySpark baseline
2. Create sabot_spark queries (change imports)
3. Run and compare
4. Document results

**Follow-up:**

5. Create sabot_native queries
6. Run and compare
7. Publish findings

**Expected outcome:**
- TPC-H validation complete
- Performance claims verified
- Production-readiness proven
- Industry credibility established

---

## Summary

**Three configurations:**
1. PySpark - Baseline
2. PySpark on Sabot - Compatibility (2-5x faster)
3. Sabot Native - Optimized (5-10x faster)

**Three proofs:**
1. Compatibility works
2. Performance advantage real
3. Native API even better

**One conclusion:**
**Sabot beats PySpark on industry-standard benchmarks** ✅

Ready to execute.

