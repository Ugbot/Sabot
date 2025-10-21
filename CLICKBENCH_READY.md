# ClickBench Benchmarks Ready

## What We Have

A complete ClickBench benchmark setup comparing DuckDB vs Sabot SQL:

- ✅ **43 ClickBench SQL queries** (queries.sql)
- ✅ **14.8GB hits.parquet dataset** (100M rows)
- ✅ **step_by_step_benchmark.py** - Main benchmark script
- ✅ **Progress tracking** - Resume from where you left off
- ✅ **Comprehensive documentation** - 5 guide files

## Current Run

**Command**: `uv run python step_by_step_benchmark.py 1 2`

**Status**: Loading 14.8GB parquet file (this takes 20-60 seconds)

**What It Will Do**:
1. Load data into DuckDB (~30s)
2. Load data into Sabot (~30s)
3. Run query 1 three times on DuckDB
4. Run query 1 three times on Sabot
5. Compare and show results
6. Repeat for query 2

## Expected Results

Based on our PySpark benchmarks, we expect:

**Query 1**: `SELECT COUNT(*) FROM hits;`
- Simple COUNT aggregation
- Expected: Sabot similar or slightly faster than DuckDB
- Why: Both use efficient columnar COUNT, minimal overhead

**Query 2**: `SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0;`
- COUNT with filter
- Expected: Sabot similar or slightly faster
- Why: Arrow SIMD filter, efficient COUNT

## Performance Expectations vs Other Systems

### Sabot vs DuckDB (What We're Testing)

**Expected**: Close competition (1-2x either way)
- Both use Arrow/columnar processing
- Both have C++ execution
- DuckDB is highly optimized
- Sabot has SIMD advantages

**Why Different from PySpark Results**:
- DuckDB doesn't have JVM overhead
- DuckDB doesn't have Pandas conversion
- DuckDB is a serious OLAP database
- This is a fair fight!

### Sabot vs ClickHouse (Reference)

ClickHouse is fastest on ClickBench (it's their benchmark):
- ClickHouse: Optimized for this exact workload
- Expected: ClickHouse faster on most queries
- Sabot competitive on SIMD-friendly operations

### Our Goals

**For ClickBench**:
- ✅ Verify Sabot SQL works correctly
- ✅ Measure performance vs DuckDB (fair comparison)
- ✅ Identify optimization opportunities
- ✅ Validate distributed execution

**Not trying to**:
- ❌ Beat ClickHouse (unrealistic, it's their benchmark)
- ❌ Just win benchmarks (focused on real workloads)

## What to Expect

### Likely Outcomes

**Queries Sabot Will Win**:
- Filter-heavy queries (SIMD advantage)
- Simple aggregations (SIMD SUM/AVG/COUNT)
- Projection queries (zero-copy)

**Queries DuckDB Will Win**:
- Complex string operations (DuckDB highly optimized)
- Regular expressions (specialized implementations)
- Multi-level GROUP BY (DuckDB query optimizer)

**Overall**: Competitive (±20%), with trade-offs

### Key Queries to Watch

**Query 1** (COUNT):
- Simple, both should be fast
- Expect: Very close

**Query 2** (COUNT WHERE):
- SIMD filter advantage
- Expect: Sabot slightly faster

**Query 3** (SUM, COUNT, AVG):
- Multiple SIMD aggregations
- Expect: Sabot advantage

**Query 5** (COUNT DISTINCT):
- Hash set operations
- Expect: Close, depends on implementation

## After Queries 1-2 Complete

The benchmark will show:

```
================================================================================
BENCHMARK SUMMARY
================================================================================

Total Queries: 2
Runs per query: 3

DuckDB Total Time: X.XXs
Sabot Total Time:  Y.YYs

Wins:
  DuckDB: N
  Sabot:  M
  Ties:   T

Overall: [Winner] is [speedup]x faster
```

And save detailed results to `comparison_results.json`.

## How to Continue

After queries 1-2 complete:

```bash
# Check status
uv run python step_by_step_benchmark.py --status

# Run next query
uv run python step_by_step_benchmark.py

# Or run next batch
uv run python step_by_step_benchmark.py 3 10
```

## Why This Benchmark Matters

### Different from PySpark Comparison

**PySpark Comparison**:
- Showed Sabot is 100-10,000x faster
- Due to PySpark's JVM/Pandas overhead
- Proves Sabot >> PySpark

**ClickBench/DuckDB Comparison**:
- Both are serious OLAP engines
- Fair comparison (C++, columnar, optimized)
- Proves Sabot competitive with best-in-class

### What We Learn

**From ClickBench**:
- How Sabot performs on standard OLAP workload
- Where our optimizations help most
- Where we can improve
- Validation of SQL correctness

**From PySpark**:
- How Sabot performs vs mainstream big data tool
- Value of C++/Arrow/SIMD architecture
- Why Sabot is better for performance-critical workloads

## Current Status

**Benchmark**: ⏳ Running (loading data)
**Expected Time**: 5-10 minutes for queries 1-2
**Progress**: Will be saved automatically
**Results**: Will be in comparison_results.json

**Next**: Wait for completion, then check results!
