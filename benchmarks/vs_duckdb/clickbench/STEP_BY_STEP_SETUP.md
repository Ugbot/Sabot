# Step-by-Step ClickBench Setup Complete

## What Was Created

I've created a comprehensive benchmark system that compares DuckDB and Sabot SQL performance on ClickBench queries, running each query multiple times and showing detailed comparisons.

### Files Created

1. **`step_by_step_benchmark.py`** - Main benchmark script
   - Runs each query individually on both DuckDB and Sabot
   - Executes each query 3 times (configurable) to get average
   - Shows real-time progress and comparison
   - Saves detailed results to JSON

2. **`COMPARISON_README.md`** - Usage documentation
   - Quick start examples
   - Command-line options
   - Output format examples

3. **`quick_test.sh`** - Quick test script
   - Runs first 5 queries only for fast validation

## How to Use

### Quick Test (First Query Only)
```bash
cd /Users/bengamble/Sabot/benchmarks/clickbench
uv run python step_by_step_benchmark.py --queries "1"
```

### Test First 5 Queries
```bash
uv run python step_by_step_benchmark.py --queries "1,2,3,4,5"
```

### Run All 44 Queries
```bash
uv run python step_by_step_benchmark.py
```

### Run Specific Queries
```bash
# Test queries 1, 10, 20, 30
uv run python step_by_step_benchmark.py --queries "1,10,20,30"
```

### More Runs for Better Averaging
```bash
# Run each query 5 times instead of 3
uv run python step_by_step_benchmark.py --queries "1,2,3" --num-runs 5
```

## Output Example

For each query, you'll see:

```
================================================================================
Query 1/44
================================================================================
SQL: SELECT COUNT(*) FROM hits

Running on DuckDB (3 times)...
  Run 1: 0.523s (1 rows)
  Run 2: 0.498s (1 rows)
  Run 3: 0.512s (1 rows)
  Average: 0.511s

Running on Sabot (3 times)...
  Run 1: 0.345s (1 rows)
  Run 2: 0.332s (1 rows)
  Run 3: 0.338s (1 rows)
  Average: 0.338s

Comparison:
  DuckDB: 0.511s (avg), 1 rows
  Sabot:  0.338s (avg), 1 rows
  Winner: Sabot (1.51x faster)

Progress: 1/44 (2.3%)
```

## Summary Report

After all queries finish:

```
================================================================================
BENCHMARK SUMMARY
================================================================================

Total Queries: 44
Runs per query: 3

DuckDB Total Time: 45.23s
Sabot Total Time:  38.67s

Wins:
  DuckDB: 12
  Sabot:  28
  Ties:   4

Overall: Sabot is 1.17x faster

Top 5 queries where Sabot wins:
  Q5: 2.34x faster - SELECT COUNT(DISTINCT UserID) FROM hits...
  Q8: 1.98x faster - SELECT AdvEngineID, COUNT(*) FROM hits WHERE...

Top 5 queries where DuckDB wins:
  Q23: 1.45x faster - SELECT SearchPhrase, MIN(URL), MIN(Title)...
```

## Key Features

1. **Step-by-Step Execution**: See each query as it runs
2. **Multiple Runs**: Each query runs 3 times (or more) for accurate averages
3. **Real-Time Comparison**: Instantly see which system is faster
4. **Detailed Results**: JSON output with all timing data
5. **Flexible Selection**: Run specific queries or all 44
6. **Interrupt Safe**: Can stop with Ctrl+C and get partial results
7. **Memory Efficient**: Loads data once, reuses for all queries

## Performance Expectations

- **Data Loading**: 10-30 seconds (one-time for each system)
- **Simple Queries**: 0.1-1 second per run
- **Complex Queries**: 1-10 seconds per run
- **Full Benchmark**: 15-45 minutes for all 44 queries

## Next Steps

### 1. Quick Test (Recommended)
Test with just one query to verify everything works:
```bash
cd /Users/bengamble/Sabot/benchmarks/clickbench
uv run python step_by_step_benchmark.py --queries "1"
```

### 2. Test First 5 Queries
Get a quick sense of relative performance:
```bash
uv run python step_by_step_benchmark.py --queries "1,2,3,4,5"
```

### 3. Run Full Benchmark
Once satisfied with initial tests:
```bash
uv run python step_by_step_benchmark.py
```

## Files and Data

- **Parquet file**: `hits.parquet` (14.8GB) - already in the directory
- **Query file**: `queries.sql` - 44 ClickBench queries
- **Results**: Saved to `comparison_results.json`

## Troubleshooting

### Use `uv run`
Always use `uv run` to ensure dependencies are available:
```bash
uv run python step_by_step_benchmark.py
```

### Check Sabot SQL Build
If Sabot queries fail, ensure it's compiled:
```bash
cd /Users/bengamble/Sabot/sabot_sql
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build
```

### Memory Issues
If you run out of memory, test fewer queries at a time:
```bash
uv run python step_by_step_benchmark.py --queries "1,2,3"
```

## Ready to Run!

Everything is set up. Just run:
```bash
cd /Users/bengamble/Sabot/benchmarks/clickbench
uv run python step_by_step_benchmark.py --queries "1"
```

This will test the first query and show you exactly how the comparison works.


