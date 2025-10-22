# ClickBench Comparison: DuckDB vs Sabot

This directory contains a step-by-step benchmark that compares DuckDB and Sabot SQL performance on ClickBench queries.

## Quick Start

### Test First 5 Queries
```bash
cd /Users/bengamble/Sabot/benchmarks/clickbench
uv run python step_by_step_benchmark.py --queries "1,2,3,4,5"
```

### Run All 44 Queries
```bash
uv run python step_by_step_benchmark.py
```

### Run Specific Queries
```bash
# Run queries 1, 5, 10, 15, 20
uv run python step_by_step_benchmark.py --queries "1,5,10,15,20"
```

### Custom Number of Runs
```bash
# Run each query 5 times instead of 3
uv run python step_by_step_benchmark.py --num-runs 5
```

## Output Format

The benchmark displays real-time progress for each query:

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

## Summary Output

After all queries complete, you'll see:

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
  Q12: 1.87x faster - SELECT MobilePhone, MobilePhoneModel, COUNT(DISTINCT...

Top 5 queries where DuckDB wins:
  Q23: 1.45x faster - SELECT SearchPhrase, MIN(URL), MIN(Title), COUNT(*)...
  Q29: 1.32x faster - SELECT REGEXP_REPLACE(Referer, '^https?://(?:www...
  Q35: 1.28x faster - SELECT 1, URL, COUNT(*) AS c FROM hits GROUP BY...
```

## Results File

Results are saved to `comparison_results.json` with detailed timing data:

```json
{
  "num_queries": 44,
  "num_runs": 3,
  "parquet_file": "hits.parquet",
  "results": [
    {
      "query_id": 1,
      "query": "SELECT COUNT(*) FROM hits;",
      "duckdb": {
        "times": [0.523, 0.498, 0.512],
        "avg_time": 0.511,
        "min_time": 0.498,
        "max_time": 0.523,
        "rows": 1
      },
      "sabot": {
        "times": [0.345, 0.332, 0.338],
        "avg_time": 0.338,
        "min_time": 0.332,
        "max_time": 0.345,
        "rows": 1
      },
      "speedup": 1.51
    }
  ]
}
```

## Command-Line Options

```
--parquet-file      Path to hits.parquet file (default: hits.parquet)
--num-runs          Number of runs per query (default: 3)
--output-file       Output JSON file (default: comparison_results.json)
--queries           Comma-separated query numbers (e.g., "1,2,3,4,5")
```

## Examples

### Quick sanity check (first 3 queries)
```bash
uv run python step_by_step_benchmark.py --queries "1,2,3"
```

### Test aggregation queries only
```bash
uv run python step_by_step_benchmark.py --queries "3,8,9,10,11,12,13,14,15"
```

### Full benchmark with 5 runs per query
```bash
uv run python step_by_step_benchmark.py --num-runs 5 --output-file full_benchmark_5runs.json
```

### Resume after interruption
The benchmark can be interrupted with Ctrl+C and will save partial results.

## Performance Notes

- **First query is slower**: Both systems have cold start overhead
- **Data loading**: Takes 10-30 seconds depending on system
- **Total benchmark time**: 15-45 minutes for all 44 queries
- **Memory usage**: ~15-20GB for the 14.8GB parquet file

## Troubleshooting

### Import errors
Use `uv run` to ensure dependencies are available:
```bash
uv run python step_by_step_benchmark.py
```

### Memory issues
Run fewer queries at a time or increase system memory.

### Sabot SQL not compiled
Make sure Sabot SQL is built:
```bash
cd /Users/bengamble/Sabot/sabot_sql
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build
```


