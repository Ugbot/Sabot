# ClickBench Step-by-Step Usage Guide

## Quick Start

The benchmark automatically tracks progress and resumes where you left off.

### Run Next Unfinished Test
```bash
cd /Users/bengamble/Sabot/benchmarks/clickbench
uv run python step_by_step_benchmark.py
```

This will automatically find and run the next unfinished query. Perfect for running tests one at a time!

### Run Specific Test
```bash
# Run query 5
uv run python step_by_step_benchmark.py 5

# Run query 10
uv run python step_by_step_benchmark.py 10
```

### Run Range of Tests
```bash
# Run queries 5 through 10
uv run python step_by_step_benchmark.py 5 10

# Run queries 1 through 5
uv run python step_by_step_benchmark.py 1 5

# Run queries 20 through 44
uv run python step_by_step_benchmark.py 20 44
```

### Check Status
```bash
# See which tests are complete and what's next
uv run python step_by_step_benchmark.py --status
```

Output:
```
============================================================
Benchmark Status
============================================================
Total queries: 44
Completed: 12
Remaining: 32
Progress: 27.3%

Next unfinished query: 13
Completed queries: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
============================================================
```

### Reset Progress
```bash
# Start fresh from the beginning
uv run python step_by_step_benchmark.py --reset
```

## Typical Workflow

### Option 1: Run One at a Time
```bash
# Run next test
uv run python step_by_step_benchmark.py

# Check status
uv run python step_by_step_benchmark.py --status

# Run next test again
uv run python step_by_step_benchmark.py

# Continue until all done...
```

### Option 2: Run in Batches
```bash
# Run first 10
uv run python step_by_step_benchmark.py 1 10

# Later, run next 10
uv run python step_by_step_benchmark.py 11 20

# Run remaining
uv run python step_by_step_benchmark.py 21 44
```

### Option 3: Skip Around
```bash
# Run easy queries first
uv run python step_by_step_benchmark.py 1
uv run python step_by_step_benchmark.py 2
uv run python step_by_step_benchmark.py 4

# Then run harder ones
uv run python step_by_step_benchmark.py 23
uv run python step_by_step_benchmark.py 30
```

## Command-Line Options

```
positional arguments:
  query_start           Query number to run (or start of range)
  query_end             End of query range (inclusive)

options:
  --parquet-file FILE   Path to hits.parquet file (default: hits.parquet)
  --num-runs N          Number of runs per query (default: 3)
  --output-file FILE    Output file for results (default: comparison_results.json)
  --progress-file FILE  Progress tracking file (default: benchmark_progress.json)
  --reset               Reset progress and start from beginning
  --status              Show status and exit
```

## Examples

### Basic Usage
```bash
# Run next unfinished test (most common)
uv run python step_by_step_benchmark.py

# Run test 1
uv run python step_by_step_benchmark.py 1

# Run tests 5-10
uv run python step_by_step_benchmark.py 5 10

# Check progress
uv run python step_by_step_benchmark.py --status
```

### Advanced Usage
```bash
# Use 5 runs per query instead of 3
uv run python step_by_step_benchmark.py 1 10 --num-runs 5

# Use different output file
uv run python step_by_step_benchmark.py 1 5 --output-file my_results.json

# Reset and run all tests
uv run python step_by_step_benchmark.py --reset
uv run python step_by_step_benchmark.py 1 44
```

### Running All Tests Progressively
```bash
# Start from beginning
uv run python step_by_step_benchmark.py --reset

# Run tests one by one (or in a loop)
for i in {1..44}; do
  echo "Running test $i..."
  uv run python step_by_step_benchmark.py $i
done

# Or just keep running next until done
while true; do
  uv run python step_by_step_benchmark.py || break
done
```

## Progress Tracking

### Progress File
- Stored in `benchmark_progress.json` by default
- Contains:
  - List of completed query numbers
  - All results so far
  - Configuration (num_runs, parquet_file)

### How It Works
1. First run creates `benchmark_progress.json`
2. Each completed query is marked as done
3. Next run loads progress and skips completed queries
4. Use `--reset` to start fresh
5. Use `--status` to see current progress

### Resume After Interruption
If you interrupt a test (Ctrl+C), only the current query is not saved. All previous queries remain marked as complete. Just run the script again to continue.

## Output

### Real-Time Output
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

### Summary (After Each Run)
```
================================================================================
BENCHMARK SUMMARY
================================================================================

Total Queries: 1
Runs per query: 3

DuckDB Total Time: 0.51s
Sabot Total Time:  0.34s

Wins:
  DuckDB: 0
  Sabot:  1
  Ties:   0

Overall: Sabot is 1.51x faster
```

### Results Files
- `comparison_results.json` - Final results with all timing data
- `benchmark_progress.json` - Progress tracking (completed queries)

## Tips

1. **Start with one test**: Run `uv run python step_by_step_benchmark.py 1` to verify setup
2. **Use --status often**: Check progress between runs
3. **Run in batches**: Do 5-10 queries at a time for good progress without long waits
4. **Resume anytime**: The script remembers what you've completed
5. **Re-run specific tests**: Just specify the query number again
6. **Increase runs for accuracy**: Use `--num-runs 5` or more for better averages

## Troubleshooting

### "All queries already completed!"
Either:
- Use `--reset` to start over
- Specify a query number to re-run it
- Check status with `--status`

### Progress file issues
Delete `benchmark_progress.json` to start fresh

### Want to run specific queries only
Just run them directly:
```bash
uv run python step_by_step_benchmark.py 1
uv run python step_by_step_benchmark.py 5
uv run python step_by_step_benchmark.py 10
```

No need to run all 44 if you only care about specific ones!
