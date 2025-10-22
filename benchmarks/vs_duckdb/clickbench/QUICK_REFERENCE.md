# ClickBench Quick Reference

## Common Commands

```bash
cd /Users/bengamble/Sabot/benchmarks/clickbench
```

| Command | Description |
|---------|-------------|
| `uv run python step_by_step_benchmark.py` | Run next unfinished test |
| `uv run python step_by_step_benchmark.py 5` | Run test 5 |
| `uv run python step_by_step_benchmark.py 5 10` | Run tests 5-10 |
| `uv run python step_by_step_benchmark.py --status` | Show progress |
| `uv run python step_by_step_benchmark.py --reset` | Reset and start over |

## Quick Start

```bash
# First time - run test 1
uv run python step_by_step_benchmark.py 1

# Run next test
uv run python step_by_step_benchmark.py

# Check what's done
uv run python step_by_step_benchmark.py --status

# Run next 5 tests
uv run python step_by_step_benchmark.py 2 6
```

## Output Files

- `benchmark_progress.json` - Tracks which tests are complete
- `comparison_results.json` - Full results with timings

## Progress Tracking

✓ **Automatic** - Script remembers what's done
✓ **Resume anytime** - Just run again
✓ **No duplicates** - Won't re-run completed tests (unless you specify them)

## Tips

💡 Run one test at a time: `uv run python step_by_step_benchmark.py`
💡 Check status often: `uv run python step_by_step_benchmark.py --status`
💡 Run in batches of 5-10 for good progress
💡 Use `--reset` to start fresh

