#!/bin/bash
# Quick test script - runs first 5 queries to verify setup

echo "Quick ClickBench Test: DuckDB vs Sabot"
echo "Running first 5 queries (3 runs each)"
echo "========================================"

cd "$(dirname "$0")"

uv run python step_by_step_benchmark.py \
    --parquet-file hits.parquet \
    --num-runs 3 \
    --queries "1,2,3,4,5" \
    --output-file quick_test_results.json


