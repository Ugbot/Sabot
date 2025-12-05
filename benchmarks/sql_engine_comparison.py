#!/usr/bin/env python3
"""
SQL Engine Performance Comparison: DuckDB vs Sabot SQLController

Compares:
1. DuckDB direct execution (via sabot_sql_duckdb_direct)
2. Sabot SQLController (distributed execution with Cython operators)

Test queries:
- Simple filtering
- Aggregation with GROUP BY
- Multi-column aggregation
- Window functions
- Joins
- Time series operations (Sabot only)
"""

import sys
sys.path.insert(0, '/Users/bengamble/Sabot')

import time
import asyncio
import statistics
from dataclasses import dataclass
from typing import List, Dict, Any, Optional
import random

from sabot import cyarrow as ca


@dataclass
class BenchmarkResult:
    """Result of a single benchmark run."""
    query_name: str
    engine: str
    rows_processed: int
    rows_returned: int
    execution_time_ms: float
    iterations: int


def generate_test_data(num_rows: int = 100_000) -> ca.Table:
    """Generate test data for benchmarks."""
    random.seed(42)

    symbols = ['AAPL', 'GOOG', 'MSFT', 'AMZN', 'META', 'NVDA', 'TSLA', 'AMD']

    data = {
        'id': list(range(num_rows)),
        'symbol': [random.choice(symbols) for _ in range(num_rows)],
        'price': [random.uniform(50, 500) for _ in range(num_rows)],
        'volume': [random.randint(100, 100000) for _ in range(num_rows)],
        'timestamp': [f'2025-01-{(i % 28) + 1:02d} {(i % 24):02d}:{(i % 60):02d}:00'
                      for i in range(num_rows)],
        'category': [random.choice(['A', 'B', 'C', 'D']) for _ in range(num_rows)],
        'value': [random.uniform(0, 1000) for _ in range(num_rows)],
    }

    return ca.table(data)


# Create a shared DuckDB engine instance to avoid re-registration overhead
_duckdb_engine = None

def get_duckdb_engine():
    """Get or create shared DuckDB engine."""
    global _duckdb_engine
    if _duckdb_engine is None:
        from sabot_sql.sabot_sql_duckdb_direct import SabotSQLBridge
        _duckdb_engine = SabotSQLBridge()
    return _duckdb_engine


def run_duckdb_query(table: ca.Table, query: str, table_name: str = 'trades',
                     use_shared_engine: bool = True) -> tuple:
    """Run query using DuckDB direct."""
    import io
    import sys

    # Suppress registration output
    old_stdout = sys.stdout
    sys.stdout = io.StringIO()

    try:
        if use_shared_engine:
            engine = get_duckdb_engine()
            # Only register if not already registered
            if table_name not in engine.tables:
                engine.register_table(table_name, table)
        else:
            from sabot_sql.sabot_sql_duckdb_direct import SabotSQLBridge
            engine = SabotSQLBridge()
            engine.register_table(table_name, table)

        start = time.perf_counter()
        result = engine.execute_sql(query)
        elapsed = (time.perf_counter() - start) * 1000

        return result, elapsed
    finally:
        sys.stdout = old_stdout


# Create a shared SQLController instance
_sabot_controller = None

def get_sabot_controller():
    """Get or create shared SQLController."""
    global _sabot_controller
    if _sabot_controller is None:
        from sabot.sql.controller import SQLController
        _sabot_controller = SQLController()
    return _sabot_controller


async def run_sabot_query(table: ca.Table, query: str, table_name: str = 'trades',
                          num_workers: int = 4) -> tuple:
    """Run query using Sabot SQLController."""
    import io
    import sys

    # Suppress registration output
    old_stdout = sys.stdout
    sys.stdout = io.StringIO()

    try:
        controller = get_sabot_controller()

        # Only register if not already registered
        if table_name not in controller.tables:
            await controller.register_table(table_name, table)

        start = time.perf_counter()
        result = await controller.execute(query, num_agents=num_workers, execution_mode="local_parallel")
        elapsed = (time.perf_counter() - start) * 1000

        return result, elapsed
    finally:
        sys.stdout = old_stdout


# Create a single event loop for all Sabot queries
_event_loop = None

def get_event_loop():
    """Get or create shared event loop."""
    global _event_loop
    if _event_loop is None or _event_loop.is_closed():
        _event_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(_event_loop)
    return _event_loop


def benchmark_query(table: ca.Table, query_name: str, query: str,
                    iterations: int = 5) -> List[BenchmarkResult]:
    """Benchmark a query on both engines."""
    results = []
    loop = get_event_loop()

    # Warm-up runs
    print(f"  Warming up {query_name}...")
    try:
        run_duckdb_query(table, query)
    except Exception as e:
        print(f"    DuckDB warmup failed: {e}")

    try:
        loop.run_until_complete(run_sabot_query(table, query))
    except Exception as e:
        print(f"    Sabot warmup failed: {e}")

    # DuckDB benchmark
    print(f"  Benchmarking DuckDB...")
    duckdb_times = []
    duckdb_rows = 0
    for i in range(iterations):
        try:
            result, elapsed = run_duckdb_query(table, query)
            duckdb_times.append(elapsed)
            duckdb_rows = result.num_rows if result else 0
        except Exception as e:
            print(f"    DuckDB iteration {i+1} failed: {e}")
            duckdb_times.append(float('inf'))

    if duckdb_times and duckdb_times[0] != float('inf'):
        results.append(BenchmarkResult(
            query_name=query_name,
            engine="DuckDB",
            rows_processed=table.num_rows,
            rows_returned=duckdb_rows,
            execution_time_ms=statistics.median(duckdb_times),
            iterations=iterations
        ))

    # Sabot SQLController benchmark
    print(f"  Benchmarking Sabot SQLController...")
    sabot_times = []
    sabot_rows = 0

    for i in range(iterations):
        try:
            result, elapsed = loop.run_until_complete(run_sabot_query(table, query))
            sabot_times.append(elapsed)
            sabot_rows = result.num_rows if result else 0
        except Exception as e:
            print(f"    Sabot iteration {i+1} failed: {e}")
            sabot_times.append(float('inf'))

    if sabot_times and sabot_times[0] != float('inf'):
        results.append(BenchmarkResult(
            query_name=query_name,
            engine="Sabot",
            rows_processed=table.num_rows,
            rows_returned=sabot_rows,
            execution_time_ms=statistics.median(sabot_times),
            iterations=iterations
        ))

    return results


def run_benchmarks(num_rows: int = 100_000, iterations: int = 5) -> List[BenchmarkResult]:
    """Run all benchmarks."""
    print(f"\n{'='*70}")
    print(f"SQL Engine Comparison Benchmark")
    print(f"{'='*70}")
    print(f"Data size: {num_rows:,} rows")
    print(f"Iterations per query: {iterations}")
    print(f"{'='*70}\n")

    # Generate test data
    print("Generating test data...")
    table = generate_test_data(num_rows)
    print(f"Generated table with {table.num_rows:,} rows, {table.num_columns} columns\n")

    all_results = []

    # Define benchmark queries
    queries = [
        ("Simple SELECT", "SELECT * FROM trades LIMIT 1000"),

        ("Filter (price > 200)", "SELECT * FROM trades WHERE price > 200"),

        ("Filter + Sort", "SELECT * FROM trades WHERE price > 200 ORDER BY volume DESC LIMIT 1000"),

        ("COUNT aggregation", "SELECT COUNT(*) as cnt FROM trades"),

        ("GROUP BY (symbol)", "SELECT symbol, COUNT(*) as cnt, AVG(price) as avg_price FROM trades GROUP BY symbol"),

        ("GROUP BY (2 columns)",
         "SELECT symbol, category, COUNT(*) as cnt, SUM(volume) as total_vol FROM trades GROUP BY symbol, category"),

        ("Complex aggregation",
         """SELECT symbol,
                   COUNT(*) as cnt,
                   AVG(price) as avg_price,
                   MIN(price) as min_price,
                   MAX(price) as max_price,
                   SUM(volume) as total_volume
            FROM trades
            GROUP BY symbol"""),

        ("HAVING clause",
         """SELECT symbol, AVG(price) as avg_price, COUNT(*) as cnt
            FROM trades
            GROUP BY symbol
            HAVING COUNT(*) > 10000"""),

        ("Subquery",
         """SELECT * FROM trades
            WHERE price > (SELECT AVG(price) FROM trades)
            LIMIT 1000"""),
    ]

    for query_name, query in queries:
        print(f"\n[{query_name}]")
        print(f"  Query: {query[:80]}{'...' if len(query) > 80 else ''}")
        results = benchmark_query(table, query_name, query, iterations)
        all_results.extend(results)

    return all_results


def print_results(results: List[BenchmarkResult]):
    """Print benchmark results in a formatted table."""
    print(f"\n{'='*90}")
    print("BENCHMARK RESULTS")
    print(f"{'='*90}")
    print(f"{'Query':<25} {'Engine':<10} {'Rows In':>12} {'Rows Out':>10} {'Time (ms)':>12} {'Throughput':>15}")
    print(f"{'-'*90}")

    # Group by query name
    query_names = []
    for r in results:
        if r.query_name not in query_names:
            query_names.append(r.query_name)

    for query_name in query_names:
        query_results = [r for r in results if r.query_name == query_name]

        for r in query_results:
            throughput = r.rows_processed / (r.execution_time_ms / 1000) if r.execution_time_ms > 0 else 0
            print(f"{r.query_name:<25} {r.engine:<10} {r.rows_processed:>12,} {r.rows_returned:>10,} "
                  f"{r.execution_time_ms:>12.2f} {throughput:>12,.0f}/s")

        # Print comparison if we have both engines
        if len(query_results) == 2:
            duckdb = next((r for r in query_results if r.engine == "DuckDB"), None)
            sabot = next((r for r in query_results if r.engine == "Sabot"), None)

            if duckdb and sabot:
                ratio = sabot.execution_time_ms / duckdb.execution_time_ms if duckdb.execution_time_ms > 0 else 0
                if ratio < 1:
                    winner = "Sabot"
                    speedup = 1 / ratio
                else:
                    winner = "DuckDB"
                    speedup = ratio
                print(f"  {'→ Winner:':<23} {winner} ({speedup:.2f}x faster)")
        print()

    # Summary
    print(f"{'='*90}")
    print("SUMMARY")
    print(f"{'='*90}")

    duckdb_wins = 0
    sabot_wins = 0

    for query_name in query_names:
        query_results = [r for r in results if r.query_name == query_name]
        if len(query_results) == 2:
            duckdb = next((r for r in query_results if r.engine == "DuckDB"), None)
            sabot = next((r for r in query_results if r.engine == "Sabot"), None)
            if duckdb and sabot:
                if duckdb.execution_time_ms < sabot.execution_time_ms:
                    duckdb_wins += 1
                else:
                    sabot_wins += 1

    print(f"DuckDB wins: {duckdb_wins}")
    print(f"Sabot wins: {sabot_wins}")

    # Calculate overall geometric mean ratio
    ratios = []
    for query_name in query_names:
        query_results = [r for r in results if r.query_name == query_name]
        if len(query_results) == 2:
            duckdb = next((r for r in query_results if r.engine == "DuckDB"), None)
            sabot = next((r for r in query_results if r.engine == "Sabot"), None)
            if duckdb and sabot and duckdb.execution_time_ms > 0:
                ratios.append(sabot.execution_time_ms / duckdb.execution_time_ms)

    if ratios:
        import math
        geo_mean = math.exp(sum(math.log(r) for r in ratios) / len(ratios))
        if geo_mean > 1:
            print(f"\nOverall: DuckDB is {geo_mean:.2f}x faster (geometric mean)")
        else:
            print(f"\nOverall: Sabot is {1/geo_mean:.2f}x faster (geometric mean)")


def main():
    """Main entry point."""
    import argparse
    parser = argparse.ArgumentParser(description="SQL Engine Comparison Benchmark")
    parser.add_argument("--rows", type=int, default=100_000, help="Number of rows (default: 100000)")
    parser.add_argument("--iterations", type=int, default=5, help="Iterations per query (default: 5)")
    args = parser.parse_args()

    results = run_benchmarks(num_rows=args.rows, iterations=args.iterations)
    print_results(results)


if __name__ == "__main__":
    main()
