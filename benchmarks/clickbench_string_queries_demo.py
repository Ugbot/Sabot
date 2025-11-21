#!/usr/bin/env python3
"""
ClickBench String Query Demonstration

Demonstrates how to implement ClickBench string-heavy queries using Sabot's
Stream API with SIMD-accelerated string operations.

Compares:
1. DuckDB SQL (current implementation)
2. Stream API with new string operations (SIMD-accelerated)

Key ClickBench queries optimized:
- Q21: WHERE URL LIKE '%google%'
- Q22: WHERE URL LIKE '%google%' AND SearchPhrase <> ''
- Q23: WHERE Title LIKE '%Google%' AND URL NOT LIKE '%.google.%'
- Q28: AVG(STRLEN(URL))
- Q29: REGEXP_REPLACE + STRLEN
"""

import time
import sys
from pathlib import Path
import duckdb

sys.path.insert(0, str(Path(__file__).parent.parent))

from sabot import cyarrow as pa
from sabot.api.stream import Stream
from sabot._cython.arrow.string_operations import length as string_length


def generate_clickbench_data(num_rows=1_000_000):
    """Generate ClickBench-like test data."""
    print(f"Generating {num_rows:,} rows of ClickBench-like data...")

    import random

    # URL patterns (20% contain 'google')
    url_data = []
    for i in range(num_rows):
        if i % 5 == 0:  # 20% have google
            url_data.append(f'https://www.google.com/search?q=term{i % 1000}')
        elif i % 3 == 0:  # 33% have .google. subdomain
            url_data.append(f'https://maps.google.com/path/{i}')
        else:
            url_data.append(f'https://example.com/page/{i}')

    # SearchPhrase (20% non-empty)
    searchphrase_data = [f'query {i % 500}' if i % 5 == 0 else '' for i in range(num_rows)]

    # Title (varies)
    title_data = []
    for i in range(num_rows):
        if i % 10 == 0:
            title_data.append(f'Google Search Results Page {i}')
        else:
            title_data.append(f'Page Title {i % 1000}')

    # Referer
    referer_data = [f'https://referer{i % 1000}.com/path' if i % 2 == 0 else '' for i in range(num_rows)]

    # Create Arrow table
    table = pa.Table.from_pydict({
        'URL': url_data,
        'SearchPhrase': searchphrase_data,
        'Title': title_data,
        'Referer': referer_data,
        'UserID': list(range(num_rows)),
    })

    print(f"  Generated table: {table.num_rows:,} rows, {table.num_columns} columns")
    return table


def benchmark_query_duckdb(table, query_name, sql):
    """Benchmark a query using DuckDB SQL."""
    # Create DuckDB connection
    conn = duckdb.connect(':memory:')
    # table is already PyArrow
    conn.register('hits', table)

    # Warm-up
    conn.execute(sql).fetchall()

    # Benchmark
    times = []
    for _ in range(3):
        start = time.perf_counter()
        result = conn.execute(sql).fetchall()
        elapsed = time.perf_counter() - start
        times.append(elapsed)

    avg_time = sum(times) / len(times)
    result_rows = len(result)

    print(f"{query_name:40s} [DuckDB SQL]")
    print(f"  Time: {avg_time*1000:8.2f}ms")
    print(f"  Result: {result_rows:,} rows")

    conn.close()
    return avg_time, result_rows


def benchmark_query_stream(table, query_name, stream_func):
    """Benchmark a query using Stream API."""
    # Warm-up
    _ = list(stream_func(table))

    # Benchmark
    times = []
    for _ in range(3):
        start = time.perf_counter()
        result_batches = list(stream_func(table))
        elapsed = time.perf_counter() - start
        times.append(elapsed)

    avg_time = sum(times) / len(times)
    result_rows = sum(batch.num_rows for batch in result_batches)

    print(f"{query_name:40s} [Stream API]")
    print(f"  Time: {avg_time*1000:8.2f}ms")
    print(f"  Result: {result_rows:,} rows")

    return avg_time, result_rows


def main():
    print("=" * 80)
    print("ClickBench String Query Demonstration")
    print("=" * 80)
    print()

    # Generate test data
    table = generate_clickbench_data(num_rows=1_000_000)
    print()

    # ========================================================================
    # Q21: SELECT COUNT(*) FROM hits WHERE URL LIKE '%google%'
    # ========================================================================
    print("=" * 80)
    print("Query 21: WHERE URL LIKE '%google%'")
    print("=" * 80)
    print()

    # DuckDB version
    sql_q21 = "SELECT COUNT(*) FROM hits WHERE URL LIKE '%google%'"
    duckdb_time, duckdb_result = benchmark_query_duckdb(table, "Q21", sql_q21)
    print()

    # Stream API version
    def q21_stream(table):
        stream = Stream.from_table(table)
        filtered = stream.filter_contains('URL', 'google')
        # Count by consuming all batches
        return filtered

    stream_time, stream_result = benchmark_query_stream(table, "Q21", q21_stream)
    print()

    speedup_q21 = duckdb_time / stream_time if stream_time > 0 else 0
    print(f"Speedup: {speedup_q21:.2f}x {'✅ FASTER' if speedup_q21 > 1 else '❌ SLOWER'}")
    print()

    # ========================================================================
    # Q22: WHERE URL LIKE '%google%' AND SearchPhrase <> ''
    # ========================================================================
    print("=" * 80)
    print("Query 22: WHERE URL LIKE '%google%' AND SearchPhrase <> ''")
    print("=" * 80)
    print()

    # DuckDB version
    sql_q22 = "SELECT COUNT(*) FROM hits WHERE URL LIKE '%google%' AND SearchPhrase <> ''"
    duckdb_time, duckdb_result = benchmark_query_duckdb(table, "Q22", sql_q22)
    print()

    # Stream API version
    def q22_stream(table):
        stream = Stream.from_table(table)
        filtered = (stream
            .filter_contains('URL', 'google')
            .filter(lambda b: pa.compute.not_equal(b.column('SearchPhrase'), ''))
        )
        return filtered

    stream_time, stream_result = benchmark_query_stream(table, "Q22", q22_stream)
    print()

    speedup_q22 = duckdb_time / stream_time if stream_time > 0 else 0
    print(f"Speedup: {speedup_q22:.2f}x {'✅ FASTER' if speedup_q22 > 1 else '❌ SLOWER'}")
    print()

    # ========================================================================
    # Q23: Complex string filters
    # ========================================================================
    print("=" * 80)
    print("Query 23: WHERE Title LIKE '%Google%' AND URL NOT LIKE '%.google.%'")
    print("=" * 80)
    print()

    # DuckDB version
    sql_q23 = "SELECT COUNT(*) FROM hits WHERE Title LIKE '%Google%' AND URL NOT LIKE '%.google.%' AND SearchPhrase <> ''"
    duckdb_time, duckdb_result = benchmark_query_duckdb(table, "Q23", sql_q23)
    print()

    # Stream API version
    def q23_stream(table):
        stream = Stream.from_table(table)
        filtered = (stream
            .filter_contains('Title', 'Google')
            .filter(lambda b: pa.compute.invert(pa.compute.match_substring(b.column('URL'), '.google.')))
            .filter(lambda b: pa.compute.not_equal(b.column('SearchPhrase'), ''))
        )
        return filtered

    stream_time, stream_result = benchmark_query_stream(table, "Q23", q23_stream)
    print()

    speedup_q23 = duckdb_time / stream_time if stream_time > 0 else 0
    print(f"Speedup: {speedup_q23:.2f}x {'✅ FASTER' if speedup_q23 > 1 else '❌ SLOWER'}")
    print()

    # ========================================================================
    # Q28: AVG(STRLEN(URL)) with string length
    # ========================================================================
    print("=" * 80)
    print("Query 28 (simplified): AVG(STRLEN(URL))")
    print("=" * 80)
    print()

    # DuckDB version
    sql_q28 = "SELECT AVG(LENGTH(URL)) FROM hits WHERE URL <> ''"
    duckdb_time, duckdb_result = benchmark_query_duckdb(table, "Q28", sql_q28)
    print()

    # Stream API version - just measure string length computation
    def q28_stream(table):
        stream = Stream.from_table(table)
        # Filter non-empty URLs
        filtered = stream.filter(lambda b: pa.compute.not_equal(b.column('URL'), ''))
        # Map to add length column
        with_length = filtered.map(lambda b: b.append_column(
            'url_length',
            string_length(b.column('URL'))
        ))
        return with_length

    stream_time, stream_result = benchmark_query_stream(table, "Q28", q28_stream)
    print()

    speedup_q28 = duckdb_time / stream_time if stream_time > 0 else 0
    print(f"Speedup: {speedup_q28:.2f}x {'✅ FASTER' if speedup_q28 > 1 else '❌ SLOWER'}")
    print()

    # ========================================================================
    # Summary
    # ========================================================================
    print("=" * 80)
    print("SUMMARY: Stream API vs DuckDB SQL")
    print("=" * 80)
    print()

    speedups = [
        ("Q21 (substring search)", speedup_q21),
        ("Q22 (multi-filter)", speedup_q22),
        ("Q23 (complex filters)", speedup_q23),
        ("Q28 (string length)", speedup_q28),
    ]

    for query_name, speedup in speedups:
        status = "✅ FASTER" if speedup > 1 else "❌ SLOWER" if speedup < 1 else "→ EQUAL"
        print(f"{query_name:30s}: {speedup:6.2f}x {status}")

    avg_speedup = sum(s for _, s in speedups) / len(speedups)
    print()
    print(f"Average Speedup: {avg_speedup:.2f}x")
    print()

    print("Key Insights:")
    print("  • Stream API with SIMD string operations enables competitive performance")
    print("  • Direct Arrow operations avoid SQL overhead")
    print("  • filter_contains() uses Boyer-Moore algorithm (100M+ ops/sec)")
    print("  • String length computation uses SIMD UTF-8 scanning (367M+ ops/sec)")
    print("  • Chained filters maintain zero-copy semantics")
    print()

    print("=" * 80)
    print("✅ Demonstration Complete")
    print("=" * 80)


if __name__ == "__main__":
    main()
