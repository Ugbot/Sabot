#!/usr/bin/env python3
"""
Sabot Graph Benchmark - Kuzu Study Comparison

Runs the 9 Cypher queries from the Kuzu benchmark study on Sabot's
graph query engine and measures performance.

Usage:
    python run_benchmark.py [--iterations N] [--warmup N]

Expected Kuzu performance (M3 MacBook Pro, 100K nodes, 2.4M edges):
    Query 1: 0.1603s
    Query 2: 0.2498s
    Query 3: 0.0085s
    Query 4: 0.0147s
    Query 5: 0.0134s
    Query 6: 0.0362s
    Query 7: 0.0151s
    Query 8: 0.0086s (2-hop: 58M paths)
    Query 9: 0.0955s (filtered: 45M paths)
"""
import sys
import os
import time
import argparse
from pathlib import Path

# Add Sabot to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

# Set library path for Arrow
os.environ['DYLD_LIBRARY_PATH'] = '/Users/bengamble/Sabot/vendor/arrow/cpp/build/install/lib'

from data_loader import load_all_data
import queries
import queries_vectorized


class BenchmarkRunner:
    """Run Kuzu benchmark queries on Sabot."""

    def __init__(self, data_dir: Path):
        """Initialize with data directory."""
        self.data_dir = data_dir
        self.data = None

    def load_data(self):
        """Load all graph data."""
        print("="*70)
        print("LOADING DATA")
        print("="*70)
        start = time.perf_counter()
        self.data = load_all_data(self.data_dir)
        elapsed = time.perf_counter() - start
        print(f"\n✅ Data loaded in {elapsed:.3f}s")
        print()

    def run_query(self, query_num: int, query_func, *args, iterations: int = 5, warmup: int = 2):
        """
        Run a single query with warmup and timing.

        Args:
            query_num: Query number (1-9)
            query_func: Query function to execute
            *args: Arguments to pass to query function
            iterations: Number of timed iterations
            warmup: Number of warmup iterations

        Returns:
            Tuple of (result_table, avg_time_ms, min_time_ms, max_time_ms)
        """
        # Warmup
        for _ in range(warmup):
            query_func(*args)

        # Timed iterations
        times = []
        result = None
        for _ in range(iterations):
            start = time.perf_counter()
            result = query_func(*args)
            elapsed = (time.perf_counter() - start) * 1000  # ms
            times.append(elapsed)

        avg_time = sum(times) / len(times)
        min_time = min(times)
        max_time = max(times)

        return result, avg_time, min_time, max_time

    def run_all_queries(self, iterations: int = 5, warmup: int = 2):
        """Run all 9 benchmark queries."""
        if self.data is None:
            raise ValueError("Data not loaded. Call load_data() first.")

        results = []

        print("="*70)
        print("RUNNING BENCHMARK QUERIES")
        print("="*70)
        print(f"Iterations: {iterations}, Warmup: {warmup}")
        print()

        # Query 1
        print("[1/9] Query 1: Top 3 most-followed persons")
        result, avg_t, min_t, max_t = self.run_query(
            1, queries.query1_top_followers,
            self.data['person_nodes'], self.data['follows_edges'],
            iterations=iterations, warmup=warmup
        )
        print(f"  Time: {avg_t:.2f}ms (min: {min_t:.2f}ms, max: {max_t:.2f}ms)")
        print(f"  Result: {result.to_pydict()}")
        results.append(('Query 1', avg_t))
        print()

        # Query 2
        print("[2/9] Query 2: City where most-followed person lives")
        result, avg_t, min_t, max_t = self.run_query(
            2, queries.query2_most_followed_city,
            self.data['person_nodes'], self.data['follows_edges'],
            self.data['lives_in_edges'], self.data['city_nodes'],
            iterations=iterations, warmup=warmup
        )
        print(f"  Time: {avg_t:.2f}ms (min: {min_t:.2f}ms, max: {max_t:.2f}ms)")
        print(f"  Result: {result.to_pydict()}")
        results.append(('Query 2', avg_t))
        print()

        # Query 3 - VECTORIZED
        print("[3/9] Query 3: 5 cities with lowest avg age in US (VECTORIZED)")
        result, avg_t, min_t, max_t = self.run_query(
            3, queries_vectorized.query3_lowest_avg_age_cities,
            self.data['person_nodes'], self.data['lives_in_edges'],
            self.data['city_nodes'], "United States",
            iterations=iterations, warmup=warmup
        )
        print(f"  Time: {avg_t:.2f}ms (min: {min_t:.2f}ms, max: {max_t:.2f}ms)")
        print(f"  Result: {result.to_pydict()}")
        results.append(('Query 3', avg_t))
        print()

        # Query 4 - VECTORIZED
        print("[4/9] Query 4: Persons aged 30-40 by country (VECTORIZED)")
        result, avg_t, min_t, max_t = self.run_query(
            4, queries_vectorized.query4_persons_by_age_country,
            self.data['person_nodes'], self.data['lives_in_edges'],
            self.data['city_nodes'], 30, 40,
            iterations=iterations, warmup=warmup
        )
        print(f"  Time: {avg_t:.2f}ms (min: {min_t:.2f}ms, max: {max_t:.2f}ms)")
        print(f"  Result: {result.to_pydict()}")
        results.append(('Query 4', avg_t))
        print()

        # Query 5
        print("[5/9] Query 5: Men in London UK interested in fine dining")
        result, avg_t, min_t, max_t = self.run_query(
            5, queries.query5_interest_city_gender,
            self.data['person_nodes'], self.data['has_interest_edges'],
            self.data['lives_in_edges'], self.data['city_nodes'],
            self.data['interest_nodes'], "male", "London", "United Kingdom", "fine dining",
            iterations=iterations, warmup=warmup
        )
        print(f"  Time: {avg_t:.2f}ms (min: {min_t:.2f}ms, max: {max_t:.2f}ms)")
        print(f"  Result: {result.to_pydict()}")
        results.append(('Query 5', avg_t))
        print()

        # Query 6 - VECTORIZED
        print("[6/9] Query 6: Top 5 cities with women interested in tennis (VECTORIZED)")
        result, avg_t, min_t, max_t = self.run_query(
            6, queries_vectorized.query6_city_with_most_interest_gender,
            self.data['person_nodes'], self.data['has_interest_edges'],
            self.data['lives_in_edges'], self.data['city_nodes'],
            self.data['interest_nodes'], "female", "tennis",
            iterations=iterations, warmup=warmup
        )
        print(f"  Time: {avg_t:.2f}ms (min: {min_t:.2f}ms, max: {max_t:.2f}ms)")
        print(f"  Result: {result.to_pydict()}")
        results.append(('Query 6', avg_t))
        print()

        # Query 7
        print("[7/9] Query 7: US state with most 23-30yo interested in photography")
        result, avg_t, min_t, max_t = self.run_query(
            7, queries.query7_state_age_interest,
            self.data['person_nodes'], self.data['lives_in_edges'],
            self.data['has_interest_edges'], self.data['city_nodes'],
            self.data['state_nodes'], self.data['city_in_edges'],
            self.data['interest_nodes'], "United States", 23, 30, "photography",
            iterations=iterations, warmup=warmup
        )
        print(f"  Time: {avg_t:.2f}ms (min: {min_t:.2f}ms, max: {max_t:.2f}ms)")
        print(f"  Result: {result.to_pydict()}")
        results.append(('Query 7', avg_t))
        print()

        # Query 8
        print("[8/9] Query 8: Count of 2-hop paths")
        result, avg_t, min_t, max_t = self.run_query(
            8, queries.query8_second_degree_paths,
            self.data['follows_edges'],
            iterations=iterations, warmup=warmup
        )
        print(f"  Time: {avg_t:.2f}ms (min: {min_t:.2f}ms, max: {max_t:.2f}ms)")
        num_paths = result.column('numPaths')[0].as_py()
        print(f"  Result: {num_paths:,} 2-hop paths")
        results.append(('Query 8', avg_t))
        print()

        # Query 9 - VECTORIZED
        print("[9/9] Query 9: Filtered 2-hop paths (b.age<50, c.age>25) (VECTORIZED)")
        result, avg_t, min_t, max_t = self.run_query(
            9, queries_vectorized.query9_filtered_paths,
            self.data['person_nodes'], self.data['follows_edges'],
            50, 25,
            iterations=iterations, warmup=warmup
        )
        print(f"  Time: {avg_t:.2f}ms (min: {min_t:.2f}ms, max: {max_t:.2f}ms)")
        num_paths = result.column('numPaths')[0].as_py()
        print(f"  Result: {num_paths:,} filtered paths")
        results.append(('Query 9', avg_t))
        print()

        return results

    def print_summary(self, results):
        """Print performance summary and comparison to Kuzu."""
        print("="*70)
        print("PERFORMANCE SUMMARY")
        print("="*70)
        print()

        # Kuzu baseline (from their README)
        kuzu_times = {
            'Query 1': 160.3,
            'Query 2': 249.8,
            'Query 3': 8.5,
            'Query 4': 14.7,
            'Query 5': 13.4,
            'Query 6': 36.2,
            'Query 7': 15.1,
            'Query 8': 8.6,
            'Query 9': 95.5,
        }

        print(f"{'Query':<12} {'Sabot (ms)':>12} {'Kuzu (ms)':>12} {'Speedup':>10}")
        print("-"*70)

        total_sabot = 0
        total_kuzu = 0

        for query_name, sabot_time in results:
            kuzu_time = kuzu_times[query_name]
            speedup = kuzu_time / sabot_time if sabot_time > 0 else 0

            total_sabot += sabot_time
            total_kuzu += kuzu_time

            speedup_str = f"{speedup:.2f}x" if speedup > 1 else f"{1/speedup:.2f}x slower"
            print(f"{query_name:<12} {sabot_time:>12.2f} {kuzu_time:>12.2f} {speedup_str:>10}")

        print("-"*70)
        overall_speedup = total_kuzu / total_sabot if total_sabot > 0 else 0
        speedup_str = f"{overall_speedup:.2f}x" if overall_speedup > 1 else f"{1/overall_speedup:.2f}x slower"
        print(f"{'Total':<12} {total_sabot:>12.2f} {total_kuzu:>12.2f} {speedup_str:>10}")
        print()

        print("Notes:")
        print("  - Kuzu times from M3 MacBook Pro (36GB RAM)")
        print("  - Kuzu uses multi-threaded execution")
        print("  - Sabot currently single-agent mode")
        print("  - Query 8-9 use Cython pattern matching (morsel parallelism)")
        print()


def main():
    parser = argparse.ArgumentParser(description="Run Sabot graph benchmark")
    parser.add_argument('--iterations', type=int, default=5,
                        help='Number of timed iterations per query (default: 5)')
    parser.add_argument('--warmup', type=int, default=2,
                        help='Number of warmup iterations (default: 2)')
    args = parser.parse_args()

    # Data directory
    data_dir = Path(__file__).parent / "reference" / "data" / "output"

    if not data_dir.exists():
        print(f"❌ Error: Data directory not found: {data_dir}")
        print("   Run: cd reference/data && bash ../generate_data.sh 100000")
        return 1

    # Run benchmark
    runner = BenchmarkRunner(data_dir)
    runner.load_data()
    results = runner.run_all_queries(iterations=args.iterations, warmup=args.warmup)
    runner.print_summary(results)

    print("✅ Benchmark complete!")
    return 0


if __name__ == "__main__":
    sys.exit(main())
