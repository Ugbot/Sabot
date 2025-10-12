"""
SPARQL Benchmark Runner for Sabot

Runs SPARQL queries against Sabot's RDF triple store and measures performance.

Metrics:
- Execution time (min, max, avg, median)
- Throughput (queries/sec)
- Result count
- Memory usage (optional)

Output:
- Console output with formatted results
- JSON file with detailed metrics
- CSV file for spreadsheet analysis
"""

import sys
import time
import json
import statistics
from pathlib import Path
from typing import Dict, List, Tuple, Any
from collections import defaultdict

# Add Sabot to path
sabot_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(sabot_root))

from sabot._cython.graph.engine import GraphQueryEngine
from sabot._cython.graph.storage.graph_storage import PyRDFTripleStore

# Import query suite and data loader
from rdf_loader import load_rdf_file, create_synthetic_dataset, load_krown_sample
from sparql_queries import get_all_queries, get_basic_queries, QUERY_SUITE


class BenchmarkRunner:
    """
    SPARQL benchmark runner.

    Runs queries multiple times, collects metrics, and generates reports.
    """

    def __init__(self, triple_store: PyRDFTripleStore, num_runs: int = 5):
        """
        Initialize benchmark runner.

        Args:
            triple_store: RDF triple store to query
            num_runs: Number of times to run each query (for averaging)
        """
        self.triple_store = triple_store
        self.num_runs = num_runs
        self.engine = GraphQueryEngine(state_store=None)
        self.results = defaultdict(list)

        print(f"Initialized benchmark runner:")
        print(f"  Triple store: {self.triple_store.num_triples()} triples")
        print(f"  Terms: {self.triple_store.num_terms()} terms")
        print(f"  Runs per query: {self.num_runs}")

    def run_query(self, query_id: str, query: str) -> Tuple[float, int]:
        """
        Run a single SPARQL query and measure execution time.

        Args:
            query_id: Query identifier (e.g., 'Q1_simple_pattern')
            query: SPARQL query string

        Returns:
            Tuple of (execution_time_ms, result_count)
        """
        start = time.perf_counter()

        try:
            # Execute SPARQL query via GraphQueryEngine
            result = self.engine.query_sparql(query)
            result_count = result.num_rows if hasattr(result, 'num_rows') else len(result)
        except Exception as e:
            print(f"    ERROR: {e}")
            return -1.0, 0

        end = time.perf_counter()
        execution_time_ms = (end - start) * 1000

        return execution_time_ms, result_count

    def benchmark_query(self, query_id: str, query_info: Dict[str, Any]) -> Dict[str, Any]:
        """
        Benchmark a single query with multiple runs.

        Args:
            query_id: Query identifier
            query_info: Query metadata (query string, description, complexity)

        Returns:
            Dict with benchmark metrics
        """
        query = query_info['query']
        description = query_info['description']
        complexity = query_info['complexity']

        print(f"\n{query_id}: {description}")
        print(f"  Complexity: {complexity}")

        # Run query multiple times
        execution_times = []
        result_counts = []

        for run in range(self.num_runs):
            exec_time, result_count = self.run_query(query_id, query)

            if exec_time < 0:
                # Query failed
                return {
                    'query_id': query_id,
                    'description': description,
                    'complexity': complexity,
                    'status': 'failed',
                    'error': 'Query execution failed'
                }

            execution_times.append(exec_time)
            result_counts.append(result_count)

            print(f"    Run {run+1}/{self.num_runs}: {exec_time:.2f}ms ({result_count} results)")

        # Calculate statistics
        avg_time = statistics.mean(execution_times)
        median_time = statistics.median(execution_times)
        min_time = min(execution_times)
        max_time = max(execution_times)
        stddev_time = statistics.stdev(execution_times) if len(execution_times) > 1 else 0

        print(f"  Summary:")
        print(f"    Avg: {avg_time:.2f}ms | Median: {median_time:.2f}ms")
        print(f"    Min: {min_time:.2f}ms | Max: {max_time:.2f}ms | StdDev: {stddev_time:.2f}ms")
        print(f"    Results: {result_counts[0]} rows")

        return {
            'query_id': query_id,
            'description': description,
            'complexity': complexity,
            'status': 'success',
            'num_runs': self.num_runs,
            'execution_times_ms': execution_times,
            'avg_time_ms': avg_time,
            'median_time_ms': median_time,
            'min_time_ms': min_time,
            'max_time_ms': max_time,
            'stddev_time_ms': stddev_time,
            'result_count': result_counts[0],
            'throughput_qps': 1000 / avg_time if avg_time > 0 else 0
        }

    def run_benchmark_suite(self, query_suite: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Run full benchmark suite.

        Args:
            query_suite: Dict of queries to benchmark

        Returns:
            List of benchmark results
        """
        print("=" * 70)
        print(f"Running SPARQL Benchmark Suite ({len(query_suite)} queries)")
        print("=" * 70)

        results = []

        for query_id, query_info in query_suite.items():
            result = self.benchmark_query(query_id, query_info)
            results.append(result)

        return results

    def print_summary(self, results: List[Dict[str, Any]]):
        """Print summary table of benchmark results."""
        print("\n" + "=" * 70)
        print("Benchmark Summary")
        print("=" * 70)

        # Header
        print(f"\n{'Query ID':<25} {'Avg Time':<12} {'Results':<10} {'Status':<10}")
        print("-" * 70)

        # Results
        for result in results:
            query_id = result['query_id']
            status = result['status']

            if status == 'success':
                avg_time = f"{result['avg_time_ms']:.2f}ms"
                result_count = result['result_count']
                print(f"{query_id:<25} {avg_time:<12} {result_count:<10} {status:<10}")
            else:
                print(f"{query_id:<25} {'N/A':<12} {'N/A':<10} {'FAILED':<10}")

        # Overall stats
        successful = [r for r in results if r['status'] == 'success']
        failed = [r for r in results if r['status'] == 'failed']

        print("\n" + "-" * 70)
        print(f"Total queries: {len(results)}")
        print(f"Successful: {len(successful)}")
        print(f"Failed: {len(failed)}")

        if successful:
            avg_times = [r['avg_time_ms'] for r in successful]
            print(f"\nOverall average time: {statistics.mean(avg_times):.2f}ms")
            print(f"Fastest query: {min(avg_times):.2f}ms")
            print(f"Slowest query: {max(avg_times):.2f}ms")

    def save_results(self, results: List[Dict[str, Any]], output_dir: Path):
        """
        Save benchmark results to JSON and CSV files.

        Args:
            results: List of benchmark results
            output_dir: Directory to save results
        """
        output_dir.mkdir(parents=True, exist_ok=True)

        # Save JSON
        json_path = output_dir / 'benchmark_results.json'
        with open(json_path, 'w') as f:
            json.dump(results, f, indent=2)
        print(f"\n✅ Saved JSON results: {json_path}")

        # Save CSV
        csv_path = output_dir / 'benchmark_results.csv'
        with open(csv_path, 'w') as f:
            # Header
            f.write("query_id,description,complexity,status,num_runs,avg_time_ms,median_time_ms,min_time_ms,max_time_ms,stddev_time_ms,result_count,throughput_qps\n")

            # Rows
            for result in results:
                if result['status'] == 'success':
                    f.write(f"{result['query_id']},\"{result['description']}\",{result['complexity']},{result['status']},{result['num_runs']},{result['avg_time_ms']:.2f},{result['median_time_ms']:.2f},{result['min_time_ms']:.2f},{result['max_time_ms']:.2f},{result['stddev_time_ms']:.2f},{result['result_count']},{result['throughput_qps']:.2f}\n")
                else:
                    f.write(f"{result['query_id']},\"{result['description']}\",{result['complexity']},FAILED,0,0,0,0,0,0,0,0\n")

        print(f"✅ Saved CSV results: {csv_path}")


def main():
    """Main benchmark entry point."""
    import argparse

    parser = argparse.ArgumentParser(description='Run SPARQL benchmarks on Sabot')
    parser.add_argument('--data', type=str, default='synthetic',
                        help='Data source: synthetic, krown:<sample_name>, or file:<path>')
    parser.add_argument('--queries', type=str, default='basic',
                        help='Query suite: basic (Q1-Q5) or all (Q1-Q10)')
    parser.add_argument('--runs', type=int, default=5,
                        help='Number of runs per query')
    parser.add_argument('--output', type=str, default='results',
                        help='Output directory for results')

    args = parser.parse_args()

    print("=" * 70)
    print("Sabot SPARQL Benchmark")
    print("=" * 70)

    # Load data
    print(f"\n[1/3] Loading data source: {args.data}")
    if args.data == 'synthetic':
        triple_store = create_synthetic_dataset()
    elif args.data.startswith('krown:'):
        sample_name = args.data.split(':', 1)[1]
        triple_store = load_krown_sample(sample_name)
    elif args.data.startswith('file:'):
        filepath = args.data.split(':', 1)[1]
        triple_store = load_rdf_file(filepath)
    else:
        print(f"ERROR: Unknown data source: {args.data}")
        sys.exit(1)

    # Select query suite
    print(f"\n[2/3] Loading query suite: {args.queries}")
    if args.queries == 'basic':
        query_suite = get_basic_queries()
    elif args.queries == 'all':
        query_suite = get_all_queries()
    else:
        print(f"ERROR: Unknown query suite: {args.queries}")
        sys.exit(1)

    print(f"  Selected {len(query_suite)} queries")

    # Run benchmarks
    print(f"\n[3/3] Running benchmarks ({args.runs} runs per query)")
    runner = BenchmarkRunner(triple_store, num_runs=args.runs)
    results = runner.run_benchmark_suite(query_suite)

    # Print summary
    runner.print_summary(results)

    # Save results
    output_dir = Path(__file__).parent / args.output
    runner.save_results(results, output_dir)

    print("\n" + "=" * 70)
    print("✅ Benchmark complete!")
    print("=" * 70)


if __name__ == '__main__':
    main()
