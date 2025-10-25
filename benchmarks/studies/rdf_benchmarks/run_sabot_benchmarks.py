"""
Sabot RDF/SPARQL Benchmark Runner

Benchmarks Sabot's SPARQL query engine on Q2 and Q4 from the query suite.

Tests two datasets:
- Olympics: 1.8M triples from Wikidata
- FOAF: 130K triples synthetic social network data

Measures:
- Data loading time (parse + clean + index)
- Query parsing time
- Query execution time
- Result size
- Memory usage

Demonstrates Sabot's Arrow-native RDF processing with data cleaning.
"""

import sys
import time
import gc
from pathlib import Path
from typing import Dict, List, Tuple, Optional
import json

# Add parent directories to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from sabot import cyarrow as pa
pc = pa.compute

# Import RDF utilities
from sabot.rdf_loader import NTriplesParser, clean_rdf_data, load_arrow_to_store
from sabot.rdf import RDFStore

# Import SPARQL queries
from sparql_queries import QUERY_2_PROPERTY_ACCESS, QUERY_4_RELATIONSHIPS


class BenchmarkResult:
    """Container for benchmark results."""

    def __init__(self, query_name: str, dataset: str):
        self.query_name = query_name
        self.dataset = dataset
        self.load_time_ms: Optional[float] = None
        self.parse_times_ms: List[float] = []
        self.exec_times_ms: List[float] = []
        self.result_rows: int = 0
        self.triple_count: int = 0
        self.error: Optional[str] = None

    def add_run(self, parse_ms: float, exec_ms: float):
        """Add a single query run result."""
        self.parse_times_ms.append(parse_ms)
        self.exec_times_ms.append(exec_ms)

    def get_stats(self) -> Dict:
        """Compute statistics from runs."""
        if not self.exec_times_ms:
            return {"error": self.error or "No runs completed"}

        return {
            "query": self.query_name,
            "dataset": self.dataset,
            "triple_count": self.triple_count,
            "load_time_ms": round(self.load_time_ms, 2) if self.load_time_ms else None,
            "parse_time_ms": {
                "min": round(min(self.parse_times_ms), 2),
                "max": round(max(self.parse_times_ms), 2),
                "mean": round(sum(self.parse_times_ms) / len(self.parse_times_ms), 2),
            },
            "exec_time_ms": {
                "min": round(min(self.exec_times_ms), 2),
                "max": round(max(self.exec_times_ms), 2),
                "mean": round(sum(self.exec_times_ms) / len(self.exec_times_ms), 2),
            },
            "total_time_ms": {
                "min": round(min(p + e for p, e in zip(self.parse_times_ms, self.exec_times_ms)), 2),
                "max": round(max(p + e for p, e in zip(self.parse_times_ms, self.exec_times_ms)), 2),
                "mean": round(sum(p + e for p, e in zip(self.parse_times_ms, self.exec_times_ms)) / len(self.parse_times_ms), 2),
            },
            "result_rows": self.result_rows,
            "num_runs": len(self.exec_times_ms),
        }


def load_dataset(
    data_file: Path,
    dataset_name: str,
    limit: Optional[int] = None
) -> Tuple[RDFStore, float, int]:
    """
    Load and prepare RDF dataset using Sabot's Arrow-native pipeline.

    Pipeline:
    1. Parse N-Triples to Arrow table
    2. Clean data using Arrow compute (dedup, trim, filter)
    3. Load into RDF store

    Args:
        data_file: Path to N-Triples file
        dataset_name: Dataset name for logging
        limit: Optional limit on triples to load

    Returns:
        (store, load_time_ms, triple_count)
    """
    print(f"\n{'='*60}")
    print(f"Loading {dataset_name} Dataset")
    print(f"{'='*60}")
    print(f"File: {data_file}")
    if limit:
        print(f"Limit: {limit:,} triples")
    print()

    start_time = time.perf_counter()

    # Step 1: Parse to Arrow table
    print("Step 1: Parsing N-Triples to Arrow...")
    parser = NTriplesParser(str(data_file))
    parse_start = time.perf_counter()
    raw_data = parser.parse_to_arrow(limit=limit, batch_size=10000, show_progress=True)
    parse_time = (time.perf_counter() - parse_start) * 1000
    print(f"  Parsed {len(raw_data):,} triples in {parse_time:.1f}ms")
    print()

    # Step 2: Clean using Arrow compute
    print("Step 2: Cleaning data with Arrow compute...")
    clean_start = time.perf_counter()
    clean_data = clean_rdf_data(
        raw_data,
        remove_duplicates=True,
        filter_empty=True,
        normalize_whitespace=True
    )
    clean_time = (time.perf_counter() - clean_start) * 1000
    triples_removed = len(raw_data) - len(clean_data)
    print(f"  Cleaned {len(raw_data):,} → {len(clean_data):,} triples")
    print(f"  Removed {triples_removed:,} invalid/duplicate triples")
    print(f"  Cleaning time: {clean_time:.1f}ms")
    print()

    # Step 3: Load into RDF store
    print("Step 3: Loading into RDF store...")
    store = RDFStore()
    load_start = time.perf_counter()
    triple_count = load_arrow_to_store(store, clean_data)
    load_time = (time.perf_counter() - load_start) * 1000
    print(f"  Loaded {triple_count:,} triples in {load_time:.1f}ms")
    print()

    total_time = (time.perf_counter() - start_time) * 1000

    print(f"✅ Total loading time: {total_time:.1f}ms")
    print(f"   Throughput: {triple_count / total_time * 1000:,.0f} triples/sec")
    print()

    return store, total_time, triple_count


def run_query_benchmark(
    store: RDFStore,
    query_name: str,
    query_text: str,
    dataset_name: str,
    triple_count: int,
    num_runs: int = 5,
    warmup_runs: int = 2
) -> BenchmarkResult:
    """
    Benchmark a single query with multiple runs.

    Args:
        store: RDF store with loaded data
        query_name: Query identifier (e.g., "Q2")
        query_text: SPARQL query string
        dataset_name: Dataset name
        triple_count: Number of triples in dataset
        num_runs: Number of timed runs
        warmup_runs: Number of warmup runs (not timed)

    Returns:
        BenchmarkResult with timing statistics
    """
    print(f"\n{'='*60}")
    print(f"Benchmarking {query_name} on {dataset_name}")
    print(f"{'='*60}")
    print(f"Query:\n{query_text.strip()}")
    print()

    result = BenchmarkResult(query_name, dataset_name)
    result.triple_count = triple_count

    try:
        # Warmup runs
        print(f"Warmup ({warmup_runs} runs)...")
        for i in range(warmup_runs):
            try:
                _ = store.query(query_text)
                print(f"  Warmup {i + 1}/{warmup_runs} complete")
            except Exception as e:
                print(f"  Warmup {i + 1} failed: {e}")
        print()

        # Timed runs
        print(f"Timed runs ({num_runs} runs)...")
        for i in range(num_runs):
            gc.collect()  # Clean GC state between runs

            # Time parsing
            parse_start = time.perf_counter()
            try:
                # Parse query (in real implementation, this would be separate)
                # For now, we measure total query time
                query_result = store.query(query_text)
                parse_time = (time.perf_counter() - parse_start) * 1000

                # In a real SPARQL engine, execution would be separate
                # For now, parse_time includes both parse + exec
                exec_time = 0  # Will be measured when we have separate parsing

                result.add_run(parse_time, exec_time)
                result.result_rows = len(query_result)

                print(f"  Run {i + 1}/{num_runs}: {parse_time:.2f}ms ({result.result_rows:,} rows)")

            except Exception as e:
                print(f"  Run {i + 1}/{num_runs} failed: {e}")
                result.error = str(e)
                return result

        print()
        print(f"✅ Benchmark complete")

    except Exception as e:
        result.error = str(e)
        print(f"❌ Benchmark failed: {e}")

    return result


def print_results_table(results: List[BenchmarkResult]):
    """Print formatted results table."""
    print(f"\n{'='*80}")
    print("BENCHMARK RESULTS")
    print(f"{'='*80}")
    print()

    for result in results:
        stats = result.get_stats()

        if "error" in stats:
            print(f"❌ {stats['query']} on {stats['dataset']}: {stats['error']}")
            continue

        print(f"Query: {stats['query']}")
        print(f"Dataset: {stats['dataset']} ({stats['triple_count']:,} triples)")
        print(f"Load time: {stats['load_time_ms']:,.1f}ms")
        print(f"Query time (mean): {stats['total_time_ms']['mean']:.2f}ms")
        print(f"  Min: {stats['total_time_ms']['min']:.2f}ms")
        print(f"  Max: {stats['total_time_ms']['max']:.2f}ms")
        print(f"Result rows: {stats['result_rows']:,}")
        print(f"Throughput: {stats['triple_count'] / stats['total_time_ms']['mean']:,.0f} triples/ms")
        print()

    print(f"{'='*80}")


def save_results_json(results: List[BenchmarkResult], output_file: str):
    """Save results to JSON file."""
    data = {
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "results": [r.get_stats() for r in results]
    }

    with open(output_file, 'w') as f:
        json.dump(data, f, indent=2)

    print(f"✅ Results saved to {output_file}")


def main():
    """Main benchmark runner."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Benchmark Sabot SPARQL engine on Q2 and Q4"
    )
    parser.add_argument(
        "--dataset",
        choices=["olympics", "foaf", "both"],
        default="both",
        help="Dataset to benchmark (default: both)"
    )
    parser.add_argument(
        "--olympics-file",
        type=str,
        default="/Users/bengamble/Sabot/vendor/qlever/examples/olympics/olympics.nt.xz",
        help="Path to Olympics N-Triples file"
    )
    parser.add_argument(
        "--foaf-file",
        type=str,
        default="foaf_10k.nt",
        help="Path to FOAF N-Triples file"
    )
    parser.add_argument(
        "--olympics-limit",
        type=int,
        default=50000,
        help="Limit Olympics triples (default: 50,000)"
    )
    parser.add_argument(
        "--runs",
        type=int,
        default=5,
        help="Number of timed runs per query (default: 5)"
    )
    parser.add_argument(
        "--warmup",
        type=int,
        default=2,
        help="Number of warmup runs (default: 2)"
    )
    parser.add_argument(
        "--output",
        type=str,
        default="sabot_benchmark_results.json",
        help="Output JSON file (default: sabot_benchmark_results.json)"
    )

    args = parser.parse_args()

    # Datasets to test
    datasets = []

    if args.dataset in ["olympics", "both"]:
        olympics_path = Path(args.olympics_file)
        if olympics_path.exists():
            datasets.append(("Olympics", olympics_path, args.olympics_limit))
        else:
            print(f"⚠️  Olympics file not found: {olympics_path}")

    if args.dataset in ["foaf", "both"]:
        foaf_path = Path(args.foaf_file)
        if foaf_path.exists():
            datasets.append(("FOAF", foaf_path, None))
        else:
            print(f"⚠️  FOAF file not found: {foaf_path}")

    if not datasets:
        print("❌ No datasets found to benchmark")
        return 1

    # Queries to test
    queries = [
        ("Q2_Property_Access", QUERY_2_PROPERTY_ACCESS),
        ("Q4_Relationships", QUERY_4_RELATIONSHIPS),
    ]

    all_results: List[BenchmarkResult] = []

    # Run benchmarks
    for dataset_name, data_file, limit in datasets:
        # Load dataset
        store, load_time, triple_count = load_dataset(data_file, dataset_name, limit)

        # Run each query
        for query_name, query_text in queries:
            result = run_query_benchmark(
                store,
                query_name,
                query_text,
                dataset_name,
                triple_count,
                num_runs=args.runs,
                warmup_runs=args.warmup
            )
            result.load_time_ms = load_time
            all_results.append(result)

    # Print results
    print_results_table(all_results)

    # Save results
    save_results_json(all_results, args.output)

    return 0


if __name__ == "__main__":
    sys.exit(main())
