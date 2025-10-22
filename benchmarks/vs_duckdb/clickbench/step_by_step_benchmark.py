#!/usr/bin/env python3
"""
Step-by-step ClickBench Benchmark: DuckDB vs Sabot

Runs each query 3 times, compares results, and displays progress.
Tracks completed tests and can resume from where it left off.
"""

import asyncio
import time
import json
import sys
from pathlib import Path
from typing import List, Dict, Any, Tuple, Optional, Set
import pandas as pd
import duckdb

# Add parent directory to path for Sabot imports
sys.path.append(str(Path(__file__).parent.parent.parent))

from sabot import cyarrow as ca
from sabot_sql import create_sabot_sql_bridge


class StepByStepBenchmark:
    """Step-by-step benchmark runner for ClickBench queries."""
    
    def __init__(self, parquet_file: str, num_runs: int = 3, progress_file: str = "benchmark_progress.json"):
        """
        Initialize benchmark.
        
        Args:
            parquet_file: Path to hits.parquet
            num_runs: Number of times to run each query (default: 3)
            progress_file: File to track progress (default: benchmark_progress.json)
        """
        self.parquet_file = Path(parquet_file)
        self.num_runs = num_runs
        self.progress_file = Path(progress_file)
        
        # DuckDB connection
        self.duckdb_conn = None
        self.duckdb_hits = None
        
        # Sabot bridge
        self.sabot_bridge = None
        self.sabot_hits = None
        
        # Queries
        self.queries: List[str] = []
        
        # Results
        self.results: List[Dict[str, Any]] = []
        
        # Progress tracking
        self.completed_queries: Set[int] = set()
        self.load_progress()
        
    def load_progress(self):
        """Load progress from file if it exists."""
        if self.progress_file.exists():
            try:
                with open(self.progress_file) as f:
                    data = json.load(f)
                    self.completed_queries = set(data.get('completed_queries', []))
                    self.results = data.get('results', [])
                    print(f"✓ Loaded progress: {len(self.completed_queries)} queries completed")
            except Exception as e:
                print(f"⚠️  Could not load progress file: {e}")
                self.completed_queries = set()
                self.results = []
    
    def save_progress(self):
        """Save current progress to file."""
        try:
            with open(self.progress_file, 'w') as f:
                json.dump({
                    'completed_queries': list(self.completed_queries),
                    'results': self.results,
                    'num_runs': self.num_runs,
                    'parquet_file': str(self.parquet_file)
                }, f, indent=2)
        except Exception as e:
            print(f"⚠️  Could not save progress: {e}")
    
    def get_next_unfinished_query(self) -> Optional[int]:
        """Get the next unfinished query number (1-based)."""
        for i in range(1, len(self.queries) + 1):
            if i not in self.completed_queries:
                return i
        return None
    
    def load_queries(self):
        """Load ClickBench queries from SQL file."""
        queries_file = Path(__file__).parent / "queries.sql"
        if not queries_file.exists():
            raise FileNotFoundError(f"Queries file not found: {queries_file}")
        
        with open(queries_file) as f:
            self.queries = [line.strip() for line in f if line.strip()]
        
        print(f"✓ Loaded {len(self.queries)} queries")
    
    def load_data_duckdb(self):
        """Load data into DuckDB."""
        print(f"\nLoading data into DuckDB from {self.parquet_file}...")
        start_time = time.time()
        
        # Create DuckDB connection
        self.duckdb_conn = duckdb.connect()
        
        # Load parquet with pandas
        self.duckdb_hits = pd.read_parquet(self.parquet_file)
        
        # Fix datetime columns
        if "EventTime" in self.duckdb_hits.columns:
            self.duckdb_hits["EventTime"] = pd.to_datetime(self.duckdb_hits["EventTime"], unit="s")
        if "EventDate" in self.duckdb_hits.columns:
            self.duckdb_hits["EventDate"] = pd.to_datetime(self.duckdb_hits["EventDate"], unit="D")
        
        # Convert object columns to string
        for col in self.duckdb_hits.columns:
            if self.duckdb_hits[col].dtype == "O":
                self.duckdb_hits[col] = self.duckdb_hits[col].astype(str)
        
        load_time = time.time() - start_time
        print(f"✓ DuckDB loaded {len(self.duckdb_hits)} rows in {load_time:.2f}s")
    
    async def load_data_sabot(self):
        """Load data into Sabot."""
        print(f"\nLoading data into Sabot from {self.parquet_file}...")
        start_time = time.time()
        
        # Load parquet with pandas first
        df = pd.read_parquet(self.parquet_file)
        
        # Fix datetime columns
        if "EventTime" in df.columns:
            df["EventTime"] = pd.to_datetime(df["EventTime"], unit="s")
        if "EventDate" in df.columns:
            df["EventDate"] = pd.to_datetime(df["EventDate"], unit="D")
        
        # Convert object columns to string
        for col in df.columns:
            if df[col].dtype == "O":
                df[col] = df[col].astype(str)
        
        # Convert to Arrow then Sabot cyarrow
        import pyarrow as pa
        arrow_table = pa.Table.from_pandas(df)
        self.sabot_hits = ca.Table.from_pyarrow(arrow_table)
        
        # Create bridge and register table
        self.sabot_bridge = create_sabot_sql_bridge()
        self.sabot_bridge.register_table("hits", self.sabot_hits)
        
        load_time = time.time() - start_time
        print(f"✓ Sabot loaded {self.sabot_hits.num_rows} rows in {load_time:.2f}s")
    
    def run_query_duckdb(self, query: str) -> Tuple[float, int, Any]:
        """
        Run a single query on DuckDB.
        
        Returns:
            Tuple of (execution_time, row_count, result)
        """
        start_time = time.time()
        try:
            result = self.duckdb_conn.execute(query).fetchall()
            execution_time = time.time() - start_time
            row_count = len(result)
            return (execution_time, row_count, result)
        except Exception as e:
            execution_time = time.time() - start_time
            print(f"    ✗ DuckDB error: {e}")
            return (execution_time, 0, None)
    
    def run_query_sabot(self, query: str) -> Tuple[float, int, Any]:
        """
        Run a single query on Sabot.
        
        Returns:
            Tuple of (execution_time, row_count, result)
        """
        start_time = time.time()
        try:
            result = self.sabot_bridge.execute_sql(query)
            execution_time = time.time() - start_time
            row_count = result.num_rows if hasattr(result, 'num_rows') else 0
            return (execution_time, row_count, result)
        except Exception as e:
            execution_time = time.time() - start_time
            print(f"    ✗ Sabot error: {e}")
            return (execution_time, 0, None)
    
    async def benchmark_query(self, query_id: int, query: str):
        """
        Benchmark a single query on both systems.
        
        Args:
            query_id: Query number (1-based)
            query: SQL query string
        """
        print(f"\n{'='*80}")
        print(f"Query {query_id}/{len(self.queries)}")
        print(f"{'='*80}")
        print(f"SQL: {query[:100]}{'...' if len(query) > 100 else ''}")
        print()
        
        # DuckDB runs
        print(f"Running on DuckDB ({self.num_runs} times)...")
        duckdb_times = []
        duckdb_rows = 0
        duckdb_result = None
        
        for run in range(self.num_runs):
            exec_time, row_count, result = self.run_query_duckdb(query)
            duckdb_times.append(exec_time)
            duckdb_rows = row_count
            if result is not None:
                duckdb_result = result
            print(f"  Run {run + 1}: {exec_time:.3f}s ({row_count} rows)")
        
        duckdb_avg = sum(duckdb_times) / len(duckdb_times)
        print(f"  Average: {duckdb_avg:.3f}s")
        
        # Sabot runs
        print(f"\nRunning on Sabot ({self.num_runs} times)...")
        sabot_times = []
        sabot_rows = 0
        sabot_result = None
        
        for run in range(self.num_runs):
            exec_time, row_count, result = self.run_query_sabot(query)
            sabot_times.append(exec_time)
            sabot_rows = row_count
            if result is not None:
                sabot_result = result
            print(f"  Run {run + 1}: {exec_time:.3f}s ({row_count} rows)")
        
        sabot_avg = sum(sabot_times) / len(sabot_times)
        print(f"  Average: {sabot_avg:.3f}s")
        
        # Comparison
        print(f"\nComparison:")
        print(f"  DuckDB: {duckdb_avg:.3f}s (avg), {duckdb_rows} rows")
        print(f"  Sabot:  {sabot_avg:.3f}s (avg), {sabot_rows} rows")
        
        if duckdb_avg > 0:
            speedup = duckdb_avg / sabot_avg if sabot_avg > 0 else 0
            if speedup > 1:
                print(f"  Winner: Sabot ({speedup:.2f}x faster)")
            elif speedup < 1 and speedup > 0:
                print(f"  Winner: DuckDB ({1/speedup:.2f}x faster)")
            else:
                print(f"  Winner: Tie")
        
        # Store results
        self.results.append({
            'query_id': query_id,
            'query': query,
            'duckdb': {
                'times': duckdb_times,
                'avg_time': duckdb_avg,
                'min_time': min(duckdb_times),
                'max_time': max(duckdb_times),
                'rows': duckdb_rows
            },
            'sabot': {
                'times': sabot_times,
                'avg_time': sabot_avg,
                'min_time': min(sabot_times),
                'max_time': max(sabot_times),
                'rows': sabot_rows
            },
            'speedup': duckdb_avg / sabot_avg if sabot_avg > 0 else 0
        })
        
        # Mark as completed and save progress
        self.completed_queries.add(query_id)
        self.save_progress()
    
    async def run_all_queries(self):
        """Run all queries."""
        print(f"\n{'='*80}")
        print(f"Starting benchmark: {len(self.queries)} queries, {self.num_runs} runs each")
        print(f"{'='*80}")
        
        for idx, query in enumerate(self.queries, 1):
            await self.benchmark_query(idx, query)
            
            # Print progress
            progress = (idx / len(self.queries)) * 100
            print(f"\nProgress: {idx}/{len(self.queries)} ({progress:.1f}%)")
    
    def print_summary(self):
        """Print benchmark summary."""
        print(f"\n\n{'='*80}")
        print(f"BENCHMARK SUMMARY")
        print(f"{'='*80}")
        
        duckdb_total = sum(r['duckdb']['avg_time'] for r in self.results)
        sabot_total = sum(r['sabot']['avg_time'] for r in self.results)
        
        duckdb_wins = sum(1 for r in self.results if r['speedup'] < 1)
        sabot_wins = sum(1 for r in self.results if r['speedup'] > 1)
        ties = len(self.results) - duckdb_wins - sabot_wins
        
        print(f"\nTotal Queries: {len(self.results)}")
        print(f"Runs per query: {self.num_runs}")
        print()
        print(f"DuckDB Total Time: {duckdb_total:.2f}s")
        print(f"Sabot Total Time:  {sabot_total:.2f}s")
        print()
        print(f"Wins:")
        print(f"  DuckDB: {duckdb_wins}")
        print(f"  Sabot:  {sabot_wins}")
        print(f"  Ties:   {ties}")
        print()
        
        if sabot_total > 0:
            overall_speedup = duckdb_total / sabot_total
            if overall_speedup > 1:
                print(f"Overall: Sabot is {overall_speedup:.2f}x faster")
            else:
                print(f"Overall: DuckDB is {1/overall_speedup:.2f}x faster")
        
        # Top 5 queries where Sabot wins
        print(f"\nTop 5 queries where Sabot wins:")
        sabot_best = sorted([r for r in self.results if r['speedup'] > 1], 
                           key=lambda x: x['speedup'], reverse=True)[:5]
        for r in sabot_best:
            print(f"  Q{r['query_id']}: {r['speedup']:.2f}x faster - {r['query'][:60]}...")
        
        # Top 5 queries where DuckDB wins
        print(f"\nTop 5 queries where DuckDB wins:")
        duckdb_best = sorted([r for r in self.results if r['speedup'] < 1], 
                            key=lambda x: x['speedup'])[:5]
        for r in duckdb_best:
            print(f"  Q{r['query_id']}: {1/r['speedup']:.2f}x faster - {r['query'][:60]}...")
        
        print(f"\n{'='*80}")
    
    def save_results(self, output_file: str):
        """Save results to JSON file."""
        output_path = Path(output_file)
        
        with open(output_path, 'w') as f:
            json.dump({
                'num_queries': len(self.results),
                'num_runs': self.num_runs,
                'parquet_file': str(self.parquet_file),
                'results': self.results
            }, f, indent=2)
        
        print(f"\n✓ Results saved to {output_path}")
    
    async def cleanup(self):
        """Clean up resources."""
        if self.duckdb_conn:
            self.duckdb_conn.close()
        print("\n✓ Cleanup complete")


async def main():
    """Main function."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Step-by-step ClickBench: DuckDB vs Sabot",
        epilog="""
Examples:
  # Run next unfinished test
  python step_by_step_benchmark.py
  
  # Run test 5
  python step_by_step_benchmark.py 5
  
  # Run tests 5-10
  python step_by_step_benchmark.py 5 10
  
  # Reset and run all tests
  python step_by_step_benchmark.py --reset
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("query_start", type=int, nargs='?',
                       help="Query number to run (or start of range)")
    parser.add_argument("query_end", type=int, nargs='?',
                       help="End of query range (inclusive)")
    parser.add_argument("--parquet-file", default="hits.parquet", 
                       help="Path to hits.parquet file")
    parser.add_argument("--num-runs", type=int, default=3,
                       help="Number of runs per query (default: 3)")
    parser.add_argument("--output-file", default="comparison_results.json",
                       help="Output file for results (JSON)")
    parser.add_argument("--progress-file", default="benchmark_progress.json",
                       help="Progress tracking file (default: benchmark_progress.json)")
    parser.add_argument("--reset", action="store_true",
                       help="Reset progress and start from beginning")
    parser.add_argument("--status", action="store_true",
                       help="Show status and exit")
    
    args = parser.parse_args()
    
    # Create benchmark
    benchmark = StepByStepBenchmark(
        parquet_file=args.parquet_file,
        num_runs=args.num_runs,
        progress_file=args.progress_file
    )
    
    try:
        # Load queries
        benchmark.load_queries()
        
        # Reset if requested
        if args.reset:
            print("Resetting progress...")
            benchmark.completed_queries = set()
            benchmark.results = []
            benchmark.save_progress()
            if Path(args.progress_file).exists():
                Path(args.progress_file).unlink()
            print("✓ Progress reset")
        
        # Show status and exit
        if args.status:
            total = len(benchmark.queries)
            completed = len(benchmark.completed_queries)
            remaining = total - completed
            print(f"\n{'='*60}")
            print(f"Benchmark Status")
            print(f"{'='*60}")
            print(f"Total queries: {total}")
            print(f"Completed: {completed}")
            print(f"Remaining: {remaining}")
            print(f"Progress: {completed/total*100:.1f}%")
            
            if remaining > 0:
                next_query = benchmark.get_next_unfinished_query()
                print(f"\nNext unfinished query: {next_query}")
                print(f"Completed queries: {sorted(benchmark.completed_queries)}")
            else:
                print("\n✓ All queries completed!")
            
            print(f"{'='*60}")
            return
        
        # Determine which queries to run
        queries_to_run = []
        
        if args.query_start is not None:
            # Specific query or range specified
            if args.query_end is not None:
                # Range: start to end (inclusive)
                start = args.query_start
                end = args.query_end
                queries_to_run = list(range(start, end + 1))
                print(f"Running queries {start} to {end}")
            else:
                # Single query
                queries_to_run = [args.query_start]
                print(f"Running query {args.query_start}")
        else:
            # No arguments - run next unfinished query
            next_query = benchmark.get_next_unfinished_query()
            if next_query:
                queries_to_run = [next_query]
                print(f"Running next unfinished query: {next_query}")
            else:
                print("✓ All queries already completed!")
                print("Use --reset to start over, or specify a query number to re-run.")
                return
        
        # Validate query numbers
        queries_to_run = [q for q in queries_to_run if 1 <= q <= len(benchmark.queries)]
        
        if not queries_to_run:
            print("✗ No valid queries to run")
            return
        
        # Load data
        benchmark.load_data_duckdb()
        await benchmark.load_data_sabot()
        
        # Run selected queries
        for query_num in queries_to_run:
            query = benchmark.queries[query_num - 1]
            await benchmark.benchmark_query(query_num, query)
        
        # Print summary for completed queries
        if benchmark.results:
            benchmark.print_summary()
        
        # Save final results
        benchmark.save_results(args.output_file)
        
    except KeyboardInterrupt:
        print("\n\n⚠️  Benchmark interrupted by user")
        if benchmark.results:
            print("Saving partial results...")
            benchmark.print_summary()
            benchmark.save_results(args.output_file.replace('.json', '_partial.json'))
    
    except Exception as e:
        print(f"\n✗ Benchmark failed: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        await benchmark.cleanup()


if __name__ == "__main__":
    asyncio.run(main())


