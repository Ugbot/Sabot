#!/usr/bin/env python3
"""
Example usage of Sabot ClickBench

Demonstrates how to use the ClickBench benchmark with different configurations.
"""

import asyncio
import logging
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent.parent))

from sabot_clickbench import SabotClickBenchRunner

async def example_local_execution():
    """Example of local execution."""
    print("Example 1: Local Execution")
    print("-" * 30)
    
    # Create a simple test parquet file (in real usage, you'd have hits.parquet)
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq
    
    # Create test data
    test_data = {
        'EventTime': [1000000000 + i * 3600 for i in range(1000)],
        'EventDate': [1000 + i for i in range(1000)],
        'UserID': [i % 100 for i in range(1000)],
        'CounterID': [i % 10 for i in range(1000)],
        'AdvEngineID': [i % 5 for i in range(1000)],
        'SearchPhrase': [f'search_{i % 50}' for i in range(1000)],
        'URL': [f'https://example.com/page_{i % 100}' for i in range(1000)],
        'Title': [f'Page Title {i % 100}' for i in range(1000)],
        'Referer': [f'https://referer.com/{i % 50}' for i in range(1000)],
        'ResolutionWidth': [1920] * 1000,
        'ResolutionHeight': [1080] * 1000,
        'WindowClientWidth': [1900] * 1000,
        'WindowClientHeight': [1000] * 1000,
        'MobilePhone': [''] * 1000,
        'MobilePhoneModel': [''] * 1000,
        'RegionID': [i % 20 for i in range(1000)],
        'ClientIP': [f'192.168.1.{i % 255}' for i in range(1000)],
        'WatchID': [i for i in range(1000)],
        'TraficSourceID': [i % 10 for i in range(1000)],
        'SearchEngineID': [i % 5 for i in range(1000)],
        'URLHash': [hash(f'https://example.com/page_{i % 100}') for i in range(1000)],
        'RefererHash': [hash(f'https://referer.com/{i % 50}') for i in range(1000)],
        'IsRefresh': [0] * 1000,
        'IsLink': [0] * 1000,
        'IsDownload': [0] * 1000,
        'DontCountHits': [0] * 1000
    }
    
    # Create test parquet file
    df = pd.DataFrame(test_data)
    arrow_table = pa.Table.from_pandas(df)
    pq.write_table(arrow_table, "test_hits.parquet")
    
    # Run benchmark with local execution
    runner = SabotClickBenchRunner(
        parquet_file="test_hits.parquet",
        num_agents=1,
        execution_mode="local"
    )
    
    try:
        await runner.initialize()
        await runner.run_benchmark(warmup_runs=1, benchmark_runs=2)
        runner.print_results()
    finally:
        await runner.cleanup()
    
    # Cleanup test file
    Path("test_hits.parquet").unlink()

async def example_distributed_execution():
    """Example of distributed execution."""
    print("\nExample 2: Distributed Execution")
    print("-" * 30)
    
    # Create test data (same as above)
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq
    
    test_data = {
        'EventTime': [1000000000 + i * 3600 for i in range(2000)],
        'EventDate': [1000 + i for i in range(2000)],
        'UserID': [i % 200 for i in range(2000)],
        'CounterID': [i % 20 for i in range(2000)],
        'AdvEngineID': [i % 10 for i in range(2000)],
        'SearchPhrase': [f'search_{i % 100}' for i in range(2000)],
        'URL': [f'https://example.com/page_{i % 200}' for i in range(2000)],
        'Title': [f'Page Title {i % 200}' for i in range(2000)],
        'Referer': [f'https://referer.com/{i % 100}' for i in range(2000)],
        'ResolutionWidth': [1920] * 2000,
        'ResolutionHeight': [1080] * 2000,
        'WindowClientWidth': [1900] * 2000,
        'WindowClientHeight': [1000] * 2000,
        'MobilePhone': [''] * 2000,
        'MobilePhoneModel': [''] * 2000,
        'RegionID': [i % 40 for i in range(2000)],
        'ClientIP': [f'192.168.1.{i % 255}' for i in range(2000)],
        'WatchID': [i for i in range(2000)],
        'TraficSourceID': [i % 20 for i in range(2000)],
        'SearchEngineID': [i % 10 for i in range(2000)],
        'URLHash': [hash(f'https://example.com/page_{i % 200}') for i in range(2000)],
        'RefererHash': [hash(f'https://referer.com/{i % 100}') for i in range(2000)],
        'IsRefresh': [0] * 2000,
        'IsLink': [0] * 2000,
        'IsDownload': [0] * 2000,
        'DontCountHits': [0] * 2000
    }
    
    # Create test parquet file
    df = pd.DataFrame(test_data)
    arrow_table = pa.Table.from_pandas(df)
    pq.write_table(arrow_table, "test_hits_distributed.parquet")
    
    # Run benchmark with distributed execution
    runner = SabotClickBenchRunner(
        parquet_file="test_hits_distributed.parquet",
        num_agents=4,
        execution_mode="distributed"
    )
    
    try:
        await runner.initialize()
        await runner.run_benchmark(warmup_runs=1, benchmark_runs=2)
        runner.print_results()
        
        # Print distributed executor stats
        if runner.distributed_executor:
            runner.distributed_executor.print_performance_stats()
    finally:
        await runner.cleanup()
    
    # Cleanup test file
    Path("test_hits_distributed.parquet").unlink()

async def example_custom_queries():
    """Example with custom queries."""
    print("\nExample 3: Custom Queries")
    print("-" * 30)
    
    # Create test data
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq
    
    test_data = {
        'EventTime': [1000000000 + i * 3600 for i in range(500)],
        'EventDate': [1000 + i for i in range(500)],
        'UserID': [i % 50 for i in range(500)],
        'CounterID': [i % 5 for i in range(500)],
        'AdvEngineID': [i % 3 for i in range(500)],
        'SearchPhrase': [f'search_{i % 25}' for i in range(500)],
        'URL': [f'https://example.com/page_{i % 50}' for i in range(500)],
        'Title': [f'Page Title {i % 50}' for i in range(500)],
        'Referer': [f'https://referer.com/{i % 25}' for i in range(500)],
        'ResolutionWidth': [1920] * 500,
        'ResolutionHeight': [1080] * 500,
        'WindowClientWidth': [1900] * 500,
        'WindowClientHeight': [1000] * 500,
        'MobilePhone': [''] * 500,
        'MobilePhoneModel': [''] * 500,
        'RegionID': [i % 20 for i in range(500)],
        'ClientIP': [f'192.168.1.{i % 255}' for i in range(500)],
        'WatchID': [i for i in range(500)],
        'TraficSourceID': [i % 10 for i in range(500)],
        'SearchEngineID': [i % 5 for i in range(500)],
        'URLHash': [hash(f'https://example.com/page_{i % 50}') for i in range(500)],
        'RefererHash': [hash(f'https://referer.com/{i % 25}') for i in range(500)],
        'IsRefresh': [0] * 500,
        'IsLink': [0] * 500,
        'IsDownload': [0] * 500,
        'DontCountHits': [0] * 500
    }
    
    # Create test parquet file
    df = pd.DataFrame(test_data)
    arrow_table = pa.Table.from_pandas(df)
    pq.write_table(arrow_table, "test_hits_custom.parquet")
    
    # Create custom queries
    custom_queries = [
        "SELECT COUNT(*) FROM hits",
        "SELECT COUNT(DISTINCT UserID) FROM hits",
        "SELECT AVG(ResolutionWidth) FROM hits",
        "SELECT RegionID, COUNT(*) FROM hits GROUP BY RegionID ORDER BY COUNT(*) DESC LIMIT 5"
    ]
    
    # Run benchmark with custom queries
    runner = SabotClickBenchRunner(
        parquet_file="test_hits_custom.parquet",
        num_agents=2,
        execution_mode="local_parallel"
    )
    
    try:
        await runner.initialize()
        
        # Override queries
        runner.queries = custom_queries
        
        await runner.run_benchmark(warmup_runs=1, benchmark_runs=2)
        runner.print_results()
    finally:
        await runner.cleanup()
    
    # Cleanup test file
    Path("test_hits_custom.parquet").unlink()

async def main():
    """Run all examples."""
    print("Sabot ClickBench Examples")
    print("=" * 50)
    
    # Setup logging
    logging.basicConfig(level=logging.INFO)
    
    try:
        await example_local_execution()
        await example_distributed_execution()
        await example_custom_queries()
        
        print("\n" + "=" * 50)
        print("All examples completed successfully!")
        
    except Exception as e:
        print(f"Example failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main())
