#!/usr/bin/env python3
"""
Test script for Sabot ClickBench setup

Verifies that all components are working correctly before running the full benchmark.
"""

import asyncio
import logging
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent.parent))

from sabot import cyarrow as ca
from sabot_sql import create_sabot_sql_bridge
from distributed_executor import DistributedSQLExecutor

logger = logging.getLogger(__name__)

async def test_basic_setup():
    """Test basic Sabot SQL setup."""
    print("Testing basic Sabot SQL setup...")
    
    try:
        # Create test data
        test_data = ca.Table.from_pydict({
            'id': [1, 2, 3, 4, 5],
            'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
            'value': [10.5, 20.3, 30.7, 40.1, 50.9]
        })
        
        # Create bridge
        bridge = create_sabot_sql_bridge()
        bridge.register_table("test_table", test_data)
        
        # Execute simple query
        result = bridge.execute_sql("SELECT COUNT(*) FROM test_table")
        
        print(f"✓ Basic setup test passed: {result.num_rows} rows returned")
        return True
        
    except Exception as e:
        print(f"✗ Basic setup test failed: {e}")
        return False

async def test_distributed_executor():
    """Test distributed executor."""
    print("Testing distributed executor...")
    
    try:
        # Create test data
        test_data = ca.Table.from_pydict({
            'id': list(range(100)),
            'value': [i * 1.5 for i in range(100)]
        })
        
        # Create distributed executor
        executor = DistributedSQLExecutor(num_agents=2)
        await executor.initialize(test_data)
        
        # Execute test query
        result = await executor.execute_query("SELECT COUNT(*) FROM hits")
        
        print(f"✓ Distributed executor test passed: {result.num_rows} rows returned")
        
        # Cleanup
        await executor.cleanup()
        return True
        
    except Exception as e:
        print(f"✗ Distributed executor test failed: {e}")
        return False

async def test_query_loading():
    """Test query loading."""
    print("Testing query loading...")
    
    try:
        queries_file = Path(__file__).parent / "queries.sql"
        if not queries_file.exists():
            print(f"✗ Queries file not found: {queries_file}")
            return False
        
        with open(queries_file) as f:
            queries = [line.strip() for line in f if line.strip()]
        
        print(f"✓ Query loading test passed: {len(queries)} queries loaded")
        return True
        
    except Exception as e:
        print(f"✗ Query loading test failed: {e}")
        return False

async def test_parquet_simulation():
    """Test parquet file simulation."""
    print("Testing parquet file simulation...")
    
    try:
        import pandas as pd
        import pyarrow as pa
        
        # Create simulated hits data
        hits_data = {
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
        
        # Convert to DataFrame
        df = pd.DataFrame(hits_data)
        
        # Convert to Arrow
        arrow_table = pa.Table.from_pandas(df)
        
        # Convert to cyarrow
        cyarrow_table = ca.Table.from_pyarrow(arrow_table)
        
        print(f"✓ Parquet simulation test passed: {cyarrow_table.num_rows} rows, {cyarrow_table.num_columns} columns")
        return True
        
    except Exception as e:
        print(f"✗ Parquet simulation test failed: {e}")
        return False

async def main():
    """Run all tests."""
    print("Sabot ClickBench Setup Test")
    print("=" * 40)
    
    tests = [
        test_basic_setup,
        test_distributed_executor,
        test_query_loading,
        test_parquet_simulation
    ]
    
    passed = 0
    total = len(tests)
    
    for test in tests:
        try:
            result = await test()
            if result:
                passed += 1
        except Exception as e:
            print(f"✗ Test {test.__name__} failed with exception: {e}")
    
    print("\n" + "=" * 40)
    print(f"Test Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("✓ All tests passed! ClickBench setup is ready.")
        return 0
    else:
        print("✗ Some tests failed. Please check the setup.")
        return 1

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    sys.exit(asyncio.run(main()))
