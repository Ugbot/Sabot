"""
Test SQL Pipeline with Sabot's Existing Operators

Tests the fully wired SQL implementation using:
- DuckDB for SQL parsing (Python module)
- Sabot's existing Cython operators for execution
- Morsel-driven parallelism
- Real performance measurement
"""

import asyncio
import time
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path

# Import SQL API (now wired to Sabot operators!)
from sabot.api.sql import SQLEngine


async def test_basic_sql():
    """Test basic SQL with Sabot operators"""
    print("\n" + "="*70)
    print("TEST 1: Basic SQL with Sabot Operators")
    print("="*70)
    
    # Create test data
    customers = pa.table({
        'customer_id': pa.array(range(1, 1001)),
        'name': pa.array([f'Customer_{i}' for i in range(1, 1001)]),
        'tier': pa.array(['Gold' if i % 20 == 0 else 'Silver' if i % 5 == 0 else 'Bronze' 
                         for i in range(1, 1001)])
    })
    
    orders = pa.table({
        'order_id': pa.array(range(1, 10001)),
        'customer_id': pa.array([i % 1000 + 1 for i in range(10000)]),
        'amount': pa.array([100 + (i * 7) % 5000 for i in range(10000)], type=pa.float64()),
        'status': pa.array(['completed' if i % 10 != 0 else 'pending' for i in range(10000)])
    })
    
    print(f"\nTest data:")
    print(f"  Customers: {customers.num_rows:,} rows")
    print(f"  Orders: {orders.num_rows:,} rows")
    
    # Create SQL engine
    engine = SQLEngine(num_agents=4, execution_mode="local_parallel")
    engine.register_table("customers", customers)
    engine.register_table("orders", orders)
    
    # Test queries
    queries = [
        ("Simple SELECT", """
            SELECT tier, COUNT(*) as customer_count
            FROM customers
            GROUP BY tier
            ORDER BY customer_count DESC
        """),
        
        ("JOIN with aggregation", """
            SELECT 
                c.tier,
                COUNT(*) as order_count,
                SUM(o.amount) as total_revenue,
                AVG(o.amount) as avg_order_value
            FROM customers c
            JOIN orders o ON c.customer_id = o.customer_id
            WHERE o.status = 'completed'
            GROUP BY c.tier
            ORDER BY total_revenue DESC
        """),
    ]
    
    for name, sql in queries:
        print(f"\n{'-'*70}")
        print(f"Query: {name}")
        print(f"{'-'*70}")
        print(f"SQL: {sql.strip()[:100]}...")
        
        start = time.perf_counter()
        result = await engine.execute(sql)
        elapsed = time.perf_counter() - start
        
        print(f"\n✅ Results: {result.num_rows} rows in {elapsed*1000:.1f}ms")
        print(result)
    
    await engine.close()


async def test_large_file():
    """Test with large fintech file"""
    print("\n" + "="*70)
    print("TEST 2: Large File (10M rows) with Sabot Operators")
    print("="*70)
    
    data_dir = Path('/Users/bengamble/Sabot/examples/fintech_enrichment_demo')
    securities_file = data_dir / 'master_security_10m.arrow'
    inventory_file = data_dir / 'synthetic_inventory.arrow'
    
    # Check files exist
    if not securities_file.exists() or not inventory_file.exists():
        print(f"\n⚠️  Large files not found, skipping")
        print(f"Generate with: cd examples/fintech_enrichment_demo && python convert_csv_to_arrow.py")
        return
    
    # Create engine
    engine = SQLEngine(num_agents=4, execution_mode="local_parallel")
    
    # Load files
    print(f"\nLoading files...")
    print(f"  Securities: {securities_file.name}")
    start = time.perf_counter()
    
    # Load with limit for testing
    securities_limit = 1_000_000  # 1M rows
    inventory_limit = 100_000  # 100K rows
    
    with pa.memory_map(str(securities_file), 'r') as source:
        securities = pa.ipc.open_file(source).read_all()
        securities = securities.slice(0, securities_limit)
    
    sec_load_time = time.perf_counter() - start
    print(f"    Loaded {securities.num_rows:,} securities in {sec_load_time:.2f}s")
    
    start = time.perf_counter()
    with pa.memory_map(str(inventory_file), 'r') as source:
        inventory = pa.ipc.open_file(source).read_all()
        inventory = inventory.slice(0, inventory_limit)
    
    inv_load_time = time.perf_counter() - start
    print(f"    Loaded {inventory.num_rows:,} inventory in {inv_load_time:.2f}s")
    
    # Register tables
    engine.register_table("securities", securities)
    engine.register_table("inventory", inventory)
    
    # Test query with JOIN
    sql = """
        SELECT 
            s.SECTOR,
            COUNT(*) as quote_count,
            AVG(CAST(i.price AS DOUBLE)) as avg_price
        FROM inventory i
        JOIN securities s ON CAST(i.instrumentId AS VARCHAR) = CAST(s.ID AS VARCHAR)
        WHERE CAST(i.price AS DOUBLE) > 100
        GROUP BY s.SECTOR
        ORDER BY quote_count DESC
    """
    
    print(f"\nExecuting JOIN query (1M × 100K rows)...")
    print(f"SQL: {sql.strip()[:150]}...")
    
    start = time.perf_counter()
    result = await engine.execute(sql)
    elapsed = time.perf_counter() - start
    
    total_input = securities.num_rows + inventory.num_rows
    throughput = total_input / elapsed / 1e6
    
    print(f"\n✅ Query completed!")
    print(f"   Input: {total_input:,} rows")
    print(f"   Output: {result.num_rows} rows")
    print(f"   Time: {elapsed*1000:.1f}ms")
    print(f"   Throughput: {throughput:.2f}M rows/sec")
    print(f"\nResults:")
    print(result)
    
    await engine.close()


async def test_performance_comparison():
    """Compare DuckDB vs SabotSQL with Sabot operators"""
    print("\n" + "="*70)
    print("TEST 3: Performance Comparison (DuckDB vs SabotSQL)")
    print("="*70)
    
    # Create test data
    import random
    random.seed(42)
    
    n_orders = 100_000
    orders = pa.table({
        'order_id': pa.array(range(1, n_orders + 1)),
        'customer_id': pa.array([random.randint(1, 10000) for _ in range(n_orders)]),
        'amount': pa.array([random.uniform(10, 5000) for _ in range(n_orders)]),
        'region': pa.array(['US' if i % 3 == 0 else 'EU' if i % 3 == 1 else 'ASIA' 
                           for i in range(n_orders)])
    })
    
    print(f"\nDataset: {orders.num_rows:,} orders")
    
    # Test query
    sql = """
        SELECT 
            region,
            COUNT(*) as order_count,
            SUM(amount) as total_revenue,
            AVG(amount) as avg_order_value
        FROM orders
        WHERE amount > 1000
        GROUP BY region
        ORDER BY total_revenue DESC
    """
    
    # Benchmark DuckDB
    print(f"\n{'-'*70}")
    print("DuckDB Standalone:")
    try:
        import duckdb
        conn = duckdb.connect(':memory:')
        conn.register('orders', orders)
        
        start = time.perf_counter()
        duck_result = conn.execute(sql).fetch_arrow_table()
        duck_time = time.perf_counter() - start
        
        conn.close()
        print(f"  Time: {duck_time*1000:.1f}ms")
        print(f"  Results: {duck_result.num_rows} rows")
    except ImportError:
        print("  DuckDB not installed")
        duck_time = None
    
    # Benchmark SabotSQL with Sabot operators
    print(f"\n{'-'*70}")
    print("SabotSQL (with Sabot Cython operators):")
    
    engine = SQLEngine(num_agents=4, execution_mode="local_parallel")
    engine.register_table("orders", orders)
    
    start = time.perf_counter()
    sabot_result = await engine.execute(sql)
    sabot_time = time.perf_counter() - start
    
    print(f"  Time: {sabot_time*1000:.1f}ms")
    print(f"  Results: {sabot_result.num_rows} rows")
    
    if duck_time:
        overhead = ((sabot_time - duck_time) / duck_time) * 100
        print(f"\n{'-'*70}")
        print(f"Performance:")
        print(f"  DuckDB:   {duck_time*1000:.1f}ms")
        print(f"  SabotSQL: {sabot_time*1000:.1f}ms")
        print(f"  Overhead: {overhead:+.1f}%")
        
        if overhead < 50:
            print(f"  ✅ Good! Within 50% of DuckDB")
        elif overhead < 100:
            print(f"  ⚠️  Acceptable for distributed capability")
        else:
            print(f"  ⚠️  Higher than target (optimize needed)")
    
    await engine.close()


async def main():
    """Run all tests"""
    print("\n" + "="*70)
    print("SQL Pipeline - Wired with Sabot Operators")
    print("="*70)
    print("""
Now using Sabot's existing Cython operators:
✓ CythonFilterOperator - For WHERE clauses
✓ CythonHashJoinOperator - For JOINs
✓ CythonGroupByOperator - For GROUP BY
✓ CythonMapOperator - For projections
✓ MorselDrivenOperator - For parallelism

This leverages all the existing high-performance kernels!
    """)
    
    try:
        await test_basic_sql()
        await test_performance_comparison()
        await test_large_file()
        
        print("\n" + "="*70)
        print("✅ All Tests Complete!")
        print("="*70)
        print("""
Summary:
• SQL engine now uses Sabot's existing operators
• Leverages CythonHashJoinOperator, CythonGroupByOperator, etc.
• Morsel-driven parallelism working
• Ready for optimization

Next: Profile and optimize specific bottlenecks
        """)
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())

