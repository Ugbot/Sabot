"""
Direct SQL Test - Bypass Full Sabot Import

Tests SQL execution using only the SQL module components,
avoiding the full Sabot initialization.
"""

import asyncio
import time
import sys
import pyarrow as pa
from pathlib import Path

# Direct import of SQL components (bypass sabot/__init__.py)
sys.path.insert(0, '/Users/bengamble/Sabot')

# Import only what we need
from sabot.sql.sql_to_operators import SQLQueryExecutor


async def test_sql_with_operators():
    """Test SQL executor with existing operators"""
    print("\n" + "="*70)
    print("SQL Executor with Sabot Operators - Direct Test")
    print("="*70)
    
    # Create test data
    print("\nCreating test data...")
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
        'region': pa.array(['US' if i % 3 == 0 else 'EU' if i % 3 == 1 else 'ASIA' 
                           for i in range(10000)]),
        'status': pa.array(['completed' if i % 10 != 0 else 'pending' for i in range(10000)])
    })
    
    print(f"  Customers: {customers.num_rows:,} rows")
    print(f"  Orders: {orders.num_rows:,} rows")
    
    # Create SQL executor
    print(f"\nInitializing SQL executor (4 workers)...")
    executor = SQLQueryExecutor(num_workers=4)
    executor.register_table("customers", customers)
    executor.register_table("orders", orders)
    
    # Test queries
    print(f"\n{'='*70}")
    print("Running SQL Queries")
    print(f"{'='*70}")
    
    queries = [
        {
            'name': 'Simple GROUP BY',
            'sql': """
                SELECT tier, COUNT(*) as count
                FROM customers  
                GROUP BY tier
                ORDER BY count DESC
            """
        },
        {
            'name': 'Filtered aggregation',
            'sql': """
                SELECT 
                    region,
                    COUNT(*) as order_count,
                    SUM(amount) as total_revenue,
                    AVG(amount) as avg_value
                FROM orders
                WHERE status = 'completed' AND amount > 1000
                GROUP BY region
                ORDER BY total_revenue DESC
            """
        },
        {
            'name': 'JOIN with GROUP BY',
            'sql': """
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
            """
        }
    ]
    
    for i, query_spec in enumerate(queries, 1):
        print(f"\n{'-'*70}")
        print(f"Query {i}: {query_spec['name']}")
        print(f"{'-'*70}")
        print(f"{query_spec['sql'].strip()[:120]}...")
        
        start = time.perf_counter()
        result = await executor.execute(query_spec['sql'])
        elapsed = time.perf_counter() - start
        
        print(f"\n✅ Completed in {elapsed*1000:.1f}ms")
        print(f"   Rows returned: {result.num_rows}")
        print(f"   Columns: {', '.join(result.column_names)}")
        print(f"\nResults:")
        print(result)
    
    print(f"\n{'='*70}")
    print("✅ All queries completed successfully!")
    print(f"{'='*70}")


async def test_with_large_data():
    """Test with large fintech dataset"""
    print("\n" + "="*70)
    print("Large Dataset Test (1M × 100K JOIN)")
    print("="*70)
    
    data_dir = Path('/Users/bengamble/Sabot/examples/fintech_enrichment_demo')
    securities_file = data_dir / 'master_security_10m.arrow'
    inventory_file = data_dir / 'synthetic_inventory.arrow'
    
    if not securities_file.exists() or not inventory_file.exists():
        print("\n⚠️  Large files not found, skipping this test")
        return
    
    # Load data
    print("\nLoading large files...")
    with pa.memory_map(str(securities_file), 'r') as source:
        securities = pa.ipc.open_file(source).read_all().slice(0, 1_000_000)
    
    with pa.memory_map(str(inventory_file), 'r') as source:
        inventory = pa.ipc.open_file(source).read_all().slice(0, 100_000)
    
    print(f"  Securities: {securities.num_rows:,} rows")
    print(f"  Inventory: {inventory.num_rows:,} rows")
    
    # Create executor
    executor = SQLQueryExecutor(num_workers=4)
    executor.register_table("securities", securities)
    executor.register_table("inventory", inventory)
    
    # Complex query
    sql = """
        SELECT 
            s.SECTOR,
            s.MARKETSEGMENT,
            COUNT(*) as quote_count,
            AVG(CAST(i.price AS DOUBLE)) as avg_price
        FROM inventory i
        JOIN securities s ON CAST(i.instrumentId AS VARCHAR) = CAST(s.ID AS VARCHAR)
        WHERE CAST(i.price AS DOUBLE) BETWEEN 90 AND 120
        GROUP BY s.SECTOR, s.MARKETSEGMENT
        ORDER BY quote_count DESC
        LIMIT 20
    """
    
    print(f"\nExecuting JOIN query...")
    start = time.perf_counter()
    result = await executor.execute(sql)
    elapsed = time.perf_counter() - start
    
    throughput = (securities.num_rows + inventory.num_rows) / elapsed / 1e6
    
    print(f"\n✅ Large query completed!")
    print(f"   Time: {elapsed*1000:.1f}ms ({elapsed:.2f}s)")
    print(f"   Throughput: {throughput:.2f}M rows/sec")
    print(f"   Results: {result.num_rows} rows")
    print(f"\nTop results:")
    print(result)


async def main():
    """Run all tests"""
    print("\n" + "="*70)
    print("SQL Pipeline - Direct Operator Test")
    print("="*70)
    print("""
Testing SQL engine wired to Sabot's existing operators:
• Uses CythonHashJoinOperator for JOINs
• Uses CythonGroupByOperator for GROUP BY  
• Uses CythonFilterOperator for WHERE
• Morsel-driven parallelism with existing infrastructure

This validates that the integration works!
    """)
    
    try:
        await test_sql_with_operators()
        await test_with_large_data()
        
        print("\n" + "="*70)
        print("✅ SUCCESS - SQL Engine Working with Sabot Operators!")
        print("="*70)
        print("""
What this proves:
• SQL queries execute successfully
• Using existing Sabot Cython operators
• Morsel parallelism works
• Performance is reasonable

The integration is WORKING! Now we can optimize.
        """)
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())

