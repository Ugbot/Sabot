"""
SQL Pipeline Demo

Demonstrates SQL query execution with Sabot's agent-based pipeline.
Shows how to:
1. Register Arrow tables
2. Execute SQL queries with CTEs and joins
3. Use distributed agent execution
4. Compare performance modes
"""

import asyncio
import time
import pyarrow as pa
import pyarrow.compute as pc
from sabot.api.sql import SQLEngine, execute_sql


def create_sample_data():
    """Create sample datasets for demonstration"""
    
    # Create customers table
    customers = pa.table({
        'customer_id': pa.array(range(1, 101)),
        'name': pa.array([f'Customer_{i}' for i in range(1, 101)]),
        'segment': pa.array(['Premium' if i % 3 == 0 else 'Standard' for i in range(1, 101)]),
        'country': pa.array(['USA' if i % 2 == 0 else 'Canada' for i in range(1, 101)])
    })
    
    # Create orders table
    import random
    random.seed(42)
    orders = pa.table({
        'order_id': pa.array(range(1, 1001)),
        'customer_id': pa.array([random.randint(1, 100) for _ in range(1000)]),
        'amount': pa.array([round(random.uniform(10, 5000), 2) for _ in range(1000)]),
        'order_date': pa.array(['2025-01-' + str(random.randint(1, 31)).zfill(2) for _ in range(1000)]),
        'status': pa.array(['completed' if random.random() > 0.1 else 'pending' for _ in range(1000)])
    })
    
    return customers, orders


async def demo_basic_queries():
    """Demo: Basic SQL queries"""
    print("=" * 60)
    print("DEMO 1: Basic SQL Queries")
    print("=" * 60)
    
    customers, orders = create_sample_data()
    
    # Create SQL engine
    engine = SQLEngine(num_agents=4, execution_mode="local_parallel")
    
    # Register tables
    engine.register_table("customers", customers)
    engine.register_table("orders", orders)
    
    print(f"\nRegistered tables:")
    for table_name in engine.list_tables():
        schema = engine.get_table_schema(table_name)
        print(f"  - {table_name}: {len(schema)} columns, {engine.controller.tables[table_name].num_rows} rows")
    
    # Query 1: Simple SELECT
    print("\n--- Query 1: High-value orders ---")
    sql1 = """
        SELECT order_id, customer_id, amount 
        FROM orders 
        WHERE amount > 1000 
        ORDER BY amount DESC 
        LIMIT 10
    """
    print(f"SQL: {sql1.strip()}")
    
    # Get EXPLAIN plan
    explain = await engine.explain(sql1)
    print(f"\nExecution Plan:\n{explain}")
    
    # Execute query
    start = time.time()
    result1 = await engine.execute(sql1)
    duration = time.time() - start
    
    print(f"\nResults: {result1.num_rows} rows in {duration:.3f}s")
    print(result1.to_pandas().head())
    
    await engine.close()


async def demo_joins_and_aggregations():
    """Demo: Joins and aggregations"""
    print("\n" + "=" * 60)
    print("DEMO 2: Joins and Aggregations")
    print("=" * 60)
    
    customers, orders = create_sample_data()
    
    engine = SQLEngine(num_agents=4, execution_mode="local_parallel")
    engine.register_table("customers", customers)
    engine.register_table("orders", orders)
    
    # Query 2: Join with aggregation
    print("\n--- Query 2: Customer order statistics ---")
    sql2 = """
        SELECT 
            c.name,
            c.segment,
            COUNT(*) as order_count,
            SUM(o.amount) as total_spent,
            AVG(o.amount) as avg_order_value
        FROM customers c
        JOIN orders o ON c.customer_id = o.customer_id
        WHERE o.status = 'completed'
        GROUP BY c.name, c.segment
        HAVING total_spent > 5000
        ORDER BY total_spent DESC
        LIMIT 20
    """
    print(f"SQL: {sql2.strip()}")
    
    start = time.time()
    result2 = await engine.execute(sql2)
    duration = time.time() - start
    
    print(f"\nResults: {result2.num_rows} rows in {duration:.3f}s")
    print(result2.to_pandas().head(10))
    
    await engine.close()


async def demo_ctes():
    """Demo: Common Table Expressions (CTEs)"""
    print("\n" + "=" * 60)
    print("DEMO 3: Common Table Expressions (CTEs)")
    print("=" * 60)
    
    customers, orders = create_sample_data()
    
    engine = SQLEngine(num_agents=4, execution_mode="local_parallel")
    engine.register_table("customers", customers)
    engine.register_table("orders", orders)
    
    # Query 3: CTE with multiple references
    print("\n--- Query 3: High-value customers using CTE ---")
    sql3 = """
        WITH high_value_orders AS (
            SELECT customer_id, SUM(amount) as total_value
            FROM orders
            WHERE status = 'completed'
            GROUP BY customer_id
            HAVING total_value > 10000
        ),
        premium_customers AS (
            SELECT customer_id, name, segment
            FROM customers
            WHERE segment = 'Premium'
        )
        SELECT 
            p.name,
            p.segment,
            h.total_value
        FROM premium_customers p
        JOIN high_value_orders h ON p.customer_id = h.customer_id
        ORDER BY h.total_value DESC
    """
    print(f"SQL: {sql3.strip()}")
    
    start = time.time()
    result3 = await engine.execute(sql3)
    duration = time.time() - start
    
    print(f"\nResults: {result3.num_rows} rows in {duration:.3f}s")
    print(result3.to_pandas())
    
    await engine.close()


async def demo_quick_execution():
    """Demo: Quick execution API"""
    print("\n" + "=" * 60)
    print("DEMO 4: Quick Execution API")
    print("=" * 60)
    
    customers, orders = create_sample_data()
    
    # Use convenience function for one-off queries
    print("\n--- Quick SQL execution (no explicit engine) ---")
    sql = """
        SELECT segment, COUNT(*) as customer_count
        FROM customers
        GROUP BY segment
        ORDER BY customer_count DESC
    """
    print(f"SQL: {sql.strip()}")
    
    start = time.time()
    result = await execute_sql(sql, customers=customers)
    duration = time.time() - start
    
    print(f"\nResults: {result.num_rows} rows in {duration:.3f}s")
    print(result.to_pandas())


async def main():
    """Run all demos"""
    print("\n" + "=" * 60)
    print("Sabot SQL Pipeline Demo")
    print("=" * 60)
    print("\nDemonstrating SQL execution with Sabot's morsel operators")
    print("and agent-based distributed processing.\n")
    
    try:
        await demo_basic_queries()
        await demo_joins_and_aggregations()
        await demo_ctes()
        await demo_quick_execution()
        
        print("\n" + "=" * 60)
        print("Demo completed successfully!")
        print("=" * 60)
        
    except Exception as e:
        print(f"\nError during demo: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())

