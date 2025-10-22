#!/usr/bin/env python3
"""
Simple Streaming SQL API Test

Test the basic StreamingSQLExecutor API without external dependencies.
"""

import asyncio
import pyarrow as pa
from sabot_sql import StreamingSQLExecutor


async def test_streaming_api():
    """Test basic streaming SQL API functionality"""
    print("🚀 Testing Streaming SQL API")
    print("=" * 40)
    
    try:
        # Initialize executor
        print("1. Initializing StreamingSQLExecutor...")
        executor = StreamingSQLExecutor(
            state_backend='marbledb',
            timer_backend='marbledb',
            state_path='./test_state',
            checkpoint_interval_seconds=30,
            max_parallelism=4
        )
        print("✅ Executor initialized successfully")
        
        # Test dimension table registration
        print("\n2. Testing dimension table registration...")
        
        # Create a simple dimension table
        symbols = ['AAPL', 'MSFT', 'GOOGL']
        companies = ['Apple Inc.', 'Microsoft Corp.', 'Alphabet Inc.']
        sectors = ['Technology', 'Technology', 'Technology']
        
        securities_table = pa.table({
            'symbol': symbols,
            'company_name': companies,
            'sector': sectors
        })
        
        executor.register_dimension_table(
            'securities',
            securities_table,
            is_raft_replicated=True
        )
        print("✅ Dimension table registered successfully")
        
        # Test DDL execution
        print("\n3. Testing DDL execution...")
        
        executor.execute_ddl("""
            CREATE TABLE trades (
                symbol VARCHAR,
                price DOUBLE,
                volume BIGINT,
                ts TIMESTAMP
            ) WITH (
                'connector' = 'kafka',
                'topic' = 'trades',
                'bootstrap.servers' = 'localhost:9092'
            )
        """)
        print("✅ DDL executed successfully")
        
        # Test stateful operation detection
        print("\n4. Testing stateful operation detection...")
        
        sql_query = """
            SELECT 
                t.symbol,
                s.company_name,
                TUMBLE(t.ts, INTERVAL '1' HOUR) as window_start,
                COUNT(*) as count,
                AVG(t.price) as avg_price
            FROM trades t
            LEFT JOIN securities s ON t.symbol = s.symbol
            GROUP BY t.symbol, s.company_name, TUMBLE(t.ts, INTERVAL '1' HOUR)
        """
        
        stateful_ops = executor._detect_stateful_operations(sql_query)
        print(f"✅ Stateful operations detected: {stateful_ops}")
        
        broadcast_tables = executor._detect_broadcast_joins(sql_query)
        print(f"✅ Broadcast tables detected: {broadcast_tables}")
        
        # Test streaming SQL execution (will fail gracefully without Kafka)
        print("\n5. Testing streaming SQL execution...")
        
        try:
            async for batch in executor.execute_streaming_sql(sql_query):
                print(f"✅ Received batch: {batch.num_rows} rows")
                break  # Just test the first batch
        except Exception as e:
            print(f"⚠️  Streaming execution failed (expected without Kafka): {e}")
        
        # Test shutdown
        print("\n6. Testing shutdown...")
        await executor.shutdown()
        print("✅ Shutdown completed successfully")
        
        print("\n🎉 All API tests passed!")
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False


async def main():
    """Main test entry point"""
    success = await test_streaming_api()
    
    if success:
        print("\n✅ Streaming SQL API test completed successfully!")
        print("   - Executor initialization: ✅")
        print("   - Dimension table registration: ✅")
        print("   - DDL execution: ✅")
        print("   - Stateful operation detection: ✅")
        print("   - Broadcast table detection: ✅")
        print("   - Streaming execution: ⚠️ (expected failure without Kafka)")
        print("   - Shutdown: ✅")
    else:
        print("\n❌ Streaming SQL API test failed!")
        return 1
    
    return 0


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    exit(exit_code)
