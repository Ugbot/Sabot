#!/usr/bin/env python3
"""
Streaming SQL with Dimension Table Broadcast

Demonstrates:
- Kafka source with partition parallelism (one morsel per partition)
- Dimension table broadcast (securities replicated to all agents)
- Stateful windowed aggregation (Tonbo for table state, RocksDB for timers)
- Checkpoint/recovery

This example shows how to run Flink-style streaming SQL with SabotSQL.
"""

import os
import sys
import asyncio
from pathlib import Path

sys.path.insert(0, '/Users/bengamble/Sabot')
os.environ['DYLD_LIBRARY_PATH'] = '/Users/bengamble/Sabot/sabot_sql/build:/Users/bengamble/Sabot/vendor/arrow/cpp/build/install/lib:/Users/bengamble/Sabot/vendor/tonbo/tonbo-ffi/target/release:' + os.environ.get('DYLD_LIBRARY_PATH', '')

from sabot_sql.sabot_sql_streaming import create_streaming_executor
from sabot import cyarrow as ca


async def main():
    """
    Streaming SQL example with:
    - Kafka trades stream (partitioned by symbol)
    - Securities dimension table (broadcast to all agents)
    - Windowed aggregation with state
    """
    print("🚀 Streaming SQL with Dimension Broadcast Example")
    print("="*70)
    
    # Create streaming executor
    executor = create_streaming_executor(
        state_backend='marbledb',  # All table state (RAFT for dimensions, local for streaming)
        timer_backend='rocksdb',  # Timers and watermarks
        state_path='./streaming_sql_state',
        checkpoint_interval_seconds=60,
        max_parallelism=8  # Max Kafka partition consumers
    )
    
    # Step 1: Register dimension table (broadcast)
    print("\n📊 Step 1: Register Dimension Table")
    print("-"*70)
    
    # Load securities (dimension table - small, static)
    securities = ca.Table.from_pydict({
        'instrumentId': ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA'],
        'NAME': ['Apple Inc', 'Microsoft Corp', 'Alphabet Inc', 'Amazon.com Inc', 'Tesla Inc'],
        'SECTOR': ['Technology', 'Technology', 'Technology', 'Consumer', 'Automotive'],
        'ISINVESTMENTGRADE': ['Y', 'Y', 'Y', 'Y', 'N']
    })
    
    # Register as RAFT-replicated table (broadcast to all agents via RAFT)
    executor.register_dimension_table(
        'securities',
        securities,
        is_raft_replicated=True  # MarbleDB RAFT replicates to all agents
    )
    
    # Step 2: Define Kafka streaming source
    print("\n📊 Step 2: Define Kafka Source (DDL)")
    print("-"*70)
    
    # Flink-style CREATE TABLE DDL
    executor.execute_ddl("""
        CREATE TABLE trades (
            symbol STRING,
            price DOUBLE,
            volume INT,
            ts TIMESTAMP,
            WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'market-trades',
            'max-parallelism' = '8',
            'batch-size' = '10000',
            'format' = 'json'
        )
    """)
    
    # Step 3: Execute streaming query
    print("\n📊 Step 3: Execute Streaming Query")
    print("-"*70)
    
    # Streaming query with:
    # - LEFT JOIN with broadcast dimension table (no shuffle)
    # - Windowed aggregation (stateful, per key+window in Tonbo)
    # - TUMBLE window (1 minute)
    sql = """
    SELECT 
        t.symbol,
        s.NAME as security_name,
        s.SECTOR as sector,
        TUMBLE_START(t.ts, INTERVAL '1' MINUTE) as window_start,
        AVG(t.price) as avg_price,
        SUM(t.volume) as total_volume,
        COUNT(*) as trade_count,
        MAX(t.price) as high,
        MIN(t.price) as low
    FROM trades t
    LEFT JOIN securities s ON t.symbol = s.instrumentId
    GROUP BY 
        t.symbol, 
        s.NAME, 
        s.SECTOR, 
        TUMBLE(t.ts, INTERVAL '1' MINUTE)
    HAVING AVG(t.price) > 100
    """
    
    print(f"Query:\n{sql}\n")
    
    # Execute streaming (returns async generator)
    print("🔄 Starting streaming execution...")
    print("   (Would consume from Kafka, process batches, emit window results)")
    
    result_count = 0
    async for batch in executor.execute_streaming_sql(sql):
        result_count += 1
        print(f"\n📦 Window Result Batch #{result_count}:")
        print(f"   Rows: {batch.num_rows}")
        print(f"   Columns: {list(batch.schema.names)}")
        
        # Show sample
        if batch.num_rows > 0:
            print(f"\n   Sample:")
            for i in range(min(3, batch.num_rows)):
                row = {col: batch.column(col)[i].as_py() for col in batch.schema.names}
                print(f"     {row}")
    
    print(f"\n✅ Streaming query complete")
    print(f"   Total window results: {result_count}")
    
    # Step 4: Show architecture
    print("\n📊 Architecture Summary")
    print("-"*70)
    print("Kafka Source:")
    print(f"  - Topic: market-trades")
    print(f"  - Max parallelism: 8 (one morsel per partition)")
    print(f"  - Batch size: 10K rows")
    print(f"  - Watermark: ts - 5 seconds")
    print()
    print("Dimension Table (MarbleDB):")
    print(f"  - securities: is_raft_replicated=true")
    print(f"  - RAFT replicates to ALL agents")
    print(f"  - Each agent reads from local MarbleDB replica")
    print(f"  - No shuffle overhead, consistent reads")
    print()
    print("State Management (MarbleDB + RocksDB):")
    print(f"  - All tables: {executor.state_backend}")
    print(f"  - Window aggregates: local MarbleDB tables (is_raft_replicated=false)")
    print(f"  - Per (symbol, window): {{sum, count, min, max}}")
    print(f"  - Timers/watermarks: {executor.timer_backend}")
    print(f"  - Window triggers when watermark advances")
    print()
    print("Checkpointing:")
    print(f"  - Interval: {executor.checkpoint_interval}s")
    print(f"  - Barrier-based (Chandy-Lamport)")
    print(f"  - Exactly-once processing")
    print()
    print("Execution Flow:")
    print("  Kafka (partitions) → Morsel Consumers")
    print("  → Read Dimension from Local MarbleDB (RAFT replica)")
    print("  → Stateful Window Aggregation (MarbleDB local tables)")
    print("  → Watermark Check (RocksDB timers)")
    print("  → Emit Window Results")
    print("  → Periodic Checkpoint (MarbleDB + RocksDB)")
    
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))

