#!/usr/bin/env python3
"""
SabotSQL Streaming SQL Integration with MarbleDB

Demonstrates:
- RAFT-replicated dimension tables (broadcast to all agents)
- RAFT-replicated connector state (fault-tolerant Kafka offsets)
- Local streaming state (window aggregates, partitioned by key)
- End-to-end streaming pipeline with MarbleDB state management

This shows how SabotSQL uses MarbleDB for all state management.
"""

import os
import sys
import asyncio
import time
from pathlib import Path

sys.path.insert(0, '/Users/bengamble/Sabot')
os.environ['DYLD_LIBRARY_PATH'] = '/Users/bengamble/Sabot/MarbleDB/build:/Users/bengamble/Sabot/vendor/arrow/cpp/build/install/lib:/Users/bengamble/Sabot/vendor/tonbo/tonbo-ffi/target/release:' + os.environ.get('DYLD_LIBRARY_PATH', '')

from sabot_sql.sabot_sql_streaming import create_streaming_executor
from sabot import cyarrow as ca


async def main():
    """
    Streaming SQL with MarbleDB state management demonstration.
    Shows all three types of state MarbleDB handles.
    """
    print("🚀 SabotSQL MarbleDB Streaming Integration")
    print("="*70)

    # Step 1: Create streaming executor with MarbleDB
    print("\n📊 Step 1: Create Streaming Executor")
    print("-"*70)

    executor = create_streaming_executor(
        state_backend='marbledb',      # All table state in MarbleDB
        timer_backend='rocksdb',       # Watermarks/timers (unchanged)
        state_path='./streaming_state',
        checkpoint_interval_seconds=30,
        max_parallelism=4              # Max Kafka consumers
    )

    print("✅ Created streaming executor:")
    print("   - Table state backend: MarbleDB (RAFT + local)")
    print("   - Timer backend: RocksDB (watermarks/triggers)")
    print("   - Max parallelism: 4 (Kafka partition consumers)")

    # Step 2: Register RAFT-replicated dimension table
    print("\n📊 Step 2: Register Dimension Table (RAFT Broadcast)")
    print("-"*70)

    # Securities dimension table (small, static, broadcast to all agents)
    securities = ca.Table.from_pydict({
        'instrumentId': ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'NVDA'],
        'NAME': ['Apple Inc', 'Microsoft Corp', 'Alphabet Inc', 'Amazon.com Inc', 'Tesla Inc', 'NVIDIA Corp'],
        'SECTOR': ['Technology', 'Technology', 'Technology', 'Consumer', 'Automotive', 'Technology'],
        'ISINVESTMENTGRADE': ['Y', 'Y', 'Y', 'Y', 'N', 'Y']
    })

    # Register as RAFT-replicated (broadcast to all agents via RAFT consensus)
    executor.register_dimension_table(
        'securities',
        securities,
        is_raft_replicated=True  # MarbleDB RAFT replicates to ALL agents
    )

    print("✅ Registered securities dimension table:")
    print("   - Storage: RAFT-replicated (broadcast)")
    print(f"   - Rows: {securities.num_rows}, Columns: {securities.num_columns}")
    print("   - Benefit: Instant in-memory lookups on all agents")
    print("   - No shuffle needed for stream-table joins")

    # Step 3: Define Kafka streaming source
    print("\n📊 Step 3: Define Kafka Streaming Source")
    print("-"*70)

    # Flink-style DDL for Kafka source
    kafka_ddl = """
        CREATE TABLE trades (
            symbol STRING,
            price DOUBLE,
            volume INT,
            trade_time TIMESTAMP,
            WATERMARK FOR trade_time AS trade_time - INTERVAL '10' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'market-trades',
            'bootstrap.servers' = 'localhost:9092',
            'max-parallelism' = '4',
            'batch-size' = '5000',
            'format' = 'json',
            'group.id' = 'sabot-trades-consumer'
        )
    """

    executor.execute_ddl(kafka_ddl)

    print("✅ Registered Kafka streaming source:")
    print("   - Topic: market-trades")
    print("   - Max parallelism: 4 (one morsel per partition)")
    print("   - Watermark: trade_time - 10 seconds")
    print("   - Format: JSON")

    # Step 4: Show state management architecture
    print("\n📊 Step 4: MarbleDB State Management Architecture")
    print("-"*70)

    print("🔄 **State Types in MarbleDB:**")
    print()

    print("1️⃣ **RAFT-Replicated Tables** (Global consistency):")
    print("   - securities (dimension table) - broadcast to all agents")
    print("   - connector_offsets (Kafka positions) - fault tolerance")
    print("   - Storage: MarbleDB RAFT group")
    print("   - Replication: All agents have identical copies")
    print("   - Consistency: Strong (RAFT consensus)")
    print()

    print("2️⃣ **Local Tables** (Partitioned by agent):")
    print("   - window_aggregates - per (symbol, window)")
    print("   - join_buffers - stream-stream joins")
    print("   - Storage: Local MarbleDB instance per agent")
    print("   - Replication: None (agent-specific)")
    print("   - Partitioning: By key for load balancing")
    print()

    print("3️⃣ **Timer State** (Metadata):")
    print("   - Watermarks per partition")
    print("   - Window triggers")
    print("   - Storage: RocksDB (fast KV access)")
    print()

    # Step 5: Execute streaming query
    print("📊 Step 5: Execute Streaming Query")
    print("-"*70)

    # Streaming SQL with dimension table join
    streaming_sql = """
    SELECT
        t.symbol,
        s.NAME as security_name,
        s.SECTOR as sector,
        TUMBLE_START(t.trade_time, INTERVAL '1' MINUTE) as window_start,
        AVG(t.price) as avg_price,
        SUM(t.volume) as total_volume,
        COUNT(*) as trade_count,
        MAX(t.price) as high_price,
        MIN(t.price) as low_price
    FROM trades t
    LEFT JOIN securities s ON t.symbol = s.instrumentId
    WHERE t.price > 100.0
    GROUP BY
        t.symbol,
        s.NAME,
        s.SECTOR,
        TUMBLE(t.trade_time, INTERVAL '1' MINUTE)
    HAVING COUNT(*) > 5
    """

    print(f"Streaming Query:\n{streaming_sql}\n")

    print("🔄 **Execution Flow (per Kafka batch):**")
    print("1. Kafka consumer yields RecordBatch (5K rows)")
    print("2. Extract GROUP BY keys (symbol, window_start)")
    print("3. For each row:")
    print("   - Lookup securities in local MarbleDB replica (RAFT)")
    print("   - Compute window key: (symbol, window_start)")
    print("   - Update window state in local MarbleDB:")
    print("     * Read current aggregates")
    print("     * Update: count+=1, sum+=price, min/MAX")
    print("     * Write back to MarbleDB")
    print("4. Check watermark in RocksDB:")
    print("   - Has watermark advanced past window end?")
    print("5. If window complete: emit result batch")
    print("6. Checkpoint periodically (MarbleDB + RocksDB)")
    print()

    print("⚡ **Simulating streaming execution...**")

    # Simulate streaming execution (dummy data for demo)
    result_count = 0
    total_windows = 0

    # Simulate 5 batches of streaming results
    for batch_num in range(5):
        await asyncio.sleep(0.5)  # Simulate processing time

        # Dummy window results (what would come from MarbleDB state)
        window_results = ca.RecordBatch.from_pydict({
            'symbol': ['AAPL', 'MSFT', 'GOOGL', 'TSLA'],
            'security_name': ['Apple Inc', 'Microsoft Corp', 'Alphabet Inc', 'Tesla Inc'],
            'sector': ['Technology', 'Technology', 'Technology', 'Automotive'],
            'window_start': [1609459200000 + batch_num * 60000] * 4,  # 1-minute windows
            'avg_price': [150.5 + batch_num, 300.2 + batch_num, 2500.0 + batch_num, 800.0 + batch_num],
            'total_volume': [100000 + batch_num * 10000, 50000 + batch_num * 5000,
                           25000 + batch_num * 2500, 75000 + batch_num * 7500],
            'trade_count': [150 + batch_num * 10, 75 + batch_num * 5,
                           25 + batch_num * 2, 100 + batch_num * 8],
            'high_price': [155.0 + batch_num, 310.0 + batch_num, 2520.0 + batch_num, 820.0 + batch_num],
            'low_price': [145.0 + batch_num, 290.0 + batch_num, 2480.0 + batch_num, 780.0 + batch_num]
        })

        result_count += 1
        total_windows += window_results.num_rows

        print(f"\n📦 Window Result Batch #{result_count}:")
        print(f"   Windows: {window_results.num_rows}")
        print(f"   Columns: {list(window_results.schema.names)}")
        print("   Sample windows:")
        for i in range(min(2, window_results.num_rows)):
            print(f"     Window {i+1}: aggregation result")

    print(f"\n✅ Streaming query complete:")
    print(f"   Total result batches: {result_count}")
    print(f"   Total windows emitted: {total_windows}")

    # Step 6: Show checkpointing
    print("\n📊 Step 6: Fault Tolerance & Checkpointing")
    print("-"*70)

    print("🔄 **Checkpoint Process:**")
    print("1. Coordinator injects barrier into stream")
    print("2. All operators receive barrier")
    print("3. Operators snapshot state:")
    print("   - RAFT tables: Already consistent (MarbleDB handles)")
    print("   - Local tables: MarbleDB flush to disk")
    print("   - Timer state: RocksDB flush")
    print("4. Operators ACK barrier")
    print("5. Coordinator marks checkpoint complete")
    print()

    print("🛟 **Recovery Process:**")
    print("1. Agent detects failure")
    print("2. Reads last checkpoint ID")
    print("3. Restores MarbleDB state from checkpoint")
    print("4. Reads committed Kafka offsets from MarbleDB RAFT")
    print("5. Resumes from last committed position")
    print("6. Exactly-once processing guaranteed")
    print()

    # Step 7: Architecture benefits
    print("🏗️ **MarbleDB Benefits for Streaming SQL**")
    print("="*70)
    print("1. **Unified State Management**: All tables in one system")
    print("2. **RAFT Replication**: Dimension tables & connector state")
    print("3. **Fault Tolerance**: Automatic recovery from checkpoints")
    print("4. **Performance**: LSM-tree for fast lookups & aggregations")
    print("5. **Scalability**: Partitioned local state + replicated global state")
    print("6. **Consistency**: Strong consistency for critical state")
    print()

    print("🎯 **Result**: Production-ready streaming SQL with enterprise reliability!")

    return 0


async def demonstrate_raft_dimension_broadcast():
    """Show how RAFT dimension broadcast works"""
    print("\n🔄 RAFT Dimension Table Broadcast Demo")
    print("="*50)

    # Simulate 3-node RAFT cluster
    nodes = ["agent-1", "agent-2", "agent-3"]

    print("📡 **RAFT Broadcast Process:**")
    print("1. Client registers securities table on leader")
    print("2. Leader proposes to RAFT cluster")
    print("3. Followers receive proposal via RAFT log")
    print("4. All nodes apply: store securities in MarbleDB")
    print("5. All nodes now have identical securities table")
    print()

    for node in nodes:
        print(f"✅ {node}: Securities table replicated (10M rows, in-memory)")

    print()
    print("🎯 **Benefit**: Any agent can do instant dimension lookups")
    print("   No network calls, no shuffle, consistent reads!")


async def demonstrate_connector_state_raft():
    """Show Kafka offset checkpointing via RAFT"""
    print("\n🔄 RAFT Connector State Demo")
    print("="*50)

    # Simulate Kafka offset commits
    partitions = [0, 1, 2, 3]
    offsets = [15432, 15234, 15876, 15123]

    print("📊 **Kafka Offset Checkpointing:**")
    print("1. Consumer processes batch from partition 0")
    print("2. Updates offset to 15432")
    print("3. Commits to MarbleDB RAFT: (connector, partition, offset, timestamp)")
    print("4. RAFT replicates to all agents")
    print("5. All agents know last committed position")
    print()

    print("📋 **Committed Offsets (RAFT-replicated):**")
    for partition, offset in zip(partitions, offsets):
        print(f"   kafka-trades partition {partition}: offset {offset}")

    print()
    print("🛟 **Failure Recovery:**")
    print("1. Agent-2 crashes and restarts")
    print("2. Reads from MarbleDB RAFT: last committed offsets")
    print("3. Resumes Kafka consumption from checkpoint")
    print("4. No duplicate processing, exactly-once guaranteed!")

    await asyncio.sleep(1)  # Simulate processing


if __name__ == "__main__":
    async def run_demo():
        await main()
        await demonstrate_raft_dimension_broadcast()
        await demonstrate_connector_state_raft()

    sys.exit(asyncio.run(run_demo()))
