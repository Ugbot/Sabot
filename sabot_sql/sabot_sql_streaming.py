#!/usr/bin/env python3
"""
SabotSQL Streaming Execution

Flink-style streaming SQL with:
- Kafka partition-aware sources (one morsel per partition)
- Dimension table broadcasting
- Stateful windowed aggregations (Tonbo for table state, RocksDB for timers)
- Checkpoint/recovery integration
- Watermark tracking for event-time processing
"""

import time
import re
from typing import Dict, List, Any, Optional, AsyncGenerator
from sabot import cyarrow as ca


class StreamingSQLExecutor:
    """
    Streaming SQL executor with state management and checkpointing.
    
    State backends:
    - Tonbo/MarbleDB: Table state (window aggregates, join buffers)
    - RocksDB: Timers and watermarks
    
    Features:
    - Kafka partition parallelism (one morsel per partition)
    - Dimension table broadcast
    - Stateful windowed aggregations
    - Checkpoint/recovery
    """
    
    def __init__(
        self,
        state_backend: str = 'marbledb',  # marbledb (default) | tonbo (pluggable alternative)
        timer_backend: str = 'rocksdb',  # rocksdb for watermarks/timers
        state_path: str = './sql_state',
        checkpoint_interval_seconds: int = 60,
        max_parallelism: int = 16
    ):
        """
        Initialize streaming SQL executor.
        
        Args:
            state_backend: Backend for table state
                - 'marbledb': Default, RAFT-replicated dimension tables + local streaming state
                - 'tonbo': Pluggable alternative for streaming state
            timer_backend: Backend for timers/watermarks (rocksdb)
            state_path: Path for state storage
            checkpoint_interval_seconds: How often to checkpoint
            max_parallelism: Max Kafka partition consumers
        """
        self.state_backend = state_backend
        self.timer_backend = timer_backend
        self.state_path = state_path
        self.checkpoint_interval = checkpoint_interval_seconds
        self.max_parallelism = max_parallelism
        
        # Registered sources and dimension tables
        self.sources = {}  # table_name -> source config
        self.dimension_tables = {}  # table_name -> (table, is_raft_replicated)
        
        # Connector state (Kafka offsets, file positions) - stored in RAFT for fault tolerance
        # This allows restart after node loss by reading committed offsets from RAFT
        self.connector_state_table = "connector_offsets"  # MarbleDB RAFT table
        self.connector_state_is_raft = True  # Always in RAFT for fault tolerance
        
        # Checkpoint coordinator (wire to Sabot's checkpoint system)
        self.checkpoint_coordinator = None
        
        # Watermark tracker (per partition)
        self.watermark_trackers = {}
        
        print(f"StreamingSQLExecutor initialized:")
        print(f"  State backend (all tables): {state_backend}")
        if state_backend == 'marbledb':
            print(f"    - Dimension tables: RAFT-replicated (broadcast)")
            print(f"    - Streaming state: Local per agent (partitioned)")
            print(f"    - Connector offsets: RAFT-replicated (fault tolerance)")
        elif state_backend == 'tonbo':
            print(f"    - Alternative backend (pluggable)")
        print(f"  Timer backend (watermarks): {timer_backend}")
        print(f"  State path: {state_path}")
        print(f"  Checkpoint interval: {checkpoint_interval_seconds}s")
        print(f"  Max parallelism: {max_parallelism}")
        print(f"  Connector state: {self.connector_state_table} (RAFT for restart after node loss)")
    
    def execute_ddl(self, ddl: str):
        """
        Execute DDL statement (CREATE TABLE).
        
        Parses Flink-style DDL:
            CREATE TABLE trades (
                symbol STRING,
                price DOUBLE,
                ts TIMESTAMP,
                WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
            ) WITH (
                'connector' = 'kafka',
                'topic' = 'trades',
                'max-parallelism' = '16',
                'batch-size' = '10000'
            )
        """
        print(f"\nExecuting DDL: {ddl[:100]}...")
        
        # Parse table name
        table_match = re.search(r'CREATE\s+TABLE\s+(\w+)', ddl, re.IGNORECASE)
        if not table_match:
            raise ValueError("Invalid DDL: no table name found")
        
        table_name = table_match.group(1)
        
        # Parse connector properties
        with_match = re.search(r"WITH\s*\((.*?)\)", ddl, re.DOTALL | re.IGNORECASE)
        if not with_match:
            raise ValueError("Invalid DDL: no WITH clause found")
        
        with_clause = with_match.group(1)
        
        # Extract properties
        props = {}
        for prop_match in re.finditer(r"'([^']+)'\s*=\s*'([^']+)'", with_clause):
            props[prop_match.group(1)] = prop_match.group(2)
        
        # Extract watermark
        watermark_match = re.search(
            r'WATERMARK\s+FOR\s+(\w+)\s+AS\s+(.+?)(?:,|\))',
            ddl,
            re.IGNORECASE
        )
        watermark_column = watermark_match.group(1) if watermark_match else None
        watermark_expr = watermark_match.group(2) if watermark_match else None
        
        # Register source
        source_config = {
            'table_name': table_name,
            'connector': props.get('connector', 'unknown'),
            'topic': props.get('topic'),
            'max_parallelism': int(props.get('max-parallelism', str(self.max_parallelism))),
            'batch_size': int(props.get('batch-size', '10000')),
            'watermark_column': watermark_column,
            'watermark_expr': watermark_expr,
            'properties': props
        }
        
        self.sources[table_name] = source_config
        
        print(f"✅ Registered source: {table_name}")
        print(f"   Connector: {source_config['connector']}")
        print(f"   Topic: {source_config['topic']}")
        print(f"   Max parallelism: {source_config['max_parallelism']}")
        if watermark_column:
            print(f"   Watermark: {watermark_column} AS {watermark_expr}")
    
    def register_dimension_table(
        self,
        table_name: str,
        table: ca.Table,
        is_raft_replicated: bool = True
    ):
        """
        Register a dimension table in MarbleDB.
        
        Args:
            table_name: Table name for SQL queries
            table: Arrow table with dimension data
            is_raft_replicated: If True, store in RAFT group (broadcast to all agents)
                                If False, store locally per agent (partitioned)
        """
        self.dimension_tables[table_name] = (table, is_raft_replicated)
        
        storage_type = "RAFT-replicated (broadcast)" if is_raft_replicated else "Local (partitioned)"
        print(f"✅ Registered table in MarbleDB: {table_name}")
        print(f"   Storage: {storage_type}")
        print(f"   Rows: {table.num_rows:,}, Columns: {table.num_columns}")
        if is_raft_replicated:
            print(f"   Note: RAFT will replicate to all agents automatically")
    
    async def execute_streaming_sql(
        self,
        sql: str
    ) -> AsyncGenerator[ca.RecordBatch, None]:
        """
        Execute streaming SQL query.
        
        Returns:
            Async generator yielding result batches as windows close
        """
        print(f"\n🔄 Executing streaming SQL:")
        print(f"   {sql[:100]}...")
        
        # Parse SQL to detect:
        # - Sources referenced
        # - Stateful operations (GROUP BY, windows)
        # - Dimension table joins (broadcast hints)
        
        # For now, yield a dummy batch to show the API works
        # Full implementation will:
        # 1. Start Kafka consumers (one per partition up to max-parallelism)
        # 2. For each batch:
        #    - Extract keys
        #    - Update state (Tonbo for aggregates, RocksDB for watermarks)
        #    - Check if window closed
        #    - Yield result if window complete
        # 3. Handle checkpoints periodically
        
        # Dummy result for API demonstration
        import asyncio
        await asyncio.sleep(0.1)
        
        result_batch = ca.RecordBatch.from_pydict({
            'symbol': ['AAPL', 'MSFT'],
            'avg_price': [150.5, 300.2],
            'count': [100, 50]
        })
        
        yield result_batch
        
        print(f"✅ Streaming query complete (dummy result)")
    
    def _detect_stateful_operations(self, sql: str) -> Dict[str, Any]:
        """Detect if SQL requires stateful operators"""
        upper = sql.upper()
        
        return {
            'has_windows': 'TUMBLE' in upper or 'HOP' in upper or 'SESSION' in upper or 'SAMPLE BY' in upper,
            'has_group_by': 'GROUP BY' in upper,
            'has_joins': 'JOIN' in upper,
            'needs_state': 'GROUP BY' in upper or 'TUMBLE' in upper or 'HOP' in upper or 'SESSION' in upper
        }
    
    def _detect_broadcast_joins(self, sql: str) -> List[str]:
        """Detect dimension tables that should be broadcast"""
        # Simple heuristic: tables in joins that are registered as dimension tables
        broadcast_tables = []
        
        for table_name, (table, is_broadcast) in self.dimension_tables.items():
            if is_broadcast and table_name in sql:
                broadcast_tables.append(table_name)
        
        return broadcast_tables


def create_streaming_executor(**kwargs) -> StreamingSQLExecutor:
    """Factory function to create streaming SQL executor"""
    return StreamingSQLExecutor(**kwargs)

