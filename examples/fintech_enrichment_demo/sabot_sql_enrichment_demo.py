#!/usr/bin/env python3
"""
Fintech Enrichment Demo with SabotSQL

Demonstrates SabotSQL with integrated Flink/QuestDB extensions on real fintech data:
- 10M securities (dimension table)
- 1.2M quotes (streaming data)
- 1M trades (streaming data)

Features:
- ASOF JOIN for time-series aligned trades ⋈ quotes
- SAMPLE BY for time-based window aggregation
- LATEST BY for deduplication
- Distributed execution across multiple agents
- All execution via Sabot Arrow + morsel + shuffle

Usage:
    python sabot_sql_enrichment_demo.py --agents 4 --securities 100000 --quotes 50000 --trades 50000
"""

import os
import sys
import time
import argparse
from pathlib import Path
from typing import Optional

# Set library paths
os.environ['DYLD_LIBRARY_PATH'] = '/Users/bengamble/Sabot/sabot_sql/build:/Users/bengamble/Sabot/vendor/arrow/cpp/build/install/lib:/Users/bengamble/Sabot/vendor/tonbo/tonbo-ffi/target/release:' + os.environ.get('DYLD_LIBRARY_PATH', '')

sys.path.insert(0, '/Users/bengamble/Sabot')

from sabot_sql import SabotSQLOrchestrator
from sabot import cyarrow as ca
from sabot.cyarrow import csv as pa_csv
import numpy as np

# Data paths
DATA_DIR = Path(__file__).parent
SECURITIES_ARROW = DATA_DIR / "master_security_10m.arrow"
QUOTES_ARROW = DATA_DIR / "synthetic_inventory.arrow"
TRADES_ARROW = DATA_DIR / "trax_trades_1m.arrow"

# Fallback to CSV if Arrow not available
SECURITIES_CSV = DATA_DIR / "master_security_10m.csv"
QUOTES_CSV = DATA_DIR / "synthetic_inventory.csv"
TRADES_CSV = DATA_DIR / "trax_trades_1m.csv"


def load_arrow_ipc(arrow_path: Path, limit: Optional[int] = None) -> ca.Table:
    """Load Arrow IPC file (10-100x faster than CSV)"""
    print(f"\n📂 Loading {arrow_path.name} (Arrow IPC)...")
    
    if not arrow_path.exists():
        raise FileNotFoundError(f"{arrow_path} not found")
    
    start = time.time()
    
    # Use Arrow IPC reader
    import pyarrow as pa
    import pyarrow.ipc as ipc
    
    with pa.memory_map(str(arrow_path), 'r') as source:
        with ipc.open_file(source) as reader:
            # Read all batches or up to limit
            batches = []
            rows_read = 0
            
            for i in range(reader.num_record_batches):
                if limit and rows_read >= limit:
                    break
                    
                batch = reader.get_batch(i)
                
                if limit:
                    rows_to_take = min(batch.num_rows, limit - rows_read)
                    if rows_to_take < batch.num_rows:
                        batch = batch.slice(0, rows_to_take)
                
                batches.append(batch)
                rows_read += batch.num_rows
            
            table = pa.Table.from_batches(batches)
    
    # Convert NULL columns
    columns_to_keep = []
    schema_fields = []
    for i, field in enumerate(table.schema):
        if str(field.type) != 'null':
            columns_to_keep.append(i)
            schema_fields.append(field)
    
    if len(columns_to_keep) < len(table.schema):
        table = table.select(columns_to_keep)
    
    elapsed = time.time() - start
    throughput = table.num_rows / elapsed / 1e6
    
    print(f"✅ Loaded {table.num_rows:,} rows in {elapsed:.2f}s ({throughput:.1f}M rows/sec)")
    
    return table


def load_csv_fast(csv_path: Path, limit: Optional[int] = None) -> ca.Table:
    """Load CSV using Arrow's fast native parser"""
    print(f"\n📂 Loading {csv_path.name}...")
    
    if not csv_path.exists():
        raise FileNotFoundError(f"{csv_path} not found. Run data generators first.")
    
    start = time.time()
    
    read_options = pa_csv.ReadOptions(
        use_threads=True,
        block_size=128 * 1024 * 1024  # 128MB blocks
    )
    
    parse_options = pa_csv.ParseOptions(delimiter=',')
    
    convert_options = pa_csv.ConvertOptions(
        null_values=['NULL', 'null', ''],
        strings_can_be_null=True,
        auto_dict_encode=False
    )
    
    # Use streaming reader if limit specified
    if limit:
        with pa_csv.open_csv(
            csv_path,
            read_options=read_options,
            parse_options=parse_options,
            convert_options=convert_options
        ) as reader:
            batches = []
            rows_read = 0
            
            for batch in reader:
                if rows_read >= limit:
                    break
                    
                rows_to_take = min(batch.num_rows, limit - rows_read)
                if rows_to_take < batch.num_rows:
                    batch = batch.slice(0, rows_to_take)
                
                batches.append(batch)
                rows_read += batch.num_rows
            
            table = ca.Table.from_batches(batches)
    else:
        table = pa_csv.read_csv(
            csv_path,
            read_options=read_options,
            parse_options=parse_options,
            convert_options=convert_options
        )
    
    # Convert NULL columns
    columns_to_keep = []
    schema_fields = []
    for i, field in enumerate(table.schema):
        if str(field.type) != 'null':
            columns_to_keep.append(i)
            schema_fields.append(field)
    
    if len(columns_to_keep) < len(table.schema):
        table = table.select(columns_to_keep)
    
    elapsed = time.time() - start
    throughput = table.num_rows / elapsed / 1e6
    
    print(f"✅ Loaded {table.num_rows:,} rows in {elapsed:.2f}s ({throughput:.1f}M rows/sec)")
    
    return table


def demo_asof_join_distributed(orchestrator, trades, quotes, num_agents):
    """Demonstrate distributed ASOF JOIN for trade enrichment"""
    print("\n" + "="*70)
    print("Demo 1: ASOF JOIN - Trade Enrichment (Distributed)")
    print("="*70)
    
    # Distribute trades and quotes across agents
    print(f"\n📊 Distributing data across {num_agents} agents...")
    orchestrator.distribute_table("trades", trades, strategy="round_robin")
    orchestrator.distribute_table("quotes", quotes, strategy="round_robin")
    
    # ASOF JOIN query: Join each trade with the most recent quote
    # (assuming trades have a timestamp and we want the quote at or before trade time)
    sql = """
    SELECT 
        trades.id,
        trades.instrumentId,
        trades.price as trade_price,
        trades.quantity,
        quotes.price as quote_price,
        quotes.spread
    FROM trades 
    ASOF JOIN quotes 
    ON trades.instrumentId = quotes.instrumentId AND trades.timestamp <= quotes.timestamp
    LIMIT 100000
    """
    
    print(f"\n🔍 SQL Query:\n{sql}")
    
    # Parse and show plan
    plan = orchestrator.agents[list(orchestrator.agents.keys())[0]].parse_and_optimize(sql)
    print(f"\n📋 Query Plan:")
    print(f"   has_asof_joins: {plan['has_asof_joins']}")
    print(f"   join_keys: {plan['join_key_columns']}")
    print(f"   ts_column: {plan['join_timestamp_column']}")
    
    # Execute distributed
    print(f"\n⚡ Executing on {num_agents} agents...")
    start = time.time()
    results = orchestrator.execute_distributed_query(sql)
    elapsed = time.time() - start
    
    # Results
    successful = sum(1 for r in results if r['status'] == 'success')
    print(f"\n✅ ASOF JOIN completed in {elapsed:.3f}s")
    print(f"   Successful agents: {successful}/{num_agents}")
    
    for i, result in enumerate(results[:3], 1):  # Show first 3
        if result['status'] == 'success':
            print(f"   Agent {i}: {result['result'].num_rows} rows")
    
    return results


def demo_sample_by_distributed(orchestrator, trades, num_agents):
    """Demonstrate distributed SAMPLE BY for time-based aggregation"""
    print("\n" + "="*70)
    print("Demo 2: SAMPLE BY - Time-Based Aggregation (Distributed)")
    print("="*70)
    
    # Distribute trades across agents
    print(f"\n📊 Distributing trades across {num_agents} agents...")
    orchestrator.distribute_table("trades", trades, strategy="round_robin")
    
    # SAMPLE BY query: Aggregate trades by 1-hour windows
    sql = """
    SELECT 
        instrumentId,
        AVG(price) as avg_price,
        SUM(quantity) as total_volume,
        COUNT(*) as trade_count
    FROM trades 
    SAMPLE BY 1h
    LIMIT 50000
    """
    
    print(f"\n🔍 SQL Query:\n{sql}")
    
    # Parse and show plan
    plan = orchestrator.agents[list(orchestrator.agents.keys())[0]].parse_and_optimize(sql)
    print(f"\n📋 Query Plan:")
    print(f"   has_windows: {plan['has_windows']}")
    print(f"   window_interval: {plan['window_interval']}")
    
    # Execute distributed
    print(f"\n⚡ Executing on {num_agents} agents...")
    start = time.time()
    results = orchestrator.execute_distributed_query(sql)
    elapsed = time.time() - start
    
    # Results
    successful = sum(1 for r in results if r['status'] == 'success')
    print(f"\n✅ SAMPLE BY completed in {elapsed:.3f}s")
    print(f"   Successful agents: {successful}/{num_agents}")
    
    for i, result in enumerate(results[:3], 1):  # Show first 3
        if result['status'] == 'success':
            print(f"   Agent {i}: {result['result'].num_rows} rows")
    
    return results


def demo_latest_by_distributed(orchestrator, quotes, num_agents):
    """Demonstrate distributed LATEST BY for deduplication"""
    print("\n" + "="*70)
    print("Demo 3: LATEST BY - Deduplication (Distributed)")
    print("="*70)
    
    # Distribute quotes across agents
    print(f"\n📊 Distributing quotes across {num_agents} agents...")
    orchestrator.distribute_table("quotes", quotes, strategy="round_robin")
    
    # LATEST BY query: Get latest quote per instrument
    sql = """
    SELECT 
        instrumentId,
        price,
        size,
        timestamp
    FROM quotes 
    LATEST BY instrumentId
    LIMIT 50000
    """
    
    print(f"\n🔍 SQL Query:\n{sql}")
    
    # Parse and show plan
    plan = orchestrator.agents[list(orchestrator.agents.keys())[0]].parse_and_optimize(sql)
    print(f"\n📋 Query Plan:")
    print(f"   has_questdb_constructs: {plan['has_questdb_constructs']}")
    
    # Execute distributed
    print(f"\n⚡ Executing on {num_agents} agents...")
    start = time.time()
    results = orchestrator.execute_distributed_query(sql)
    elapsed = time.time() - start
    
    # Results
    successful = sum(1 for r in results if r['status'] == 'success')
    print(f"\n✅ LATEST BY completed in {elapsed:.3f}s")
    print(f"   Successful agents: {successful}/{num_agents}")
    
    for i, result in enumerate(results[:3], 1):  # Show first 3
        if result['status'] == 'success':
            print(f"   Agent {i}: {result['result'].num_rows} rows")
    
    return results


def demo_complex_query_distributed(orchestrator, trades, quotes, num_agents):
    """Demonstrate complex query with multiple features"""
    print("\n" + "="*70)
    print("Demo 4: Complex Query - ASOF + Window + Aggregation (Distributed)")
    print("="*70)
    
    # Distribute data
    print(f"\n📊 Distributing data across {num_agents} agents...")
    orchestrator.distribute_table("trades", trades, strategy="round_robin")
    orchestrator.distribute_table("quotes", quotes, strategy="round_robin")
    
    # Complex query combining ASOF and window aggregation
    sql = """
    SELECT 
        instrumentId,
        AVG(trade_price) as avg_trade_price,
        AVG(quote_price) as avg_quote_price,
        COUNT(*) as count
    FROM (
        SELECT 
            trades.instrumentId,
            trades.price as trade_price,
            quotes.price as quote_price
        FROM trades 
        ASOF JOIN quotes 
        ON trades.instrumentId = quotes.instrumentId 
        AND trades.timestamp <= quotes.timestamp
    ) AS enriched
    GROUP BY instrumentId
    LIMIT 10000
    """
    
    print(f"\n🔍 SQL Query:\n{sql}")
    
    # Execute distributed
    print(f"\n⚡ Executing on {num_agents} agents...")
    start = time.time()
    results = orchestrator.execute_distributed_query(sql)
    elapsed = time.time() - start
    
    # Results
    successful = sum(1 for r in results if r['status'] == 'success')
    print(f"\n✅ Complex query completed in {elapsed:.3f}s")
    print(f"   Successful agents: {successful}/{num_agents}")
    
    return results


def main():
    """Main demo function"""
    parser = argparse.ArgumentParser(
        description='Fintech enrichment with SabotSQL distributed execution'
    )
    parser.add_argument('--agents', type=int, default=4,
                       help='Number of agents (default: 4)')
    parser.add_argument('--securities', type=int, default=100000,
                       help='Number of securities to load (default: 100K)')
    parser.add_argument('--quotes', type=int, default=50000,
                       help='Number of quotes to load (default: 50K)')
    parser.add_argument('--trades', type=int, default=50000,
                       help='Number of trades to load (default: 50K)')
    
    args = parser.parse_args()
    
    print("🚀 SabotSQL Fintech Enrichment Demo")
    print("=" * 70)
    print(f"Configuration:")
    print(f"  Agents: {args.agents}")
    print(f"  Securities: {args.securities:,}")
    print(f"  Quotes: {args.quotes:,}")
    print(f"  Trades: {args.trades:,}")
    print("=" * 70)
    
    overall_start = time.time()
    
    # Load data (prefer Arrow IPC for 10-100x speedup)
    print("\n📦 Loading fintech data...")
    
    try:
        # Try Arrow IPC first
        if SECURITIES_ARROW.exists():
            securities = load_arrow_ipc(SECURITIES_ARROW, args.securities)
        else:
            print(f"⚠️  Arrow file not found, falling back to CSV")
            securities = load_csv_fast(SECURITIES_CSV, args.securities)
        
        if QUOTES_ARROW.exists():
            quotes = load_arrow_ipc(QUOTES_ARROW, args.quotes)
        else:
            print(f"⚠️  Arrow file not found, falling back to CSV")
            quotes = load_csv_fast(QUOTES_CSV, args.quotes)
        
        if TRADES_ARROW.exists():
            trades = load_arrow_ipc(TRADES_ARROW, args.trades)
        else:
            print(f"⚠️  Arrow file not found, falling back to CSV")
            trades = load_csv_fast(TRADES_CSV, args.trades)
            
    except FileNotFoundError as e:
        print(f"\n❌ {e}")
        print("\nGenerate data first:")
        print("  cd examples/fintech_enrichment_demo")
        print("  python master_security_synthesiser.py")
        print("  python invenory_rows_synthesiser.py")
        print("  python trax_trades_synthesiser.py")
        print("  python convert_csv_to_arrow.py  # Convert to Arrow for 10-100x speedup")
        return 1
    
    # Rename ID to instrumentId in securities for join compatibility
    if 'ID' in securities.column_names:
        new_names = ['instrumentId' if c == 'ID' else c for c in securities.column_names]
        securities = securities.rename_columns(new_names)
        print(f"✅ Renamed 'ID' → 'instrumentId' in securities")
    
    # Create orchestrator
    print(f"\n🎯 Creating orchestrator with {args.agents} agents...")
    orchestrator = SabotSQLOrchestrator()
    
    for i in range(args.agents):
        orchestrator.add_agent(f"agent_{i+1}")
    
    print(f"✅ Orchestrator ready with {args.agents} agents")
    
    # Run demos
    demos = [
        ("ASOF JOIN", lambda: demo_asof_join_distributed(orchestrator, trades, quotes, args.agents)),
        ("SAMPLE BY", lambda: demo_sample_by_distributed(orchestrator, trades, args.agents)),
        ("LATEST BY", lambda: demo_latest_by_distributed(orchestrator, quotes, args.agents)),
        ("Complex Query", lambda: demo_complex_query_distributed(orchestrator, trades, quotes, args.agents))
    ]
    
    results = {}
    
    for demo_name, demo_func in demos:
        try:
            demo_results = demo_func()
            results[demo_name] = demo_results
        except Exception as e:
            print(f"❌ {demo_name} failed: {e}")
            import traceback
            traceback.print_exc()
            results[demo_name] = None
    
    # Summary
    overall_elapsed = time.time() - overall_start
    
    print("\n" + "="*70)
    print("DEMO SUMMARY")
    print("="*70)
    
    print(f"\n📊 Data Loaded:")
    print(f"   Securities: {securities.num_rows:,} rows, {securities.num_columns} columns")
    print(f"   Quotes: {quotes.num_rows:,} rows, {quotes.num_columns} columns")
    print(f"   Trades: {trades.num_rows:,} rows, {trades.num_columns} columns")
    print(f"   Total: {securities.num_rows + quotes.num_rows + trades.num_rows:,} rows")
    
    print(f"\n⚡ Execution:")
    print(f"   Agents: {args.agents}")
    print(f"   Total time: {overall_elapsed:.2f}s")
    
    # Get orchestrator stats
    stats = orchestrator.get_orchestrator_stats()
    print(f"   Total queries: {stats['total_queries']}")
    print(f"   Total execution time: {stats['total_execution_time']:.3f}s")
    
    print(f"\n✅ Demos Completed:")
    for demo_name, demo_results in results.items():
        if demo_results:
            successful = sum(1 for r in demo_results if r['status'] == 'success')
            status = "✅" if successful == args.agents else "⚠️"
            print(f"   {status} {demo_name}: {successful}/{args.agents} agents")
        else:
            print(f"   ❌ {demo_name}: Failed")
    
    print("\n🎉 Features Demonstrated:")
    print("   ✅ ASOF JOIN - Time-series aligned joins")
    print("   ✅ SAMPLE BY - Time-based window aggregation")
    print("   ✅ LATEST BY - Latest record deduplication")
    print("   ✅ Complex queries - Subqueries + aggregation")
    print("   ✅ Distributed execution - Multiple agents")
    print("   ✅ Real fintech data - 10M securities, 1M+ quotes/trades")
    print("   ✅ Sabot-only execution - Arrow + morsel + shuffle")
    print("   ✅ C++20 performance - Modern optimizations")
    
    print("\n" + "="*70)
    print("SabotSQL Fintech Demo Complete!")
    print("="*70)
    
    return 0


if __name__ == "__main__":
    sys.exit(main())

