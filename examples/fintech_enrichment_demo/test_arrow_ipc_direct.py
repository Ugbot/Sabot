#!/usr/bin/env python3
"""
Direct Arrow IPC Performance Test - No Sabot Dependencies

Tests the ArrowIPCReader directly on fintech demo files.
"""

import time
import sys
import importlib.util
from pathlib import Path

# Import pyarrow FIRST to load Arrow symbols
import pyarrow as pa
import pyarrow.compute as pc

# Now direct import of compiled ipc_reader module (bypass sabot/__init__.py)
spec = importlib.util.spec_from_file_location(
    "ipc_reader",
    "/Users/bengamble/Sabot/sabot/_c/ipc_reader.cpython-311-darwin.so"
)
ipc_reader_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ipc_reader_module)

ArrowIPCReader = ipc_reader_module.ArrowIPCReader

def load_arrow_file(filepath, limit_rows=None, columns=None):
    """Load Arrow IPC file using streaming reader."""
    start = time.perf_counter()

    reader = ArrowIPCReader(str(filepath))
    batches = reader.read_batches(limit_rows=limit_rows if limit_rows else -1)
    table = pa.Table.from_batches(batches)

    if columns:
        table = table.select(columns)

    elapsed = time.perf_counter() - start

    return table, elapsed, reader

def main():
    # Find Arrow files
    data_dir = Path('/Users/bengamble/Sabot/examples/fintech_enrichment_demo')

    # Use actual files
    securities_arrow = data_dir / 'master_security_10m.arrow'
    inventory_arrow = data_dir / 'synthetic_inventory.arrow'
    trades_arrow = data_dir / 'trax_trades_1m.arrow'

    print("=" * 80)
    print("Arrow IPC Direct Performance Test")
    print("=" * 80)
    print()

    # Test 1: Load 100K securities
    print("Test 1: Load 100K securities from master_security_10m.arrow")
    print("-" * 80)

    securities, load_time, reader = load_arrow_file(securities_arrow, limit_rows=100000)
    throughput = securities.num_rows / load_time / 1e6

    print(f"✅ Loaded {securities.num_rows:,} rows in {load_time*1000:.1f}ms")
    print(f"   Throughput: {throughput:.1f}M rows/sec")
    print(f"   Columns: {securities.num_columns}")
    print(f"   Total batches in file: {reader.num_batches}")
    print(f"   Schema: {securities.schema}")
    print()

    # Test 2: Load 10K inventory
    print("Test 2: Load 10K inventory from synthetic_inventory.arrow")
    print("-" * 80)

    inventory, load_time, reader = load_arrow_file(inventory_arrow, limit_rows=10000)
    throughput = inventory.num_rows / load_time / 1e6

    print(f"✅ Loaded {inventory.num_rows:,} rows in {load_time*1000:.1f}ms")
    print(f"   Throughput: {throughput:.1f}M rows/sec")
    print(f"   Columns: {inventory.num_columns}")
    print(f"   Total batches in file: {reader.num_batches}")
    print(f"   Schema: {inventory.schema}")
    print()

    # Test 3: Load 100K trades
    print("Test 3: Load 100K trades from trax_trades_1m.arrow")
    print("-" * 80)

    trades, load_time, reader = load_arrow_file(trades_arrow, limit_rows=100000)
    throughput = trades.num_rows / load_time / 1e6

    print(f"✅ Loaded {trades.num_rows:,} rows in {load_time*1000:.1f}ms")
    print(f"   Throughput: {throughput:.1f}M rows/sec")
    print(f"   Columns: {trades.num_columns}")
    print(f"   Total batches in file: {reader.num_batches}")
    print(f"   Schema: {trades.schema}")
    print()

    # Test 4: Load FULL 10M securities (the big one!)
    print("Test 4: Load FULL 10M securities (2.4GB file)")
    print("-" * 80)

    start = time.perf_counter()
    securities_full, load_time, reader = load_arrow_file(securities_arrow)
    throughput = securities_full.num_rows / load_time / 1e6

    print(f"✅ Loaded ALL securities: {securities_full.num_rows:,} rows in {load_time:.2f}s")
    print(f"   Throughput: {throughput:.1f}M rows/sec")
    print(f"   Batches: {reader.num_batches}")
    print(f"   File size: 2.4GB")
    print()

    # Test 5: Load full inventory
    print("Test 5: Load FULL inventory (63MB file)")
    print("-" * 80)

    inventory_full, load_time, reader = load_arrow_file(inventory_arrow)
    throughput = inventory_full.num_rows / load_time / 1e6

    print(f"✅ Loaded ALL inventory: {inventory_full.num_rows:,} rows in {load_time*1000:.1f}ms")
    print(f"   Throughput: {throughput:.1f}M rows/sec")
    print(f"   Batches: {reader.num_batches}")
    print()

    # Test 6: Load full trades
    print("Test 6: Load FULL trades (360MB file)")
    print("-" * 80)

    trades_full, load_time, reader = load_arrow_file(trades_arrow)
    throughput = trades_full.num_rows / load_time / 1e6

    print(f"✅ Loaded ALL trades: {trades_full.num_rows:,} rows in {load_time:.2f}s")
    print(f"   Throughput: {throughput:.1f}M rows/sec")
    print(f"   Batches: {reader.num_batches}")
    print()

    # Total dataset summary
    total_rows = securities_full.num_rows + inventory_full.num_rows + trades_full.num_rows
    print("=" * 80)
    print(f"📊 Total dataset: {total_rows:,} rows across 3 files")
    print(f"   Securities: {securities_full.num_rows:,} rows (2.4GB)")
    print(f"   Inventory: {inventory_full.num_rows:,} rows (63MB)")
    print(f"   Trades: {trades_full.num_rows:,} rows (360MB)")
    print("=" * 80)
    print()

    print("✅ All tests complete!")

if __name__ == '__main__':
    main()
