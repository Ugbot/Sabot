#!/usr/bin/env python3
"""Test CyArrow DataLoader"""

import os
from pathlib import Path
from sabot.cyarrow import DataLoader, load_data

# Enable Arrow IPC
os.environ['SABOT_USE_ARROW'] = '1'

print("Testing CyArrow DataLoader\n" + "="*60)

# Initialize loader
loader = DataLoader(num_threads=8)
print(f"Loader initialized successfully")

# Test with Arrow IPC
data_dir = Path('examples/fintech_enrichment_demo')
securities_path = str(data_dir / 'master_security_10m.csv')

print(f"\nLoading securities (Arrow IPC mode)...")
securities = loader.load(securities_path, limit=100_000)
print(f"✅ Loaded {securities.num_rows:,} rows, {securities.num_columns} columns")

# Test convenience function
print(f"\nLoading with convenience function...")
quotes = load_data(str(data_dir / 'synthetic_inventory.csv'), limit=50_000)
print(f"✅ Loaded {quotes.num_rows:,} rows, {quotes.num_columns} columns")

print("\n✅ All tests passed!")
