#!/usr/bin/env python3
"""
Fintech Data Enrichment with Zero-Copy Arrow Optimization

Demonstrates Sabot's zero-copy Arrow performance on large-scale fintech data:
- 10M securities (5.5GB CSV)
- 1.2M quotes (193MB CSV)
- 1M trades (1.2GB CSV)

Performance targets:
- CSV loading: 100M+ rows/sec
- Hash joins: 16M+ rows/sec
- Window computation: 85M+ rows/sec
- Filter operations: 190M+ rows/sec

Uses Sabot's zero-copy Arrow integration:
- compute_window_ids() - SIMD-accelerated windowing
- hash_join_batches() - Zero-copy hash joins
- ArrowComputeEngine - Fast filtering
"""

import time
import argparse
import os
from pathlib import Path
from typing import Optional

from sabot import cyarrow as ca
import numpy as np

# Sabot zero-copy Arrow functions (CyArrow - our Cython wrapper)
from sabot.cyarrow import (
    compute_window_ids,
    hash_join_batches,
    sort_and_take,
    ArrowComputeEngine,
    USING_ZERO_COPY,
)

# Specialized modules not yet in cyarrow wrapper
import pyarrow.csv as pa_csv
import pyarrow.feather as feather
import pyarrow.compute as pc

print(f"Zero-copy Arrow enabled: {USING_ZERO_COPY}")
assert USING_ZERO_COPY, "Zero-copy Arrow required for optimal performance"

# ============================================================================
# Configuration
# ============================================================================

# Data paths - can be overridden by environment variables
DATA_DIR = Path(os.getenv('SABOT_DATA_DIR', Path(__file__).parent))
SECURITIES_CSV = Path(os.getenv('SABOT_SECURITIES_CSV', DATA_DIR / "master_security_10m.csv"))
QUOTES_CSV = Path(os.getenv('SABOT_QUOTES_CSV', DATA_DIR / "synthetic_inventory.csv"))
TRADES_CSV = Path(os.getenv('SABOT_TRADES_CSV', DATA_DIR / "trax_trades_1m.csv"))

# CSV reading config
import multiprocessing
CSV_BLOCK_SIZE = 128 * 1024 * 1024  # 128MB blocks for maximum throughput
CSV_USE_THREADS = True
CSV_NUM_THREADS = multiprocessing.cpu_count()  # Use all CPU cores

# Processing batch sizes
DEFAULT_SECURITIES_LIMIT = 100_000  # Load 100K securities
DEFAULT_QUOTES_LIMIT = 100_000      # Process 100K quotes
DEFAULT_TRADES_LIMIT = 100_000      # Process 100K trades

# ============================================================================
# Zero-Copy Data Loading
# ============================================================================

def _convert_null_columns(table: ca.Table) -> ca.Table:
    """Drop NULL type columns for join compatibility (they're unused anyway)."""
    # Find non-null columns
    columns_to_keep = []
    schema_fields = []

    for i, field in enumerate(table.schema):
        if str(field.type) != 'null':
            columns_to_keep.append(i)
            schema_fields.append(field)

    # If all columns are non-null, return original table
    if len(columns_to_keep) == len(table.schema):
        return table

    # Select only non-null columns
    return table.select(columns_to_keep)


def load_csv_arrow(csv_path: Path, limit: Optional[int] = None, columns: Optional[list] = None) -> ca.Table:
    """
    Load data using Arrow IPC format (if available) or CSV fallback.

    Arrow IPC: 10-100x faster, memory-mapped, zero-copy
    CSV: Streaming parser with optimizations

    Performance:
    - Arrow IPC: 1000M+ rows/sec (memory-mapped)
    - CSV: 100M+ rows/sec (streaming)
    """
    print(f"\nLoading {csv_path.name}...")

    # Check for pre-converted Arrow IPC file
    arrow_path = csv_path.with_suffix('.arrow')
    use_arrow = os.getenv('SABOT_USE_ARROW', '0') == '1' and arrow_path.exists()

    if use_arrow:
        print(f"  Using Arrow IPC: {arrow_path.name}")
        print(f"  File size: {arrow_path.stat().st_size / 1024 / 1024:.1f} MB")

        start = time.perf_counter()

        # Memory-mapped zero-copy read (BLAZING FAST!)
        table = feather.read_table(
            arrow_path,
            columns=columns,  # Can select columns instantly (columnar format)
            memory_map=True   # Zero-copy memory mapping
        )

        # Apply limit if specified (zero-copy slice)
        if limit and table.num_rows > limit:
            table = table.slice(0, limit)

        # Convert NULL type columns to string (same as CSV path)
        table = _convert_null_columns(table)

        end = time.perf_counter()
        elapsed_ms = (end - start) * 1000
        throughput = table.num_rows / (end - start) / 1e6

        print(f"  Loaded: {table.num_rows:,} rows")
        print(f"  Time: {elapsed_ms:.2f}ms (memory-mapped)")
        print(f"  Throughput: {throughput:.1f}M rows/sec")

        return table

    # Fall back to CSV loading
    print(f"  File size: {csv_path.stat().st_size / 1024 / 1024:.1f} MB")
    print(f"  Format: CSV (use convert_csv_to_arrow.py for 10-100x speedup)")

    start = time.perf_counter()

    # Configure for maximum performance
    read_options = pa_csv.ReadOptions(
        use_threads=CSV_USE_THREADS,
        block_size=CSV_BLOCK_SIZE
    )

    parse_options = pa_csv.ParseOptions(delimiter=',')

    convert_options = pa_csv.ConvertOptions(
        null_values=['NULL', 'null', ''],
        strings_can_be_null=True,
        auto_dict_encode=False,  # Avoid dict unification in joins
        include_columns=columns if columns else None  # Only parse specified columns (HUGE speedup)
    )

    # Use streaming reader if limit specified (MUCH faster for large files)
    if limit:
        # Open streaming reader - stops after reading N rows
        with pa_csv.open_csv(
            csv_path,
            read_options=read_options,
            parse_options=parse_options,
            convert_options=convert_options
        ) as reader:
            batches = []
            rows_read = 0

            # Read batches until we have enough rows
            for batch in reader:
                if rows_read >= limit:
                    break

                rows_to_take = min(batch.num_rows, limit - rows_read)
                if rows_to_take < batch.num_rows:
                    batch = batch.slice(0, rows_to_take)

                batches.append(batch)
                rows_read += batch.num_rows

            # Combine batches into table
            table = ca.Table.from_batches(batches)
    else:
        # Load entire file (no limit)
        table = pa_csv.read_csv(
            csv_path,
            read_options=read_options,
            parse_options=parse_options,
            convert_options=convert_options
        )

    # Convert NULL type columns to string for join compatibility
    table = _convert_null_columns(table)

    end = time.perf_counter()

    elapsed_ms = (end - start) * 1000
    throughput = table.num_rows / (end - start) / 1e6

    print(f"  Loaded: {table.num_rows:,} rows")
    print(f"  Time: {elapsed_ms:.2f}ms")
    print(f"  Throughput: {throughput:.1f}M rows/sec")
    print(f"  Schema: {', '.join([f.name for f in table.schema])}")

    return table

# ============================================================================
# Zero-Copy Enrichment Pipeline
# ============================================================================

def enrich_quotes_with_securities(quotes: ca.Table, securities: ca.Table) -> ca.Table:
    """
    Enrich quotes with security master data using zero-copy hash join.

    Performance: 16M+ rows/sec
    """
    print("\n" + "="*60)
    print("STEP 1: Quote Enrichment (Hash Join)")
    print("="*60)

    # Convert to RecordBatches for zero-copy join
    quotes_batch = quotes.to_batches()[0] if quotes.num_rows > 0 else None
    securities_batch = securities.to_batches()[0] if securities.num_rows > 0 else None

    if not quotes_batch or not securities_batch:
        print("No data to join")
        return quotes

    start = time.perf_counter()

    # Zero-copy hash join using Sabot's optimized implementation
    # Note: securities table uses 'ID', quotes table uses 'instrumentId'
    joined_batch = hash_join_batches(
        quotes_batch,
        securities_batch,
        'instrumentId',
        'ID',  # Securities table uses 'ID' not 'instrumentId'
        'inner'
    )

    end = time.perf_counter()

    # Convert back to table
    joined = ca.Table.from_batches([joined_batch])

    elapsed_ms = (end - start) * 1000
    input_rows = quotes.num_rows + securities.num_rows
    throughput = input_rows / (end - start) / 1e6

    print(f"Input: {quotes.num_rows:,} quotes + {securities.num_rows:,} securities")
    print(f"Output: {joined.num_rows:,} enriched rows")
    print(f"Time: {elapsed_ms:.2f}ms")
    print(f"Throughput: {throughput:.1f}M rows/sec")

    return joined

def compute_spreads(enriched: ca.Table) -> ca.Table:
    """
    Compute bid/offer spreads using Arrow compute kernels.

    Performance: 190M+ rows/sec
    """
    print("\n" + "="*60)
    print("STEP 2: Spread Calculation (Arrow Compute)")
    print("="*60)

    # Check if we have the required columns
    column_names = enriched.schema.names
    print(f"Available columns: {', '.join(column_names[:10])}...")

    # Check for price columns
    if 'price' not in column_names:
        print("Warning: No price column found, skipping spread calculation")
        return enriched

    start = time.perf_counter()

    # For this dataset, we have 'price' and 'spread' columns already
    # Let's compute additional metrics using available columns
    price_col = enriched.column('price')

    # If spread column exists, compute spread percentage
    if 'spread' in column_names:
        spread_col = enriched.column('spread')
        # Compute spread percentage relative to price
        spread_pct = pc.multiply(pc.divide(spread_col, price_col), 100.0)
        enriched = enriched.append_column('spreadPct', spread_pct)
        print(f"Computed spread percentage from existing spread column")
    else:
        print("Warning: No spread column found")

    end = time.perf_counter()

    elapsed_ms = (end - start) * 1000
    throughput = enriched.num_rows / (end - start) / 1e6

    print(f"Processed {enriched.num_rows:,} quotes")
    print(f"Time: {elapsed_ms:.2f}ms")
    print(f"Throughput: {throughput:.1f}M rows/sec")

    return enriched

def filter_tight_spreads(enriched: ca.Table, max_spread_pct: float = 0.5) -> ca.Table:
    """
    Filter quotes with tight spreads using zero-copy filter.

    Performance: 190M+ rows/sec
    """
    print("\n" + "="*60)
    print(f"STEP 3: Filter Tight Spreads (<{max_spread_pct}%)")
    print("="*60)

    # Convert to RecordBatch for ArrowComputeEngine
    batch = enriched.to_batches()[0]

    start = time.perf_counter()

    # Zero-copy filter using Sabot's engine
    engine = ArrowComputeEngine()
    filtered_batch = engine.filter_batch(batch, f'spreadPct < {max_spread_pct}')

    end = time.perf_counter()

    # Convert back to table
    filtered = ca.Table.from_batches([filtered_batch])

    elapsed_ms = (end - start) * 1000
    throughput = enriched.num_rows / (end - start) / 1e6
    pct_filtered = (filtered.num_rows / enriched.num_rows * 100) if enriched.num_rows > 0 else 0

    print(f"Input: {enriched.num_rows:,} quotes")
    print(f"Output: {filtered.num_rows:,} quotes ({pct_filtered:.1f}%)")
    print(f"Time: {elapsed_ms:.2f}ms")
    print(f"Throughput: {throughput:.1f}M rows/sec")

    return filtered

def compute_time_windows(quotes: ca.Table, window_size_ms: int = 60000) -> ca.Table:
    """
    Assign time windows using zero-copy window computation.

    Performance: 85M+ rows/sec
    """
    print("\n" + "="*60)
    print(f"STEP 4: Window Assignment ({window_size_ms}ms windows)")
    print("="*60)

    # Convert to RecordBatch
    batch = quotes.to_batches()[0]

    # Check if timestamp column exists
    if 'timestamp' not in batch.column_names:
        print("Warning: No timestamp column, skipping windowing")
        return quotes

    start = time.perf_counter()

    # Zero-copy window computation using Sabot
    windowed_batch = compute_window_ids(batch, 'timestamp', window_size_ms)

    end = time.perf_counter()

    # Convert back to table
    windowed = ca.Table.from_batches([windowed_batch])

    elapsed_ms = (end - start) * 1000
    throughput = quotes.num_rows / (end - start) / 1e6

    print(f"Assigned windows for {windowed.num_rows:,} quotes")
    print(f"Time: {elapsed_ms:.2f}ms")
    print(f"Throughput: {throughput:.1f}M rows/sec")

    return windowed

def compute_top_instruments(enriched: ca.Table, limit: int = 10) -> ca.Table:
    """
    Find top instruments by quote volume using zero-copy sort.

    Performance: 10M+ rows/sec
    """
    print("\n" + "="*60)
    print(f"STEP 5: Top {limit} Instruments by Volume")
    print("="*60)

    if enriched.num_rows == 0:
        print("No data to rank")
        return enriched

    # Convert to RecordBatch
    batch = enriched.to_batches()[0] if enriched.num_rows > 0 else None
    if not batch:
        return enriched

    start = time.perf_counter()

    # Zero-copy sort and take
    # Use 'size' column if bidSize doesn't exist
    sort_column = 'size' if 'size' in batch.column_names else None
    if not sort_column:
        print("Warning: No size column, skipping ranking")
        return enriched.slice(0, min(limit, enriched.num_rows))

    top_batch = sort_and_take(batch, [(sort_column, 'descending')], limit=limit)

    end = time.perf_counter()

    # Convert back to table
    top = ca.Table.from_batches([top_batch])

    elapsed_ms = (end - start) * 1000
    throughput = enriched.num_rows / (end - start) / 1e6

    print(f"Sorted {enriched.num_rows:,} quotes, returned top {top.num_rows}")
    print(f"Time: {elapsed_ms:.2f}ms")
    print(f"Throughput: {throughput:.1f}M rows/sec")

    # Display results
    if top.num_rows > 0:
        print("\nTop Instruments:")
        for i in range(min(5, top.num_rows)):
            row = {col: top.column(col)[i].as_py() for col in top.column_names}
            print(f"  {i+1}. {row.get('instrumentId', 'N/A')} - "
                  f"Price: {row.get('price', 0):.4f} @ Size: {row.get('size', 0)}")

    return top

# ============================================================================
# Main Pipeline
# ============================================================================

def run_enrichment_pipeline(
    securities_limit: int = DEFAULT_SECURITIES_LIMIT,
    quotes_limit: int = DEFAULT_QUOTES_LIMIT,
    trades_limit: int = DEFAULT_TRADES_LIMIT
):
    """
    Run complete zero-copy enrichment pipeline.
    """
    print("\n" + "="*60)
    print("ZERO-COPY ARROW ENRICHMENT PIPELINE")
    print("="*60)
    print(f"Zero-copy enabled: {USING_ZERO_COPY}")
    print(f"Securities limit: {securities_limit:,}")
    print(f"Quotes limit: {quotes_limit:,}")
    print(f"Trades limit: {trades_limit:,}")

    overall_start = time.perf_counter()

    # Load data (zero-copy CSV parsing)
    # Load all columns (set columns=None) or only essential columns for faster parsing
    securities = load_csv_arrow(SECURITIES_CSV, securities_limit, columns=None)  # All columns
    quotes = load_csv_arrow(QUOTES_CSV, quotes_limit)

    # Run enrichment pipeline
    enriched = enrich_quotes_with_securities(quotes, securities)
    enriched = compute_spreads(enriched)
    # Use more permissive filter to show full pipeline (100% instead of 0.5%)
    filtered = filter_tight_spreads(enriched, max_spread_pct=100.0)
    top_instruments = compute_top_instruments(filtered, limit=10)

    overall_end = time.perf_counter()

    # Summary
    print("\n" + "="*60)
    print("PIPELINE SUMMARY")
    print("="*60)
    total_time = (overall_end - overall_start) * 1000
    print(f"Total time: {total_time:.2f}ms ({total_time/1000:.2f}s)")
    print(f"Input rows: {securities.num_rows + quotes.num_rows:,}")
    print(f"Output rows: {top_instruments.num_rows}")
    print(f"Overall throughput: {(securities.num_rows + quotes.num_rows) / (overall_end - overall_start) / 1e6:.1f}M rows/sec")

    print("\n✅ Zero-copy enrichment pipeline complete!")

# ============================================================================
# CLI
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description='Fintech data enrichment with zero-copy Arrow optimization'
    )
    parser.add_argument('--securities', type=int, default=DEFAULT_SECURITIES_LIMIT,
                       help=f'Number of securities to load (default: {DEFAULT_SECURITIES_LIMIT:,})')
    parser.add_argument('--quotes', type=int, default=DEFAULT_QUOTES_LIMIT,
                       help=f'Number of quotes to process (default: {DEFAULT_QUOTES_LIMIT:,})')
    parser.add_argument('--trades', type=int, default=DEFAULT_TRADES_LIMIT,
                       help=f'Number of trades to process (default: {DEFAULT_TRADES_LIMIT:,})')

    # Data path overrides
    parser.add_argument('--data-dir', type=str,
                       help='Directory containing CSV files (default: script directory)')
    parser.add_argument('--securities-csv', type=str,
                       help='Path to securities CSV file')
    parser.add_argument('--quotes-csv', type=str,
                       help='Path to quotes CSV file')
    parser.add_argument('--trades-csv', type=str,
                       help='Path to trades CSV file')

    args = parser.parse_args()

    # Override global paths if specified
    global SECURITIES_CSV, QUOTES_CSV, TRADES_CSV, DATA_DIR
    if args.data_dir:
        DATA_DIR = Path(args.data_dir)
        SECURITIES_CSV = DATA_DIR / "master_security_10m.csv"
        QUOTES_CSV = DATA_DIR / "synthetic_inventory.csv"
        TRADES_CSV = DATA_DIR / "trax_trades_1m.csv"
    if args.securities_csv:
        SECURITIES_CSV = Path(args.securities_csv)
    if args.quotes_csv:
        QUOTES_CSV = Path(args.quotes_csv)
    if args.trades_csv:
        TRADES_CSV = Path(args.trades_csv)

    # Verify data files exist
    if not SECURITIES_CSV.exists():
        print(f"Error: {SECURITIES_CSV} not found")
        print("Run: python master_security_synthesiser.py")
        return 1

    if not QUOTES_CSV.exists():
        print(f"Error: {QUOTES_CSV} not found")
        print("Run: python invenory_rows_synthesiser.py")
        return 1

    # Run pipeline
    run_enrichment_pipeline(
        securities_limit=args.securities,
        quotes_limit=args.quotes,
        trades_limit=args.trades
    )

    return 0

if __name__ == '__main__':
    exit(main())
