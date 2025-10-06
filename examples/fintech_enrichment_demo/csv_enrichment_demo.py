#!/usr/bin/env python3
"""
Fintech CSV Data Enrichment Demo

Reads CSV files directly and performs stream joins and ranking using Sabot/Arrow.
No Kafka required - processes data directly from CSV files.

Usage:
    python csv_enrichment_demo.py [options]

    Options:
        --csv-block-size SIZE    CSV read block size in bytes (default: 8MB)
        --csv-threads            Enable multi-threaded CSV reading (default: True)
        --join-threads           Enable multi-threaded joins (default: True)
        --batch-securities N     Number of securities to load (default: 100000)
        --batch-quotes N         Number of quotes to load (default: 50000)
        --batch-trades N         Number of trades to load (default: 50000)
"""

import asyncio
import argparse
import time
from pathlib import Path
from typing import Optional
from sabot import cyarrow as ca
import pyarrow as pa  # For specialized types
import pyarrow.csv as pa_csv
import pyarrow.compute as pc  # For compute functions

# ============================================================================
# Performance Tuning Parameters (Native PyArrow for maximum speed)
# ============================================================================

# CSV Reading (PyArrow native C++ parser)
CSV_BLOCK_SIZE = 8 * 1024 * 1024  # 8MB blocks (default 1MB) - larger = more parallelism
CSV_USE_THREADS = True             # Multi-threaded CSV parsing

# Join Operations
JOIN_USE_THREADS = True            # Multi-threaded hash table construction

# Batch Sizes (how many rows to load from each CSV)
BATCH_SIZE_SECURITIES = 100000
BATCH_SIZE_QUOTES = 50000
BATCH_SIZE_TRADES = 50000


def load_csv_native(csv_path: Path, limit: Optional[int] = None) -> pa.Table:
    """
    Load CSV file using PyArrow's native C++ parser for maximum performance.

    This is 5-10x faster than Python's csv module because:
    - Native C++ implementation with SIMD optimizations
    - Multi-threaded parsing (if CSV_USE_THREADS=True)
    - Block-based parallel reading
    - Automatic type inference

    Args:
        csv_path: Path to CSV file
        limit: Maximum number of rows to read (None = read all)

    Returns:
        PyArrow Table with data
    """
    # Configure native CSV reader for maximum performance
    read_options = pa_csv.ReadOptions(
        use_threads=CSV_USE_THREADS,
        block_size=CSV_BLOCK_SIZE
    )

    # Parse options - handle NULL strings
    parse_options = pa_csv.ParseOptions(
        delimiter=','
    )

    # Convert options - let Arrow infer types automatically
    # Note: Disable dict encoding to avoid "unifying differing dictionaries" join errors
    convert_options = pa_csv.ConvertOptions(
        null_values=['NULL', 'null', ''],  # Treat these as NULL
        strings_can_be_null=True,
        auto_dict_encode=False  # Disabled for join compatibility
    )

    # Read CSV natively with Arrow
    table = pa_csv.read_csv(
        csv_path,
        read_options=read_options,
        parse_options=parse_options,
        convert_options=convert_options
    )

    # Apply row limit if specified
    if limit is not None and table.num_rows > limit:
        table = table.slice(0, limit)

    # Cast null-type columns to string to avoid join errors
    # (happens when entire column is NULL)
    schema_fields = []
    for field in table.schema:
        if pa.types.is_null(field.type):
            schema_fields.append(pa.field(field.name, pa.string()))
        else:
            schema_fields.append(field)

    if schema_fields != list(table.schema):
        new_schema = pa.schema(schema_fields)
        table = table.cast(new_schema)

    return table


async def main():
    print("\n🚀 Fintech CSV Data Enrichment Demo")
    print("=" * 60)
    print("📊 Configuration:")
    print(f"   CSV Block Size: {CSV_BLOCK_SIZE/1024/1024:.0f}MB")
    print(f"   CSV Multi-threading: {CSV_USE_THREADS}")
    print(f"   Join Multi-threading: {JOIN_USE_THREADS}")
    print(f"   Batch Sizes: Securities={BATCH_SIZE_SECURITIES:,}, Quotes={BATCH_SIZE_QUOTES:,}, Trades={BATCH_SIZE_TRADES:,}")
    print("=" * 60)

    # File paths
    securities_csv = Path("master_security_10m.csv")  # Dimension table (reference data)
    quotes_csv = Path("synthetic_inventory.csv")      # Fact table (transactional data)
    trades_csv = Path("trax_trades_1m.csv")          # Fact table (transactional data)

    # Check files exist
    for f in [securities_csv, quotes_csv, trades_csv]:
        if not f.exists():
            print(f"❌ Error: {f} not found!")
            print(f"   Please run the data generators first")
            return

    print(f"\n📂 Loading CSV files (native PyArrow C++ parser)...")
    print(f"   Note: Securities = dimension table, Quotes/Trades = fact tables")

    # Load securities (reference data) - load more for better coverage
    print(f"   Loading securities from {securities_csv.name}...")
    start = time.time()
    securities_table = load_csv_native(securities_csv, limit=BATCH_SIZE_SECURITIES)
    load_time = time.time() - start
    # Rename ID column to instrumentId for join compatibility
    if 'ID' in securities_table.column_names:
        securities_table = securities_table.rename_columns(['instrumentId' if c == 'ID' else c for c in securities_table.column_names])
    rows_per_sec = securities_table.num_rows / load_time
    print(f"   ✅ Loaded {securities_table.num_rows:,} securities in {load_time:.2f}s ({rows_per_sec:,.0f} rows/sec)")

    # Load quotes
    print(f"   Loading quotes from {quotes_csv.name}...")
    start = time.time()
    quotes_table = load_csv_native(quotes_csv, limit=BATCH_SIZE_QUOTES)
    load_time = time.time() - start
    rows_per_sec = quotes_table.num_rows / load_time
    print(f"   ✅ Loaded {quotes_table.num_rows:,} quotes in {load_time:.2f}s ({rows_per_sec:,.0f} rows/sec)")

    # Load trades
    print(f"   Loading trades from {trades_csv.name}...")
    start = time.time()
    trades_table = load_csv_native(trades_csv, limit=BATCH_SIZE_TRADES)
    load_time = time.time() - start
    rows_per_sec = trades_table.num_rows / load_time
    print(f"   ✅ Loaded {trades_table.num_rows:,} trades in {load_time:.2f}s ({rows_per_sec:,.0f} rows/sec)")

    print(f"\n🔗 Performing stream joins...")

    # Join 1: Enrich quotes with security master data
    print(f"\n   Join 1: Quotes ⋈ Securities (on instrumentId)")
    start = time.time()

    try:
        # Perform inner join on instrumentId with multi-threading
        enriched_quotes = quotes_table.join(
            securities_table,
            keys=['instrumentId'],
            join_type='inner',
            use_threads=JOIN_USE_THREADS
        )
        join1_time = time.time() - start
        print(f"   ✅ Joined {enriched_quotes.num_rows:,} rows in {join1_time:.3f}s")
        print(f"      Columns: {enriched_quotes.num_columns} ({', '.join(enriched_quotes.column_names[:5])}...)")

        # Note: These quotes have a single 'price' field, not bid/offer
        # We'll use price * size as quote value for ranking
        if 'price' in enriched_quotes.column_names and 'size' in enriched_quotes.column_names:
            prices = enriched_quotes.column('price')
            sizes = enriched_quotes.column('size')
            quote_values = pc.multiply(prices, sizes)
            enriched_quotes = enriched_quotes.append_column('quoteValue', quote_values)
            print(f"      ✅ Added quote value calculations (price * size)")

    except Exception as e:
        print(f"   ❌ Join failed: {e}")
        return

    # Join 2: Enrich trades with enriched quotes
    print(f"\n   Join 2: Trades ⋈ Enriched Quotes (on instrumentId)")
    start = time.time()

    try:
        # Join trades with enriched quotes (with multi-threading)
        enriched_trades = trades_table.join(
            enriched_quotes,
            keys=['instrumentId'],
            join_type='inner',
            use_threads=JOIN_USE_THREADS
        )
        join2_time = time.time() - start
        print(f"   ✅ Joined {enriched_trades.num_rows:,} rows in {join2_time:.3f}s")

    except Exception as e:
        print(f"   ❌ Join failed: {e}")
        enriched_trades = None

    print(f"\n📊 Computing analytics and rankings...")

    # Rank securities by quote value
    print(f"\n   Ranking: Securities by total quote value")
    start = time.time()

    # Group by instrumentId and calculate totals
    grouped = enriched_quotes.group_by('instrumentId').aggregate([
        ('quoteValue', 'sum'),
        ('price', 'mean'),
        ('size', 'sum')
    ])

    # Sort by total quote value descending
    sorted_values = grouped.sort_by([('quoteValue_sum', 'descending')])
    rank_time = time.time() - start

    print(f"   ✅ Ranked {sorted_values.num_rows:,} instruments in {rank_time:.3f}s")
    print(f"\n   📈 Top 10 instruments by quote value:")
    print(f"   {'Instrument ID':<20} {'Total Value':>15} {'Avg Price':>12} {'Total Size':>15}")
    print(f"   {'-'*65}")

    for i in range(min(10, sorted_values.num_rows)):
        inst_id = sorted_values.column('instrumentId')[i].as_py()
        total_value = sorted_values.column('quoteValue_sum')[i].as_py()
        avg_price = sorted_values.column('price_mean')[i].as_py()
        total_size = sorted_values.column('size_sum')[i].as_py()
        print(f"   {inst_id:<20} {total_value:>15,.0f} {avg_price:>11.2f} {total_size:>15,.0f}")

    # Rank by trade volume if we have enriched trades
    if enriched_trades and enriched_trades.num_rows > 0:
        print(f"\n   Ranking: Securities by trade volume")
        start = time.time()

        trade_volumes = enriched_trades.group_by('instrumentId').aggregate([
            ('quantity', 'sum'),
            ('price', 'mean')
        ])

        sorted_volumes = trade_volumes.sort_by([('quantity_sum', 'descending')])
        vol_rank_time = time.time() - start

        print(f"   ✅ Ranked {sorted_volumes.num_rows:,} instruments in {vol_rank_time:.3f}s")
        print(f"\n   📈 Top 10 instruments by trade volume:")
        print(f"   {'Instrument ID':<20} {'Total Volume':>15} {'Avg Price':>12}")
        print(f"   {'-'*50}")

        for i in range(min(10, sorted_volumes.num_rows)):
            inst_id = sorted_volumes.column('instrumentId')[i].as_py()
            volume = sorted_volumes.column('quantity_sum')[i].as_py()
            avg_price = sorted_volumes.column('price_mean')[i].as_py()
            print(f"   {inst_id:<20} {volume:>15,.0f} {avg_price:>11.2f}")

    # Market segment analysis
    print(f"\n   Analysis: Quote volume by market segment")
    start = time.time()

    if 'MARKETSEGMENT' in enriched_quotes.column_names:
        segment_analysis = enriched_quotes.group_by('MARKETSEGMENT').aggregate([
            ('quoteValue', 'sum'),
            ('price', 'mean'),
            ('instrumentId', 'count')
        ])

        sorted_segments = segment_analysis.sort_by([('quoteValue_sum', 'descending')])
        seg_time = time.time() - start

        print(f"   ✅ Analyzed {sorted_segments.num_rows} market segments in {seg_time:.3f}s")
        print(f"\n   📊 Market segments ranked by quote volume:")
        print(f"   {'Segment':<15} {'Total Value':>15} {'Avg Price':>12} {'Quotes':>10}")
        print(f"   {'-'*55}")

        for i in range(min(sorted_segments.num_rows, 10)):
            segment = sorted_segments.column('MARKETSEGMENT')[i].as_py()
            total_value = sorted_segments.column('quoteValue_sum')[i].as_py()
            avg_price = sorted_segments.column('price_mean')[i].as_py()
            count = sorted_segments.column('instrumentId_count')[i].as_py()
            print(f"   {segment:<15} {total_value:>15,.0f} {avg_price:>11.2f} {count:>10}")
    else:
        seg_time = 0
        print(f"   ⚠️  MARKETSEGMENT column not found in joined data")

    # Summary
    print(f"\n" + "="*60)
    print(f"🎉 Enrichment Complete!")
    print(f"\n📈 Performance Summary:")
    print(f"   • Securities loaded: {securities_table.num_rows:,}")
    print(f"   • Quotes loaded: {quotes_table.num_rows:,}")
    print(f"   • Trades loaded: {trades_table.num_rows:,}")
    print(f"   • Quote enrichment join: {join1_time:.3f}s ({quotes_table.num_rows/join1_time:,.0f} rows/sec)")
    if enriched_trades:
        print(f"   • Trade enrichment join: {join2_time:.3f}s")
    print(f"   • Value ranking: {rank_time:.3f}s")
    print(f"   • Segment analysis: {seg_time:.3f}s")
    print(f"\n✅ Successfully demonstrated:")
    print(f"   ✓ Native PyArrow CSV loading ({CSV_BLOCK_SIZE/1024/1024:.0f}MB blocks, multi-threaded)")
    print(f"   ✓ Multi-stream joins (dimension ⋈ facts)")
    print(f"   ✓ Computed metrics (quote values)")
    print(f"   ✓ Aggregations and rankings")
    print(f"   ✓ Market segment analysis")
    print("="*60)


def parse_args():
    """Parse command line arguments for performance tuning."""
    parser = argparse.ArgumentParser(
        description='Fintech CSV Data Enrichment Demo with PyArrow',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument('--csv-block-size', type=int, default=CSV_BLOCK_SIZE,
                        help='CSV read block size in bytes')
    parser.add_argument('--no-csv-threads', action='store_false', dest='csv_threads',
                        help='Disable multi-threaded CSV reading')
    parser.add_argument('--no-join-threads', action='store_false', dest='join_threads',
                        help='Disable multi-threaded joins')
    parser.add_argument('--batch-securities', type=int, default=BATCH_SIZE_SECURITIES,
                        help='Number of securities to load')
    parser.add_argument('--batch-quotes', type=int, default=BATCH_SIZE_QUOTES,
                        help='Number of quotes to load')
    parser.add_argument('--batch-trades', type=int, default=BATCH_SIZE_TRADES,
                        help='Number of trades to load')
    return parser.parse_args()


if __name__ == "__main__":
    # Parse CLI arguments and update global config
    args = parse_args()
    CSV_BLOCK_SIZE = args.csv_block_size
    CSV_USE_THREADS = args.csv_threads
    JOIN_USE_THREADS = args.join_threads
    BATCH_SIZE_SECURITIES = args.batch_securities
    BATCH_SIZE_QUOTES = args.batch_quotes
    BATCH_SIZE_TRADES = args.batch_trades

    asyncio.run(main())
