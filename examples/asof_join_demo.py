#!/usr/bin/env python3
"""
ASOF Join Demo - Time-series joins for financial data.

Demonstrates batch and streaming ASOF joins for common fintech patterns:
1. Trade-Quote matching (join each trade with most recent quote)
2. Multi-source price alignment (sync different exchanges)
3. Reference data enrichment (join with dimension tables)
4. Event correlation (match trades with market events)

ASOF joins are critical for financial data because timestamps are rarely
exact matches. ASOF join finds the "nearest" match within a tolerance.
"""

import numpy as np
import pyarrow as pa
from datetime import datetime, timedelta

print("Importing Sabot fintech kernels...")
try:
    from sabot.fintech import (
        asof_join,
        asof_join_table,
        asof_join_streaming,
        AsofJoinKernel,
    )
    ASOF_AVAILABLE = True
except ImportError as e:
    print(f"⚠️  ASOF join not available: {e}")
    print("Run 'python build.py' to compile Cython extensions")
    ASOF_AVAILABLE = False
    exit(1)


def generate_trades_and_quotes(n_trades=100, n_quotes=500):
    """
    Generate synthetic trade and quote data with realistic timing.
    
    Trades are sparse (every ~50ms)
    Quotes are dense (every ~10ms)
    """
    np.random.seed(42)
    
    # Base timestamp (milliseconds)
    base_ts = int(datetime(2025, 10, 12, 9, 30).timestamp() * 1000)
    
    # Generate quotes (dense)
    quote_timestamps = base_ts + np.sort(np.random.randint(0, 50000, n_quotes))
    quote_symbols = np.random.choice(['AAPL', 'GOOGL', 'MSFT'], n_quotes)
    quote_bids = np.random.uniform(100, 200, n_quotes)
    quote_asks = quote_bids + np.random.uniform(0.01, 0.10, n_quotes)
    quote_bid_sizes = np.random.exponential(1000, n_quotes)
    quote_ask_sizes = np.random.exponential(1000, n_quotes)
    
    quotes = pa.record_batch({
        'timestamp': pa.array(quote_timestamps, type=pa.int64()),
        'symbol': quote_symbols,
        'bid': quote_bids,
        'ask': quote_asks,
        'bid_size': quote_bid_sizes,
        'ask_size': quote_ask_sizes,
    }, names=['timestamp', 'symbol', 'bid', 'ask', 'bid_size', 'ask_size'])
    
    # Generate trades (sparse, between quote times)
    trade_timestamps = base_ts + np.sort(np.random.randint(0, 50000, n_trades))
    trade_symbols = np.random.choice(['AAPL', 'GOOGL', 'MSFT'], n_trades)
    trade_prices = np.random.uniform(100, 200, n_trades)
    trade_volumes = np.random.exponential(500, n_trades)
    trade_sides = np.random.choice([1, -1], n_trades)  # 1=buy, -1=sell
    
    trades = pa.record_batch({
        'timestamp': pa.array(trade_timestamps, type=pa.int64()),
        'symbol': trade_symbols,
        'price': trade_prices,
        'volume': trade_volumes,
        'side': trade_sides,
    }, names=['timestamp', 'symbol', 'price', 'volume', 'side'])
    
    return trades, quotes


def demo_basic_asof_join():
    """Demo 1: Basic ASOF join - trades with quotes."""
    print("\n" + "="*70)
    print("DEMO 1: Basic ASOF Join (Trades → Quotes)")
    print("="*70)
    
    trades, quotes = generate_trades_and_quotes(n_trades=50, n_quotes=200)
    
    print(f"\nData:")
    print(f"  Trades: {trades.num_rows} rows")
    print(f"  Quotes: {quotes.num_rows} rows")
    print(f"  Symbols: AAPL, GOOGL, MSFT")
    
    print(f"\n🔍 Joining each trade with most recent quote (per symbol)...")
    
    # Perform ASOF join
    joined = asof_join(
        trades,
        quotes,
        on='timestamp',
        by='symbol',
        tolerance_ms=1000,  # 1 second tolerance
        direction='backward'
    )
    
    print(f"\n✅ Join complete!")
    print(f"  Output rows: {joined.num_rows}")
    print(f"  Output columns: {joined.schema.names}")
    
    # Show sample results
    print(f"\n📊 Sample joined data (first 5 rows):")
    df = joined.to_pandas().head(5)
    print(df[['timestamp', 'symbol', 'price', 'side', 'bid', 'ask']].to_string(index=False))
    
    # Compute derived features
    print(f"\n💡 Computing derived features...")
    midprice = (df['bid'] + df['ask']) / 2
    spread_bps = ((df['ask'] - df['bid']) / midprice) * 10000
    trade_vs_mid = ((df['price'] - midprice) / midprice) * 10000
    
    print(f"  Average quoted spread: {spread_bps.mean():.2f} bps")
    print(f"  Average trade vs mid: {trade_vs_mid.mean():.2f} bps")
    
    return joined


def demo_asof_join_kernel():
    """Demo 2: Using AsofJoinKernel for incremental joins."""
    print("\n" + "="*70)
    print("DEMO 2: Streaming ASOF Join (Incremental Updates)")
    print("="*70)
    
    print("\n💡 This pattern is critical for real-time systems:")
    print("  - Build index from historical quotes")
    print("  - Process live trades as they arrive")
    print("  - Join each trade with best available quote")
    
    # Generate data
    trades, quotes = generate_trades_and_quotes(n_trades=100, n_quotes=500)
    
    # Create kernel
    print("\n🔧 Initializing ASOF join kernel...")
    kernel = AsofJoinKernel(
        time_column='timestamp',
        by_column='symbol',
        direction='backward',
        tolerance_ms=5000,  # 5 second tolerance
        max_lookback_ms=60000  # 1 minute lookback
    )
    
    # Add quote data (build index)
    print(f"📚 Building quote index ({quotes.num_rows} quotes)...")
    kernel.add_right_batch(quotes)
    
    stats = kernel.get_stats()
    print(f"✓ Index built:")
    print(f"    Symbols: {stats['num_symbols']}")
    print(f"    Total indexed rows: {stats['total_indexed_rows']}")
    
    # Split trades into mini-batches (simulate streaming)
    print(f"\n📊 Processing {trades.num_rows} trades in mini-batches...")
    
    batch_size = 10
    total_joined = 0
    
    for i in range(0, trades.num_rows, batch_size):
        mini_batch = trades.slice(i, min(batch_size, trades.num_rows - i))
        joined = kernel.process_left_batch(mini_batch)
        
        if joined and joined.num_rows > 0:
            total_joined += joined.num_rows
            
            # Show first batch details
            if i == 0:
                print(f"\n  First batch joined ({joined.num_rows} rows):")
                df = joined.to_pandas()
                print(f"    Columns: {list(df.columns)}")
                print(f"    Sample: trade_price={df['price'].iloc[0]:.2f}, quote_bid={df['bid'].iloc[0]:.2f}")
    
    print(f"\n✅ Processed {total_joined} joined rows")
    print(f"   Lookup performance: ~{total_joined / max((trades.num_rows / 1000000), 0.001):.1f} joins/μs")


def demo_multi_exchange_alignment():
    """Demo 3: Align prices from multiple exchanges."""
    print("\n" + "="*70)
    print("DEMO 3: Multi-Exchange Price Alignment")
    print("="*70)
    
    print("\n💡 Use case: Sync BTC prices from Binance, Coinbase, Kraken")
    print("  Different exchanges have different tick times")
    print("  ASOF join creates time-aligned view")
    
    # Generate price data from 3 "exchanges"
    np.random.seed(42)
    base_ts = int(datetime.now().timestamp() * 1000)
    n = 200
    
    # Exchange A (Binance) - high frequency
    ts_a = base_ts + np.sort(np.random.randint(0, 10000, n))
    px_a = 50000 + np.cumsum(np.random.normal(0, 50, n))
    
    # Exchange B (Coinbase) - medium frequency
    ts_b = base_ts + np.sort(np.random.randint(0, 10000, n//2))
    px_b = 50000 + np.cumsum(np.random.normal(0, 50, n//2))
    
    # Exchange C (Kraken) - low frequency
    ts_c = base_ts + np.sort(np.random.randint(0, 10000, n//3))
    px_c = 50000 + np.cumsum(np.random.normal(0, 50, n//3))
    
    exchange_a = pa.record_batch({
        'timestamp': pa.array(ts_a, type=pa.int64()),
        'symbol': pa.array(['BTC'] * len(ts_a)),
        'price_a': px_a,
    })
    
    exchange_b = pa.record_batch({
        'timestamp': pa.array(ts_b, type=pa.int64()),
        'symbol': pa.array(['BTC'] * len(ts_b)),
        'price_b': px_b,
    })
    
    exchange_c = pa.record_batch({
        'timestamp': pa.array(ts_c, type=pa.int64()),
        'symbol': pa.array(['BTC'] * len(ts_c)),
        'price_c': px_c,
    })
    
    print(f"\nExchange data:")
    print(f"  Binance: {exchange_a.num_rows} ticks")
    print(f"  Coinbase: {exchange_b.num_rows} ticks")
    print(f"  Kraken: {exchange_c.num_rows} ticks")
    
    # Join A with B
    print(f"\n🔗 Joining Binance ← Coinbase...")
    ab_joined = asof_join(
        exchange_a,
        exchange_b,
        on='timestamp',
        by='symbol',
        tolerance_ms=2000
    )
    
    # Join AB with C
    print(f"🔗 Joining (Binance+Coinbase) ← Kraken...")
    abc_joined = asof_join(
        ab_joined,
        exchange_c,
        on='timestamp',
        by='symbol',
        tolerance_ms=2000
    )
    
    print(f"\n✅ Multi-exchange alignment complete!")
    print(f"  Aligned rows: {abc_joined.num_rows}")
    print(f"  Columns: {abc_joined.schema.names}")
    
    # Compute cross-exchange stats
    df = abc_joined.to_pandas()
    
    # Filter rows where all exchanges have data
    complete_rows = df.dropna(subset=['price_a', 'price_b', 'price_c'])
    
    if len(complete_rows) > 0:
        print(f"\n📊 Cross-exchange statistics:")
        print(f"  Complete rows: {len(complete_rows)}/{len(df)}")
        print(f"  Avg Binance-Coinbase spread: ${np.mean(np.abs(complete_rows['price_a'] - complete_rows['price_b'])):.2f}")
        print(f"  Avg Binance-Kraken spread: ${np.mean(np.abs(complete_rows['price_a'] - complete_rows['price_c'])):.2f}")
        print(f"  Max 3-way spread: ${np.max([complete_rows['price_a'].max(), complete_rows['price_b'].max(), complete_rows['price_c'].max()]) - np.min([complete_rows['price_a'].min(), complete_rows['price_b'].min(), complete_rows['price_c'].min()]):.2f}")


def demo_asof_with_reference_data():
    """Demo 4: Join streaming trades with static reference table."""
    print("\n" + "="*70)
    print("DEMO 4: Stream-Table ASOF Join (Reference Data)")
    print("="*70)
    
    print("\n💡 Pattern: Join live trades with historical VWAP benchmarks")
    
    # Generate historical VWAP data (static table)
    np.random.seed(42)
    base_ts = int(datetime.now().timestamp() * 1000)
    n_vwap = 100
    
    vwap_timestamps = base_ts + np.arange(0, 60000, 600)  # Every 600ms
    vwap_symbols = np.tile(['AAPL', 'GOOGL', 'MSFT'], len(vwap_timestamps)//3 + 1)[:len(vwap_timestamps)]
    vwap_prices = np.random.uniform(100, 200, len(vwap_timestamps))
    vwap_volumes = np.random.exponential(10000, len(vwap_timestamps))
    
    vwap_table = pa.Table.from_pydict({
        'timestamp': pa.array(vwap_timestamps, type=pa.int64()),
        'symbol': vwap_symbols,
        'vwap': vwap_prices,
        'volume': vwap_volumes,
    })
    
    print(f"\nReference data (VWAP):")
    print(f"  Rows: {vwap_table.num_rows}")
    print(f"  Time span: {(vwap_timestamps[-1] - vwap_timestamps[0]) / 1000:.1f} seconds")
    
    # Generate live trades
    trade_timestamps = base_ts + np.sort(np.random.randint(0, 60000, 50))
    trade_symbols = np.random.choice(['AAPL', 'GOOGL', 'MSFT'], 50)
    trade_prices = np.random.uniform(100, 200, 50)
    trade_volumes = np.random.exponential(500, 50)
    
    trades_batch = pa.record_batch({
        'timestamp': pa.array(trade_timestamps, type=pa.int64()),
        'symbol': trade_symbols,
        'trade_price': trade_prices,
        'trade_volume': trade_volumes,
    })
    
    print(f"\nLive trades:")
    print(f"  Rows: {trades_batch.num_rows}")
    
    # Join trades with VWAP benchmarks
    print(f"\n🔗 Joining trades with nearest VWAP benchmark (per symbol)...")
    
    joined = asof_join_table(
        trades_batch,
        vwap_table,
        on='timestamp',
        by='symbol',
        tolerance_ms=5000,  # 5 second tolerance
        direction='backward'
    )
    
    print(f"✅ Join complete!")
    print(f"  Output rows: {joined.num_rows}")
    
    # Compute slippage vs VWAP
    df = joined.to_pandas()
    df['slippage_bps'] = ((df['trade_price'] - df['vwap']) / df['vwap']) * 10000
    
    print(f"\n📊 Trade Performance Analysis:")
    print(f"  Average slippage vs VWAP: {df['slippage_bps'].mean():.2f} bps")
    print(f"  Median slippage: {df['slippage_bps'].median():.2f} bps")
    print(f"  Max slippage: {df['slippage_bps'].max():.2f} bps")
    
    # Show sample
    print(f"\n  Sample trades:")
    print(df[['symbol', 'trade_price', 'vwap', 'slippage_bps']].head(5).to_string(index=False))


def demo_asof_performance():
    """Demo 5: Performance benchmarks."""
    print("\n" + "="*70)
    print("DEMO 5: ASOF Join Performance")
    print("="*70)
    
    # Generate large datasets
    trades, quotes = generate_trades_and_quotes(n_trades=10000, n_quotes=50000)
    
    print(f"\nLarge dataset:")
    print(f"  Trades: {trades.num_rows:,} rows")
    print(f"  Quotes: {quotes.num_rows:,} rows")
    
    import time
    
    # Benchmark: Basic asof_join function
    print(f"\n⏱️  Benchmark 1: asof_join() function...")
    start = time.perf_counter()
    
    joined = asof_join(
        trades,
        quotes,
        on='timestamp',
        by='symbol',
        tolerance_ms=1000,
        direction='backward'
    )
    
    elapsed = time.perf_counter() - start
    
    print(f"  ✓ Time: {elapsed*1000:.2f} ms")
    print(f"  ✓ Throughput: {trades.num_rows / elapsed:,.0f} joins/sec")
    print(f"  ✓ Latency: {elapsed / trades.num_rows * 1_000_000:.2f} μs/join")
    
    # Benchmark: Kernel-based approach (reusable index)
    print(f"\n⏱️  Benchmark 2: AsofJoinKernel (reusable index)...")
    
    kernel = AsofJoinKernel(
        time_column='timestamp',
        by_column='symbol',
        direction='backward',
        tolerance_ms=1000
    )
    
    # Build index (one-time cost)
    index_start = time.perf_counter()
    kernel.add_right_batch(quotes)
    index_time = time.perf_counter() - index_start
    
    print(f"  ✓ Index build: {index_time*1000:.2f} ms")
    
    # Process trades in batches
    batch_size = 1000
    total_time = 0
    num_processed = 0
    
    for i in range(0, trades.num_rows, batch_size):
        mini_batch = trades.slice(i, min(batch_size, trades.num_rows - i))
        
        batch_start = time.perf_counter()
        joined = kernel.process_left_batch(mini_batch)
        batch_time = time.perf_counter() - batch_start
        
        total_time += batch_time
        num_processed += mini_batch.num_rows
    
    print(f"  ✓ Join time: {total_time*1000:.2f} ms (excluding index build)")
    print(f"  ✓ Throughput: {num_processed / total_time:,.0f} joins/sec")
    print(f"  ✓ Latency: {total_time / num_processed * 1_000_000:.2f} μs/join")
    print(f"  ✓ Index reuse advantage: {(elapsed / total_time):.1f}x faster")


def demo_forward_asof():
    """Demo 6: Forward ASOF join."""
    print("\n" + "="*70)
    print("DEMO 6: Forward ASOF Join (Next Event)")
    print("="*70)
    
    print("\n💡 Use case: Join each event with NEXT occurrence")
    print("  Example: Match order with next trade execution")
    
    # Generate orders and executions
    np.random.seed(42)
    base_ts = int(datetime.now().timestamp() * 1000)
    
    # Orders (sparse)
    order_timestamps = base_ts + np.sort(np.random.randint(0, 30000, 20))
    orders = pa.record_batch({
        'timestamp': pa.array(order_timestamps, type=pa.int64()),
        'symbol': np.random.choice(['AAPL', 'GOOGL'], 20),
        'order_id': np.arange(1000, 1020),
        'order_price': np.random.uniform(100, 150, 20),
    })
    
    # Executions (denser)
    exec_timestamps = base_ts + np.sort(np.random.randint(0, 30000, 50))
    executions = pa.record_batch({
        'timestamp': pa.array(exec_timestamps, type=pa.int64()),
        'symbol': np.random.choice(['AAPL', 'GOOGL'], 50),
        'exec_price': np.random.uniform(100, 150, 50),
        'exec_size': np.random.exponential(100, 50),
    })
    
    print(f"\n  Orders: {orders.num_rows}")
    print(f"  Executions: {executions.num_rows}")
    
    # Forward ASOF: match each order with NEXT execution
    print(f"\n🔗 Joining orders with next execution (forward direction)...")
    
    joined = asof_join(
        orders,
        executions,
        on='timestamp',
        by='symbol',
        tolerance_ms=5000,
        direction='forward'  # Match NEXT event
    )
    
    print(f"✅ Forward join complete!")
    print(f"  Output rows: {joined.num_rows}")
    
    # Compute time-to-execution
    df = joined.to_pandas()
    df['time_to_exec_ms'] = df['timestamp'] - df['order_id']  # Simplified
    
    print(f"\n📊 Order execution statistics:")
    print(df[['order_id', 'order_price', 'exec_price', 'exec_size']].head(5).to_string(index=False))


def main():
    """Run all ASOF join demos."""
    print("\n" + "="*70)
    print("🚀 SABOT ASOF JOIN DEMO")
    print("="*70)
    print("\nTime-series joins for financial data")
    print("Binary search O(log n) with per-symbol indices")
    
    if not ASOF_AVAILABLE:
        print("\n❌ ASOF join kernels not available. Please compile first.")
        return
    
    # Run demos
    demo_basic_asof_join()
    demo_asof_join_kernel()
    demo_multi_exchange_alignment()
    demo_forward_asof()
    
    print("\n" + "="*70)
    print("✅ ALL ASOF JOIN DEMOS COMPLETE")
    print("="*70)
    print("\n💡 Key takeaways:")
    print("  - asof_join() for one-off joins (~1-2μs per join)")
    print("  - AsofJoinKernel for reusable indices (amortized <1μs)")
    print("  - Supports per-symbol grouping (critical for multi-asset)")
    print("  - Forward and backward directions")
    print("  - Configurable tolerance for fuzzy matching")
    print("\n📚 Use cases:")
    print("  - Trade-quote matching (every HFT system)")
    print("  - Multi-exchange price alignment")
    print("  - Reference data enrichment")
    print("  - Event sequence correlation")
    print("\nSee sabot/fintech/README.md for more examples!")


if __name__ == "__main__":
    main()

