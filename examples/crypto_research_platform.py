#!/usr/bin/env python3
"""
Crypto Trading Research Platform - Sabot + Fintech Kernels

Complete research setup for cryptocurrency markets using:
1. Coinbase WebSocket data (via coinbase2parquet.py)
2. Sabot streaming pipeline with fintech kernels
3. Real-time feature engineering
4. ASOF joins for trade-quote matching
5. State backends for persistence
6. Backtesting and live trading modes

Data flow:
  Coinbase WS → Kafka → Sabot Pipeline → Features → Research/Trading
"""

import asyncio
import argparse
import logging
from pathlib import Path
from datetime import datetime
import numpy as np

# Sabot imports
from sabot.api import Stream
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

# Fintech kernels
from sabot.fintech import (
    # Core statistics
    log_returns,
    ewma,
    rolling_zscore,
    welford_mean_var,
    # Microstructure
    midprice,
    microprice,
    l1_imbalance,
    ofi,
    vpin,
    quoted_spread,
    # Volatility
    realized_var,
    bipower_var,
    riskmetrics_vol,
    # Crypto-specific
    funding_apr,
    basis_annualized,
    # Momentum
    ema,
    macd,
    rsi,
    bollinger_bands,
    # Features
    lag,
    diff,
    rolling_std,
    autocorr,
    # Risk
    cusum,
    historical_var,
    # ASOF join
    asof_join,
    AsofJoinKernel,
)

# Distributed operators (for larger-than-memory state)
try:
    from sabot._cython.fintech.stateful_kernels import (
        create_stateful_ewma_operator,
        create_stateful_ofi_operator,
    )
    STATEFUL_OPS_AVAILABLE = True
except ImportError:
    STATEFUL_OPS_AVAILABLE = False


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ============================================================================
# Configuration
# ============================================================================

class ResearchConfig:
    """Configuration for crypto research platform."""
    
    # Kafka
    KAFKA_BOOTSTRAP = 'localhost:19092'
    KAFKA_TOPIC_TICKERS = 'coinbase-ticker'
    KAFKA_TOPIC_FEATURES = 'crypto-features'
    KAFKA_GROUP_ID = 'crypto-research'
    
    # Products to track
    PRODUCTS = [
        'BTC-USD', 'ETH-USD', 'DOGE-USD', 'XRP-USD',
        'LTC-USD', 'BCH-USD', 'ADA-USD', 'SOL-USD',
        'DOT-USD', 'LINK-USD', 'XLM-USD', 'UNI-USD',
        'ALGO-USD', 'MATIC-USD',
    ]
    
    # Feature engineering params
    EWMA_ALPHA = 0.94  # ~20-period half-life
    ZSCORE_WINDOW = 100
    MACD_FAST = 12
    MACD_SLOW = 26
    MACD_SIGNAL = 9
    RSI_PERIOD = 14
    BOLLINGER_WINDOW = 20
    RV_WINDOW = 20
    
    # State backend
    STATE_BACKEND = 'memory'  # 'memory', 'rocksdb', or 'tonbo'
    STATE_PATH = './state/crypto_research'
    
    # Output
    OUTPUT_PARQUET = './data/crypto_features.parquet'
    OUTPUT_BATCH_SIZE = 1000


# ============================================================================
# Data Schema Conversion
# ============================================================================

def coinbase_ticker_to_sabot_schema():
    """
    Define Arrow schema for Coinbase ticker data.
    
    Matches the Pydantic model from coinbase2parquet.py
    """
    return pa.schema([
        ('timestamp', pa.timestamp('ms')),
        ('product_id', pa.string()),
        ('type', pa.string()),
        # Price data
        ('price', pa.float64()),
        ('best_bid', pa.float64()),
        ('best_ask', pa.float64()),
        ('best_bid_quantity', pa.float64()),
        ('best_ask_quantity', pa.float64()),
        # Volume data
        ('volume_24_h', pa.float64()),
        ('last_size', pa.float64()),
        # OHLC
        ('open_24h', pa.float64()),
        ('high_24_h', pa.float64()),
        ('low_24_h', pa.float64()),
        ('price_percent_chg_24_h', pa.float64()),
        # Metadata
        ('parent_timestamp', pa.string()),
        ('parent_sequence_num', pa.int64()),
    ])


# ============================================================================
# Feature Engineering Pipeline
# ============================================================================

def build_crypto_feature_pipeline(
    stream,
    use_stateful_ops: bool = False,
    state_backend: str = 'memory'
):
    """
    Build complete feature engineering pipeline for crypto research.
    
    Features computed:
    - Price returns and changes
    - Online statistics (EWMA, z-scores, variance)
    - Microstructure (midprice, microprice, imbalance, OFI, VPIN)
    - Volatility (realized variance, bipower variation)
    - Momentum indicators (EMA, MACD, RSI, Bollinger Bands)
    - Statistical features (rolling std, autocorrelation)
    - Risk signals (CUSUM change detection, VaR)
    
    Args:
        stream: Input Stream from Kafka
        use_stateful_ops: Use stateful operators with backends (for >10K symbols)
        state_backend: 'memory', 'rocksdb', or 'tonbo'
    
    Returns:
        Stream with all features computed
    """
    logger.info("Building crypto feature pipeline...")
    logger.info(f"  Mode: {'Stateful operators' if use_stateful_ops else 'Simple functions'}")
    logger.info(f"  State backend: {state_backend}")
    
    # Rename product_id to symbol for consistency with fintech kernels
    def rename_columns(batch):
        """Rename product_id to symbol."""
        if 'product_id' in batch.schema.names:
            idx = batch.schema.names.index('product_id')
            return batch.set_column(idx, 'symbol', batch.column('product_id'))
        return batch
    
    pipeline = stream.map(rename_columns)
    
    # ========================================================================
    # Phase 1: Basic Price Features
    # ========================================================================
    
    logger.info("  Phase 1: Price features (returns, EWMA, z-scores)")
    
    if use_stateful_ops and STATEFUL_OPS_AVAILABLE:
        # Use stateful operators with backend
        log_returns_op = create_stateful_log_returns_operator(
            source=pipeline._source,
            symbol_column='symbol',
            state_backend=state_backend,
            state_path=f'{ResearchConfig.STATE_PATH}/log_returns'
        )
        pipeline = Stream(log_returns_op, None)
        
        ewma_op = create_stateful_ewma_operator(
            source=pipeline._source,
            alpha=ResearchConfig.EWMA_ALPHA,
            symbol_column='symbol',
            state_backend=state_backend,
            state_path=f'{ResearchConfig.STATE_PATH}/ewma'
        )
        pipeline = Stream(ewma_op, None)
    else:
        # Use simple functions (auto morsels)
        pipeline = (pipeline
            .map(lambda b: log_returns(b, 'price'))
            .map(lambda b: ewma(b, alpha=ResearchConfig.EWMA_ALPHA))
        )
    
    # Add rolling z-score (always simple function for now)
    pipeline = pipeline.map(lambda b: rolling_zscore(b, window=ResearchConfig.ZSCORE_WINDOW))
    
    # Add lagged features
    pipeline = (pipeline
        .map(lambda b: lag(b, 'log_return', k=1))
        .map(lambda b: diff(b, 'price', k=1))
    )
    
    # ========================================================================
    # Phase 2: Microstructure Features
    # ========================================================================
    
    logger.info("  Phase 2: Microstructure (midprice, imbalance, OFI)")
    
    pipeline = (pipeline
        # Price formation
        .map(lambda b: midprice(b))
        .map(lambda b: microprice(b))
        .map(lambda b: quoted_spread(b))
        
        # Order flow
        .map(lambda b: l1_imbalance(b))
    )
    
    # OFI - stateful (tracks last bid/ask)
    if use_stateful_ops and STATEFUL_OPS_AVAILABLE:
        ofi_op = create_stateful_ofi_operator(
            source=pipeline._source,
            symbol_column='symbol',
            state_backend=state_backend,
            state_path=f'{ResearchConfig.STATE_PATH}/ofi'
        )
        pipeline = Stream(ofi_op, None)
    else:
        pipeline = pipeline.map(lambda b: ofi(b))
    
    # ========================================================================
    # Phase 3: Volatility Features
    # ========================================================================
    
    logger.info("  Phase 3: Volatility (RV, BPV, RiskMetrics)")
    
    pipeline = (pipeline
        .map(lambda b: realized_var(b, 'log_return', window=ResearchConfig.RV_WINDOW))
        .map(lambda b: bipower_var(b, 'log_return', window=ResearchConfig.RV_WINDOW))
        .map(lambda b: riskmetrics_vol(b, 'log_return', lambda_param=0.94))
        .map(lambda b: rolling_std(b, 'log_return', window=ResearchConfig.RV_WINDOW))
    )
    
    # ========================================================================
    # Phase 4: Momentum Indicators
    # ========================================================================
    
    logger.info("  Phase 4: Momentum (MACD, RSI, Bollinger)")
    
    pipeline = (pipeline
        .map(lambda b: ema(b, alpha=0.1))
        .map(lambda b: macd(b, fast=ResearchConfig.MACD_FAST, 
                           slow=ResearchConfig.MACD_SLOW,
                           signal=ResearchConfig.MACD_SIGNAL))
        .map(lambda b: rsi(b, period=ResearchConfig.RSI_PERIOD))
        .map(lambda b: bollinger_bands(b, window=ResearchConfig.BOLLINGER_WINDOW, k=2.0))
    )
    
    # ========================================================================
    # Phase 5: Statistical Features
    # ========================================================================
    
    logger.info("  Phase 5: Statistical features (autocorr, higher moments)")
    
    pipeline = (pipeline
        .map(lambda b: autocorr(b, 'log_return', lag=5, window=100))
    )
    
    # ========================================================================
    # Phase 6: Risk Monitoring
    # ========================================================================
    
    logger.info("  Phase 6: Risk signals (CUSUM, VaR)")
    
    # Add PnL column (simplified - just cumulative returns)
    def add_pnl(batch):
        """Compute cumulative PnL."""
        returns = batch.column('log_return').to_numpy()
        pnl = np.nancumsum(returns)
        return batch.append_column('pnl', pa.array(pnl, type=pa.float64()))
    
    pipeline = (pipeline
        .map(add_pnl)
        .map(lambda b: cusum(b, 'log_return', k=0.0005, h=0.01))
        .map(lambda b: historical_var(b, 'pnl', alpha=0.05, window=100))
    )
    
    logger.info("✅ Feature pipeline complete!")
    logger.info(f"   Total features: ~40 computed columns")
    
    return pipeline


# ============================================================================
# Live Trading Mode
# ============================================================================

async def run_live_trading():
    """
    Run live trading with Coinbase data.
    
    Pipeline:
    1. Stream tickers from Kafka (populated by coinbase2parquet.py)
    2. Compute features in real-time
    3. Generate trading signals
    4. Execute trades (simulated)
    """
    logger.info("="*70)
    logger.info("LIVE TRADING MODE")
    logger.info("="*70)
    
    logger.info("\n📡 Streaming from Kafka topic: %s", ResearchConfig.KAFKA_TOPIC_TICKERS)
    logger.info("   (Start coinbase2parquet.py -k to populate)")
    
    # Create stream from Kafka
    stream = Stream.from_kafka(
        bootstrap_servers=ResearchConfig.KAFKA_BOOTSTRAP,
        topic=ResearchConfig.KAFKA_TOPIC_TICKERS,
        group_id=ResearchConfig.KAFKA_GROUP_ID,
        codec_type='json',
        batch_size=100
    )
    
    # Build feature pipeline
    feature_stream = build_crypto_feature_pipeline(
        stream,
        use_stateful_ops=False,  # Simple mode for live
        state_backend='memory'
    )
    
    # Select relevant columns for trading
    trading_stream = feature_stream.select(
        'timestamp', 'symbol', 'price',
        # Price features
        'log_return', 'ewma', 'zscore',
        # Microstructure
        'midprice', 'microprice', 'l1_imbalance', 'ofi',
        # Volatility
        'realized_var', 'riskmetrics_vol',
        # Momentum
        'macd', 'macd_signal', 'macd_hist', 'rsi',
        'bb_mid', 'bb_upper', 'bb_lower',
        # Risk
        'cusum_stat', 'cusum_alarm', 'var'
    )
    
    logger.info("\n🚀 Starting live processing...")
    logger.info("   Press Ctrl+C to stop\n")
    
    batch_count = 0
    signal_count = 0
    
    try:
        async for batch in trading_stream:
            batch_count += 1
            
            if batch and batch.num_rows > 0:
                # Generate trading signals
                df = batch.to_pandas()
                
                # Example signals
                for idx, row in df.iterrows():
                    # Mean reversion signal (z-score based)
                    if row['zscore'] < -2.0 and row['rsi'] < 30:
                        logger.info(f"🟢 BUY SIGNAL: {row['symbol']} @ ${row['price']:.2f}")
                        logger.info(f"   Z-score: {row['zscore']:.2f}, RSI: {row['rsi']:.1f}")
                        signal_count += 1
                        # execute_buy(row['symbol'], row['price'])
                    
                    elif row['zscore'] > 2.0 and row['rsi'] > 70:
                        logger.info(f"🔴 SELL SIGNAL: {row['symbol']} @ ${row['price']:.2f}")
                        logger.info(f"   Z-score: {row['zscore']:.2f}, RSI: {row['rsi']:.1f}")
                        signal_count += 1
                        # execute_sell(row['symbol'], row['price'])
                    
                    # Volatility regime change
                    if row['cusum_alarm'] == 1:
                        logger.info(f"⚠️  REGIME CHANGE: {row['symbol']}")
                        logger.info(f"   CUSUM: {row['cusum_stat']:.4f}")
                        # adjust_position_sizing()
                
                if batch_count % 10 == 0:
                    logger.info(f"Processed {batch_count} batches, {signal_count} signals generated")
    
    except KeyboardInterrupt:
        logger.info("\n⏹️  Stopped by user")
    
    logger.info(f"\n✅ Live trading session complete")
    logger.info(f"   Batches: {batch_count}")
    logger.info(f"   Signals: {signal_count}")


# ============================================================================
# Research Mode (Historical Analysis)
# ============================================================================

async def run_research_mode(parquet_file: Path):
    """
    Run research analysis on historical data.
    
    Pipeline:
    1. Load data from Parquet (created by coinbase2parquet.py -F)
    2. Compute all features
    3. Analyze patterns
    4. Save results
    """
    logger.info("="*70)
    logger.info("RESEARCH MODE - Historical Analysis")
    logger.info("="*70)
    
    if not parquet_file.exists():
        logger.error(f"❌ Parquet file not found: {parquet_file}")
        logger.info("   Run: python examples/coinbase2parquet.py -F")
        return
    
    logger.info(f"\n📊 Loading data from: {parquet_file}")
    
    # Load parquet data
    table = pq.read_table(parquet_file)
    logger.info(f"   Rows: {table.num_rows:,}")
    logger.info(f"   Columns: {table.schema.names}")
    
    # Convert to timestamp
    if 'parent_timestamp' in table.schema.names:
        # Parse ISO timestamp to milliseconds
        timestamps_str = table.column('parent_timestamp').to_pylist()
        timestamps_ms = []
        for ts_str in timestamps_str:
            try:
                dt = datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
                timestamps_ms.append(int(dt.timestamp() * 1000))
            except:
                timestamps_ms.append(0)
        
        table = table.append_column('timestamp', pa.array(timestamps_ms, type=pa.int64()))
    
    # Create stream
    stream = Stream.from_table(table, batch_size=10000)
    
    logger.info("\n🔧 Building feature pipeline...")
    
    # Build pipeline with larger-than-memory support if needed
    use_persistent = table.num_rows > 100000  # Use persistent backend for large datasets
    
    feature_stream = build_crypto_feature_pipeline(
        stream,
        use_stateful_ops=use_persistent and STATEFUL_OPS_AVAILABLE,
        state_backend='rocksdb' if use_persistent else 'memory'
    )
    
    logger.info("\n⚙️  Processing historical data...")
    
    # Collect results
    all_batches = []
    batch_count = 0
    
    for batch in feature_stream:
        if batch and batch.num_rows > 0:
            all_batches.append(batch)
            batch_count += 1
            
            if batch_count % 10 == 0:
                logger.info(f"  Processed {batch_count} batches...")
    
    if not all_batches:
        logger.error("❌ No data processed")
        return
    
    # Combine all batches
    logger.info(f"\n📊 Combining {len(all_batches)} batches...")
    results_table = pa.Table.from_batches(all_batches)
    
    logger.info(f"✅ Feature computation complete!")
    logger.info(f"   Total rows: {results_table.num_rows:,}")
    logger.info(f"   Total columns: {len(results_table.schema.names)}")
    
    # Save to parquet
    output_path = Path(ResearchConfig.OUTPUT_PARQUET)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"\n💾 Saving results to: {output_path}")
    pq.write_table(results_table, output_path, compression='snappy')
    
    logger.info(f"✅ Saved {results_table.num_rows:,} rows")
    
    # Analyze results
    analyze_features(results_table)
    
    return results_table


def analyze_features(table):
    """Analyze computed features."""
    logger.info("\n" + "="*70)
    logger.info("FEATURE ANALYSIS")
    logger.info("="*70)
    
    df = table.to_pandas()
    
    # Group by symbol
    logger.info("\n📊 Per-symbol statistics:\n")
    
    for symbol in df['symbol'].unique()[:5]:  # Show first 5
        symbol_df = df[df['symbol'] == symbol]
        
        logger.info(f"{symbol}:")
        logger.info(f"  Observations: {len(symbol_df):,}")
        
        if 'log_return' in df.columns:
            logger.info(f"  Avg return: {symbol_df['log_return'].mean()*100:.4f}%")
            logger.info(f"  Volatility: {symbol_df['log_return'].std()*100:.2f}%")
        
        if 'rsi' in df.columns:
            logger.info(f"  RSI: {symbol_df['rsi'].mean():.1f} (avg)")
        
        if 'l1_imbalance' in df.columns:
            logger.info(f"  Imbalance: {symbol_df['l1_imbalance'].mean():.3f}")
        
        if 'cusum_alarm' in df.columns:
            alarms = symbol_df['cusum_alarm'].sum()
            logger.info(f"  Regime changes: {alarms}")
        
        logger.info("")
    
    # Cross-symbol analysis
    logger.info("📈 Cross-symbol correlation:")
    
    if len(df['symbol'].unique()) >= 2:
        # Pivot to get returns by symbol
        pivot = df.pivot_table(
            index='timestamp',
            columns='symbol',
            values='log_return',
            aggfunc='first'
        )
        
        # Compute correlation matrix
        corr = pivot.corr()
        
        logger.info(f"\n{corr.head()}\n")


# ============================================================================
# Backtesting Mode
# ============================================================================

async def run_backtest(parquet_file: Path):
    """
    Run strategy backtest on historical data.
    
    Pipeline:
    1. Load historical features
    2. Generate signals
    3. Simulate execution
    4. Compute performance metrics
    """
    logger.info("="*70)
    logger.info("BACKTEST MODE")
    logger.info("="*70)
    
    # Load features from research mode output
    if not parquet_file.exists():
        logger.error(f"❌ Feature file not found: {parquet_file}")
        logger.info("   Run research mode first to generate features")
        return
    
    logger.info(f"\n📊 Loading features from: {parquet_file}")
    table = pq.read_table(parquet_file)
    logger.info(f"   Rows: {table.num_rows:,}")
    
    df = table.to_pandas()
    
    # Simple mean reversion strategy
    logger.info("\n📈 Backtesting mean reversion strategy:")
    logger.info("   Entry: z-score < -2.0 AND RSI < 30")
    logger.info("   Exit: z-score > 0 OR RSI > 50")
    
    # Generate signals
    df['signal'] = 0
    df.loc[(df['zscore'] < -2.0) & (df['rsi'] < 30), 'signal'] = 1  # Buy
    df.loc[(df['zscore'] > 2.0) & (df['rsi'] > 70), 'signal'] = -1  # Sell
    
    # Compute returns
    df['strategy_return'] = df['signal'].shift(1) * df['log_return']
    
    # Performance metrics
    total_return = df['strategy_return'].sum()
    sharpe = df['strategy_return'].mean() / df['strategy_return'].std() * np.sqrt(252 * 24 * 60)  # Annualized
    max_dd = (df['strategy_return'].cumsum() - df['strategy_return'].cumsum().cummax()).min()
    
    num_trades = (df['signal'].diff() != 0).sum()
    
    logger.info(f"\n📊 Backtest Results:")
    logger.info(f"   Total return: {total_return*100:.2f}%")
    logger.info(f"   Sharpe ratio: {sharpe:.2f}")
    logger.info(f"   Max drawdown: {max_dd*100:.2f}%")
    logger.info(f"   Number of trades: {num_trades}")
    
    # Per-symbol performance
    logger.info(f"\n📈 Per-symbol returns:")
    for symbol in df['symbol'].unique()[:5]:
        symbol_df = df[df['symbol'] == symbol]
        symbol_return = symbol_df['strategy_return'].sum()
        logger.info(f"   {symbol}: {symbol_return*100:.2f}%")


# ============================================================================
# Multi-Exchange Arbitrage Mode
# ============================================================================

async def run_arbitrage_monitor():
    """
    Monitor cross-exchange arbitrage opportunities.
    
    Requires multiple Kafka topics (one per exchange).
    Uses ASOF join to align timestamps.
    """
    logger.info("="*70)
    logger.info("ARBITRAGE MONITOR MODE")
    logger.info("="*70)
    
    logger.info("\n💡 This mode requires data from multiple exchanges")
    logger.info("   Currently showing structure with Coinbase data\n")
    
    # In production, would have streams from multiple exchanges:
    # coinbase_stream = Stream.from_kafka(..., 'coinbase-ticker', ...)
    # binance_stream = Stream.from_kafka(..., 'binance-ticker', ...)
    # kraken_stream = Stream.from_kafka(..., 'kraken-ticker', ...)
    
    # For demo, simulate with single stream
    stream = Stream.from_kafka(
        bootstrap_servers=ResearchConfig.KAFKA_BOOTSTRAP,
        topic=ResearchConfig.KAFKA_TOPIC_TICKERS,
        group_id='arbitrage-monitor',
        codec_type='json',
        batch_size=100
    )
    
    # Add exchange column
    def add_exchange(batch):
        """Tag with exchange name."""
        n = batch.num_rows
        return batch.append_column('exchange', pa.array(['coinbase'] * n))
    
    coinbase = stream.map(add_exchange)
    
    # Compute midprice for each exchange
    coinbase_with_mid = coinbase.map(lambda b: midprice(b))
    
    logger.info("📊 Monitoring for arbitrage opportunities...")
    logger.info("   Threshold: >10 bps spread between exchanges\n")
    
    batch_count = 0
    
    async for batch in coinbase_with_mid:
        batch_count += 1
        
        if batch and batch.num_rows > 0:
            df = batch.to_pandas()
            
            # In multi-exchange setup, would ASOF join here:
            # aligned = asof_join(coinbase_batch, binance_batch, 
            #                     on='timestamp', by='symbol')
            # spread = aligned['coinbase_mid'] - aligned['binance_mid']
            
            # For demo, just show current prices
            if batch_count % 10 == 0:
                logger.info(f"Latest prices (batch {batch_count}):")
                for symbol in df['symbol'].unique()[:3]:
                    symbol_row = df[df['symbol'] == symbol].iloc[-1]
                    logger.info(f"  {symbol}: ${symbol_row['price']:.2f} "
                              f"(bid: ${symbol_row['best_bid']:.2f}, "
                              f"ask: ${symbol_row['best_ask']:.2f})")


# ============================================================================
# Feature Export Mode
# ============================================================================

async def export_features_to_kafka():
    """
    Compute features and export back to Kafka for downstream consumers.
    
    Pipeline:
    Kafka (raw) → Features → Kafka (enriched)
    """
    logger.info("="*70)
    logger.info("FEATURE EXPORT MODE")
    logger.info("="*70)
    
    # Source
    stream = Stream.from_kafka(
        bootstrap_servers=ResearchConfig.KAFKA_BOOTSTRAP,
        topic=ResearchConfig.KAFKA_TOPIC_TICKERS,
        group_id='feature-compute',
        codec_type='json',
        batch_size=100
    )
    
    # Compute features
    feature_stream = build_crypto_feature_pipeline(stream)
    
    # TODO: Export to Kafka
    # feature_stream.to_kafka(ResearchConfig.KAFKA_TOPIC_FEATURES)
    
    logger.info("📤 Exporting features to Kafka topic: %s",
                ResearchConfig.KAFKA_TOPIC_FEATURES)
    logger.info("   (Kafka sink integration coming soon)")
    
    # For now, just log
    async for batch in feature_stream:
        if batch and batch.num_rows > 0:
            logger.info(f"  Computed features for {batch.num_rows} ticks "
                       f"({len(batch.schema.names)} columns)")


# ============================================================================
# Main Entry Point
# ============================================================================

async def main():
    """Main entry point with mode selection."""
    parser = argparse.ArgumentParser(
        description='Crypto Trading Research Platform with Sabot + Fintech Kernels'
    )
    
    parser.add_argument(
        'mode',
        choices=['live', 'research', 'backtest', 'arbitrage', 'export'],
        help='Execution mode'
    )
    
    parser.add_argument(
        '--parquet',
        type=Path,
        default=Path('./coinbase_ticker_data.parquet'),
        help='Parquet file for research/backtest mode'
    )
    
    parser.add_argument(
        '--output',
        type=Path,
        default=Path(ResearchConfig.OUTPUT_PARQUET),
        help='Output parquet file for research mode'
    )
    
    parser.add_argument(
        '--state-backend',
        choices=['memory', 'rocksdb', 'tonbo'],
        default='memory',
        help='State backend for operators'
    )
    
    args = parser.parse_args()
    
    # Update config
    ResearchConfig.STATE_BACKEND = args.state_backend
    ResearchConfig.OUTPUT_PARQUET = str(args.output)
    
    logger.info("\n" + "="*70)
    logger.info("🚀 CRYPTO TRADING RESEARCH PLATFORM")
    logger.info("="*70)
    logger.info(f"\nMode: {args.mode}")
    logger.info(f"State backend: {args.state_backend}")
    
    if args.mode == 'live':
        await run_live_trading()
    
    elif args.mode == 'research':
        await run_research_mode(args.parquet)
    
    elif args.mode == 'backtest':
        # Run research first to generate features
        results = await run_research_mode(args.parquet)
        if results:
            await run_backtest(args.output)
    
    elif args.mode == 'arbitrage':
        await run_arbitrage_monitor()
    
    elif args.mode == 'export':
        await export_features_to_kafka()
    
    logger.info("\n✅ Platform shutdown complete")


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("\n⏹️  Interrupted by user")

