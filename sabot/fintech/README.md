# Sabot Fintech Kernels

**High-performance streaming financial analytics with Arrow and Cython.**

A comprehensive library of composable kernels for real-time market analytics, risk management, and execution analysis.

**NEW**: ✅ **ASOF Joins** - Time-series joins with O(log n) binary search for trade-quote matching, multi-exchange alignment, and reference data enrichment.

## Overview

All kernels are:
- **Batch-first**: Process `RecordBatch → RecordBatch` (same code for batch and streaming)
- **O(1) updates**: Amortized constant time per tick with online algorithms
- **Numerically stable**: Welford's algorithm, Kahan summation, log-space computations
- **SIMD-accelerated**: Arrow compute kernels for vectorized operations
- **Stateful**: Maintain running state across batches for true streaming

## Performance

- **Online stats**: 5-10ns per update (Cython + SIMD)
- **Microstructure**: ~20ns per tick (pure C loops with GIL released)
- **ASOF joins**: ~1-10μs per join (O(log n) binary search)
- **Aggregations**: 1M+ updates/sec
- **Memory**: O(1) for most kernels, O(window) for rolling operations

## Architecture

```
RecordBatch (Arrow)
       ↓
  [Kernel State]  ← C++ struct (online accumulators)
       ↓
  process_batch() ← Cython (releases GIL)
       ↓
RecordBatch (enriched)
```

### Example Kernel: EWMA

```cython
cdef class EWMAKernel:
    cdef EWMAState _state  # C++ struct
    
    cpdef object process_batch(self, object batch):
        # Extract Arrow column
        values = batch.column(0).to_numpy()
        
        # C loop with GIL released
        with nogil:
            for i in range(n):
                ewma[i] = self._state.update(values[i])
        
        # Return enriched batch
        return batch.append_column('ewma', pa.array(ewma))
```

## Kernel Categories

### Tier-0: Online Statistics (Must-Have)

**Numerically stable streaming primitives:**

| Kernel | Description | State | Performance |
|--------|-------------|-------|-------------|
| `log_returns` | r_t = log(p_t / p_{t-1}) | Last price | ~5ns/update |
| `welford_mean_var` | Online mean/variance (Welford) | Count, M2 | ~10ns/update |
| `ewma` | Exponentially weighted MA | EWMA value | ~5ns/update |
| `ewcov` | EW covariance (2 series) | Means, cov | ~15ns/update |
| `rolling_zscore` | (x - μ) / σ with rolling window | Deque | ~20ns/update |

**Usage:**
```python
from sabot.fintech import log_returns, ewma, rolling_zscore

# Process batch
batch = log_returns(batch, 'price')
batch = ewma(batch, alpha=0.94)
batch = rolling_zscore(batch, window=100)
```

### Microstructure (HFT & Market Making)

**Price formation and order flow:**

| Kernel | Description | Formula | Use Case |
|--------|-------------|---------|----------|
| `midprice` | Midpoint | (bid + ask) / 2 | Reference price |
| `microprice` | Size-weighted mid | Weighted by sizes | Short-term prediction |
| `l1_imbalance` | Order book imbalance | (bid_sz - ask_sz) / (bid_sz + ask_sz) | Flow direction |
| `ofi` | Order flow imbalance | Cont et al. (2010) | Price prediction |
| `vpin` | Informed trading prob | Easley et al. (2012) | Volatility warning |

**Usage:**
```python
from sabot.fintech import midprice, microprice, ofi

batch = midprice(batch)
batch = microprice(batch)
batch = ofi(batch)  # Stateful: tracks last bid/ask
```

### Volatility (Realized Measures)

**High-frequency volatility estimators:**

| Kernel | Description | Robustness |
|--------|-------------|------------|
| `realized_var` | Σ r² | Standard |
| `bipower_var` | (π/2) Σ \|r_i\| \|r_{i-1}\| | Jump-robust |
| `medrv` | Median-based RV | Microstructure noise |
| `riskmetrics_vol` | EWMA of r² | JP Morgan (1996) |

### Liquidity & Impact

**Transaction cost and market depth:**

| Kernel | Description | Paper |
|--------|-------------|-------|
| `kyle_lambda` | Price impact coefficient | Kyle (1985) |
| `amihud_illiquidity` | E[\|r\| / volume] | Amihud (2002) |
| `roll_spread` | 2√(-Cov(r_t, r_{t-1})) | Roll (1984) |
| `corwin_schultz_spread` | High-low spread estimator | Corwin & Schultz (2012) |

### FX Kernels

**Currency markets:**

| Kernel | Description | Use Case |
|--------|-------------|----------|
| `triangular_arbitrage` | Detect cross-rate mispricing | Arb detection |
| `forward_points` | F - S with interest parity | Hedging |
| `cip_basis` | CIP deviation | Funding stress |
| `carry_signal` | r_dom - r_for - risk_adj | Carry trades |

### Crypto Kernels

**Digital assets:**

| Kernel | Description | Use Case |
|--------|-------------|----------|
| `funding_apr` | Annualize perp funding | Yield farming |
| `funding_cost` | Cumulative funding P&L | Position cost |
| `basis_annualized` | Perp-spot basis APR | Cash-and-carry |
| `open_interest_delta` | ΔOI_t | Positioning |
| `perp_fair_price` | Fair = spot × (1 + funding + basis) | Pricing |

### Execution & TCA

**Transaction cost analysis:**

| Kernel | Description | Benchmark |
|--------|-------------|-----------|
| `vwap` | Volume-weighted avg price | Execution quality |
| `implementation_shortfall` | (exec - arrival) / arrival | Slippage |
| `optimal_ac_schedule` | Almgren-Chriss slices | Optimal execution |
| `arrival_price_impact` | Impact vs entry | Delay cost |

### Risk Management

**Monitoring and VaR:**

| Kernel | Description | Use Case |
|--------|-------------|----------|
| `cusum` | Cumulative sum change detection | Regime shifts |
| `historical_var` | α-quantile of losses | Risk limit |
| `historical_cvar` | Expected shortfall | Tail risk |
| `cornish_fisher_var` | Skew/kurtosis-adjusted VaR | Fat tails |

## ASOF Joins - Time-Series Data Alignment

**NEW in v0.2.0**: Production-ready ASOF joins for financial time-series.

### What is ASOF Join?

Match rows by **nearest timestamp** (not exact):

```python
from sabot.fintech import asof_join

# Join each trade with most recent quote
joined = asof_join(
    trades_batch,      # Left: trades at irregular times
    quotes_batch,      # Right: quotes at different times
    on='timestamp',    # Match on this column
    by='symbol',       # Group by symbol
    tolerance_ms=1000, # Max 1 second difference
    direction='backward'  # Most recent quote <= trade time
)

# Result: Each trade has matched quote data
```

### Common Use Cases

**1. Trade-Quote Matching** (Every HFT System):
```python
from sabot.fintech import asof_join, midprice, effective_spread

# Join trades with quotes
joined = asof_join(trades, quotes, on='timestamp', by='symbol')

# Compute execution quality
joined = midprice(joined)
joined = effective_spread(joined)

# Slippage analysis
slippage = (joined['trade_price'] - joined['midprice']) / joined['midprice'] * 10000
```

**2. Multi-Exchange Alignment**:
```python
# Sync BTC prices from 3 exchanges
binance = load_prices('binance', 'BTC-USD')
coinbase = load_prices('coinbase', 'BTC-USD')

aligned = asof_join(binance, coinbase, on='timestamp', by='symbol')

# Arbitrage detection
spread_bps = (aligned['price_binance'] - aligned['price_coinbase']) / aligned['price_binance'] * 10000
```

**3. Reference Data Enrichment**:
```python
from sabot.fintech import asof_join_table

# Load VWAP benchmarks (static table)
vwap_table = pa.parquet.read_table('vwap_benchmarks.parquet')

# Join live trades with benchmarks
for trade_batch in trade_stream:
    enriched = asof_join_table(trade_batch, vwap_table, on='timestamp', by='symbol')
    tca_analysis(enriched)
```

### API Quick Reference

```python
# Simple function (one-shot)
asof_join(left, right, on='timestamp', by='symbol')

# Reusable kernel (persistent index)
kernel = AsofJoinKernel(time_column='timestamp', by_column='symbol')
kernel.add_right_batch(quotes)
joined = kernel.process_left_batch(trades)

# Stream-table join
asof_join_table(streaming_batch, static_table, ...)
```

**Performance**: ~1-10μs per join, O(log n) binary search

**See**: `sabot/fintech/ASOF_JOIN_GUIDE.md` for complete documentation

## Integration with Sabot Streams

All kernels work seamlessly with Sabot's batch/stream API:

```python
from sabot.api import Stream
from sabot.fintech import (
    log_returns, ewma, rolling_zscore,
    midprice, ofi,
    realized_var, kyle_lambda,
    triangular_arbitrage,
    vwap, cusum,
    asof_join, AsofJoinKernel
)

# Streaming pipeline with ASOF join
quotes_kernel = AsofJoinKernel(time_column='timestamp', by_column='symbol')

# Pre-load quotes
for quote_batch in historical_quotes:
    quotes_kernel.add_right_batch(quote_batch)

stream = (
    Stream.from_kafka('localhost:9092', 'trades', 'trade-processor')
    
    # ASOF join with quotes
    .map(lambda b: quotes_kernel.process_left_batch(b))
    
    # Online statistics
    .map(lambda b: log_returns(b, 'price'))
    .map(lambda b: ewma(b, alpha=0.94))
    .map(lambda b: rolling_zscore(b, window=100))
    
    # Microstructure
    .map(lambda b: midprice(b))
    .map(lambda b: ofi(b))
    
    # Volatility & liquidity
    .map(lambda b: realized_var(b, 'log_return', window=20))
    .map(lambda b: kyle_lambda(b, 'log_return', 'signed_volume'))
    
    # Risk monitoring
    .map(lambda b: cusum(b, 'log_return', k=0.0005, h=0.01))
    
    # Execution metrics
    .map(lambda b: vwap(b, 'price', 'volume'))
)

# Process (infinite stream)
for batch in stream:
    # Each batch has quotes joined + all computed features
    process_batch(batch)
```

## Morsel-Driven Parallelism

Large batches automatically use morsel parallelism:

```python
from sabot.api import Stream
from sabot.fintech import log_returns, ewma

# Batches > 10K rows automatically split into morsels
# and processed in parallel (C++ work-stealing)
stream = (
    Stream.from_parquet('huge_dataset.parquet', batch_size=100_000)
    .map(lambda b: log_returns(b, 'price'))  # Parallel morsels
    .map(lambda b: ewma(b, alpha=0.94))      # Parallel morsels
)
```

## Larger-than-Memory State

**NEW**: Support for persistent state backends when state exceeds RAM:

```python
from sabot._cython.fintech.stateful_kernels import (
    create_stateful_ewma_operator
)

# RocksDB backend for persistent state (10K-100K symbols)
ewma_op = create_stateful_ewma_operator(
    source=stream,
    alpha=0.94,
    state_backend='rocksdb',  # ← Persistent to disk
    state_path='./state/ewma_db'
)

# Tonbo backend for large-scale (>100K symbols)
ewma_op = create_stateful_ewma_operator(
    source=stream,
    alpha=0.94,
    state_backend='tonbo',  # ← Columnar LSM storage
    state_path='./state/ewma_tonbo'
)
```

**Three backends**:
- **Memory**: <10K symbols, ~10ns, fastest
- **RocksDB**: 10K-100K symbols, ~1μs, persistent
- **Tonbo**: >100K symbols, ~10μs, columnar

**See**: `sabot/fintech/STATE_BACKENDS.md` for complete guide

## Building from Source

Kernels are Cython extensions that must be compiled:

```bash
# Build all Cython extensions (including fintech kernels)
python build.py

# Or build just fintech kernels
cd sabot/_cython/fintech
cython -3 online_stats.pyx
cython -3 microstructure.pyx
cython -3 volatility.pyx
cython -3 liquidity.pyx
cython -3 fx_crypto.pyx
cython -3 execution_risk.pyx
```

## Examples

Run the comprehensive demo:

```bash
python examples/fintech_kernels_demo.py
```

Demos include:
1. Online statistics (EWMA, z-scores)
2. Microstructure (midprice, OFI)
3. Volatility & liquidity (RV, Kyle lambda)
4. FX arbitrage detection
5. Crypto funding rates
6. Execution TCA (VWAP, IS)
7. Risk monitoring (CUSUM, VaR)

## Mathematical References

### Online Algorithms
- Welford, B.P. (1962) - Online mean/variance
- Kahan, W. (1965) - Compensated summation

### Microstructure
- Cont, R., Stoikov, S., Talreja, R. (2010) - Order flow imbalance
- Easley, D., et al. (2012) - VPIN
- Stoikov, S. (2018) - Microprice

### Volatility
- Barndorff-Nielsen, O., Shephard, N. (2004) - Bipower variation
- Andersen, T., et al. (2012) - Realized volatility handbook

### Liquidity
- Kyle, A. (1985) - Market microstructure
- Amihud, Y. (2002) - Illiquidity measure
- Roll, R. (1984) - Spread estimator
- Corwin, S., Schultz, P. (2012) - High-low spread

### Execution
- Almgren, R., Chriss, N. (2001) - Optimal execution
- Kissell, R., Glantz, M. (2003) - Transaction cost analysis

## License

Same as Sabot: AGPL-3.0

## Credits

Built on:
- Apache Arrow (columnar data, SIMD compute)
- Cython (C-level performance in Python)
- NumPy (numerical arrays)

Academic foundations from decades of market microstructure research.

