# ASOF Join Quick Start

**Get up and running with ASOF joins in 5 minutes.**

## Install & Build

```bash
cd /Users/bengamble/Sabot
python build.py  # Compile Cython extensions (includes ASOF join)
```

## Basic Example (30 seconds)

```python
import numpy as np
import pyarrow as pa
from sabot.fintech import asof_join

# Create sample data
trades = pa.record_batch({
    'timestamp': pa.array([100, 200, 300], type=pa.int64()),
    'symbol': ['AAPL', 'AAPL', 'AAPL'],
    'price': [150.0, 151.0, 152.0],
})

quotes = pa.record_batch({
    'timestamp': pa.array([90, 180, 290], type=pa.int64()),
    'symbol': ['AAPL', 'AAPL', 'AAPL'],
    'bid': [149.5, 150.5, 151.5],
    'ask': [150.5, 151.5, 152.5],
})

# Join each trade with most recent quote
joined = asof_join(
    trades,
    quotes,
    on='timestamp',
    by='symbol',
    tolerance_ms=50,
    direction='backward'
)

print(joined.to_pandas())
```

**Output**:
```
   timestamp symbol  price    bid    ask
0        100   AAPL  150.0  149.5  150.5
1        200   AAPL  151.0  150.5  151.5
2        300   AAPL  152.0  151.5  152.5
```

## Common Patterns

### 1. Trade-Quote Matching

```python
from sabot.fintech import asof_join, midprice, effective_spread

# Join trades with quotes
joined = asof_join(trades, quotes, on='timestamp', by='symbol')

# Compute execution quality
joined = midprice(joined)  # Add midprice column
joined = effective_spread(joined)  # Add effective spread
```

### 2. Multi-Exchange Alignment

```python
# Align prices from 3 exchanges
prices_binance = load_batch('binance')
prices_coinbase = load_batch('coinbase')
prices_kraken = load_batch('kraken')

# Chain ASOF joins
aligned = asof_join(prices_binance, prices_coinbase, on='timestamp', by='symbol')
aligned = asof_join(aligned, prices_kraken, on='timestamp', by='symbol')

# Now all exchanges aligned by timestamp
```

### 3. Stream Processing

```python
from sabot.api import Stream
from sabot.fintech import AsofJoinKernel

# Build quote index
kernel = AsofJoinKernel(time_column='timestamp', by_column='symbol')

for quote_batch in historical_quotes:
    kernel.add_right_batch(quote_batch)

# Process live trades
for trade_batch in Stream.from_kafka('localhost:9092', 'trades', 'processor-group'):
    joined = kernel.process_left_batch(trade_batch)
    analyze(joined)
```

## Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `on` | str | 'timestamp' | Timestamp column (int64 ms) |
| `by` | str | None | Grouping column (e.g., 'symbol') |
| `tolerance_ms` | int | 1000 | Max time difference (ms) |
| `direction` | str | 'backward' | 'backward' (<= target) or 'forward' (>= target) |

## Timestamp Format

**Must be int64 milliseconds since epoch:**

```python
# Correct
timestamps = np.array([1697123456789], dtype=np.int64)  # Milliseconds

# Convert from datetime
from datetime import datetime
dt = datetime.now()
ts_ms = int(dt.timestamp() * 1000)

# Convert from nanoseconds
ts_ns = pa.array([...], type=pa.timestamp('ns'))
ts_ms = (ts_ns.cast(pa.int64()) / 1_000_000).cast(pa.int64())
```

## Next Steps

- **Full guide**: `sabot/fintech/ASOF_JOIN_GUIDE.md`
- **Demos**: `python examples/asof_join_demo.py`
- **Tests**: `pytest tests/test_asof_join.py -v`

## Common Issues

### "Column not found"
```python
# Make sure timestamp column exists and has correct name
asof_join(trades, quotes, on='ts')  # ❌ If column is 'timestamp'
asof_join(trades, quotes, on='timestamp')  # ✅ Correct
```

### "No matches"
```python
# Increase tolerance if data is sparse
asof_join(..., tolerance_ms=1000)  # ❌ Too tight
asof_join(..., tolerance_ms=60000)  # ✅ 1 minute tolerance
```

### "Wrong symbol matches"
```python
# Always use by='symbol' for multi-asset data
asof_join(trades, quotes, on='timestamp')  # ❌ Mixes symbols
asof_join(trades, quotes, on='timestamp', by='symbol')  # ✅ Per-symbol
```

---

**Ready to use**: Compile, test, integrate! 🚀

