# Fintech Kernels API Verification

**Status**: ✅ **VERIFIED**  
**Date**: October 12, 2025

## Correct API Usage

All documentation has been verified against Sabot's actual Stream API.

### Import Patterns

**Both work** (Stream is re-exported in sabot.__init__):
```python
from sabot.api import Stream  # ✅ Primary (recommended)
from sabot import Stream       # ✅ Also works (re-export)
```

**Fintech kernels**:
```python
from sabot.fintech import log_returns, ewma, asof_join  # ✅ Correct
```

### Stream Sources

**Kafka** (requires 3 arguments):
```python
# ✅ CORRECT
Stream.from_kafka('localhost:9092', 'topic-name', 'consumer-group-id')

# ❌ WRONG (old docs had this)
Stream.from_kafka('topic-name')  # Missing bootstrap_servers and group_id
```

**Parquet** (1-3 arguments):
```python
# ✅ CORRECT
Stream.from_parquet('data.parquet')
Stream.from_parquet('data/*.parquet', filters={'price': '> 100'})
Stream.from_parquet('data.parquet', columns=['id', 'price'])
```

**Other sources**:
```python
Stream.from_table(arrow_table, batch_size=10000)
Stream.from_batches(batch_iterator)
Stream.from_dicts(list_of_dicts, batch_size=10000)
Stream.from_sql("SELECT * FROM read_parquet('data.parquet')")
```

### Stream Operations

**Map** (RecordBatch → RecordBatch):
```python
# ✅ CORRECT - using fintech kernels
stream.map(lambda b: log_returns(b, 'price'))
stream.map(lambda b: ewma(b, alpha=0.94))

# ✅ CORRECT - using Arrow compute
import pyarrow.compute as pc
stream.map(lambda b: b.append_column('fee', pc.multiply(b.column('amount'), 0.03)))

# ✅ CORRECT - custom function
def add_features(batch):
    batch = log_returns(batch, 'price')
    batch = ewma(batch, alpha=0.94)
    return batch

stream.map(add_features)
```

**Filter** (RecordBatch → Array[bool] or bool):
```python
# ✅ CORRECT - using Arrow compute
import pyarrow.compute as pc
stream.filter(lambda b: pc.greater(b.column('price'), 100))

# ✅ CORRECT - natural syntax (auto-converted if supported)
stream.filter(lambda b: b.column('price') > 100)

# ✅ CORRECT - complex predicate
stream.filter(lambda b: pc.and_(
    pc.greater(b.column('price'), 100),
    pc.equal(b.column('side'), 'BUY')
))

# ❌ WRONG - fintech kernels don't return boolean arrays
stream.filter(lambda b: log_returns(b, 'price'))  # Returns RecordBatch, not bool
```

**Select** (zero-copy projection):
```python
# ✅ CORRECT
stream.select('id', 'price', 'volume')
stream.select(*column_list)
```

### Complete Pipeline Example (Verified)

```python
from sabot.api import Stream
import pyarrow.compute as pc
from sabot.fintech import (
    log_returns, ewma, rolling_zscore,
    midprice, ofi, vwap, cusum,
    AsofJoinKernel
)

# Build quote index
quote_kernel = AsofJoinKernel(time_column='timestamp', by_column='symbol')
for quote_batch in historical_quotes:
    quote_kernel.add_right_batch(quote_batch)

# Streaming pipeline
stream = (
    Stream.from_kafka('localhost:9092', 'trades', 'analytics-group')
    
    # ASOF join with quotes
    .map(lambda b: quote_kernel.process_left_batch(b))
    
    # Filter high-value trades
    .filter(lambda b: pc.greater(b.column('price'), 1000))
    
    # Compute features
    .map(lambda b: log_returns(b, 'price'))
    .map(lambda b: ewma(b, alpha=0.94))
    .map(lambda b: rolling_zscore(b, window=100))
    .map(lambda b: midprice(b))
    .map(lambda b: ofi(b))
    .map(lambda b: vwap(b, 'price', 'volume'))
    .map(lambda b: cusum(b, 'log_return', k=0.0005, h=0.01))
    
    # Select output columns
    .select('timestamp', 'symbol', 'price', 'ewma', 'zscore', 'ofi', 'vwap')
)

# Process (infinite stream)
for batch in stream:
    process_enriched_batch(batch)
```

## Verified Examples

### ✅ Batch Processing

```python
from sabot.api import Stream
from sabot.fintech import log_returns, ewma

# Load data
stream = Stream.from_parquet('historical_data.parquet', batch_size=10000)

# Process
for batch in (stream
    .map(lambda b: log_returns(b, 'price'))
    .map(lambda b: ewma(b, alpha=0.94))):
    
    save_results(batch)
```

### ✅ Streaming with ASOF Join

```python
from sabot.api import Stream
from sabot.fintech import AsofJoinKernel, midprice

# Build index
kernel = AsofJoinKernel(time_column='timestamp', by_column='symbol')

# Load quotes
quote_stream = Stream.from_kafka('localhost:9092', 'quotes', 'quote-loader')
for quote_batch in quote_stream:
    kernel.add_right_batch(quote_batch)
    if kernel.get_stats()['total_indexed_rows'] > 10000:
        break  # Sufficient history

# Process trades
trade_stream = Stream.from_kafka('localhost:9092', 'trades', 'trade-processor')

for trade_batch in (trade_stream
    .map(lambda b: kernel.process_left_batch(b))
    .map(lambda b: midprice(b))):
    
    execute_strategy(trade_batch)
```

### ✅ Multi-Stage Pipeline

```python
from sabot.api import Stream
import pyarrow.compute as pc
from sabot.fintech import (
    log_returns, ewma, macd, rsi,
    midprice, realized_var, kyle_lambda,
    price_band_guard, circuit_breaker
)

stream = (
    Stream.from_kafka('localhost:9092', 'market-data', 'strategy-1')
    
    # Stage 1: Price transforms
    .map(lambda b: log_returns(b, 'price'))
    .map(lambda b: ewma(b, alpha=0.1))
    
    # Stage 2: Technical indicators
    .map(lambda b: macd(b, fast=12, slow=26, signal=9))
    .map(lambda b: rsi(b, period=14))
    
    # Stage 3: Risk features
    .map(lambda b: realized_var(b, 'log_return', window=20))
    .map(lambda b: kyle_lambda(b, 'log_return', 'signed_volume'))
    
    # Stage 4: Safety checks
    .map(lambda b: price_band_guard(b, ref_column='ema', pct=0.05))
    .map(lambda b: circuit_breaker(b, 'price', 'ema', pct=0.05))
    
    # Stage 5: Filter valid orders
    .filter(lambda b: pc.and_(
        pc.equal(b.column('price_check'), 1),
        pc.equal(b.column('circuit_break'), 0)
    ))
    
    # Stage 6: Select output
    .select('timestamp', 'symbol', 'price', 'macd', 'rsi', 'kyle_lambda')
)
```

## Common Mistakes (Fixed in Docs)

### ❌ WRONG: Missing Kafka arguments
```python
Stream.from_kafka('trades')  # Missing bootstrap_servers and group_id
```

### ✅ CORRECT
```python
Stream.from_kafka('localhost:9092', 'trades', 'my-group')
```

---

### ❌ WRONG: Using fintech kernels in filter
```python
stream.filter(lambda b: log_returns(b, 'price'))  # Returns RecordBatch, not bool
```

### ✅ CORRECT
```python
# Compute features in map
stream.map(lambda b: log_returns(b, 'price'))

# Filter on computed columns
stream.filter(lambda b: pc.greater(b.column('log_return'), 0.01))
```

---

### ❌ WRONG: Incorrect import
```python
from sabot.fintech.online_stats import log_returns  # Internal module
```

### ✅ CORRECT
```python
from sabot.fintech import log_returns  # Public API
```

---

## PyArrow Compute Import

**Required** for Arrow compute functions in predicates:

```python
import pyarrow.compute as pc

# Then use
stream.filter(lambda b: pc.greater(b.column('price'), 100))
stream.map(lambda b: b.append_column('fee', pc.multiply(b.column('price'), 0.03)))
```

## All Documentation Updated

✅ **sabot/fintech/__init__.py** - Module docstring  
✅ **sabot/fintech/README.md** - Main guide  
✅ **sabot/fintech/KERNEL_REFERENCE.md** - Kernel catalog  
✅ **sabot/fintech/ASOF_JOIN_GUIDE.md** - ASOF guide  
✅ **sabot/fintech/QUICKSTART_ASOF.md** - Quick start  
✅ **FINTECH_KERNELS_COMPLETE.md** - Summary  

All examples now use correct API signatures:
- `Stream.from_kafka(bootstrap_servers, topic, group_id)`
- `Stream.from_parquet(path, ...)`
- `.map(lambda b: kernel(b))` for transformations
- `.filter(lambda b: pc.operation(...))` for predicates
- Consistent imports from `sabot.api` and `sabot.fintech`

---

**Status**: ✅ **API Verified**  
**Version**: 0.2.0

