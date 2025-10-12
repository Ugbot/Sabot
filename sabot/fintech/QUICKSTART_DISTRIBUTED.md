# Quick Start: Distributed Execution

**Get fintech kernels running on single node OR cluster in 5 minutes.**

---

## Single Node (Works TODAY)

**Just import and use** - automatic local morsels:

```python
from sabot.api import Stream
from sabot.fintech import log_returns, ewma, ofi, vwap

stream = (
    Stream.from_kafka('localhost:9092', 'market-data', 'analytics')
    .map(lambda b: log_returns(b, 'price'))   # Automatic morsels
    .map(lambda b: ewma(b, alpha=0.94))       # Automatic morsels
    .map(lambda b: midprice(b))
    .map(lambda b: ofi(b))
    .map(lambda b: vwap(b, 'price', 'volume'))
)

for batch in stream:
    process(batch)
```

**What happens**:
- Small batches (<10K rows): direct execution, no overhead
- Large batches (≥10K rows): split into morsels, C++ threads
- 2-4x speedup on multi-core
- **No cluster setup needed**

---

## Multi-Node (Future Cluster Deployment)

**Use operator wrappers** for distributed execution:

```python
from sabot.api import Stream
from sabot._cython.fintech.operators import create_fintech_operator
from sabot._cython.fintech.online_stats import (
    LogReturnsKernel, EWMAKernel
)
from sabot._cython.fintech.microstructure import (
    MidpriceKernel, OFIKernel
)

# Source
source = Stream.from_kafka('localhost:9092', 'market-data', 'analytics')

# Build distributed pipeline
log_returns_op = create_fintech_operator(
    LogReturnsKernel,
    source=source,
    symbol_column='symbol'  # Partition by symbol
)

ewma_op = create_fintech_operator(
    EWMAKernel,
    source=log_returns_op,
    symbol_column='symbol',
    alpha=0.94
)

midprice_op = create_fintech_operator(
    MidpriceKernel,
    source=ewma_op,
    symbol_keyed=False  # Stateless, no shuffle
)

ofi_op = create_fintech_operator(
    OFIKernel,
    source=midprice_op,
    symbol_column='symbol'
)

# Deploy to cluster
for batch in ofi_op:
    process(batch)
```

**What happens**:
- JobManager detects stateful operators
- Inserts network shuffle by symbol
- Node 0: AAPL, MSFT (with states)
- Node 1: GOOGL, AMZN (with states)
- Node 2: NVDA, META (with states)
- 6-7x scaling on 8 nodes

---

## When to Use Which

| Your Situation | Use |
|----------------|-----|
| Single machine | **Simple functions** |
| <100 symbols | **Simple functions** |
| Development | **Simple functions** |
| Multi-node cluster | **Operator wrappers** |
| >1000 symbols | **Operator wrappers** |
| >10M ops/sec needed | **Operator wrappers** |

---

## Migration Path

**Start simple** (single node):
```python
from sabot.fintech import ewma
stream.map(lambda b: ewma(b, alpha=0.94))
```

**Scale up** (cluster):
```python
from sabot._cython.fintech.operators import create_fintech_operator
from sabot._cython.fintech.online_stats import EWMAKernel

op = create_fintech_operator(EWMAKernel, source, symbol_column='symbol', alpha=0.94)
```

**Same kernel, different execution!**

---

## Demos

```bash
# See both modes in action
python examples/fintech_distributed_demo.py

# Performance comparison
python examples/fintech_kernels_demo.py
```

---

**Recommendation**: Start with simple functions, scale to operators when needed!

