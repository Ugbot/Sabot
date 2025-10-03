# Fintech Enrichment Demo - Quick Start

Zero-copy Arrow enrichment pipeline for fintech data processing.

## Prerequisites

- CSV data files generated (see `examples/fintech_enrichment_demo/README.md`)
- Already-built Cython extensions (`.so` files in `sabot/_c/` and `sabot/_cython/`)

## Running the Demo

### Option 1: Using the runner script

```bash
./run_demo.sh --securities 500000 --quotes 500000
```

### Option 2: Direct Python execution

```bash
export PYTHONPATH=/Users/bengamble/Sabot:$PYTHONPATH
source .venv/bin/activate
python examples/fintech_enrichment_demo/arrow_optimized_enrichment.py --securities 500000 --quotes 500000
```

### Option 3: Using the quick launcher

```bash
./demo --securities 500000 --quotes 500000
```

## Configuring CSV Paths

### Method 1: Environment Variables

Create a `.env` file:

```bash
cp .env.example .env
# Edit .env to set your paths
```

Example `.env`:
```bash
SABOT_DATA_DIR=/path/to/your/csv/files
```

### Method 2: Command-Line Arguments

```bash
./run_demo.sh --data-dir /path/to/csvs --securities 500000 --quotes 500000
```

Or specify individual files:

```bash
./run_demo.sh \
  --securities-csv /path/to/master_security_10m.csv \
  --quotes-csv /path/to/synthetic_inventory.csv \
  --securities 500000 \
  --quotes 500000
```

## Performance Benchmarks

Tested on M1 Pro with 500K securities + 500K quotes:

- **CSV Loading**: 0.9M rows/sec (quotes)
- **Hash Join**: 7.2M rows/sec
- **Spread Calculation**: SIMD-accelerated (2.5ms for 102 rows)
- **Filtering**: 45 microseconds per row
- **Top-K Sorting**: Zero-copy sort (2.9ms)

Total pipeline time: ~17 seconds for 1M rows processed

## Command-Line Options

```bash
python examples/fintech_enrichment_demo/arrow_optimized_enrichment.py --help

Options:
  --securities N          Number of securities to load (default: 100,000)
  --quotes N              Number of quotes to process (default: 100,000)
  --trades N              Number of trades to process (default: 100,000)
  --data-dir PATH         Directory containing CSV files
  --securities-csv PATH   Path to securities CSV
  --quotes-csv PATH       Path to quotes CSV
  --trades-csv PATH       Path to trades CSV
```

## Example Runs

### Small test run (fast)
```bash
./run_demo.sh --securities 50000 --quotes 50000
```

### Medium run (benchmark)
```bash
./run_demo.sh --securities 500000 --quotes 500000
```

### Large run (stress test)
```bash
./run_demo.sh --securities 5000000 --quotes 1000000
```

## Zero-Copy Features Used (CyArrow)

**CyArrow** is Sabot's custom Cython wrapper around PyArrow's C++ API, providing:

1. ✅ `compute_window_ids()` - SIMD-accelerated windowing (~2-3ns per element)
2. ✅ `hash_join_batches()` - Zero-copy hash joins (O(n+m) with SIMD)
3. ✅ `ArrowComputeEngine` - Fast filtering (50-100x faster than Python)
4. ✅ `sort_and_take()` - Zero-copy sorting and slicing
5. ✅ Direct buffer access - ~5ns per element

### CyArrow vs PyArrow

```python
# Standard PyArrow (for general use)
import pyarrow as pa
import pyarrow.compute as pc

# Sabot's CyArrow (for zero-copy performance)
from sabot import cyarrow
from sabot.cyarrow import compute_window_ids, hash_join_batches
```

**Use CyArrow when:** You need maximum performance with zero-copy operations
**Use PyArrow when:** You need standard Arrow functionality without Sabot's optimizations

## Data Formats

### Arrow IPC (Recommended) - 50x Faster

Convert CSV to Arrow IPC format for dramatically faster loading:

```bash
# One-time conversion
cd examples/fintech_enrichment_demo
python convert_csv_to_arrow.py

# Enable Arrow IPC
export SABOT_USE_ARROW=1

# Run demo (50x faster!)
./run_demo.sh --securities 10000000 --quotes 1200000
```

**Performance comparison (10M rows):**
- CSV: 103 seconds
- Arrow IPC: 2 seconds ⚡

See [DATA_FORMATS.md](DATA_FORMATS.md) for complete format guide.

## Troubleshooting

### Import Error: "No module named 'sabot'"

Set PYTHONPATH:
```bash
export PYTHONPATH=/Users/bengamble/Sabot:$PYTHONPATH
```

Or use the runner scripts which set it automatically.

### CSV Files Not Found

Check that CSVs exist:
```bash
ls -lh examples/fintech_enrichment_demo/*.csv
```

Generate if missing:
```bash
cd examples/fintech_enrichment_demo
python master_security_synthesiser.py
python invenory_rows_synthesiser.py
python trax_trades_synthesiser.py
```

### Zero-copy Arrow Not Available

Rebuild Cython extensions:
```bash
python setup.py build_ext --inplace
```

Check that `.so` files exist:
```bash
ls -la sabot/_c/*.so
ls -la sabot/_cython/arrow/*.so
```
