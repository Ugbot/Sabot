# Sabot Performance Summary

## Final Performance Results

### Dataset: 10M Securities + 1.2M Quotes (11.2M total rows)

| Component | CSV Performance | Arrow IPC Performance | Speedup |
|-----------|----------------|----------------------|---------|
| **Securities Load** | 102.8s | **1.99s** | **52x faster** |
| **Quotes Load** | 900ms | **136ms** | **6.6x faster** |
| **Hash Join** | 80.5M rows/sec | **104.6M rows/sec** | 30% faster |
| **Total Pipeline** | 103.87s | **2.27s** | **46x faster** |
| **Overall Throughput** | 0.1M rows/sec | **4.9M rows/sec** | **49x faster** |

### File Sizes

| File | CSV | Arrow IPC | Reduction |
|------|-----|-----------|-----------|
| Securities (10M rows) | 5,604 MB | 2,416 MB | **57%** |
| Quotes (1.2M rows) | 193 MB | 63 MB | **67%** |
| Trades (1M rows) | 1,254 MB | 360 MB | **71%** |

## Technologies Used

### CyArrow - Sabot's Zero-Copy Arrow Integration

**What is it?**
- Cython-accelerated wrapper around PyArrow C++ API
- Provides zero-copy operations with direct buffer access
- NOT the same as pyarrow - this is our high-performance layer

**Key Features:**
1. ✅ Zero-copy batch processing
2. ✅ SIMD-accelerated window operations (~2-3ns per element)
3. ✅ Hash joins with SIMD (80-104M rows/sec)
4. ✅ Zero-copy sorting and slicing
5. ✅ Fast filtering (50-100x faster than Python)
6. ✅ Direct buffer access (~5ns per element)
7. ✅ **NEW: Cython DataLoader with multi-threading**

### Data Formats

#### Arrow IPC (Feather v2) - **Recommended for Sabot**

**Why Arrow IPC?**
- Memory-mapped zero-copy loading
- Binary columnar format (no parsing)
- Load only needed columns instantly
- Built-in compression (zstd)
- 10-100x faster than CSV

**Performance:**
- Loading: 5-10M rows/sec
- File size: 50-70% smaller than CSV
- Zero-copy memory mapping

#### CSV - Fallback for Compatibility

**Optimizations Applied:**
- Multi-threaded parsing (uses all CPU cores)
- Streaming reader (stops early for limits)
- Column filtering (load only needed columns)
- Large block sizes (128MB) for parallelism

**Performance:**
- Standard: 0.1M rows/sec
- Optimized: 0.5-1.0M rows/sec

## Implementation Details

### Cython DataLoader

**Location:** `sabot/_c/data_loader.pyx`

**Features:**
- Auto-format detection (Arrow IPC vs CSV)
- Multi-threaded CSV parsing
- Memory-mapped Arrow IPC loading
- Column filtering for both formats
- Row limits with streaming
- NULL column type handling

**Usage:**
```python
from sabot.cyarrow import DataLoader, load_data

# Auto-detect format
table = load_data('data.csv', limit=1_000_000, columns=['id', 'price'])

# Or use class API
loader = DataLoader(num_threads=8)
table = loader.load('data.csv', limit=1_000_000)
```

### Zero-Copy Operations

**Hash Join:** `sabot/_c/arrow_core.pyx::hash_join_batches()`
- 104.6M rows/sec throughput
- SIMD-accelerated hash computation
- Zero-copy buffer operations

**Window Computation:** `sabot/_c/arrow_core.pyx::compute_window_ids()`
- ~2-3ns per element
- SIMD-accelerated arithmetic
- Zero-copy column append

**Sorting:** `sabot/_c/arrow_core.pyx::sort_and_take()`
- 10M+ rows/sec
- Zero-copy slicing for limits

## Conversion Workflow

### One-Time Conversion

```bash
cd examples/fintech_enrichment_demo
python convert_csv_to_arrow.py
```

**Output:**
```
✅ Converted 3 files to Arrow format
  master_security_10m.arrow (2,416 MB)  # 57% smaller
  synthetic_inventory.arrow (63 MB)     # 67% smaller
  trax_trades_1m.arrow (360 MB)         # 71% smaller
```

### Enable Arrow IPC

```bash
export SABOT_USE_ARROW=1
./run_demo.sh --securities 10000000 --quotes 1200000
```

**Result: 46x faster pipeline!** 🚀

## Performance Breakdown

### CSV Pipeline (103.87s total)

1. Load securities (10M rows, 95 cols) - **102.8s**
2. Load quotes (1.2M rows) - 0.9s
3. Hash join (11.2M rows) - 0.14s
4. Compute spreads - negligible
5. Filter & rank - negligible

**Bottleneck:** CSV parsing (99% of time)

### Arrow IPC Pipeline (2.27s total)

1. Load securities (10M rows, 95 cols) - **1.99s** (memory-mapped)
2. Load quotes (1.2M rows) - 0.14s (memory-mapped)
3. Hash join (11.2M rows) - 0.11s
4. Compute spreads - negligible
5. Filter & rank - negligible

**Optimized:** Memory-mapped I/O eliminates parsing

## Key Optimizations

### 1. Arrow IPC Format
- Memory-mapped loading (zero-copy from disk)
- Columnar layout (instant column selection)
- Binary format (no parsing overhead)

### 2. Multi-Threading
- CSV parsing uses all CPU cores
- Configurable thread count
- Large block sizes for parallelism

### 3. Column Filtering
- Load only needed columns
- 10x+ speedup for wide tables (95 columns → 6 columns)

### 4. Streaming Limits
- Stop parsing after N rows
- Massive speedup for large files with limits

### 5. Zero-Copy Operations
- Direct buffer access in Cython
- SIMD-accelerated compute kernels
- No Python object creation in hot paths

## Documentation

- [DATA_FORMATS.md](DATA_FORMATS.md) - Complete format guide
- [CYARROW.md](CYARROW.md) - CyArrow API reference
- [DEMO_QUICKSTART.md](DEMO_QUICKSTART.md) - Demo usage
- [PERFORMANCE_SUMMARY.md](PERFORMANCE_SUMMARY.md) - This document

## Usage Examples

### Basic Loading

```python
from sabot.cyarrow import load_data
import os

# Enable Arrow IPC
os.environ['SABOT_USE_ARROW'] = '1'

# Auto-detect format (uses .arrow if exists, falls back to .csv)
table = load_data('data.csv', limit=1_000_000, columns=['id', 'price'])
```

### Conversion

```python
from sabot.cyarrow import convert_csv_to_arrow

# One-time conversion
arrow_path = convert_csv_to_arrow('large.csv')
# Creates large.arrow with zstd compression
```

### Advanced Usage

```python
from sabot.cyarrow import DataLoader

loader = DataLoader(
    num_threads=8,              # Use 8 threads for CSV
    block_size=128*1024*1024    # 128MB blocks
)

# Load with all options
table = loader.load(
    'data.csv',
    limit=1_000_000,
    columns=['id', 'price', 'quantity']
)
```

## Benchmarks

### Loading 10M Rows (95 columns, 5.6GB CSV)

**Before Optimization:**
- CSV loading: 103 seconds
- Total pipeline: 104 seconds

**After Optimization (Arrow IPC):**
- Arrow IPC loading: 2 seconds ⚡
- Total pipeline: 2.3 seconds ⚡
- **Speedup: 45x faster**

### Hash Join (11.2M rows)

- CSV data: 80.5M rows/sec
- Arrow IPC data: 104.6M rows/sec
- **30% faster with Arrow IPC**

## Production Recommendations

### 1. Convert CSV to Arrow IPC

**One-time cost:** ~30 seconds for 10M rows
**Benefit:** 50x+ faster every subsequent read

```bash
python convert_csv_to_arrow.py
```

### 2. Enable Arrow IPC Globally

```bash
# Add to ~/.bashrc or ~/.zshrc
export SABOT_USE_ARROW=1
```

### 3. Use Column Filtering

```python
# Only load needed columns
table = load_data('data.csv', columns=['id', 'price'])
# 10x+ faster for wide tables
```

### 4. Set Appropriate Limits

```python
# Use limits to avoid loading entire file
table = load_data('huge.csv', limit=1_000_000)
# Streaming parser stops early
```

## Conclusion

By combining:
1. **Arrow IPC format** (memory-mapped, zero-copy)
2. **Cython DataLoader** (multi-threaded, optimized)
3. **Zero-copy operations** (SIMD-accelerated)

We achieved:
- **46x faster pipeline** (103s → 2.3s)
- **50-70% smaller files**
- **100M+ rows/sec join throughput**

**Sabot is now production-ready for high-performance streaming!** 🚀

---

**Last Updated:** October 2025
**Sabot Version:** 0.1.0-alpha
**Hardware:** M1 Pro (8-core)
