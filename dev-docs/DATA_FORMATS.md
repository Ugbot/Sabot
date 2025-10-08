# Sabot Data Formats Guide

Complete guide to data formats supported by Sabot and when to use each one.

## Format Comparison

| Format | Type | Speed | Size | Use Case |
|--------|------|-------|------|----------|
| **Arrow IPC** | Binary columnar | ⭐⭐⭐⭐⭐ | Medium | **Best for streaming** - Memory-mapped, zero-copy |
| **Parquet** | Binary columnar | ⭐⭐⭐⭐ | Smallest | Long-term storage, analytics |
| **CSV** | Text row-based | ⭐⭐ | Largest | Human-readable, interop |
| **JSON** | Text structured | ⭐ | Large | APIs, nested data |
| **Avro** | Binary row-based | ⭐⭐⭐ | Small | Schema evolution, Kafka |
| **MessagePack** | Binary structured | ⭐⭐⭐ | Small | Fast serialization |

## Arrow IPC (Feather v2) - Recommended for Sabot

### What is Arrow IPC?

Arrow IPC (Inter-Process Communication) is a **memory-mapped columnar format** designed for zero-copy streaming.

### Performance Characteristics

**Measured on M1 Pro (10M rows, 95 columns, 5.6GB CSV):**

- **Loading**: 5.0M rows/sec (2 seconds for 10M rows)
- **CSV equivalent**: 0.1M rows/sec (103 seconds)
- **Speedup**: **52x faster than CSV!**

**Why so fast?**
- ✅ Memory-mapped I/O (no copying)
- ✅ Binary format (no parsing)
- ✅ Columnar layout (load only needed columns)
- ✅ Built-in compression (zstd)

### File Size Comparison

```
master_security_10m.csv     5,604 MB (original)
master_security_10m.arrow   2,416 MB (57% smaller)

synthetic_inventory.csv       193 MB (original)
synthetic_inventory.arrow      63 MB (67% smaller)

trax_trades_1m.csv          1,254 MB (original)
trax_trades_1m.arrow          360 MB (71% smaller)
```

### Converting CSV to Arrow IPC

#### Method 1: Using the Converter Script

```bash
cd examples/fintech_enrichment_demo
python convert_csv_to_arrow.py
```

Output:
```
✅ Converted 3 files to Arrow format
  master_security_10m.arrow (2,416 MB)
  synthetic_inventory.arrow (63 MB)
  trax_trades_1m.arrow (360 MB)
```

#### Method 2: Python API

```python
import pyarrow as pa
import pyarrow.csv as pa_csv
import pyarrow.feather as feather

# Read CSV
table = pa_csv.read_csv('data.csv')

# Write Arrow IPC
feather.write_feather(
    table,
    'data.arrow',
    compression='zstd',
    compression_level=3
)
```

#### Method 3: Using Sabot's Loader

```python
from pathlib import Path
import pyarrow.csv as pa_csv
import pyarrow.feather as feather

def convert_to_arrow(csv_path: Path) -> Path:
    """Convert CSV to Arrow IPC format."""
    arrow_path = csv_path.with_suffix('.arrow')

    # Load CSV
    table = pa_csv.read_csv(csv_path)

    # Save as Arrow
    feather.write_feather(table, arrow_path, compression='zstd')

    return arrow_path
```

### Using Arrow IPC in Sabot

#### Method 1: Cython DataLoader (Recommended) ⭐

Sabot's high-performance Cython data loader with auto-format detection:

```python
from sabot.cyarrow import DataLoader, load_data
import os

# Enable Arrow IPC auto-detection
os.environ['SABOT_USE_ARROW'] = '1'

# Method 1: Using DataLoader class
loader = DataLoader(num_threads=8, block_size=128*1024*1024)
table = loader.load('data.csv', limit=1_000_000, columns=['id', 'price'])
# Automatically uses data.arrow if it exists!

# Method 2: Convenience function
table = load_data('data.csv', limit=1_000_000, columns=['id', 'price'])
# Same auto-detection, simpler API
```

**Features:**
- ✅ Auto-detects Arrow IPC vs CSV
- ✅ Multi-threaded CSV parsing (uses all CPU cores)
- ✅ Memory-mapped Arrow IPC loading
- ✅ Column filtering for both formats
- ✅ Row limits with streaming

#### Method 2: PyArrow Direct

For manual control:

```python
import os
from pathlib import Path
import pyarrow.feather as feather

# Enable Arrow IPC
os.environ['SABOT_USE_ARROW'] = '1'

# Load Arrow IPC file
table = feather.read_table(
    'data.arrow',
    columns=['id', 'price', 'quantity'],
    memory_map=True
)

# Apply limit (zero-copy slice)
limited = table.slice(0, 100_000)
```

#### Method 3: Automatic Detection (Bash)

Sabot automatically uses Arrow IPC files if they exist and `SABOT_USE_ARROW=1` is set:

```bash
# Enable Arrow IPC
export SABOT_USE_ARROW=1

# Run demo (automatically uses .arrow files)
./run_demo.sh --securities 10000000 --quotes 1200000
```

#### Sabot Demo Script

```python
from examples.fintech_enrichment_demo.arrow_optimized_enrichment import load_csv_arrow

# Automatically uses Arrow IPC if SABOT_USE_ARROW=1 and .arrow file exists
securities = load_csv_arrow(
    Path('master_security_10m.csv'),
    limit=10_000_000,
    columns=['ID', 'NAME', 'CUSIP']  # Columnar: instant column selection
)
```

### Best Practices

1. **Convert once, read many times**
   - One-time conversion cost (~30s for 10M rows)
   - 50x+ faster every subsequent read

2. **Use compression**
   - `compression='zstd'` - Fast and effective
   - `compression_level=3` - Good balance (1-9 scale)

3. **Memory-map large files**
   - `memory_map=True` - Zero-copy loading
   - Works on files larger than RAM

4. **Select columns early**
   - `columns=['col1', 'col2']` - Instant column filtering
   - Columnar format advantage

---

## CSV Format

### When to Use CSV

✅ **Good for:**
- Human readability
- Data interchange with non-Arrow tools
- Small datasets (< 1M rows)
- One-time reads

❌ **Bad for:**
- Large datasets (> 1M rows)
- Repeated reads
- Performance-critical pipelines
- Wide tables (many columns)

### CSV Loading Performance

**Measured (10M rows, 95 columns, 5.6GB):**
- Standard: 0.1M rows/sec (103 seconds)
- Streaming: 0.5M rows/sec (20 seconds with limit)
- Arrow IPC: **5.0M rows/sec (2 seconds)** ← 50x faster

### CSV Loading Options in Sabot

```python
import pyarrow.csv as pa_csv

# Full file (slow for large files)
table = pa_csv.read_csv('data.csv')

# Streaming with limit (faster)
with pa_csv.open_csv('data.csv') as reader:
    batches = []
    for i, batch in enumerate(reader):
        batches.append(batch)
        if len(batches) * batch.num_rows >= 100_000:
            break
    table = pa.Table.from_batches(batches)

# Column selection (faster for wide tables)
convert_options = pa_csv.ConvertOptions(
    include_columns=['id', 'price', 'quantity']
)
table = pa_csv.read_csv('data.csv', convert_options=convert_options)
```

### CSV Optimization Tips

1. **Use column filtering**
   ```python
   # Load only 3 of 95 columns (10x+ faster)
   include_columns=['ID', 'NAME', 'PRICE']
   ```

2. **Increase parallelism**
   ```python
   read_options = pa_csv.ReadOptions(
       use_threads=True,
       block_size=128 * 1024 * 1024  # 128MB blocks
   )
   ```

3. **Skip parsing for unused columns**
   ```python
   convert_options = pa_csv.ConvertOptions(
       include_columns=['needed_cols'],
       auto_dict_encode=False  # Disable if not needed
   )
   ```

4. **Use streaming for limits**
   ```python
   # Don't parse entire 10M row file to get 100K rows
   with pa_csv.open_csv('large.csv') as reader:
       # Stops after 100K rows parsed
   ```

---

## Parquet Format

### When to Use Parquet

✅ **Good for:**
- Long-term storage
- Data lakes / warehouses
- Compressed archival (smallest size)
- Complex analytics queries
- Column-heavy workloads

❌ **Bad for:**
- Streaming (not append-friendly)
- Frequent updates
- Sub-millisecond latency requirements

### Parquet vs Arrow IPC

| Feature | Parquet | Arrow IPC |
|---------|---------|-----------|
| **Compression** | Better (smaller) | Good |
| **Read Speed** | Fast | Faster (memory-mapped) |
| **Write Speed** | Slower | Faster |
| **Streaming** | Not designed for it | Designed for it |
| **Updates** | Not supported | Not supported |
| **Use Case** | Storage | Streaming |

### Using Parquet in Sabot

```python
import pyarrow.parquet as pq

# Write Parquet
pq.write_table(table, 'data.parquet', compression='zstd')

# Read Parquet
table = pq.read_table('data.parquet', columns=['id', 'price'])

# Read with filters (predicate pushdown)
table = pq.read_table(
    'data.parquet',
    filters=[('price', '>', 100)]
)
```

### Parquet Best Practices

1. **Use appropriate compression**
   - `snappy` - Fastest, larger files
   - `zstd` - Good balance (recommended)
   - `brotli` - Smallest, slowest

2. **Partition large datasets**
   ```python
   pq.write_to_dataset(
       table,
       'output_dir',
       partition_cols=['date', 'region']
   )
   ```

3. **Use predicate pushdown**
   ```python
   # Only reads rows where price > 100
   table = pq.read_table('data.parquet', filters=[('price', '>', 100)])
   ```

---

## JSON Format

### When to Use JSON

✅ **Good for:**
- APIs and web services
- Nested/hierarchical data
- Schema flexibility
- Human readability

❌ **Bad for:**
- Large datasets (inefficient)
- Performance-critical paths
- Wide tables
- Numeric-heavy data

### JSON in Sabot

```python
import pyarrow.json as pa_json

# Read JSON
table = pa_json.read_json('data.json')

# Read NDJSON (newline-delimited)
table = pa_json.read_json('data.ndjson')

# Write JSON (not recommended for large data)
# Use Arrow IPC or Parquet instead
```

---

## Avro Format

### When to Use Avro

✅ **Good for:**
- Kafka integration
- Schema evolution
- Row-based access patterns
- RPC systems

❌ **Bad for:**
- Column-heavy analytics
- Memory-mapped access
- Maximum performance

### Avro in Sabot

Sabot supports Avro via Kafka serialization:

```python
from sabot import create_app

app = create_app('my-app', broker='kafka://localhost:9092')

@app.agent(value_serializer='avro')
async def process(stream):
    async for event in stream:
        # Avro-deserialized event
        print(event)
```

---

## MessagePack Format

### When to Use MessagePack

✅ **Good for:**
- Fast serialization
- Binary JSON alternative
- Small messages
- Network protocols

❌ **Bad for:**
- Large datasets
- Analytics workloads
- Human readability

### MessagePack in Sabot

```python
import msgpack

# Serialize
data = {'id': 1, 'price': 99.99}
binary = msgpack.packb(data)

# Deserialize
data = msgpack.unpackb(binary)
```

---

## Format Selection Guide

### Decision Tree

```
┌─ Need human readability?
│   ├─ Yes → CSV or JSON
│   └─ No ↓
│
├─ Streaming workload?
│   ├─ Yes → Arrow IPC (Feather v2) ⭐ BEST FOR SABOT
│   └─ No ↓
│
├─ Long-term storage?
│   ├─ Yes → Parquet
│   └─ No ↓
│
├─ Schema evolution?
│   ├─ Yes → Avro
│   └─ No ↓
│
└─ Fast serialization?
    └─ Yes → MessagePack
```

### By Use Case

| Use Case | Recommended Format |
|----------|-------------------|
| **Sabot streaming pipelines** | Arrow IPC (Feather v2) |
| **Data warehouse/lake** | Parquet |
| **Kafka messages** | Avro or MessagePack |
| **API responses** | JSON |
| **One-time analysis** | CSV (then convert to Arrow) |
| **Memory-mapped queries** | Arrow IPC |
| **Maximum compression** | Parquet (brotli) |
| **Fastest loading** | Arrow IPC (memory-mapped) |

---

## Performance Benchmarks

### Loading 10M rows (95 columns, 5.6GB)

| Format | Time | Throughput | Size | Notes |
|--------|------|------------|------|-------|
| **Arrow IPC** | **2.0s** | **5.0M rows/sec** | 2.4GB | Memory-mapped |
| CSV (streaming) | 20s | 0.5M rows/sec | 5.6GB | With limit |
| CSV (full) | 103s | 0.1M rows/sec | 5.6GB | Parse all |
| Parquet | ~5s | ~2M rows/sec | 1.8GB | Compressed |

### Join Performance (1.2M + 10M rows)

| Format | Load Time | Join Time | Total |
|--------|-----------|-----------|-------|
| **Arrow IPC** | 2.1s | 0.14s | **2.24s** |
| CSV | 103.7s | 0.14s | 103.84s |

**Result: Arrow IPC is 46x faster overall!**

---

## Code Examples

### Convert CSV to Arrow IPC

```python
#!/usr/bin/env python3
import pyarrow as pa
import pyarrow.csv as pa_csv
import pyarrow.feather as feather
from pathlib import Path

def csv_to_arrow(csv_path: str, arrow_path: str = None):
    """Convert CSV to Arrow IPC format."""
    csv_path = Path(csv_path)
    arrow_path = Path(arrow_path) if arrow_path else csv_path.with_suffix('.arrow')

    print(f"Converting {csv_path} → {arrow_path}")

    # Load CSV
    table = pa_csv.read_csv(csv_path)
    print(f"  Loaded {table.num_rows:,} rows")

    # Write Arrow IPC
    feather.write_feather(table, arrow_path, compression='zstd')
    print(f"  Saved {arrow_path.stat().st_size / 1024 / 1024:.1f} MB")

    return arrow_path

# Example usage
csv_to_arrow('large_data.csv')
```

### Load with Auto-Format Detection

```python
import os
from pathlib import Path
import pyarrow.feather as feather
import pyarrow.csv as pa_csv

def load_data(path: str, prefer_arrow: bool = True):
    """Load data with automatic format detection."""
    path = Path(path)
    arrow_path = path.with_suffix('.arrow')

    # Try Arrow IPC first (if enabled)
    if prefer_arrow and arrow_path.exists():
        print(f"Loading Arrow IPC: {arrow_path}")
        return feather.read_table(arrow_path, memory_map=True)

    # Fall back to CSV
    print(f"Loading CSV: {path}")
    return pa_csv.read_csv(path)

# Usage
table = load_data('data.csv', prefer_arrow=True)
```

### Batch Convert Directory

```bash
# Convert all CSVs in directory to Arrow IPC
for csv in *.csv; do
    python -c "
import pyarrow.csv as pa_csv
import pyarrow.feather as feather
table = pa_csv.read_csv('$csv')
feather.write_feather(table, '${csv%.csv}.arrow', compression='zstd')
print(f'✓ ${csv} → ${csv%.csv}.arrow')
    "
done
```

---

## Environment Variables

### SABOT_USE_ARROW

Enable automatic Arrow IPC loading:

```bash
export SABOT_USE_ARROW=1
```

When enabled, Sabot's loader will:
1. Check if `.arrow` file exists for given `.csv` path
2. Use Arrow IPC if available (50x+ faster)
3. Fall back to CSV if not

### Data Path Configuration

```bash
# Set data directory
export SABOT_DATA_DIR=/path/to/data

# Or individual files
export SABOT_SECURITIES_CSV=/path/to/securities.csv
export SABOT_QUOTES_CSV=/path/to/quotes.csv
```

---

## Quick Start

### 1. Convert Your Data

```bash
cd examples/fintech_enrichment_demo
python convert_csv_to_arrow.py
```

### 2. Enable Arrow IPC

```bash
export SABOT_USE_ARROW=1
```

### 3. Run Your Pipeline

```bash
./run_demo.sh --securities 10000000 --quotes 1200000
```

**Result: 46x faster pipeline execution!** 🚀

---

## FAQ

### Q: Should I use Arrow IPC or Parquet?

**A: Use Arrow IPC for streaming, Parquet for storage.**

- Arrow IPC: Memory-mapped, instant loading, perfect for Sabot pipelines
- Parquet: Better compression, better for long-term storage

### Q: Can I use Arrow IPC with Spark/Pandas?

**A: Yes!**

```python
# Pandas
import pandas as pd
df = pd.read_feather('data.arrow')

# Spark
spark.read.format('arrow').load('data.arrow')
```

### Q: How much disk space will I save?

**A: 50-70% reduction from CSV:**
- CSV → Arrow IPC: 57% smaller (with zstd)
- CSV → Parquet: 60-70% smaller

### Q: Is Arrow IPC production-ready?

**A: Yes!**
- Used by: Apache Spark, Pandas, Dask, Ray
- Format: Stable (part of Arrow specification)
- Performance: Proven in production at scale

### Q: Can I append to Arrow IPC files?

**A: Not directly. For append workloads:**
1. Use Arrow IPC for read-heavy streaming
2. Use Parquet with partitioning for write-heavy
3. Or buffer in memory, write periodically

---

## Additional Resources

- [Apache Arrow IPC Format Spec](https://arrow.apache.org/docs/format/Columnar.html#ipc-file-format)
- [PyArrow Documentation](https://arrow.apache.org/docs/python/)
- [Sabot CyArrow Guide](CYARROW.md)
- [Demo Quickstart](DEMO_QUICKSTART.md)

---

**Last Updated:** October 2025
**Sabot Version:** 0.1.0-alpha
