# MarbleDB Python Bindings

Python bindings for MarbleDB using Cython, providing a Pythonic interface to the high-performance C++ LSM-tree storage engine.

## Features

- **Zero-copy integration** with Apache Arrow via PyArrow
- **Full MarbleDB API** exposed to Python
- **Pythonic interface** with context managers, iterators, and properties
- **Type safety** with Arrow schemas
- **High performance** through direct C++ integration

## Requirements

- Python 3.8+
- PyArrow >= 10.0.0
- Cython >= 0.29.0
- MarbleDB C++ library (built)
- Apache Arrow C++ library (vendored in Sabot)

## Installation

### 1. Build MarbleDB C++ Library

First, ensure MarbleDB is built:

```bash
cd /Users/bengamble/Sabot/MarbleDB
mkdir -p build
cd build
cmake -DCMAKE_BUILD_TYPE=Release \
      -DARROW_DIR=/Users/bengamble/Sabot/vendor/arrow/cpp/build/install/lib/cmake/Arrow \
      ..
make -j$(nproc)
```

This creates `libmarble.a` in `MarbleDB/build/`.

### 2. Build Python Bindings

```bash
cd /Users/bengamble/Sabot/MarbleDB/python

# Install dependencies
pip install pyarrow cython

# Build the extension in-place
python setup.py build_ext --inplace
```

This generates `marbledb.so` (or `.pyd` on Windows) in the current directory.

### 3. Install (Optional)

```bash
# Install system-wide
python setup.py install

# Or install in development mode
pip install -e .
```

## Quick Start

```python
import pyarrow as pa
import marbledb

# Configure database
options = marbledb.PyDBOptions()
options.db_path = '/tmp/my_db'
options.enable_wal = True
options.enable_sparse_index = True

# Open database
db = marbledb.MarbleDB.open(options)

# Define schema
schema = pa.schema([
    pa.field('id', pa.int64()),
    pa.field('name', pa.string()),
])

# Create column family
cf_options = marbledb.PyColumnFamilyOptions()
cf_options.set_schema(schema)
cf_options.enable_bloom_filter = True
db.create_column_family('users', cf_options)

# Insert data
ids = pa.array([1, 2, 3], type=pa.int64())
names = pa.array(['Alice', 'Bob', 'Charlie'], type=pa.string())
batch = pa.RecordBatch.from_arrays([ids, names], schema=schema)
db.insert_batch('users', batch)

# Query data
result = db.scan_table('users')
table = result.to_table()
print(table.to_pandas())

# Close database
db.close()
```

## API Reference

### PyDBOptions

Database configuration options:

```python
options = marbledb.PyDBOptions()
options.db_path = '/path/to/db'        # Database directory path
options.enable_wal = True               # Enable write-ahead log
options.enable_sparse_index = True      # Enable sparse indexing
options.wal_buffer_size = 64 * 1024 * 1024  # WAL buffer size (bytes)
```

### PyColumnFamilyOptions

Column family configuration:

```python
cf_options = marbledb.PyColumnFamilyOptions()
cf_options.set_schema(arrow_schema)     # Set Arrow schema
cf_options.enable_bloom_filter = True   # Enable bloom filter
```

### MarbleDB

Main database class:

```python
# Open database
db = marbledb.MarbleDB.open(options, column_families=None)

# Create column family
db.create_column_family(name, cf_options)

# Insert data
db.insert_batch(cf_name, record_batch)

# Scan table
result = db.scan_table(cf_name)  # Returns PyQueryResult
table = result.to_table()         # Convert to Arrow Table

# Create iterator
iterator = db.new_iterator(cf_name, start_key=None, end_key=None)

# Flush to disk
db.flush()

# Compact data
db.compact_range()

# Close database
db.close()
```

### Context Manager Support

```python
with marbledb.MarbleDB.open(options) as db:
    # Database automatically closes on exit
    db.create_column_family('data', cf_options)
    db.insert_batch('data', batch)
```

### PyQueryResult

Iterator over query results:

```python
result = db.scan_table('users')

# Check if more batches available
if result.has_next():
    batch = result.next()  # Get next RecordBatch

# Get schema
schema = result.schema()

# Convert all results to Table
table = result.to_table()

# Iterate in Python
for batch in result:
    print(batch)
```

### PyIterator

Range scanning iterator:

```python
iterator = db.new_iterator('data')

# Seek to key
start_key = marbledb.PyTripleKey(0, 0, 0)
iterator.seek(start_key)

# Check validity
if iterator.valid():
    key = iterator.key()     # Get current key
    value = iterator.value() # Get current value (RecordBatch)
    iterator.next()          # Move to next

# Python iteration
for key, value in iterator:
    print(f"Key: {key}, Value: {value}")
```

### PyTripleKey

Key type for RDF triples:

```python
key = marbledb.PyTripleKey(subject=1, predicate=2, obj=3)

# Access components
print(key.subject)    # 1
print(key.predicate)  # 2
print(key.object)     # 3

# Compare keys
cmp = key.compare(other_key)  # Returns -1, 0, or 1
```

## Examples

### Example 1: RDF Triple Store

```python
import marbledb

# Open database
options = marbledb.PyDBOptions()
options.db_path = '/tmp/rdf_store'
db = marbledb.MarbleDB.open(options)

# Create SPO index
schema = pa.schema([
    pa.field('s', pa.int64()),
    pa.field('p', pa.int64()),
    pa.field('o', pa.int64()),
])

cf_opts = marbledb.PyColumnFamilyOptions()
cf_opts.set_schema(schema)
db.create_column_family('spo', cf_opts)

# Insert triples
subjects = pa.array([1, 1, 2], type=pa.int64())
predicates = pa.array([10, 11, 10], type=pa.int64())
objects = pa.array([100, 101, 100], type=pa.int64())
batch = pa.RecordBatch.from_arrays([subjects, predicates, objects], schema=schema)
db.insert_batch('spo', batch)

# Query triples
result = db.scan_table('spo')
print(result.to_table())

db.close()
```

### Example 2: Batch Processing

```python
import marbledb
import pyarrow as pa

# Open database
options = marbledb.PyDBOptions()
options.db_path = '/tmp/batch_db'
db = marbledb.MarbleDB.open(options)

schema = pa.schema([
    pa.field('id', pa.int64()),
    pa.field('value', pa.int64()),
])

cf_opts = marbledb.PyColumnFamilyOptions()
cf_opts.set_schema(schema)
db.create_column_family('data', cf_opts)

# Insert multiple batches
for i in range(10):
    ids = pa.array(range(i * 1000, (i+1) * 1000), type=pa.int64())
    values = pa.array([x * 10 for x in range(1000)], type=pa.int64())
    batch = pa.RecordBatch.from_arrays([ids, values], schema=schema)
    db.insert_batch('data', batch)
    print(f"Inserted batch {i+1}/10")

# Flush to disk
db.flush()

# Scan all data
result = db.scan_table('data')
table = result.to_table()
print(f"Total rows: {table.num_rows}")

db.close()
```

## Testing

Run the test suite:

```bash
python test_marbledb.py
```

Tests cover:
- Basic operations (create, insert, scan)
- Iterator functionality
- Context manager usage
- TripleKey operations
- Large batch insertion (10K records)

## Performance Tips

1. **Batch Size**: Use larger batches (1K-10K rows) for better throughput
2. **WAL Buffer**: Increase `wal_buffer_size` for write-heavy workloads
3. **Bloom Filters**: Enable for column families with many lookups
4. **Sparse Index**: Enable for large datasets to reduce memory usage
5. **Flush Strategy**: Call `flush()` periodically to control memory usage
6. **Context Managers**: Use `with` to ensure proper cleanup

## Troubleshooting

### Import Error: `marbledb.so` not found

Make sure you built the extension:
```bash
python setup.py build_ext --inplace
```

### Arrow Version Mismatch

Ensure PyArrow version matches the vendored Arrow C++ version:
```bash
pip install pyarrow==10.0.1  # Match your Arrow version
```

### Linker Errors

Check that `libmarble.a` exists:
```bash
ls /Users/bengamble/Sabot/MarbleDB/build/libmarble.a
```

### Runtime Errors

Set library path on macOS:
```bash
export DYLD_LIBRARY_PATH=/Users/bengamble/Sabot/vendor/arrow/cpp/build/install/lib:$DYLD_LIBRARY_PATH
```

On Linux:
```bash
export LD_LIBRARY_PATH=/Users/bengamble/Sabot/vendor/arrow/cpp/build/install/lib:$LD_LIBRARY_PATH
```

## Architecture

```
┌─────────────────────────────────────┐
│   Python Application                │
│   (test_marbledb.py)               │
└──────────────┬──────────────────────┘
               │
               │ marbledb module
               ▼
┌─────────────────────────────────────┐
│   Cython Wrapper                    │
│   (marbledb.pyx)                   │
│   - PyDBOptions                     │
│   - MarbleDB                        │
│   - PyIterator                      │
│   - PyQueryResult                   │
└──────────────┬──────────────────────┘
               │
               │ C++ API
               ▼
┌─────────────────────────────────────┐
│   MarbleDB C++ Library              │
│   (libmarble.a)                    │
│   - LSM-tree storage                │
│   - Column families                 │
│   - Iterators                       │
│   - WAL                             │
└──────────────┬──────────────────────┘
               │
               │ Arrow C++
               ▼
┌─────────────────────────────────────┐
│   Apache Arrow C++                  │
│   (vendored)                        │
│   - Zero-copy buffers               │
│   - RecordBatch                     │
│   - Schema                          │
└─────────────────────────────────────┘
```

## License

Part of the Sabot project.
