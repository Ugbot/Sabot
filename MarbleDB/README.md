# MarbleDB

**Arrow-native LSM storage engine**

*⚠️ Alpha status - experimental, not production ready*

---

## What is MarbleDB?

MarbleDB is an **Arrow-native LSM storage engine** that provides:

- **Arrow RecordBatch storage** - Native Arrow format, zero-copy reads
- **LSM-tree architecture** - Write-optimized with background compaction
- **Bloom filters** - Fast negative lookups
- **Pluggable optimization strategies** - Configurable per workload

## Current Status

**What Works:**
- ✅ Library compiles (`libmarble.a`)
- ✅ Basic LSM operations
- ✅ Bloom filter strategy
- ✅ Cache strategy
- ✅ Optimization pipeline
- ✅ 19/19 unit tests passing

**What's Experimental:**
- ⚠️ Arrow-native read/write path
- ⚠️ Skip list memtable
- ⚠️ SSTable format

**Not Implemented:**
- ❌ Distributed/Raft consensus
- ❌ Full-text search
- ❌ Production hardening

## Quick Start

### Prerequisites

- C++20 compiler (GCC 10+, Clang 12+, Apple Clang 13+)
- CMake 3.20+
- Apache Arrow (vendored in `vendor/arrow/`)

### Build

```bash
cd MarbleDB
mkdir -p build && cd build
cmake ..
make -j$(nproc)
```

### Run Tests

```bash
cd build/tests
env DYLD_LIBRARY_PATH=/path/to/arrow/lib ./test_optimization_strategies
```

### Simple Example

```cpp
#include <marble/marble.h>

// Create database
marble::DBOptions options;
options.db_path = "/tmp/mydb";
options.enable_bloom_filter = true;

std::unique_ptr<marble::MarbleDB> db;
marble::MarbleDB::Open(options, schema, &db);

// Insert Arrow RecordBatch
auto batch = arrow::RecordBatch::Make(schema, num_rows, arrays);
db->InsertBatch("my_table", batch);

// Query
auto iter = db->NewIterator(marble::ReadOptions{});
for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    // Process records
}
```

## Benchmarks

**Arrow-Native vs Legacy (100K rows):**

| Path | Write | Read | Total |
|------|-------|------|-------|
| Legacy | 1.87ms | 2.58ms | 4.45ms |
| Arrow-Native | 3.27ms | 0.07ms | 3.34ms |

- Arrow reads are ~39x faster (zero-copy)
- Arrow writes have overhead
- Overall ~1.3x faster

**RocksDB Baseline (100K keys, 512B values):**

| Operation | Throughput | Latency |
|-----------|------------|---------|
| Writes | 178 K/sec | 5.6 μs |
| Lookups | 357 K/sec | 2.8 μs |
| Scans | 4.4 M/sec | 0.23 μs |

See `benchmarks/` for runnable benchmarks.

## Project Structure

```
MarbleDB/
├── include/marble/       # C++ headers
│   ├── db.h             # Database interface
│   ├── table.h          # Table operations
│   └── optimizations/   # Strategy implementations
├── src/core/            # Implementation
├── tests/               # Test suite
├── benchmarks/          # Performance tests
└── docs/                # Documentation
```

## Sabot Integration

MarbleDB integrates with Sabot as a state backend:

```python
from sabot._cython.state import MarbleDBBackend

backend = MarbleDBBackend(path="/tmp/state")
await backend.set("key", value)
result = await backend.get("key")
```

See `sabot/_cython/state/marbledb_backend.pyx`

## Comparison

### vs RocksDB
- ✅ Arrow-native format (zero-copy)
- ✅ Columnar storage
- ⚠️ Less mature
- ⚠️ Fewer features

### vs DuckDB
- ✅ LSM write optimization
- ✅ Streaming-friendly
- ❌ No SQL engine
- ❌ Less query optimization

## Known Limitations

- Not production tested
- Single-threaded benchmarks only
- No distributed features
- Limited documentation

## Contributing

Focus areas:
1. Testing and stability
2. Performance optimization
3. Documentation
4. Feature completion

## License

Apache License 2.0

---

**Status:** Alpha - builds and tests pass, not production ready
