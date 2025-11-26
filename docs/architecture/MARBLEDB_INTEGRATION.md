# MarbleDB Integration Architecture

## Overview

MarbleDB is Sabot's primary storage engine, providing Arrow-native LSM storage with sub-microsecond read latencies. This document describes how MarbleDB is integrated into Sabot's architecture.

## Architecture Layers

```
┌─────────────────────────────────────────────────────────────┐
│                     Python Application                       │
│              (sabot.stores.marbledb.MarbleDBStoreBackend)   │
└─────────────────────────┬───────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────────┐
│                  Cython Layer (State Backend)                │
│           sabot/_cython/state/marbledb_backend.pyx          │
│                                                              │
│  • MarbleDBStateBackend class                               │
│  • Pickle serialization for Python objects                  │
│  • String key hashing to uint64_t                           │
│  • Flink-compatible state API                               │
└─────────────────────────┬───────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────────┐
│                C++ Storage Shim Layer                        │
│                 sabot/storage/interface.h                    │
│                                                              │
│  • IStateBackend: Key-value operations                      │
│  • IStoreBackend: Arrow table operations                    │
│  • Zero-cost abstraction (inline where possible)            │
│  • Backend-agnostic API                                     │
└─────────────────────────┬───────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────────┐
│                    MarbleDB Core                             │
│                  MarbleDB/include/marble/                    │
│                                                              │
│  • LSMTree: Log-Structured Merge Tree                       │
│  • SSTable: Sorted String Tables with Arrow IPC             │
│  • Bloom Filters: Fast negative lookups                     │
│  • Zone Maps: Data skipping indexes                         │
│  • Hot Key Cache: Frequently accessed values                │
└─────────────────────────────────────────────────────────────┘
```

## Integration Points

### 1. State Backend (`marbledb_backend.pyx`)

The primary integration point for key-value state management.

**Location**: `sabot/_cython/state/marbledb_backend.pyx`

**Key Features**:
- String key hashing to uint64_t for LSMTree compatibility
- Encoded key-value format: `[key_len(4)][key_bytes][value_bytes]`
- Flink-compatible state API (ValueState, ListState, MapState)
- Pickle serialization for arbitrary Python objects

**API**:
```python
backend = MarbleDBStateBackend("/path/to/state")
backend.open()

# ValueState
backend.put_value("count", 42)
count = backend.get_value("count")

# Raw key-value
backend.put_raw("key", b"value")
value = backend.get_raw("key")

backend.close()
```

### 2. Store Backend (`marbledb_store.pyx`)

Arrow table storage with InsertBatch/ScanTable APIs.

**Location**: `sabot/_cython/stores/marbledb_store.pyx`

**Key Features**:
- CreateColumnFamily for schema-aware tables
- InsertBatch for bulk Arrow RecordBatch insertion
- ScanTable/NewIterator for full and range scans
- Predicate pushdown via zone maps

### 3. Python Wrapper (`sabot/stores/marbledb.py`)

High-level async API with import fallback.

**Import Chain**:
```python
try:
    # Prefer shim-based implementation
    from sabot._cython.stores.marbledb_store_shim import MarbleDBStoreBackend
except ImportError:
    # Fallback to direct implementation
    from sabot._cython.stores.marbledb_store import MarbleDBStoreBackend
```

### 4. SPARQL Storage (`sabot_ql/`)

RDF triple storage using MarbleDB's 3-index strategy.

**Indexes**:
- SPO (Subject-Predicate-Object)
- POS (Predicate-Object-Subject)
- OSP (Object-Subject-Predicate)

## Storage Shim Design

The storage shim provides backend abstraction:

```cpp
// interface.h
namespace sabot::storage {

class IStateBackend {
public:
    virtual Status Open(const StorageConfig& config) = 0;
    virtual Status Close() = 0;
    virtual Status Get(const std::string& key, std::string* value) = 0;
    virtual Status Put(const std::string& key, const std::string& value) = 0;
    virtual Status Delete(const std::string& key) = 0;
    virtual Status Scan(const std::string& start, const std::string& end,
                        ScanCallback callback) = 0;
};

class IStoreBackend {
public:
    virtual Status CreateTable(const std::string& name,
                               std::shared_ptr<arrow::Schema> schema) = 0;
    virtual Status InsertBatch(const std::string& table,
                               std::shared_ptr<arrow::RecordBatch> batch) = 0;
    virtual std::shared_ptr<arrow::Table> ScanTable(const std::string& table) = 0;
};

}  // namespace sabot::storage
```

## Performance Characteristics

### Measured Performance (Apple Silicon)

| Metric | Value |
|--------|-------|
| Sequential Write | 445K ops/sec |
| Sequential Read | 2M ops/sec |
| Hot Key Read | 4.3M ops/sec |
| Mixed (90% read) | 1.4M ops/sec |
| Read Latency (avg) | 0.72µs |
| Read Latency (P99) | 3µs |

### Why So Fast?

1. **Bloom Filters**: Skip SSTable reads for non-existent keys
2. **Zone Maps**: Skip SSTable reads based on value ranges
3. **Hot Key Cache**: O(1) lookup for frequently accessed keys
4. **MMAP**: Memory-mapped SSTable reads
5. **Arrow IPC**: Zero-copy SSTable format

## Build Integration

MarbleDB is built as part of Sabot's unified build system:

```bash
# Full build (includes MarbleDB)
python build.py

# MarbleDB phase in build.py
def build_marbledb():
    """Build MarbleDB C++ library."""
    # Creates libmarble.a in MarbleDB/build/
```

The Cython extension links against:
- `libmarble.a` (MarbleDB static library)
- `libarrow.so` (Vendored Arrow)
- `libparquet.so` (Vendored Parquet)

## Directory Structure

```
Sabot/
├── MarbleDB/
│   ├── include/marble/      # C++ headers
│   │   ├── api.h            # Main MarbleDB API
│   │   ├── lsm_storage.h    # LSMTree interface
│   │   ├── bloom_filter.h   # Bloom filter
│   │   └── ...
│   ├── src/core/            # C++ implementations
│   └── build/               # Build output
│       └── libmarble.a      # Static library
│
├── sabot/
│   ├── storage/             # Storage shim layer
│   │   ├── interface.h      # C++ interfaces
│   │   ├── marbledb_backend.h/cpp
│   │   ├── storage_shim.pyx # Cython wrapper
│   │   └── README.md
│   │
│   ├── _cython/state/       # Cython state backends
│   │   ├── marbledb_backend.pyx  # Direct API
│   │   └── marbledb_backend_shim.pyx  # Via shim
│   │
│   └── stores/              # Python store wrappers
│       └── marbledb.py
│
└── tests/integration/
    ├── test_marbledb_integration.py
    └── test_marbledb_performance.py
```

## Known Limitations

1. **ListState/MapState**: Use in-memory storage due to Cython limitations
   - Workaround: Use ValueState with serialized lists/dicts

2. **Persistence**: Requires proper WAL handling
   - Data is consistent within session
   - May not persist without explicit flush

3. **Checkpoint/Restore**: Depends on persistence
   - Works for in-session checkpoints
   - Cross-session restore needs WAL

## Future Work

1. **Pluggable Optimizations**: Per-table optimization strategies
   - See `MarbleDB/docs/planning/PLUGGABLE_OPTIMIZATIONS_DESIGN.md`

2. **Persistent ListState/MapState**: Convert to proper cdef attributes

3. **WAL Integration**: Full durability support

4. **Distributed State**: Multi-node state partitioning
