# MarbleDB

**High-performance analytical database with LSM-tree storage, columnar format, and distributed consistency**

[![Status](https://img.shields.io/badge/status-alpha-orange)]()
[![License](https://img.shields.io/badge/license-Apache%202.0-blue)]()
[![Language](https://img.shields.io/badge/language-C%2B%2B20-blue)]()

---

## What is MarbleDB?

MarbleDB is a unified analytical database that combines:

- **Time-Series Ingestion**: QuestDB-style high-throughput append-only writes
- **Analytical Performance**: ClickHouse-class query performance with columnar storage
- **Distributed Consistency**: Raft-based strong consistency without sacrificing performance
- **Arrow-Native**: Zero-copy operations with Apache Arrow integration
- **Full-Text Search**: Lucene-style inverted indexes (optional)

**Key Features:**
- ✅ LSM-tree storage with columnar Arrow format
- ✅ Sparse indexes + zone maps + bloom filters
- ✅ Hot key caching (5-10 μs point lookups)
- ✅ Raft replication for strong consistency
- ✅ OLTP features (transactions, merge operators, column families)
- ✅ Advanced features (TTL, schema evolution, compaction tuning)

---

## Quick Start

### Prerequisites

- C++20 compiler (GCC 10+, Clang 12+, Apple Clang 13+)
- CMake 3.20+
- Apache Arrow 15.0+ (vendored in `vendor/arrow/`)
- NuRaft (vendored in `vendor/nuraft/`)

### Build from Source

```bash
# Clone repository
git clone <repo-url>
cd MarbleDB

# Build
mkdir build && cd build
cmake ..
make -j$(nproc)

# Run tests
ctest --output-on-failure
```

### Simple Example

```cpp
#include <marble/marble.h>

// Create database
marble::DBOptions options;
options.db_path = "/tmp/mydb";
options.enable_sparse_index = true;
options.enable_bloom_filter = true;

std::unique_ptr<marble::MarbleDB> db;
marble::MarbleDB::Open(options, schema, &db);

// Insert data
auto batch = arrow::RecordBatch::Make(schema, num_rows, arrays);
db->InsertBatch("my_table", batch);

// Query data
marble::KeyRange range = marble::KeyRange::All();
std::unique_ptr<marble::Iterator> iter;
db->NewIterator(marble::ReadOptions{}, range, &iter);

for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    auto record = iter->value();
    // Process record
}
```

**More examples:** See [`examples/`](examples/)

---

## Performance

**Point Lookups** (with hot key cache):
- Hot keys (80%): **5-10 μs**
- Cold keys (20%): **20-50 μs**

**Analytical Scans** (columnar + SIMD):
- Throughput: **20-50M rows/sec**
- I/O reduction: **10-100×** (via zone maps & bloom filters)

**Distributed Writes** (Raft replication):
- Latency: **Sub-100ms** (3-node cluster)
- Throughput: **10,000+ ops/sec**

**Full benchmark results:** [docs/BENCHMARK_RESULTS.md](docs/BENCHMARK_RESULTS.md)

---

## Documentation

### 📚 Getting Started

- **[Quick Start](docs/quick-start.md)** - Get running in 5 minutes
- **[Examples](examples/README.md)** - Working code examples
- **[Architecture Overview](docs/architecture/)** - High-level design

### 🏗️ Architecture & Design

- **[Storage Engine](docs/architecture/storage-engine.md)** - LSM-tree, columnar format, Arrow integration
- **[Indexing](docs/architecture/indexing.md)** - Sparse indexes, zone maps, bloom filters, hot key cache
- **[Query Processing](docs/architecture/query-processing.md)** - Vectorized execution, pruning strategies
- **[Distributed Systems](docs/architecture/distributed.md)** - Raft consensus, replication, fault tolerance

### ⚡ Features

- **[OLTP Features](docs/features/OLTP_FEATURES.md)** - Transactions, merge operators, column families, multi-get
- **[Advanced Features](docs/features/ADVANCED_FEATURES.md)** - TTL, schema evolution, compaction tuning
- **[Monitoring & Metrics](docs/features/MONITORING_METRICS.md)** - Production observability
- **[Full-Text Search](docs/features/search-index.md)** - Build Lucene-style indexes

### 🔌 Integration Guides

- **[Sabot Integration](docs/integrations/SABOT_INTEGRATION_GUIDE.md)** - Use MarbleDB as Sabot state backend
- **[Raft Setup](docs/integrations/RAFT_INTEGRATION.md)** - Configure distributed clusters
- **[Arrow Flight](docs/integrations/ARROW_FLIGHT_RAFT_SETUP.md)** - High-performance data transfer

### 📖 Reference

- **[API Reference](docs/api/API_SURFACE.md)** - Complete API documentation
- **[Configuration](docs/reference/configuration.md)** - DBOptions, tuning parameters
- **[Performance Tuning](docs/reference/performance-tuning.md)** - Optimization guide

### 🗺️ Project Status & Roadmap

- **[Next Features Proposal](docs/NEXT_FEATURES_PROPOSAL.md)** ⭐⭐ - **NEW:** Join implementations, OLTP & OLAP improvements (October 2025)
- **[Technical Plan](docs/TECHNICAL_PLAN.md)** - Complete vision and implementation strategy
- **[Roadmap Review](docs/MARBLEDB_ROADMAP_REVIEW.md)** - Feature roadmap and priorities

---

## Project Structure

```
MarbleDB/
├── include/marble/       # Public C++ headers
│   ├── db.h             # Main database interface
│   ├── record.h         # Record and key abstractions
│   ├── table.h          # Table operations
│   └── ...
├── src/                 # Implementation
│   ├── core/            # Core storage engine
│   ├── raft/            # Raft consensus
│   └── ...
├── examples/            # Example applications
│   ├── basic/           # Simple examples
│   └── advanced/        # Advanced features
├── tests/               # Test suite
│   ├── unit/            # Unit tests
│   └── integration/     # Integration tests
├── benchmarks/          # Performance benchmarks
├── docs/                # Documentation
└── vendor/              # Vendored dependencies
    ├── arrow/           # Apache Arrow C++
    ├── nuraft/          # NuRaft consensus
    └── rocksdb/         # RocksDB C++
```

**Full structure:** [PROJECT_STRUCTURE.md](PROJECT_STRUCTURE.md)

---

## Use Cases

### 1. Time-Series Analytics
Store and query IoT sensor data, financial ticks, application metrics with high ingestion rates and fast analytical queries.

### 2. Real-Time Dashboards
Power live dashboards with sub-second query latency on streaming data.

### 3. Log Analytics
Index and query structured logs with full-text search and fast aggregations.

### 4. State Backend for Stream Processing
Use as a state store for Sabot or other streaming systems with strong consistency guarantees.

### 5. Analytical Database with Search
Combine analytical queries (GROUP BY, aggregations) with full-text search in a single system.

---

## Comparison

### vs RocksDB
- ✅ **10× faster analytical queries** (columnar vs row-oriented)
- ✅ **Zone maps & sparse indexes** (data skipping)
- ⚖️ **Comparable point lookup** (5-10 μs vs 5 μs with caching)

### vs Tonbo
- ✅ **Arrow-native** (Tonbo is also Arrow-based)
- ✅ **Additional indexes** (sparse, zone maps, bloom filters)
- ✅ **OLTP features** (transactions, merge operators)
- ✅ **C++ API** (vs Rust FFI)

### vs ClickHouse
- ⚖️ **Similar analytical performance** (columnar + zone maps)
- ✅ **Stronger consistency** (Raft vs eventual)
- ⚖️ **Simpler architecture** (embedded vs distributed server)

### vs Lucene/Elasticsearch
- ✅ **10× faster analytical queries** (columnar vs doc values)
- ⚖️ **Comparable search** (inverted indexes)
- ✅ **Strong consistency** (Raft vs eventual)
- ✅ **Lower storage** (better compression)

**Detailed comparison:** [docs/comparisons/](docs/comparisons/)

---

## Contributing

We welcome contributions! See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

**Areas we need help with:**
- 📝 Documentation improvements
- 🧪 Test coverage expansion
- ⚡ Performance optimizations
- 🔌 Language bindings (Python, Rust, Go)
- 📊 Benchmarking and profiling

---

## License

Apache License 2.0 - See [LICENSE](LICENSE) for details.

---

## Credits

**Core Technologies:**
- [Apache Arrow](https://arrow.apache.org/) - Columnar in-memory format
- [NuRaft](https://github.com/eBay/NuRaft) - Raft consensus library
- [RocksDB](https://rocksdb.org/) - LSM-tree storage engine (reference)

**Inspired By:**
- **QuestDB** - Time-series ingestion patterns
- **ClickHouse** - Analytical indexing (sparse index, zone maps)
- **ArcticDB** - Bitemporal versioning patterns
- **Apache Lucene** - Inverted index design
- **DuckDB** - Columnar analytics execution

---

## Contact & Support

- **Issues:** [GitHub Issues](../../issues)
- **Discussions:** [GitHub Discussions](../../discussions)
- **Documentation:** [docs/](docs/)

---

**Built for:** High-performance analytical workloads with strong consistency requirements.

**Status:** Alpha - Active development. Not recommended for production use yet.

**Version:** 0.1.0-alpha
