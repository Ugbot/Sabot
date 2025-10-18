# MarbleDB Documentation

Complete documentation for MarbleDB - a high-performance analytical database with LSM-tree storage and distributed consistency.

---

## 📖 Documentation Index

### 🚀 Getting Started

Start here if you're new to MarbleDB:

1. **[Quick Start](../README.md#quick-start)** - Get running in 5 minutes
2. **[Examples](../examples/README.md)** - Browse working code examples
3. **[Project Structure](../PROJECT_STRUCTURE.md)** - Understand the codebase layout

---

### 🏗️ Architecture & Design

Deep dive into MarbleDB's design and implementation:

#### Core Documents
- **[Technical Plan](TECHNICAL_PLAN.md)** ⭐ - Complete vision and implementation strategy
- **[Next Features Proposal](NEXT_FEATURES_PROPOSAL.md)** ⭐⭐ - **NEW:** Join implementations, OLTP & OLAP improvements (October 2025)
- **[Flexible Table Capabilities](FLEXIBLE_TABLE_CAPABILITIES.md)** ⭐⭐ - **NEW:** Per-table/column-family feature configuration (October 2025)
- **[Roadmap Review](MARBLEDB_ROADMAP_REVIEW.md)** ⭐ - Feature roadmap and priorities
- **[Requirements](../MARBLEDB_REQUIREMENTS.md)** - System requirements

#### Storage & Indexing
- **[Point Lookup Optimizations](POINT_LOOKUP_OPTIMIZATIONS.md)** ⭐ - Hot key cache, sparse index, performance tuning
- **[Hot Key Cache](HOT_KEY_CACHE.md)** ⭐ - LRU caching for frequent keys (5-10 μs)
- **[Optimizations Implemented](OPTIMIZATIONS_IMPLEMENTED.md)** - Complete list of optimizations

#### Distributed Systems
- **[Raft Integration](RAFT_INTEGRATION.md)** ⭐ - Strong consistency with Raft consensus
- **[Arrow Flight + Raft](ARROW_FLIGHT_RAFT_SETUP.md)** ⭐ - High-performance distributed data transfer

---

### ⚡ Features

Comprehensive feature documentation:

#### OLTP & Transactional Features
- **[OLTP Features](OLTP_FEATURES.md)** ⭐ - Transactions, merge operators, column families, multi-get
  - Zero-copy RecordRef (10-100× less memory)
  - Merge operators (atomic counters, append-only logs)
  - Column families (multi-tenant, isolated)
  - Multi-get batching (10-50× faster)
  - Delete range (1000× faster bulk deletion)

#### Advanced Analytics Features
- **[Advanced Features](ADVANCED_FEATURES.md)** ⭐ - TTL, schema evolution, compaction tuning
  - Time To Live (TTL) management
  - Online schema evolution
  - Adaptive compaction tuning
  - Production monitoring

#### Full-Text Search
- **[Build Search Index](BUILD_SEARCH_INDEX_WITH_MARBLEDB.md)** ⭐ - Step-by-step guide to build Lucene-style indexes
- **[Search Integration Summary](SEARCH_INDEX_INTEGRATION_SUMMARY.md)** - Optional Tantivy integration (5-week roadmap)
- **[MarbleDB vs Lucene](MARBLEDB_VS_LUCENE_RESEARCH.md)** ⭐ - Detailed comparison (18K words)
- **[Search Quickstart](../SEARCH_INDEX_QUICKSTART.md)** - 30-minute overview

#### Monitoring & Operations
- **[Monitoring & Metrics](MONITORING_METRICS.md)** ⭐ - Production observability
  - Metrics collection (counters, gauges, histograms)
  - Structured logging
  - Health checks
  - Performance monitoring

---

### 🔌 Integration Guides

Integrate MarbleDB with your applications:

- **[Sabot Integration](SABOT_INTEGRATION_GUIDE.md)** ⭐ - Use MarbleDB as Sabot state backend
- **[Arrow Flight Setup](ARROW_FLIGHT_RAFT_SETUP.md)** - Configure high-performance data transfer
- **[DuckDB Integration](DUCKDB_INTEGRATION_PLAN.md)** - Query MarbleDB with DuckDB
- **[simdjson Integration](SIMDJSON_INTEGRATION.md)** ⭐ - **NEW:** High-performance JSON parsing (4GB/s+)

---

### 📊 Performance & Benchmarks

Performance characteristics and optimization guides:

- **[Benchmark Results](BENCHMARK_RESULTS.md)** ⭐ - Complete benchmark suite
- **[Optimizations Implemented](OPTIMIZATIONS_IMPLEMENTED.md)** - What's been optimized
- **[Comparison: Tonbo & RocksDB](TONBO_ROCKSDB_COMPARISON.md)** ⭐ - Storage engine comparison

**Performance Baseline:**
- Point lookups (hot keys, 80%): **5-10 μs**
- Point lookups (cold keys, 20%): **20-50 μs**
- Analytical scans: **20-50M rows/sec**
- Distributed writes (Raft): **Sub-100ms** (3-node cluster)
- Full-text search: **1-15ms** (1M documents)

---

### 📖 API Reference

Complete API and configuration reference:

- **[API Surface](api/API_SURFACE.md)** ⭐ - Complete C++ API documentation
  - C++ API (modern RAII interface)
  - C API (language-agnostic bindings)
  - Python, Java, Rust, Go examples

---

### 📚 Guides & Tutorials

Detailed guides for specific tasks:

- **[Arctic/Tonbo Analysis](guides/arctic_tonbo_analysis.md)** - Storage format analysis
- **[Tonbo Comparison](guides/tonbo_comparison.md)** - Detailed Tonbo comparison
- **[CMake Usage](guides/example_usage_cmake.md)** - Build and link with MarbleDB
- **[Implementation Plan](guides/implementation_plan.md)** - Development phases

---

## 🗂️ Document Organization by Topic

### Storage Engine
1. [Technical Plan](TECHNICAL_PLAN.md) - LSM-tree architecture
2. [Point Lookup Optimizations](POINT_LOOKUP_OPTIMIZATIONS.md) - Caching & indexing
3. [Hot Key Cache](HOT_KEY_CACHE.md) - LRU cache design
4. [Tonbo & RocksDB Comparison](TONBO_ROCKSDB_COMPARISON.md) - Storage comparison

### Query Processing
1. [Optimizations Implemented](OPTIMIZATIONS_IMPLEMENTED.md) - Query optimizations
2. [DuckDB Integration](DUCKDB_INTEGRATION_PLAN.md) - Analytical query integration

### Distributed Systems
1. [Raft Integration](RAFT_INTEGRATION.md) - Consensus protocol
2. [Arrow Flight + Raft](ARROW_FLIGHT_RAFT_SETUP.md) - Network layer

### Features
1. [OLTP Features](OLTP_FEATURES.md) - Transactional capabilities
2. [Advanced Features](ADVANCED_FEATURES.md) - TTL, schema evolution
3. [Monitoring & Metrics](MONITORING_METRICS.md) - Observability

### Search
1. [Build Search Index](BUILD_SEARCH_INDEX_WITH_MARBLEDB.md) - Implementation guide
2. [Search Integration](SEARCH_INDEX_INTEGRATION_SUMMARY.md) - Tantivy integration
3. [MarbleDB vs Lucene](MARBLEDB_VS_LUCENE_RESEARCH.md) - Deep comparison

---

## 📝 Quick Reference

### Top 11 Essential Documents (⭐ Start Here)

If you only read 11 documents, read these:

1. **[Next Features Proposal](NEXT_FEATURES_PROPOSAL.md)** ⭐⭐ - **NEW:** What's next for MarbleDB (joins, SIMD, parallel execution)
2. **[Flexible Table Capabilities](FLEXIBLE_TABLE_CAPABILITIES.md)** ⭐⭐ - **NEW:** Per-table feature configuration (MVCC, temporal, search, TTL)
3. **[Technical Plan](TECHNICAL_PLAN.md)** - Understand the complete vision
4. **[OLTP Features](OLTP_FEATURES.md)** - Core transactional capabilities
5. **[Advanced Features](ADVANCED_FEATURES.md)** - Advanced analytics features
6. **[Raft Integration](RAFT_INTEGRATION.md)** - Distributed consistency
7. **[Point Lookup Optimizations](POINT_LOOKUP_OPTIMIZATIONS.md)** - Performance tuning
8. **[Build Search Index](BUILD_SEARCH_INDEX_WITH_MARBLEDB.md)** - Add full-text search
9. **[Sabot Integration](SABOT_INTEGRATION_GUIDE.md)** - Use with Sabot streaming
10. **[Monitoring & Metrics](MONITORING_METRICS.md)** - Production observability
11. **[Benchmark Results](BENCHMARK_RESULTS.md)** - Performance characteristics

---

## 🎯 Documentation by Use Case

### "I want to understand MarbleDB"
1. [README.md](../README.md) - Overview
2. [Technical Plan](TECHNICAL_PLAN.md) - Architecture
3. [Roadmap Review](MARBLEDB_ROADMAP_REVIEW.md) - Vision

### "I want to use MarbleDB for OLTP"
1. [OLTP Features](OLTP_FEATURES.md) - Features
2. [Point Lookup Optimizations](POINT_LOOKUP_OPTIMIZATIONS.md) - Performance
3. [API Surface](api/API_SURFACE.md) - API reference

### "I want to use MarbleDB for analytics"
1. [Technical Plan](TECHNICAL_PLAN.md) - Columnar design
2. [Optimizations Implemented](OPTIMIZATIONS_IMPLEMENTED.md) - Query optimization
3. [Advanced Features](ADVANCED_FEATURES.md) - TTL, compaction

### "I want to add full-text search"
1. [Search Quickstart](../SEARCH_INDEX_QUICKSTART.md) - 30-min overview
2. [Build Search Index](BUILD_SEARCH_INDEX_WITH_MARBLEDB.md) - Implementation
3. [MarbleDB vs Lucene](MARBLEDB_VS_LUCENE_RESEARCH.md) - Deep dive

### "I want to build a distributed system"
1. [Raft Integration](RAFT_INTEGRATION.md) - Consensus
2. [Arrow Flight + Raft](ARROW_FLIGHT_RAFT_SETUP.md) - Network layer
3. [Monitoring & Metrics](MONITORING_METRICS.md) - Observability

### "I want to integrate with Sabot"
1. [Sabot Integration Guide](SABOT_INTEGRATION_GUIDE.md) - Integration
2. [OLTP Features](OLTP_FEATURES.md) - State backend capabilities
3. [Raft Integration](RAFT_INTEGRATION.md) - Consistency

---

## 📂 Document Status & Maintenance

### ✅ Current & Maintained (Updated Oct 2025)

These documents are up-to-date with consistent performance numbers:

**Architecture & Design:**
- [Technical Plan](TECHNICAL_PLAN.md)
- [Next Features Proposal](NEXT_FEATURES_PROPOSAL.md)
- [Flexible Table Capabilities](FLEXIBLE_TABLE_CAPABILITIES.md)
- [Roadmap Review](MARBLEDB_ROADMAP_REVIEW.md)
- [Point Lookup Optimizations](POINT_LOOKUP_OPTIMIZATIONS.md)
- [Hot Key Cache](HOT_KEY_CACHE.md)
- [Optimizations Implemented](OPTIMIZATIONS_IMPLEMENTED.md)

**Features:**
- [OLTP Features](OLTP_FEATURES.md)
- [Advanced Features](ADVANCED_FEATURES.md)
- [Monitoring & Metrics](MONITORING_METRICS.md)

**Distributed:**
- [Raft Integration](RAFT_INTEGRATION.md)
- [Arrow Flight + Raft](ARROW_FLIGHT_RAFT_SETUP.md)

**Search:**
- [Build Search Index](BUILD_SEARCH_INDEX_WITH_MARBLEDB.md)
- [Search Integration Summary](SEARCH_INDEX_INTEGRATION_SUMMARY.md)
- [MarbleDB vs Lucene](MARBLEDB_VS_LUCENE_RESEARCH.md)

**Integration:**
- [Sabot Integration Guide](SABOT_INTEGRATION_GUIDE.md)

**Reference:**
- [API Surface](api/API_SURFACE.md)
- [Benchmark Results](BENCHMARK_RESULTS.md)

### 📦 Reference (Stable but Less Critical)

- [DuckDB Integration Plan](DUCKDB_INTEGRATION_PLAN.md)
- [Tonbo & RocksDB Comparison](TONBO_ROCKSDB_COMPARISON.md)
- [Feature Complete Guide](FEATURE_COMPLETE_GUIDE.md)
- [Arctic/Tonbo Analysis](guides/arctic_tonbo_analysis.md)
- [Tonbo Comparison](guides/tonbo_comparison.md)

### 🗄️ Archived (See `archive/` directory)

Moved to `docs/archive/` - kept for historical reference:

- Old implementation plans (superseded by [Technical Plan](TECHNICAL_PLAN.md))
- Legacy comparison documents
- Outdated guides

---

## 💡 Documentation Conventions

### Performance Numbers

All performance numbers use **consistent baselines**:

**Test Environment:**
- CPU: Apple M1 Pro
- Memory: 16 GB
- Storage: SSD
- Dataset: 1M keys, Zipfian distribution

**Metrics:**
- **Point lookups:**
  - Hot keys (80%): 5-10 μs
  - Cold keys (20%): 20-50 μs
- **Analytical scans:**
  - Throughput: 20-50M rows/sec
  - Pruning: 10-100× I/O reduction (via zone maps)
- **Distributed writes:**
  - Latency: Sub-100ms (3-node Raft cluster)
  - Throughput: 10,000+ ops/sec
- **Full-text search:**
  - Simple query: 1-5ms
  - Complex query: 10-30ms

### Hyperlink Style

**Internal links** (within docs directory):
```markdown
[Document](DOCUMENT.md)
[Section](DOCUMENT.md#section-name)
[Subdir](subdir/doc.md)
[Parent](../file.md)
```

**External links**:
```markdown
[Apache Arrow](https://arrow.apache.org/)
[NuRaft](https://github.com/eBay/NuRaft)
```

---

## 🔗 External Resources

**Technologies:**
- [Apache Arrow](https://arrow.apache.org/docs/) - Columnar format
- [NuRaft](https://github.com/eBay/NuRaft) - Raft consensus
- [RocksDB](https://rocksdb.org/) - LSM-tree reference

**Inspirations:**
- [ClickHouse](https://clickhouse.com/docs) - Analytical database design
- [QuestDB](https://questdb.io/docs/) - Time-series ingestion
- [DuckDB](https://duckdb.org/docs/) - Columnar analytics
- [Apache Lucene](https://lucene.apache.org/) - Search index design

---

## 🆘 Getting Help

**Quick Questions:**
- Check [README.md](../README.md)
- Browse [Examples](../examples/README.md)
- See [API Surface](api/API_SURFACE.md)

**Detailed Questions:**
- Search this documentation
- Check relevant guides above
- See [Troubleshooting](../README.md#troubleshooting)

**Report Issues:**
- [GitHub Issues](../../../../issues)
- [GitHub Discussions](../../../../discussions)

---

## 🤝 Contributing to Documentation

When adding or updating documentation:

1. **Maintain consistency:** Use standard performance numbers
2. **Add hyperlinks:** Link to related documents
3. **Update index:** Add entry to this README
4. **Follow conventions:** Use standard formatting
5. **Test links:** Ensure all hyperlinks work

**Documentation Standards:**
- Use Markdown format
- Include code examples
- Document performance characteristics
- Provide build/usage instructions
- Add hyperlinks to related docs

---

**Documentation Version:** 2.0
**Last Updated:** October 16, 2025
**MarbleDB Version:** 0.1.0-alpha

---

**Legend:**
⭐ = Essential reading / Key document
📊 = Contains performance benchmarks
🔗 = Integration guide
🏗️ = Architecture deep-dive
