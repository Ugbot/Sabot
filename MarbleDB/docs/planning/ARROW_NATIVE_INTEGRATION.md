# MarbleDB Arrow-Native Integration - Comprehensive Planning Document

**Version**: 1.0
**Date**: November 7, 2025
**Status**: Planning Phase

---

## Executive Summary

This document outlines the architecture and implementation plan for making MarbleDB a **truly Arrow-native storage engine** that serves as the primary storage layer for the **Sabot streaming and graph platform**.

### Vision

MarbleDB should be the **ideal storage backend for Sabot**, supporting:

**Primary Use Case: Sabot Platform**
- **Graph storage**: Property graphs with Cypher queries
- **RDF/SPARQL**: Triple stores with 3-index strategy (SPO, POS, OSP)
- **Streaming state**: State backends for Flink-style operators
- **Materialized views**: Incremental computation results
- **Analytics**: Fast OLAP queries via Arrow compute engines

**Secondary Use Case: RocksDB Replacement**
- Drop-in compatibility for existing RocksDB users
- Migration path with Arrow benefits

**Design Goal: Universal Arrow Integration**
- Works with multiple Arrow compute engines that Sabot uses:
  - Apache Arrow Acero (C++ query engine)
  - Apache DataFusion (Rust query engine)
  - DuckDB (OLAP SQL queries)
  - PyArrow compute (Python API)

### Core Principle

**MarbleDB is Sabot's storage layer. The RocksDB API is for compatibility/testing only. The real API should be Arrow-first.**

### Current State (Based on Research)

**Strengths** ✅:
- Data stored in Arrow IPC format (zero-copy potential)
- `ArrowRecordBatchIterator` for streaming scans
- Predicate pushdown (bloom filters, sparse index, column statistics)
- Existing optimization infrastructure (hot key cache, negative cache)

**Gaps** ❌:
- Not exposing standard Arrow interfaces (`RecordBatchReader`, `Dataset`, etc.)
- No integration with Arrow compute ecosystem
- LSM merge logic not exposed as Arrow-native iterator
- Custom query engine instead of leveraging Arrow compute libraries

### Success Criteria

**Primary: Sabot Integration**
1. **Graph queries work**: Cypher queries via Acero/DataFusion with MarbleDB storage
2. **RDF queries work**: SPARQL queries with 100x speedup (vs current implementation)
3. **Streaming state works**: State backends for stream-stream joins, windowing
4. **Performance targets met**: Graph (1M matches/sec), RDF (10M triples/sec), State (500K ops/sec)

**Secondary: Arrow Ecosystem Compatibility**
1. **Open** a MarbleDB table as an Arrow `RecordBatchReader` or `Dataset`
2. **Scan** with column projection (only read needed columns)
3. **Filter** with predicate pushdown (bloom filters, sparse indexes)
4. **Stream** results incrementally (no materialization)
5. **Join/Aggregate** using Arrow compute engines (Acero, DataFusion, DuckDB)
6. **Zero-copy** access to data (memory-mapped or shared buffers)

---

## Table of Contents

1. [Current Architecture Analysis](#1-current-architecture-analysis)
2. [Arrow Ecosystem Landscape](#2-arrow-ecosystem-landscape)
3. [Why Target the Entire Arrow Ecosystem](#3-why-target-the-entire-arrow-ecosystem)
4. [Primary Use Cases](#4-primary-use-cases)
5. [Standard Arrow Interfaces](#5-standard-arrow-interfaces)
6. [Proposed Arrow-Native API Design](#6-proposed-arrow-native-api-design)
7. [LSM-Aware Iterator Architecture](#7-lsm-aware-iterator-architecture)
8. [Predicate Pushdown Design](#8-predicate-pushdown-design)
9. [Zero-Copy Memory Management](#9-zero-copy-memory-management)
10. [Integration Patterns by Tool](#10-integration-patterns-by-tool)
11. [Performance Targets](#11-performance-targets)
12. [Implementation Stages](#12-implementation-stages)
13. [Testing Strategy](#13-testing-strategy)
14. [Migration Path](#14-migration-path)
15. [Open Questions](#15-open-questions)
16. [Success Metrics](#16-success-metrics)
17. [Conclusion](#17-conclusion)

---

## 1. Current Architecture Analysis

### 1.1 Storage Layer

**File**: `MarbleDB/src/core/arrow_sstable_writer.cpp`

```
SSTable On-Disk Format:
┌─────────────────────────────────────┐
│ [Arrow IPC Stream Data]             │ ← RecordBatches (columnar)
│   - Batch 0 (Schema + Data)         │
│   - Batch 1 (Data)                  │
│   - ...                             │
│   - Batch N (Data)                  │
├─────────────────────────────────────┤
│ [Sparse Index Section]              │ ← [key → batch_idx] mappings
│   - Every Nth key (e.g., every 100) │
│   - Enables fast seek to key range  │
├─────────────────────────────────────┤
│ [Metadata Section]                  │
│   - Column Statistics               │
│     * min/max per column            │
│     * null count                    │
│     * distinct count (estimate)     │
│   - Bloom Filter (serialized)       │
│   - Skipping Index (zone maps)      │
│   - Schema metadata                 │
├─────────────────────────────────────┤
│ [Footer]                            │
│   - Sparse index offset             │
│   - Metadata offset                 │
│   - Magic number: "ARROWSST"        │
└─────────────────────────────────────┘
```

**Key Characteristics**:
- ✅ Arrow IPC format (industry standard, zero-copy compatible)
- ✅ Self-describing (schema embedded)
- ✅ Rich metadata (statistics for query optimization)
- ✅ Sorted by key (enables merge joins)

**Strengths**:
- Already Arrow-native at storage level
- Can be read by any Arrow IPC reader
- Metadata enables advanced optimizations

**Limitations**:
- Custom footer format (not standard Arrow IPC footer)
- Sparse index is MarbleDB-specific (not Arrow metadata)
- Bloom filter format is custom

### 1.2 Read Path

**File**: `MarbleDB/include/marble/arrow_sstable_reader.h` (lines 50-150)

```cpp
class ArrowSSTableReader {
    // Opens Arrow IPC stream
    Status LoadArrowBatches() {
        auto ipc_reader = arrow::ipc::RecordBatchStreamReader::Open(file);
        while (ipc_reader->ReadNext(&batch)) {
            batches_.push_back(batch);  // Load all batches into memory
        }
    }

    // Point lookup (uses sparse index + bloom filter)
    Status Get(const Key& key, std::string* value);

    // Range scan
    Status Scan(KeyRange range, std::vector<Record>* results);
};
```

**Current API**: Row-oriented (Get/Scan return individual records)

**What We Need**: Columnar API (return RecordBatches)

### 1.3 LSM Tree Architecture

**File**: `MarbleDB/include/marble/lsm_tree.h`

```
LSM Tree Structure:
┌──────────────────────────────────────┐
│ MemTable (L0)                        │ ← Active writes
│   - SkipList or B-Tree               │
│   - Sorted by key                    │
│   - Size: 64-128 MB                  │
├──────────────────────────────────────┤
│ Immutable MemTable (L0)              │ ← Flushing to disk
├──────────────────────────────────────┤
│ Level 0 (4-8 SSTables)               │ ← May overlap
│   - SSTable_000001.sst               │
│   - SSTable_000002.sst               │
│   - ...                              │
├──────────────────────────────────────┤
│ Level 1 (10-20 SSTables)             │ ← Non-overlapping
│   - Size: ~640 MB                    │
├──────────────────────────────────────┤
│ Level 2 (100-200 SSTables)           │
│   - Size: ~6.4 GB                    │
├──────────────────────────────────────┤
│ ...                                  │
├──────────────────────────────────────┤
│ Level 6 (Largest)                    │
│   - Size: TB scale                   │
└──────────────────────────────────────┘

Read Path:
1. Check MemTable (in-memory)
2. Check Immutable MemTable
3. Check L0 SSTables (may need to check all)
4. Check L1-L6 (binary search by key range)
5. Merge results (deduplication, most recent wins)
```

**Challenge for Arrow Integration**:
- Need to merge across multiple SSTables efficiently
- Must deduplicate (same key may exist in multiple levels)
- Must return sorted, merged RecordBatches

### 1.4 Optimization Infrastructure

**File**: `MarbleDB/include/marble/arrow_sstable_reader.h` (lines 200-300)

**Current Optimizations**:

1. **Bloom Filter** (per-SSTable):
   - False positive rate: 1%
   - Size: ~10 bits per key
   - Benefit: Skip SSTables that definitely don't contain key
   - Performance: ~500ns per lookup

2. **Sparse Index** (ClickHouse-style):
   - Entry every N keys (e.g., N=100)
   - Enables binary search to locate block
   - Performance: ~5μs to locate block

3. **Zone Maps** (min/max per block):
   - Per-column min/max values
   - Block size: 8192 rows
   - Benefit: Skip entire blocks for range predicates
   - Performance: 10-1000x speedup on selective queries

4. **Hot Key Cache**:
   - LRU cache for frequently accessed keys
   - Size: 10,000 entries
   - Performance: ~100ns per hit

5. **Negative Cache**:
   - Remember keys that definitely don't exist
   - Avoid repeated lookups for missing keys
   - Performance: ~50ns per hit

**These optimizations must be preserved in Arrow-native API!**

---

## 2. Arrow Ecosystem Landscape

### 2.1 Arrow Compute Engines

| Engine | Language | Integration Method | API Needed |
|--------|----------|-------------------|------------|
| **Acero** | C++ | Direct linking | `RecordBatchReader`, custom `ExecNode` |
| **DataFusion** | Rust | FFI or Arrow C Data | `RecordBatchReader` via C Data Interface |
| **DuckDB** | C++ | Arrow integration | Arrow `Scanner`, `RecordBatchReader` |
| **Polars** | Rust | Arrow FFI | Arrow C Data Interface |
| **PyArrow** | Python (C++) | Python bindings | `pyarrow.RecordBatchReader` |
| **Velox** | C++ (Meta) | Direct linking | Custom scan interface |
| **Substrait** | Universal | Abstract plan | Implement Substrait producer/consumer |

### 2.2 Common Arrow Interfaces

All these engines expect one or more of:

1. **`arrow::RecordBatchReader`** (C++)
   - Standard streaming interface
   - `schema()`, `ReadNext(batch)`
   - Used by: Acero, DuckDB, PyArrow

2. **Arrow C Data Interface** (FFI)
   - Zero-copy across languages
   - Used by: DataFusion, Polars, R, Julia
   - Structures: `ArrowSchema`, `ArrowArray`

3. **`arrow::Dataset`** (C++)
   - Higher-level abstraction
   - Supports partitioning, filtering
   - Used by: Acero, PyArrow datasets

4. **Arrow Flight** (RPC)
   - Distributed query protocol
   - Streaming RecordBatches over network
   - Used by: Distributed systems

### 2.3 What Engines Expect from Storage

**Minimum Requirements**:
1. Stream RecordBatches (not materialize all at once)
2. Provide schema upfront
3. Handle backpressure (don't overwhelm consumer)

**Advanced Features** (competitive advantage):
1. **Predicate pushdown**: Apply filters at storage layer
2. **Column projection**: Only read needed columns
3. **Statistics**: Provide min/max/null counts for optimization
4. **Partition pruning**: Skip entire partitions based on metadata
5. **Adaptive batch sizing**: Tune batch size based on consumer needs

### 2.4 Competitive Landscape

**Parquet** (current gold standard for Arrow ecosystem):
- ✅ Columnar storage (Arrow-native)
- ✅ RecordBatchReader interface
- ✅ Predicate pushdown (row group filtering)
- ✅ Column projection (only read needed columns)
- ❌ Read-only (no mutations)
- ❌ No indexes for point lookups
- ❌ No LSM for updates

**Delta Lake / Iceberg** (lakehouse formats):
- ✅ ACID transactions
- ✅ Time travel
- ✅ Partition pruning
- ❌ Still backed by Parquet (no point lookup optimization)
- ❌ Metadata overhead
- ❌ Not optimized for OLTP

**RocksDB** (key-value store):
- ✅ Fast point lookups (1-10μs)
- ✅ LSM tree (handles updates)
- ❌ Row-oriented (not Arrow-native)
- ❌ No predicate pushdown
- ❌ Poor analytical query performance

**MarbleDB's Unique Position**:
- ✅ Arrow-native storage (like Parquet)
- ✅ Fast point lookups (like RocksDB)
- ✅ LSM tree (handles updates, unlike Parquet)
- ✅ Predicate pushdown (bloom filters, zone maps)
- 🎯 **Can be best of all worlds if API is right!**

---

## 3. Why Target the Entire Arrow Ecosystem (Not Just Acero)

### 3.1 The Broader Goal

MarbleDB's goal is to be the **universal Arrow-native storage backend** that works with **any** Arrow compute engine, not just one specific tool.

**Supported Engines**:
- **Apache Arrow Acero** (C++ query engine)
- **Apache DataFusion** (Rust query engine)
- **DuckDB** (OLAP database with Arrow support)
- **Polars** (DataFrame library)
- **PyArrow compute** (Python)
- **Velox** (Meta's query engine)
- **Any future Arrow tool**

### 3.2 Why This Approach

**1. Standard Interfaces = Universal Compatibility**

By implementing standard Arrow interfaces (`RecordBatchReader`, `Dataset`, C Data Interface), MarbleDB automatically works with the entire ecosystem. We build once, it works everywhere.

**2. No Lock-In**

Users can choose the query engine that best fits their needs:
- Want Rust? → Use DataFusion
- Want SQL? → Use DuckDB
- Want DataFrames? → Use Polars
- Want custom query plans? → Use Acero

MarbleDB doesn't dictate the query engine.

**3. Focus on Storage, Not Query Execution**

MarbleDB should be **world-class storage**, not a query engine:
- ✅ Fast point lookups (2.18 μs/op)
- ✅ Fast analytical scans (3.52 M rows/sec)
- ✅ Predicate pushdown (bloom filters, zone maps)
- ✅ Zero-copy access
- ✅ LSM tree for updates

Let Acero/DataFusion/DuckDB handle query execution - they're better at it!

### 3.3 Implementation Strategy

**Stages 1-5**: Build standard Arrow APIs (universal)
- RecordBatchReader (streaming interface)
- Dataset API (high-level abstraction)
- Expression conversion (predicate pushdown)
- Zero-copy optimizations
- LSM merge iterator

**Stage 6**: Validate with multiple engines
- Acero integration (primary validation)
- DataFusion integration (Rust FFI validation)
- DuckDB integration (SQL validation)

**Stage 7-8**: Cross-language support and hardening
- C Data Interface (FFI)
- Python bindings
- Production quality

### 3.4 Acero's Role

**Acero is one validation target, not the only goal.**

We use Acero in Stage 6 to validate that our Arrow APIs work correctly with a production query engine. But the APIs should work equally well with DataFusion, DuckDB, or any other Arrow-native tool.

**Why Acero is a good validation target**:
- Part of official Arrow project (reference implementation)
- C++ (same language as MarbleDB, easy integration)
- Comprehensive operators (hash join, aggregate, filter, project)
- Well-documented and actively maintained

But users can choose any Arrow engine they prefer.

### 3.5 Sabot's Multi-Engine Architecture

**Why Sabot needs universal Arrow compatibility:**

Sabot is a **unified streaming, graph, and analytics platform** that uses **multiple Arrow engines** for different workloads:

1. **Apache Arrow Acero** (C++):
   - SQL query execution
   - Hash joins, aggregations
   - Physical query plans

2. **Apache DataFusion** (Rust):
   - Rust components
   - Distributed query planning
   - Advanced optimization rules

3. **DuckDB** (C++ with SQL):
   - Interactive SQL analytics
   - ClickBench-style OLAP queries
   - User-facing SQL API

4. **PyArrow compute** (Python):
   - Python API
   - Jupyter notebooks
   - Data science workflows

**Sabot's Query Flow Example**:
```
User Query (SQL or Cypher)
    ↓
Sabot Query Planner
    ↓
Chooses optimal engine:
  - Simple query? → DuckDB (fastest compile time)
  - Complex join? → Acero (best join optimization)
  - Distributed? → DataFusion (Rust, network-aware)
    ↓
MarbleDB provides data via RecordBatchReader
    ↓
Zero-copy streaming to chosen engine
    ↓
Results back to user
```

**Why MarbleDB must work with all engines**:
- Sabot optimizes per-query (engine selection is dynamic)
- Can't be locked into one engine
- Standard Arrow APIs enable this flexibility

**Example**: SPARQL query execution
1. Parse SPARQL → translate to logical plan
2. Choose engine based on complexity:
   - 2-pattern join → Acero (fast hash join)
   - 10-pattern join → DataFusion (distributed)
3. MarbleDB streams RDF triples as RecordBatches
4. Engine executes join (SIMD-optimized)
5. Result: 100x speedup vs custom implementation

---

## 4. Primary Use Cases

### 4.1 Sabot Streaming & Graph Platform (Primary Use Case)

**Sabot is the main customer for MarbleDB.** All design decisions prioritize Sabot's needs.

**Graph Storage**:
- Store property graphs (nodes + edges with properties)
- Support Cypher queries (MATCH, WHERE, RETURN, aggregations)
- Fast graph navigation (1-hop, multi-hop traversals)
- Requirements: Fast point lookups (2-3 μs/op) + fast scans (10M rows/sec)

**RDF/SPARQL Triple Stores**:
- Store RDF triples with 3-index strategy: SPO, POS, OSP
- Support SPARQL 1.1 queries (SELECT, FILTER, JOIN, aggregations)
- Handle large datasets (1M+ triples)
- Current problem: O(n²) scaling in query execution (5K triples/sec on 130K dataset)
- Target: 100x speedup (10M triples/sec) via Arrow compute engines

**Streaming State Management**:
- State backends for Flink-style streaming operators
- Stream-stream joins (maintain windowed state)
- Checkpoint/restore for exactly-once semantics
- Requirements: Fast writes (100K ops/sec), fast reads (500K ops/sec), fast checkpoints (<100ms)

**Materialized Views**:
- Store incrementally computed results
- Fast updates (LSM tree advantage)
- Fast analytical queries (Arrow columnar advantage)

**Why MarbleDB for Sabot**:
1. **Arrow-native** → Zero-copy integration with Sabot's compute engines (Acero, DataFusion, DuckDB)
2. **LSM tree** → Fast updates for streaming/graph workloads
3. **Fast point lookups** → Graph navigation, triple pattern matching (2.18 μs/op proven)
4. **Fast analytical scans** → SPARQL joins, graph analytics (3.52 M rows/sec proven)
5. **Predicate pushdown** → Bloom filters + zone maps for selective queries
6. **Persistent optimizations** → Index persistence for fast restart

### 4.2 RocksDB Replacement (Secondary Use Case)

**Goal**: Provide a migration path for existing RocksDB users.

**RocksDB Compatibility Layer**:
- MarbleDB implements RocksDB-compatible API: `DB::Open()`, `Get()`, `Put()`, `NewIterator()`
- File: `marble/rocksdb_compat.h`
- **Purpose**: Testing, benchmarking, and migration ease
- **Not the primary API** - Arrow APIs are preferred

**Why Users Might Migrate from RocksDB**:
1. **Better analytical performance**: 2-10x faster range scans (columnar format)
2. **Arrow integration**: Direct integration with DataFusion, DuckDB, Polars, Acero
3. **Lower memory usage**: 2.5x reduction (proven in benchmarks)
4. **Faster restart**: 2.72x faster (persistent indexes)

**Benchmark Results** (MarbleDB vs RocksDB):
- Write throughput: **1.26x faster** (166.44 vs 131.64 K ops/sec)
- Point lookup: **1.69x faster** (2.18 vs 3.68 μs/op)
- Range scan: Tied (3.52 vs 3.47 M rows/sec)
- Database restart: **2.72x faster** (8.87 vs 24.15 ms)

Reference: `MarbleDB/docs/ROCKSDB_BENCHMARK_RESULTS_2025.md`

### 4.3 Sabot Requirements Summary

**Performance Requirements**:

| Workload | Target Performance |
|----------|-------------------|
| **Graph Queries** |
| Property graph scan | 10M rows/sec |
| Cypher pattern match | 1M matches/sec |
| Graph navigation (1-hop) | 100K traversals/sec |
| **RDF/SPARQL Queries** |
| Triple pattern scan | 10M triples/sec (100x current) |
| SPARQL 2-pattern join | 1M triples/sec (200x current) |
| SPARQL aggregation | 100M triples/sec |
| **Streaming State** |
| State write (Put) | 100K ops/sec |
| State read (Get) | 500K ops/sec |
| Checkpoint/restore | <100ms |
| **Analytics** |
| Selective scan (1% selectivity) | 100M rows/sec (bloom filter) |
| Aggregation (via Acero) | 100-200M rows/sec |

**Functional Requirements**:
1. **RecordBatchReader interface** → Sabot can stream batches to compute engines
2. **Expression pushdown** → Cypher/SPARQL predicates pushed to MarbleDB
3. **LSM merge** → Deduplicated, latest-version scans across levels
4. **Zero-copy** → Shared buffers between MarbleDB and Acero/DataFusion
5. **State backend** → Checkpoint/restore for streaming operators

---

## 5. Standard Arrow Interfaces

### 4.1 RecordBatchReader (C++)

**File**: `vendor/arrow/cpp/src/arrow/record_batch.h`

```cpp
class RecordBatchReader {
public:
    // Get schema (called once at start)
    virtual std::shared_ptr<Schema> schema() const = 0;

    // Read next batch (called repeatedly)
    // Returns nullptr when exhausted
    virtual Status ReadNext(std::shared_ptr<RecordBatch>* batch) = 0;

    // Optional: read all remaining batches
    virtual Status ReadAll(std::shared_ptr<Table>* table) {
        std::vector<std::shared_ptr<RecordBatch>> batches;
        while (true) {
            std::shared_ptr<RecordBatch> batch;
            ARROW_RETURN_NOT_OK(ReadNext(&batch));
            if (!batch) break;
            batches.push_back(batch);
        }
        return Table::FromRecordBatches(schema(), batches, table);
    }

    // Cleanup
    virtual Status Close() { return Status::OK(); }
};
```

**Usage Pattern**:
```cpp
auto reader = OpenMarbleTable("orders");

// Get schema
auto schema = reader->schema();
std::cout << "Schema: " << schema->ToString() << std::endl;

// Stream batches
while (true) {
    std::shared_ptr<arrow::RecordBatch> batch;
    reader->ReadNext(&batch);
    if (!batch) break;  // EOF

    // Process batch (e.g., pass to Acero, write to Parquet, etc.)
    ProcessBatch(batch);
}

reader->Close();
```

**Why This Interface?**:
- **Standard**: All Arrow compute engines understand it
- **Streaming**: Constant memory (no materialization)
- **Simple**: Easy to implement, easy to consume
- **Composable**: Can wrap/chain multiple readers

### 17.2 Arrow C Data Interface (FFI)

**File**: `vendor/arrow/cpp/src/arrow/c/bridge.h`

```c
// C structures for zero-copy FFI
struct ArrowSchema {
    const char* format;        // Type encoding (e.g., "i" for int32)
    const char* name;          // Column name
    const char* metadata;      // Optional metadata
    int64_t flags;            // Nullability, etc.
    int64_t n_children;       // Number of child schemas
    struct ArrowSchema** children;
    struct ArrowSchema* dictionary;
    void (*release)(struct ArrowSchema*);
    void* private_data;
};

struct ArrowArray {
    int64_t length;           // Number of elements
    int64_t null_count;       // -1 if unknown
    int64_t offset;           // Offset into buffers
    int64_t n_buffers;        // Number of buffers
    int64_t n_children;       // Number of child arrays
    const void** buffers;     // Data buffers
    struct ArrowArray** children;
    struct ArrowArray* dictionary;
    void (*release)(struct ArrowArray*);
    void* private_data;
};
```

**Usage (C++ → Rust/DataFusion)**:
```cpp
// C++ side (MarbleDB)
auto batch = ReadBatchFromMarbleDB();

ArrowArray c_array;
ArrowSchema c_schema;
ExportRecordBatch(*batch, &c_array, &c_schema);

// Pass to Rust/DataFusion (zero-copy!)
rust_process_batch(&c_array, &c_schema);
```

**Why This Interface?**:
- **Zero-copy**: Shares buffers across language boundaries
- **Universal**: Works with Rust, Python, R, Julia, etc.
- **Standardized**: Part of Arrow specification
- **Performance**: No serialization overhead

### 17.3 Arrow Dataset API (C++)

**File**: `vendor/arrow/cpp/src/arrow/dataset/dataset.h`

```cpp
class Dataset {
public:
    // Get schema
    virtual std::shared_ptr<Schema> schema() const = 0;

    // Scan with filters and projection
    virtual Result<std::shared_ptr<Scanner>> Scan(
        const ScanOptions& options) = 0;
};

class Scanner {
public:
    // Get RecordBatchReader
    virtual Result<RecordBatchReader> ToReader() = 0;

    // Statistics for optimization
    virtual Result<int64_t> CountRows() = 0;
};

struct ScanOptions {
    // Column projection
    std::vector<std::string> columns;

    // Filter expression (e.g., "age > 30")
    std::shared_ptr<Expression> filter;

    // Batch size hint
    int64_t batch_size = 32768;
};
```

**Usage**:
```cpp
auto dataset = OpenMarbleDataset("orders");

ScanOptions options;
options.columns = {"order_id", "amount"};
options.filter = greater(field_ref("amount"), literal(1000));

auto scanner = dataset->Scan(options).ValueOrDie();
auto reader = scanner->ToReader().ValueOrDie();

// Stream filtered, projected batches
while (true) {
    std::shared_ptr<RecordBatch> batch;
    reader->ReadNext(&batch);
    if (!batch) break;
    ProcessBatch(batch);
}
```

**Why This Interface?**:
- **Higher-level**: Expresses intent (filter + project)
- **Optimization**: Storage can pushdown predicates
- **Partitioning**: Supports multi-file datasets
- **PyArrow**: Python `pyarrow.dataset` expects this

---

## 6. Proposed Arrow-Native API Design

### 17.1 Core Interfaces

**File**: `MarbleDB/include/marble/arrow/reader.h` (NEW)

```cpp
namespace marble {
namespace arrow {

// Streaming RecordBatch reader for MarbleDB table
class MarbleRecordBatchReader : public ::arrow::RecordBatchReader {
public:
    // Open table with optional projection and filter
    static ::arrow::Result<std::shared_ptr<MarbleRecordBatchReader>> Open(
        DB* db,
        const std::string& table_name,
        const std::vector<std::string>& projection = {},
        const std::vector<ColumnPredicate>& predicates = {});

    // Arrow RecordBatchReader interface
    std::shared_ptr<::arrow::Schema> schema() const override;
    ::arrow::Status ReadNext(std::shared_ptr<::arrow::RecordBatch>* batch) override;
    ::arrow::Status Close() override;

    // MarbleDB-specific: get statistics
    const ColumnStatistics& GetStatistics() const;

    // MarbleDB-specific: get optimization metrics
    struct Metrics {
        int64_t sstables_opened = 0;
        int64_t sstables_skipped_bloom = 0;
        int64_t batches_read = 0;
        int64_t batches_skipped_zone_map = 0;
        int64_t rows_read = 0;
        int64_t bytes_read = 0;
    };
    const Metrics& GetMetrics() const;

private:
    // Implementation uses LSMBatchIterator internally
    std::unique_ptr<LSMBatchIterator> iterator_;
    std::shared_ptr<::arrow::Schema> schema_;
    Metrics metrics_;
};

}}  // namespace marble::arrow
```

### 17.2 Dataset Interface

**File**: `MarbleDB/include/marble/arrow/dataset.h` (NEW)

```cpp
namespace marble {
namespace arrow {

// Arrow Dataset implementation for MarbleDB
class MarbleDataset : public ::arrow::dataset::Dataset {
public:
    // Open MarbleDB column family as dataset
    static ::arrow::Result<std::shared_ptr<MarbleDataset>> Open(
        DB* db,
        const std::string& table_name);

    // Dataset interface
    std::shared_ptr<::arrow::Schema> schema() const override;

    ::arrow::Result<std::shared_ptr<::arrow::dataset::Scanner>> Scan(
        const ::arrow::dataset::ScanOptions& options) override;

    // MarbleDB-specific: get table statistics
    struct TableStatistics {
        int64_t total_rows = 0;
        int64_t total_bytes = 0;
        int64_t num_sstables = 0;
        std::unordered_map<std::string, ColumnStatistics> column_stats;
    };
    ::arrow::Result<TableStatistics> GetStatistics() const;

private:
    DB* db_;
    std::string table_name_;
    std::shared_ptr<::arrow::Schema> schema_;
};

// Scanner implementation
class MarbleScanner : public ::arrow::dataset::Scanner {
public:
    ::arrow::Result<::arrow::RecordBatchReader> ToReader() override;

    ::arrow::Result<int64_t> CountRows() override;

private:
    // Uses MarbleRecordBatchReader with pushdown
    DB* db_;
    std::string table_name_;
    std::vector<std::string> projection_;
    std::vector<ColumnPredicate> predicates_;
};

}}  // namespace marble::arrow
```

### 17.3 C Data Interface Bindings

**File**: `MarbleDB/include/marble/arrow/c_data.h` (NEW)

```cpp
namespace marble {
namespace arrow {

// Export MarbleDB table to Arrow C Data Interface
// Enables zero-copy integration with Rust/DataFusion/Polars
struct CDataExporter {
    // Export schema
    static ::arrow::Status ExportSchema(
        const std::shared_ptr<::arrow::Schema>& schema,
        struct ArrowSchema* out);

    // Export single RecordBatch
    static ::arrow::Status ExportRecordBatch(
        const std::shared_ptr<::arrow::RecordBatch>& batch,
        struct ArrowArray* out_array,
        struct ArrowSchema* out_schema);

    // Stream interface: export RecordBatchReader as C stream
    static ::arrow::Status ExportRecordBatchReader(
        std::shared_ptr<MarbleRecordBatchReader> reader,
        struct ArrowArrayStream* out);
};

}}  // namespace marble::arrow
```

**Usage (C++ → Rust)**:
```cpp
auto reader = marble::arrow::MarbleRecordBatchReader::Open(db, "orders");

ArrowArrayStream stream;
marble::arrow::CDataExporter::ExportRecordBatchReader(reader, &stream);

// Pass to Rust (zero-copy!)
rust_process_stream(&stream);
```

### 17.4 Python Bindings

**File**: `MarbleDB/python/marbledb/arrow.py` (NEW)

```python
import pyarrow as pa
from marbledb import _marbledb_core  # C++ extension

class MarbleRecordBatchReader:
    """PyArrow-compatible RecordBatchReader for MarbleDB tables."""

    def __init__(self, db: Database, table_name: str,
                 columns: list[str] = None,
                 filter: pa.Expression = None):
        # Create C++ reader with predicate pushdown
        predicates = _convert_expression_to_predicates(filter) if filter else []
        self._reader = _marbledb_core.MarbleRecordBatchReader(
            db._db, table_name, columns or [], predicates
        )

    @property
    def schema(self) -> pa.Schema:
        return self._reader.schema()

    def __iter__(self):
        """Iterate batches (Pythonic interface)."""
        while True:
            batch = self._reader.read_next()
            if batch is None:
                break
            yield batch

    def read_all(self) -> pa.Table:
        """Read all batches into a Table."""
        batches = list(self)
        return pa.Table.from_batches(batches, schema=self.schema)

    def to_pandas(self) -> pd.DataFrame:
        """Convert to pandas DataFrame."""
        return self.read_all().to_pandas()

# Usage
reader = MarbleRecordBatchReader(
    db,
    "orders",
    columns=["order_id", "amount"],
    filter=pa.compute.greater(pa.compute.field("amount"), pa.scalar(1000))
)

# Pythonic iteration
for batch in reader:
    print(f"Batch: {batch.num_rows} rows")

# Or materialize
df = reader.to_pandas()
```

---

## 7. LSM-Aware Iterator Architecture

### 17.1 The Challenge

LSM tree has multiple levels, each with multiple SSTables. A scan must:
1. Read from all relevant SSTables
2. Merge results in sorted order
3. Deduplicate (same key may exist in multiple levels)
4. Keep most recent version (lower level = newer)
5. Return as streaming RecordBatches

### 17.2 LSMBatchIterator Design

**File**: `MarbleDB/include/marble/arrow/lsm_batch_iterator.h` (NEW)

```cpp
namespace marble {
namespace arrow {

// Multi-level merge iterator that returns RecordBatches
class LSMBatchIterator {
public:
    // Constructor takes all SSTable levels
    LSMBatchIterator(
        std::shared_ptr<MemTable> memtable,  // L0: active memtable
        std::shared_ptr<MemTable> imm_memtable,  // L0: immutable memtable
        std::vector<std::vector<std::shared_ptr<ArrowSSTable>>> levels,  // L1-L6
        const std::vector<std::string>& projection,
        const std::vector<ColumnPredicate>& predicates,
        int64_t target_batch_size = 32768);

    // Streaming interface
    bool HasNext() const;
    ::arrow::Status Next(std::shared_ptr<::arrow::RecordBatch>* batch);

    std::shared_ptr<::arrow::Schema> schema() const { return schema_; }

private:
    // Internal: merge source for one SSTable
    struct MergeSource {
        std::unique_ptr<ArrowRecordBatchIterator> iterator;
        std::shared_ptr<::arrow::RecordBatch> current_batch;
        int64_t current_row_idx = 0;
        int level;  // 0 = most recent, 6 = oldest

        // Get current key
        Key GetCurrentKey() const;

        // Advance to next row
        void Advance();

        // Check if exhausted
        bool IsExhausted() const;
    };

    // Priority queue: min-heap ordered by key
    // (smaller key = higher priority)
    struct MergeComparator {
        bool operator()(const MergeSource* a, const MergeSource* b) const {
            auto cmp = a->GetCurrentKey().Compare(b->GetCurrentKey());
            if (cmp != 0) return cmp > 0;  // Smaller key first
            return a->level > b->level;    // If same key, newer (lower level) first
        }
    };

    std::priority_queue<
        MergeSource*,
        std::vector<MergeSource*>,
        MergeComparator
    > merge_queue_;

    std::vector<std::unique_ptr<MergeSource>> sources_;
    std::shared_ptr<::arrow::Schema> schema_;
    int64_t target_batch_size_;

    // Accumulator for building output batch
    struct BatchAccumulator {
        std::vector<std::shared_ptr<::arrow::Array>> columns;
        int64_t num_rows = 0;

        void Append(const MergeSource& source);
        std::shared_ptr<::arrow::RecordBatch> Finish(
            const std::shared_ptr<::arrow::Schema>& schema);
    };

    BatchAccumulator accumulator_;
    Key last_emitted_key_;  // For deduplication
};

}}  // namespace marble::arrow
```

### 17.3 Merge Algorithm

```
Algorithm: K-Way Merge with Deduplication
==========================================

Initialization:
1. Open iterator for each SSTable in each level
2. Push all iterators into priority queue (min-heap by key)
3. Create accumulator for building output batch

Main Loop:
while (merge_queue not empty):
    1. Pop source with smallest key from queue
    2. current_key = source.GetCurrentKey()

    3. If current_key == last_emitted_key:
       // Duplicate - skip (already emitted from newer level)
       source.Advance()
       if (!source.IsExhausted()):
           Push source back to queue
       continue

    4. Emit current record:
       accumulator.Append(source)
       last_emitted_key = current_key

    5. If accumulator is full (>= target_batch_size):
       batch = accumulator.Finish()
       return batch

    6. Advance source to next row:
       source.Advance()
       if (!source.IsExhausted()):
           Push source back to queue

Final Batch:
if (accumulator has rows):
    batch = accumulator.Finish()
    return batch
return nullptr  // EOF
```

### 17.4 Optimization: Bloom Filter Pruning

```cpp
// Before opening iterators, check bloom filters
std::vector<std::shared_ptr<ArrowSSTable>> relevant_sstables;

for (auto& sstable : level_sstables) {
    // Check bloom filter for each predicate
    bool may_contain = true;
    for (auto& predicate : predicates) {
        if (predicate.type == kEqual) {
            if (!sstable->GetBloomFilter()->MayContain(predicate.value)) {
                may_contain = false;
                break;
            }
        }
    }

    if (may_contain) {
        relevant_sstables.push_back(sstable);
        metrics_.sstables_opened++;
    } else {
        metrics_.sstables_skipped_bloom++;
    }
}

// Only open iterators for relevant SSTables
// Speedup: 10-1000x for selective queries
```

### 17.5 Optimization: Zone Map Pruning

```cpp
// Within each SSTable, skip batches using zone maps
class ArrowRecordBatchIterator {
    Status Next(std::shared_ptr<RecordBatch>* batch) {
        while (current_batch_idx_ < batches_.size()) {
            auto& batch_candidate = batches_[current_batch_idx_];

            // Check zone map (min/max per column)
            bool may_contain = CheckZoneMap(batch_candidate, predicates_);

            if (may_contain) {
                *batch = batch_candidate;
                current_batch_idx_++;
                metrics_.batches_read++;
                return Status::OK();
            } else {
                current_batch_idx_++;
                metrics_.batches_skipped_zone_map++;
                // Continue to next batch
            }
        }

        *batch = nullptr;  // EOF
        return Status::OK();
    }
};
```

---

## 8. Predicate Pushdown Design

### 17.1 Expression Language Mapping

**Goal**: Support Arrow `compute::Expression` for portability

**File**: `MarbleDB/include/marble/arrow/expression.h` (NEW)

```cpp
namespace marble {
namespace arrow {

// Convert Arrow expression → MarbleDB predicates
class ExpressionConverter {
public:
    // Main entry point
    static ::arrow::Result<std::vector<ColumnPredicate>> Convert(
        const ::arrow::compute::Expression& expr);

private:
    // Recursive visitor for expression tree
    static ::arrow::Result<std::vector<ColumnPredicate>> VisitCall(
        const ::arrow::compute::Expression::Call& call);

    // Map operator
    static ::arrow::Result<PredicateType> MapOperator(
        const std::string& function_name);
};

}}  // namespace marble::arrow
```

**Supported Expressions**:

| Arrow Expression | MarbleDB Predicate | Optimization |
|-----------------|-------------------|--------------|
| `equal(field("x"), literal(42))` | `{column="x", type=kEqual, value=42}` | Bloom filter |
| `greater(field("x"), literal(42))` | `{column="x", type=kGreaterThan, value=42}` | Zone map (min) |
| `less(field("x"), literal(100))` | `{column="x", type=kLessThan, value=100}` | Zone map (max) |
| `and_(expr1, expr2)` | `[predicate1, predicate2]` | All predicates |
| `or_(expr1, expr2)` | ❌ Not supported | Evaluate in Arrow |
| `is_null(field("x"))` | `{column="x", type=kIsNull}` | Null count |
| `is_not_null(field("x"))` | `{column="x", type=kIsNotNull}` | Null count |

**Unsupported Expressions**:
- `or_()` - Cannot pushdown OR (would require duplicate scans)
- Complex functions (substring, regex, etc.) - Evaluate in Arrow compute
- User-defined functions - Not available at storage layer

**Hybrid Approach**:
```
Arrow Filter Expression: (age > 30) AND (country = "US" OR country = "UK")

Pushdown to MarbleDB:
  ✅ age > 30  (zone map pruning)
  ❌ country = "US" OR country = "UK"  (cannot pushdown OR)

Execution Plan:
1. MarbleDB scans with predicate: age > 30
   → Returns ~10K rows (filtered from 100K)
2. Arrow FilterNode applies: country = "US" OR country = "UK"
   → Returns ~5K rows (final result)

Benefit: 10x reduction in data scanned (100K → 10K)
```

### 17.2 Statistics-Based Optimization

**File**: `MarbleDB/include/marble/arrow/statistics.h` (NEW)

```cpp
namespace marble {
namespace arrow {

struct ColumnStatistics {
    std::string column_name;

    // Basic stats
    int64_t row_count = 0;
    int64_t null_count = 0;
    int64_t distinct_count_estimate = 0;  // HyperLogLog

    // Min/max (zone map)
    std::shared_ptr<::arrow::Scalar> min_value;
    std::shared_ptr<::arrow::Scalar> max_value;

    // Histogram (optional, for cost estimation)
    std::vector<std::pair<::arrow::Scalar, int64_t>> histogram;
};

struct TableStatistics {
    int64_t total_rows = 0;
    int64_t total_bytes = 0;
    int64_t num_sstables = 0;
    std::unordered_map<std::string, ColumnStatistics> column_stats;

    // Estimate selectivity of predicate
    double EstimateSelectivity(const ColumnPredicate& predicate) const;

    // Estimate rows after filter
    int64_t EstimateFilteredRows(
        const std::vector<ColumnPredicate>& predicates) const;
};

}}  // namespace marble::arrow
```

**Usage by Query Optimizers**:

```cpp
// DataFusion/DuckDB can query statistics
auto stats = marble_dataset->GetStatistics().ValueOrDie();

// Estimate join cost
int64_t left_rows = stats.EstimateFilteredRows(left_predicates);
int64_t right_rows = right_table_stats.EstimateFilteredRows(right_predicates);

// Choose join side
if (left_rows < right_rows) {
    // Build hash table on left (smaller)
    join_plan = BuildHashJoin(left_table, right_table, BUILD_LEFT);
} else {
    join_plan = BuildHashJoin(left_table, right_table, BUILD_RIGHT);
}
```

---

## 9. Zero-Copy Memory Management

### 17.1 Current State: Memory Copying

**Problem**:
```cpp
// Current: Loads all batches into memory
Status ArrowSSTableReader::LoadArrowBatches() {
    while (ipc_reader->ReadNext(&batch)) {
        batches_.push_back(batch);  // Copy into vector
    }
}
```

**Issues**:
- Entire SSTable loaded into memory (GB scale)
- Memory copy from disk buffer → application buffer
- High memory usage for large tables

### 17.2 Target: Memory-Mapped Reads

**File**: `MarbleDB/include/marble/arrow/mmap_reader.h` (NEW)

```cpp
namespace marble {
namespace arrow {

// Memory-mapped SSTable reader (zero-copy)
class MmapArrowSSTableReader {
public:
    static ::arrow::Result<std::shared_ptr<MmapArrowSSTableReader>> Open(
        const std::string& path);

    // RecordBatches backed by mmap'd memory (no copy!)
    ::arrow::Status ReadNext(std::shared_ptr<::arrow::RecordBatch>* batch);

private:
    // Memory-mapped file
    std::shared_ptr<::arrow::io::MemoryMappedFile> mmap_file_;

    // Arrow IPC reader (reads from mmap'd memory)
    std::shared_ptr<::arrow::ipc::RecordBatchStreamReader> ipc_reader_;
};

}}  // namespace marble::arrow
```

**Implementation**:
```cpp
Status MmapArrowSSTableReader::Open(const std::string& path) {
    // Memory-map entire file
    auto mmap_result = ::arrow::io::MemoryMappedFile::Open(
        path, ::arrow::io::FileMode::READ);
    mmap_file_ = mmap_result.ValueOrDie();

    // Create IPC reader from mmap'd file
    // Arrow RecordBatches will point directly into mmap'd memory!
    auto reader_result = ::arrow::ipc::RecordBatchStreamReader::Open(mmap_file_);
    ipc_reader_ = reader_result.ValueOrDie();

    return Status::OK();
}

Status MmapArrowSSTableReader::ReadNext(
    std::shared_ptr<::arrow::RecordBatch>* batch) {

    // Read batch (backed by mmap'd memory, no copy!)
    return ipc_reader_->ReadNext(batch);
}
```

**Benefits**:
- **Zero-copy**: RecordBatch arrays point directly to mmap'd memory
- **Lazy loading**: OS loads pages on demand (not entire file)
- **Shared memory**: Multiple processes can share same mmap
- **Performance**: 2-10x faster than read() syscalls

### 17.3 Buffer Lifecycle Management

**Challenge**: RecordBatch lifetime must not exceed mmap lifetime

**Solution**: Reference counting + lifetime extension

```cpp
class MmapArrowSSTableReader {
private:
    // Keep mmap file alive as long as any RecordBatch exists
    std::shared_ptr<::arrow::io::MemoryMappedFile> mmap_file_;

    // Custom memory pool that holds reference to mmap file
    class MmapMemoryPool : public ::arrow::MemoryPool {
    public:
        MmapMemoryPool(std::shared_ptr<::arrow::io::MemoryMappedFile> mmap_file)
            : mmap_file_(mmap_file) {}

        // MemoryPool interface (delegates to default pool)
        // But holds reference to mmap_file_, keeping it alive

    private:
        std::shared_ptr<::arrow::io::MemoryMappedFile> mmap_file_;
    };
};
```

**Result**:
- RecordBatch keeps shared_ptr to mmap file
- Mmap file stays open until all batches destroyed
- Safe zero-copy access

### 17.4 Arrow C Data Interface (Zero-Copy FFI)

**Cross-Language Zero-Copy**:

```cpp
// Export RecordBatch to C Data Interface
// Ownership transfers to receiver (Rust/Python/etc.)
Status ExportRecordBatch(
    const std::shared_ptr<::arrow::RecordBatch>& batch,
    struct ArrowArray* out_array,
    struct ArrowSchema* out_schema) {

    // Buffers are NOT copied!
    // C struct points to same memory as C++ batch
    return ::arrow::ExportRecordBatch(*batch, out_array, out_schema);
}

// Receiver (e.g., Rust) takes ownership
// When Rust drops ArrowArray, release callback is called
// Which then decrements C++ refcount
```

**Key Point**: Arrow C Data Interface is **designed for zero-copy FFI**.

---

## 10. Integration Patterns by Tool

### 17.1 Apache Arrow Acero (C++)

**Use Case**: Build query plans with MarbleDB as source

**Integration**:
```cpp
#include <marble/arrow/reader.h>
#include <arrow/acero/exec_plan.h>

// Create exec plan
auto plan = ::arrow::acero::ExecPlan::Make().ValueOrDie();

// Source: MarbleDB table
auto reader = marble::arrow::MarbleRecordBatchReader::Open(
    db, "orders",
    /*projection=*/{"order_id", "customer_id", "amount"},
    /*predicates=*/{{"amount", marble::PredicateType::kGreaterThan, 1000}}
).ValueOrDie();

auto source_node = ::arrow::acero::MakeExecNode(
    "record_batch_reader_source", plan.get(), {},
    ::arrow::acero::RecordBatchReaderSourceNodeOptions{reader}
).ValueOrDie();

// Filter (additional filter in Acero)
auto filter_node = ::arrow::acero::MakeExecNode(
    "filter", plan.get(), {source_node},
    ::arrow::acero::FilterNodeOptions{
        ::arrow::compute::greater(
            ::arrow::compute::field_ref("amount"),
            ::arrow::compute::literal(5000))
    }
).ValueOrDie();

// Aggregate
auto agg_node = ::arrow::acero::MakeExecNode(
    "aggregate", plan.get(), {filter_node},
    ::arrow::acero::AggregateNodeOptions{
        /*aggregates=*/{{"hash_sum", ::arrow::compute::field_ref("amount")}},
        /*keys=*/{::arrow::compute::field_ref("customer_id")}
    }
).ValueOrDie();

// Sink
AsyncGenerator<std::optional<ExecBatch>> sink_gen;
auto sink_node = ::arrow::acero::MakeExecNode(
    "sink", plan.get(), {agg_node},
    ::arrow::acero::SinkNodeOptions{&sink_gen}
).ValueOrDie();

// Execute
plan->StartProducing();
while (auto batch = sink_gen().result()) {
    ProcessBatch(*batch);
}
```

**Performance**:
- MarbleDB: Bloom filter + zone map pruning (1000x reduction)
- Acero: SIMD-optimized aggregation (2-3x faster than scalar)
- Result: Best of both worlds

### 17.2 Apache DataFusion (Rust)

**Use Case**: Query MarbleDB from Rust/DataFusion

**Integration via Arrow C Data Interface**:

```rust
// Rust side
use arrow::ffi::FFI_ArrowArrayStream;
use datafusion::prelude::*;

// Call C++ to get ArrowArrayStream
let mut stream: FFI_ArrowArrayStream = unsafe {
    let db_ptr = db.as_ptr();
    marble_open_table_stream(
        db_ptr,
        c"orders".as_ptr(),
        // Projection
        &[c"order_id".as_ptr(), c"amount".as_ptr()],
        2,
        // Predicates (serialized)
        predicates_json.as_ptr(),
    )
};

// Import into Rust (zero-copy!)
let arrow_stream = ArrowArrayStreamReader::from_raw(&mut stream)?;

// Create DataFusion table
let table = MemTable::try_new(
    arrow_stream.schema()?,
    vec![arrow_stream.collect()],
)?;

// Query with DataFusion
let ctx = SessionContext::new();
ctx.register_table("orders", Arc::new(table))?;

let df = ctx.sql("SELECT customer_id, SUM(amount) FROM orders GROUP BY customer_id").await?;
df.show().await?;
```

**Key Points**:
- Zero-copy via FFI
- MarbleDB predicates pushed down before FFI boundary
- DataFusion handles query execution

### 17.3 DuckDB (C++)

**Use Case**: Query MarbleDB via DuckDB SQL

**Integration via Arrow Scanner**:

```cpp
// Register MarbleDB table as DuckDB table
auto marble_reader = marble::arrow::MarbleRecordBatchReader::Open(
    db, "orders"
).ValueOrDie();

// DuckDB can scan Arrow RecordBatchReader!
auto duckdb_table = duckdb::make_shared<duckdb::ArrowTableFunction>(
    marble_reader
);

duckdb::Connection conn(duckdb_db);
conn.TableFunction("marble_table", duckdb_table)->CreateView("orders");

// Query with DuckDB SQL
auto result = conn.Query(
    "SELECT customer_id, SUM(amount) FROM orders WHERE amount > 1000 GROUP BY customer_id"
);
result->Print();
```

**Predicate Pushdown**:
DuckDB will push filters down to MarbleDB via Arrow Expression API (if we implement it).

### 17.4 Polars (Rust)

**Use Case**: DataFrame operations on MarbleDB

**Integration**:

```rust
use polars::prelude::*;
use arrow::ffi::FFI_ArrowArrayStream;

// Get MarbleDB stream via FFI
let mut stream = unsafe {
    marble_open_table_stream(db, c"orders".as_ptr(), ...)
};

// Import into Polars (zero-copy)
let arrow_stream = ArrowArrayStreamReader::from_raw(&mut stream)?;
let batches: Vec<RecordBatch> = arrow_stream.collect()?;

let df = DataFrame::try_from_arrow_chunks(&batches)?;

// Polars operations
let result = df
    .lazy()
    .filter(col("amount").gt(1000))
    .groupby([col("customer_id")])
    .agg([col("amount").sum()])
    .collect()?;

println!("{:?}", result);
```

### 17.5 PyArrow (Python)

**Use Case**: Python data science workflows

**Integration**:

```python
import pyarrow as pa
import pyarrow.compute as pc
from marbledb import Database

# Open database
db = Database.open("/path/to/db")

# Get RecordBatchReader
reader = db.table("orders").to_reader(
    columns=["order_id", "customer_id", "amount"],
    filter=pc.greater(pc.field("amount"), pa.scalar(1000))
)

# Stream batches
for batch in reader:
    print(f"Batch: {batch.num_rows} rows")
    # Process batch...

# Or materialize to pandas
df = reader.read_all().to_pandas()
print(df.head())

# Or use with PyArrow datasets
import pyarrow.dataset as ds

dataset = db.table("orders").to_dataset()
table = dataset.to_table(
    columns=["customer_id", "amount"],
    filter=pc.greater(pc.field("amount"), 1000)
)
print(table.to_pandas())
```

---

## 11. Performance Targets

### 11.1 Baseline (Current RocksDB API)

From recent benchmarks (`docs/ROCKSDB_BENCHMARK_RESULTS_2025.md`):

| Metric | MarbleDB (RocksDB API) | Target (Arrow API) |
|--------|------------------------|-------------------|
| Point lookup | 2.18 μs/op | 2.0 μs/op (10% improvement) |
| Sequential scan | 3.52 M rows/sec | 5-10 M rows/sec (2-3x improvement) |
| Selective scan (1%) | N/A | 100 M rows/sec (bloom filter) |
| Hash join (100K rows) | N/A | 50-100 M rows/sec (Acero) |
| Aggregation | N/A | 100-200 M rows/sec (Acero) |

### 11.2 Competitive Analysis

**vs Parquet**:
- Parquet read: 5-20 M rows/sec
- Target: Match or exceed Parquet
- Advantage: Bloom filters + sparse indexes (not in Parquet)

**vs DuckDB (native)**:
- DuckDB scan: 10-50 M rows/sec
- Target: Match DuckDB with Acero
- Advantage: Better predicate pushdown (bloom + zone maps)

**vs RocksDB**:
- RocksDB scan: 3-5 M rows/sec (row-oriented)
- Target: 2-10x faster (columnar advantage)
- Advantage: Arrow columnar format

### 11.3 Memory Usage Targets

| Workload | Current (RocksDB API) | Target (Arrow API) |
|----------|----------------------|-------------------|
| 100K row scan | ~50 MB (load all) | ~10 MB (streaming) |
| 1M row scan | ~500 MB (load all) | ~10 MB (streaming) |
| 10M row scan | ~5 GB (OOM!) | ~10 MB (streaming) |

**Key**: Constant memory via streaming, regardless of table size.

### 11.4 Benchmark Suite

**File**: `benchmarks/arrow/marble_arrow_benchmarks.cpp` (NEW)

**Benchmarks**:

1. **Full Table Scan**:
   - Query: `SELECT * FROM orders`
   - Metric: Throughput (rows/sec)
   - Target: > 5 M rows/sec

2. **Selective Scan (Bloom Filter)**:
   - Query: `SELECT * FROM orders WHERE customer_id = 12345`
   - Metric: Throughput (rows/sec), SSTables skipped
   - Target: > 100 M rows/sec, 99% SSTables skipped

3. **Range Scan (Zone Map)**:
   - Query: `SELECT * FROM orders WHERE amount BETWEEN 1000 AND 2000`
   - Metric: Throughput (rows/sec), batches skipped
   - Target: > 50 M rows/sec, 90% batches skipped

4. **Column Projection**:
   - Query: `SELECT customer_id, amount FROM orders` (2 of 10 columns)
   - Metric: Throughput (rows/sec), bytes read
   - Target: 5x faster than full scan, 80% less I/O

5. **Hash Join (Acero)**:
   - Query: `SELECT * FROM orders JOIN customers ON orders.customer_id = customers.id`
   - Metric: Throughput (rows/sec)
   - Target: > 50 M rows/sec

6. **Aggregation (Acero)**:
   - Query: `SELECT customer_id, SUM(amount) FROM orders GROUP BY customer_id`
   - Metric: Throughput (rows/sec)
   - Target: > 100 M rows/sec

### 11.5 Sabot Workload Targets (Primary Use Case)

**Sabot is the main customer for MarbleDB**. These targets are critical for production deployment.

#### 11.5.1 Graph Queries (Property Graphs + Cypher)

| Operation | Current (Sabot/Cypher) | Target (MarbleDB + Acero) | Improvement |
|-----------|------------------------|---------------------------|-------------|
| Node scan (all properties) | ~2 M nodes/sec | **10 M nodes/sec** | 5x |
| Edge scan (all properties) | ~1.5 M edges/sec | **10 M edges/sec** | 6.7x |
| Cypher pattern match (2-hop) | ~200K matches/sec | **1 M matches/sec** | 5x |
| Cypher pattern match (3-hop) | ~50K matches/sec | **500K matches/sec** | 10x |
| Property filter (indexed) | ~500K nodes/sec | **5 M nodes/sec** | 10x |
| Aggregation (COUNT, SUM) | ~5 M rows/sec | **100 M rows/sec** | 20x |

**Key Requirements**:
- Cypher queries compiled to Acero/DataFusion execution plans
- Property projection (only load needed properties, not all columns)
- Multi-hop traversal optimization (avoid repeated storage reads)
- Index integration (bloom filters for node/edge IDs)

**Benchmark Queries**:
```cypher
// 1. Simple node scan with filter
MATCH (n:Person) WHERE n.age > 30 RETURN n.name, n.age

// 2. Two-hop pattern match
MATCH (p:Person)-[:KNOWS]->(f:Person)-[:WORKS_AT]->(c:Company)
RETURN p.name, f.name, c.name

// 3. Aggregation
MATCH (p:Person)-[:LIKES]->(m:Movie)
RETURN m.title, COUNT(p) as fans
ORDER BY fans DESC LIMIT 10
```

#### 11.5.2 RDF/SPARQL Queries (Triple Stores)

**Critical**: Current SPARQL implementation has O(n²) scaling bug (5,044 triples/sec for 130K triples). Target 100x improvement.

| Operation | Current (Sabot/RDF) | Target (MarbleDB + Acero) | Improvement |
|-----------|---------------------|---------------------------|-------------|
| Triple pattern scan (SPO) | ~147K triples/sec | **10 M triples/sec** | 68x |
| SPARQL 1-pattern query | ~40K triples/sec | **10 M triples/sec** | 250x |
| SPARQL 2-pattern join | **5K triples/sec** ❌ | **1 M triples/sec** | **200x** |
| SPARQL 3-pattern join | **575 triples/sec** ❌ | **500K triples/sec** | **870x** |
| SPARQL aggregation (COUNT) | ~10K triples/sec | **100 M triples/sec** | 10,000x |
| PREFIX resolution | Instant | Instant | - |

**Critical Fixes Needed**:
- Replace nested loop joins with hash joins (Acero/DataFusion)
- Use 3-index strategy (SPO, POS, OSP) for optimal join ordering
- Bloom filters per index (predicate pushdown)
- Statistics for query planning

**Benchmark Queries**:
```sparql
# 1. Simple pattern (all triples with a property)
SELECT ?s ?o WHERE { ?s foaf:name ?o }

# 2. Two-pattern join (people and their friends)
SELECT ?person ?friendName WHERE {
  ?person foaf:name ?personName .
  ?person foaf:knows ?friend .
  ?friend foaf:name ?friendName
}

# 3. Complex 4-pattern join with filter
SELECT ?person ?movie WHERE {
  ?person rdf:type :Person .
  ?person :age ?age .
  FILTER(?age > 30)
  ?person :likes ?movie .
  ?movie rdf:type :Movie
}

# 4. Aggregation
SELECT ?movie (COUNT(?person) as ?fans) WHERE {
  ?person :likes ?movie
} GROUP BY ?movie ORDER BY DESC(?fans) LIMIT 10
```

#### 11.5.3 Streaming State (State Backends for Flink-Style Operators)

| Operation | Current (Sabot/State) | Target (MarbleDB) | Improvement |
|-----------|----------------------|-------------------|-------------|
| State write (Put) | ~150K ops/sec | **200K ops/sec** | 1.3x |
| State read (Get) | ~450K ops/sec | **500K ops/sec** | 1.1x |
| State scan (range) | ~3 M rows/sec | **10 M rows/sec** | 3.3x |
| Checkpoint creation | ~500 ms | **<100 ms** | 5x |
| Checkpoint restore | ~1000 ms | **<200 ms** | 5x |

**Key Requirements**:
- Fast point lookups (hot key cache integration)
- Efficient range scans (window state, session state)
- Incremental checkpointing (only changed state)
- Exactly-once semantics (atomic commits)
- Low latency (P99 < 10ms for Gets)

**Benchmark Workloads**:
```python
# 1. Stream-stream join state (session window)
# - 100K active keys
# - 10K writes/sec, 50K reads/sec
# - Target: P99 read < 5ms, P99 write < 10ms

# 2. Aggregation state (tumbling window)
# - 1M unique keys
# - 50K updates/sec
# - Target: Checkpoint < 100ms

# 3. Enrichment state (broadcast join)
# - 10M dimension table rows
# - 100K lookups/sec
# - Target: P99 lookup < 2ms (bloom filter skip)
```

#### 11.5.4 Materialized Views (Incremental Computation)

| Operation | Target Performance |
|-----------|-------------------|
| View refresh (incremental) | < 1 sec for 100K updates |
| View scan (full) | 10 M rows/sec |
| View scan (selective) | 100 M rows/sec |
| View join (with base table) | 50 M rows/sec |

**Key Requirements**:
- Efficient delta tracking (changed rows only)
- Fast merge (new deltas + existing view)
- Predicate pushdown (bloom filters, zone maps)
- Integration with Acero/DataFusion query engine

---

## 12. Implementation Stages

### Stage 1: Foundation

**Goal**: RecordBatchReader interface working

**Tasks**:
1. Create `marble::arrow::MarbleRecordBatchReader` class
2. Wrap existing `ArrowRecordBatchIterator`
3. Implement `schema()`, `ReadNext()`, `Close()`
4. Add factory function `Open(db, table_name, projection, predicates)`
5. Unit tests: open table, iterate batches, verify schema
6. **Sabot Integration**: Test with Sabot graph storage (node/edge tables)

**Deliverables**:
- `MarbleDB/include/marble/arrow/reader.h`
- `MarbleDB/src/arrow/reader.cpp`
- `MarbleDB/tests/arrow/test_reader.cpp`
- **`MarbleDB/tests/sabot/test_graph_reader.cpp`** (Sabot-specific)

**Success Criteria**:
- Can open MarbleDB table as RecordBatchReader
- Can iterate all batches
- Schema matches table schema
- Column projection works
- **Sabot graph tables (nodes, edges) readable via Arrow API**

### Stage 2: LSM Merge Iterator

**Goal**: Multi-level scan with deduplication

**Tasks**:
1. Create `LSMBatchIterator` class
2. Implement K-way merge algorithm
3. Deduplication logic (same key across levels)
4. Integration with bloom filters (skip SSTables)
5. Integration with zone maps (skip batches)
6. Tests: insert, update, scan, verify latest version returned

**Deliverables**:
- `MarbleDB/include/marble/arrow/lsm_iterator.h`
- `MarbleDB/src/arrow/lsm_iterator.cpp`
- `MarbleDB/tests/arrow/test_lsm_merge.cpp`

**Success Criteria**:
- Scans across all LSM levels
- Returns latest version of each key (no duplicates)
- Bloom filter pruning working (SSTables skipped)
- Zone map pruning working (batches skipped)

### Stage 3: Expression Conversion

**Goal**: Arrow Expression → MarbleDB predicate pushdown

**Tasks**:
1. Create `ExpressionConverter` class
2. Support basic operators: equal, greater, less, is_null, etc.
3. Support AND (multiple predicates)
4. Update `MarbleRecordBatchReader` to accept Arrow expressions
5. Tests: verify predicates pushed down, optimizations applied
6. **Sabot Integration**: Support Cypher/SPARQL predicate pushdown
   - Node/edge ID filters (bloom filter optimization)
   - Property filters (zone map optimization)
   - Triple pattern filters (SPO index selection)

**Deliverables**:
- `MarbleDB/include/marble/arrow/expression.h`
- `MarbleDB/src/arrow/expression.cpp`
- `MarbleDB/tests/arrow/test_expression.cpp`
- **`MarbleDB/tests/sabot/test_graph_predicates.cpp`** (Cypher predicates)
- **`MarbleDB/tests/sabot/test_rdf_predicates.cpp`** (SPARQL predicates)

**Success Criteria**:
- Arrow expressions converted to predicates
- Bloom filter uses predicates
- Zone maps use predicates
- Unsupported expressions gracefully handled (evaluated in Arrow)
- **Cypher property filters use bloom filters (10x speedup)**
- **SPARQL triple patterns use optimal index (SPO/POS/OSP selection)**

### Stage 4: Dataset Interface

**Goal**: Arrow Dataset API implementation

**Tasks**:
1. Create `MarbleDataset` class
2. Create `MarbleScanner` class
3. Implement `Scan(options)` with filter + projection
4. Expose table statistics
5. Tests: PyArrow datasets integration

**Deliverables**:
- `MarbleDB/include/marble/arrow/dataset.h`
- `MarbleDB/src/arrow/dataset.cpp`
- `MarbleDB/tests/arrow/test_dataset.cpp`

**Success Criteria**:
- Can create PyArrow dataset from MarbleDB table
- Filter + projection work via dataset API
- Statistics accessible via Python

### Stage 5: Zero-Copy Optimizations

**Goal**: Memory-mapped reads

**Tasks**:
1. Create `MmapArrowSSTableReader` class
2. Memory-map SSTable files
3. Return RecordBatches backed by mmap (no copy)
4. Buffer lifecycle management (keep mmap alive)
5. Tests: verify zero-copy, benchmark memory usage

**Deliverables**:
- `MarbleDB/include/marble/arrow/mmap_reader.h`
- `MarbleDB/src/arrow/mmap_reader.cpp`
- `MarbleDB/tests/arrow/test_mmap.cpp`

**Success Criteria**:
- RecordBatches point to mmap'd memory (no copy)
- Memory usage constant regardless of table size
- 2-10x faster than current implementation

### Stage 6: Query Engine Integration & Validation

**Goal**: Validate MarbleDB with Arrow compute engines (Acero, DataFusion, DuckDB) and Sabot's query planners

**Note**: This stage validates that the standard Arrow APIs (Stages 1-5) work correctly with real query engines. Acero is used as the primary validation target, but the APIs should work with any Arrow-native engine.

**Tasks**:
1. Create Acero integration examples (scan, filter, join, aggregate)
2. Create DataFusion integration example (Rust FFI)
3. Create DuckDB integration example (Arrow scanner)
4. **Sabot Integration**: Test Cypher and SPARQL queries
   - Integrate MarbleDB with Sabot's Cypher planner
   - Integrate MarbleDB with Sabot's SPARQL planner
   - Run benchmark queries from Section 11.5
   - Validate 100x speedup for SPARQL (fix O(n²) bug)
5. Performance testing vs native implementations
6. Benchmark report
7. Documentation

**Deliverables**:
- `MarbleDB/examples/acero/simple_scan.cpp`
- `MarbleDB/examples/acero/hash_join.cpp`
- `MarbleDB/examples/datafusion/integration.rs`
- `MarbleDB/examples/duckdb/arrow_scan.cpp`
- **`MarbleDB/examples/sabot/cypher_integration.cpp`** (Sabot Cypher)
- **`MarbleDB/examples/sabot/sparql_integration.cpp`** (Sabot SPARQL)
- **`MarbleDB/benchmarks/sabot/graph_benchmarks.cpp`** (Cypher performance)
- **`MarbleDB/benchmarks/sabot/rdf_benchmarks.cpp`** (SPARQL performance)
- `MarbleDB/benchmarks/arrow/multi_engine_benchmarks.cpp`
- `MarbleDB/docs/ARROW_INTEGRATION.md`
- **`MarbleDB/docs/SABOT_INTEGRATION.md`** (Sabot-specific guide)

**Success Criteria**:
- Hash join working with Acero
- DataFusion can query MarbleDB via FFI
- DuckDB can scan MarbleDB via Arrow interface
- **Sabot Cypher queries work (pattern matching, aggregations)**
- **Sabot SPARQL queries work with 100x speedup (1M triples/sec for 2-pattern joins)**
- **Graph queries meet performance targets (Section 11.5.1)**
- **RDF queries meet performance targets (Section 11.5.2)**
- Performance competitive with native implementations
- Comprehensive examples and docs for all engines

### Stage 7: C Data Interface (Cross-Language Support)

**Goal**: FFI for Rust/Python/R integration

**Tasks**:
1. Implement Arrow C Data Interface export
2. Export schema, RecordBatch, RecordBatchReader
3. Rust example (DataFusion integration)
4. Python bindings (PyArrow)

**Deliverables**:
- `MarbleDB/include/marble/arrow/c_data.h`
- `MarbleDB/src/arrow/c_data.cpp`
- `MarbleDB/examples/rust/datafusion_integration.rs`
- `MarbleDB/python/marbledb/arrow.py`

**Success Criteria**:
- Can export MarbleDB table to Rust (zero-copy)
- DataFusion can query MarbleDB tables
- PyArrow integration working

### Stage 8: Production Hardening

**Goal**: Production-ready quality for Sabot deployment

**Tasks**:
1. Error handling audit
2. Memory leak testing (valgrind)
3. Thread safety testing
4. Stress testing (large datasets, memory pressure)
5. **Sabot End-to-End Integration**:
   - Full Sabot pipeline tests (Kafka → Graph storage → SPARQL queries)
   - Production-scale graph datasets (1M nodes, 5M edges)
   - Production-scale RDF datasets (1M triples)
   - Streaming state backend integration tests
   - Checkpoint/restore validation (exactly-once semantics)
6. Documentation completion
7. Benchmark suite expansion

**Deliverables**:
- Clean valgrind runs
- Thread-safety validation
- **Sabot integration test suite (100+ tests)**
- **Production benchmark report (Sabot workloads)**
- Comprehensive docs
- Benchmark report

**Success Criteria**:
- No memory leaks
- Thread-safe for concurrent queries
- All tests passing (including Sabot integration tests)
- **Sabot can use MarbleDB as primary storage**
- **All Sabot performance targets met (Section 11.5)**
- Documentation complete

---

## 13. Testing Strategy

### 13.1 Unit Tests

**File**: `MarbleDB/tests/arrow/`

**Test Cases**:

1. **RecordBatchReader**:
   - Open table, iterate batches
   - Schema validation
   - EOF handling
   - Error cases (table not found, etc.)

2. **LSM Merge**:
   - Single SSTable scan
   - Multi-level scan with deduplication
   - Empty table
   - Large dataset (1M rows)

3. **Predicate Pushdown**:
   - Bloom filter pruning (verify SSTables skipped)
   - Zone map pruning (verify batches skipped)
   - Combined predicates
   - Unsupported predicates (graceful fallback)

4. **Expression Conversion**:
   - Each supported operator
   - AND combinations
   - Unsupported expressions (OR, UDF)

5. **Dataset API**:
   - Scanner with filter + projection
   - Statistics retrieval
   - Multiple scans in parallel

6. **Zero-Copy**:
   - Mmap read correctness
   - Memory usage validation
   - Buffer lifecycle

### 13.2 Integration Tests

**File**: `MarbleDB/tests/arrow/integration/`

**Test Cases**:

1. **Acero Integration**:
   - Source node scan
   - Filter + project pipeline
   - Hash join
   - Aggregation
   - Complex query plan

2. **DataFusion Integration** (Rust):
   - FFI export
   - Query from Rust
   - Join with Parquet table

3. **PyArrow Integration** (Python):
   - RecordBatchReader in Python
   - Dataset API in Python
   - Pandas conversion

4. **DuckDB Integration**:
   - Register MarbleDB table
   - SQL queries
   - Join with DuckDB table

### 13.3 Performance Tests

**File**: `benchmarks/arrow/`

**Benchmarks**:

1. **Throughput Benchmarks**:
   - Full table scan
   - Selective scan (1%, 10%, 50%)
   - Column projection (1, 2, 5, 10 columns)
   - Large dataset (1M, 10M, 100M rows)

2. **Optimization Benchmarks**:
   - Bloom filter effectiveness (SSTables skipped)
   - Zone map effectiveness (batches skipped)
   - Memory-mapped vs buffered reads

3. **Comparative Benchmarks**:
   - MarbleDB vs Parquet (scan)
   - MarbleDB vs DuckDB (scan + join + aggregate)
   - MarbleDB vs RocksDB (point lookup + scan)

4. **Memory Benchmarks**:
   - Memory usage during scan
   - Memory usage during join
   - Verify constant memory (streaming)

### 13.4 Stress Tests

**File**: `tests/arrow/stress/`

**Test Cases**:

1. **Large Dataset**:
   - 100M rows, 10GB table
   - Full scan
   - Selective scan
   - Verify constant memory

2. **Memory Pressure**:
   - Scan with limited memory (1GB limit)
   - Verify no OOM
   - Verify graceful degradation

3. **Concurrent Access**:
   - 10 readers in parallel
   - Verify thread safety
   - Verify no data corruption

4. **LSM Complexity**:
   - 1000 SSTables across 7 levels
   - Scan with heavy deduplication
   - Verify correctness and performance

### 13.5 Sabot Integration Tests (Primary Use Case)

**File**: `MarbleDB/tests/sabot/`

**Critical**: These tests validate MarbleDB as Sabot's primary storage layer. All must pass for production deployment.

#### 13.5.1 Graph Storage Tests

**File**: `tests/sabot/graph/`

**Test Cases**:

1. **Node/Edge Storage**:
   - Insert 1M nodes with properties
   - Insert 5M edges with properties
   - Verify schema (node_id, labels, properties)
   - Verify schema (edge_id, src, dst, label, properties)

2. **Cypher Pattern Matching**:
   - Single-node pattern: `MATCH (n:Person) WHERE n.age > 30`
   - Two-hop pattern: `MATCH (a)-[:KNOWS]->(b)-[:WORKS_AT]->(c)`
   - Three-hop pattern: `MATCH (a)-[:KNOWS*3]->(b)`
   - Pattern with aggregation: `MATCH (p)-[:LIKES]->(m) RETURN m, COUNT(p)`

3. **Property Projection**:
   - Query with subset of properties (2 of 10)
   - Verify only requested columns read (I/O reduction)
   - Measure performance improvement vs full scan

4. **Predicate Pushdown**:
   - Node ID filter (bloom filter should skip 99% SSTables)
   - Edge ID filter (bloom filter should skip 99% SSTables)
   - Property filter (zone maps should skip 90% batches)

5. **Performance Validation**:
   - Node scan: > 10 M nodes/sec
   - Edge scan: > 10 M edges/sec
   - 2-hop pattern: > 1 M matches/sec
   - Aggregation: > 100 M rows/sec

#### 13.5.2 RDF/SPARQL Tests

**File**: `tests/sabot/rdf/`

**Critical**: Must achieve 100x speedup vs current implementation (fix O(n²) bug).

**Test Cases**:

1. **Triple Storage** (3-Index Strategy):
   - Insert 1M triples
   - Verify SPO index (subject, predicate, object)
   - Verify POS index (predicate, object, subject)
   - Verify OSP index (object, subject, predicate)

2. **SPARQL Single-Pattern Queries**:
   - `SELECT ?s ?o WHERE { ?s foaf:name ?o }` (POS index)
   - `SELECT ?p ?o WHERE { <http://example.org/person1> ?p ?o }` (SPO index)
   - `SELECT ?s ?p WHERE { ?s ?p <http://example.org/person2> }` (OSP index)
   - Verify optimal index selected (logs show SPO/POS/OSP)

3. **SPARQL Multi-Pattern Joins**:
   - Two-pattern join: Person + knows + friend
   - Three-pattern join: Person + age filter + likes movie
   - Four-pattern join: Complex query from Section 11.5.2
   - Verify hash joins used (not nested loops)
   - Measure join cardinality estimation accuracy

4. **SPARQL Aggregations**:
   - `SELECT ?movie (COUNT(?person) as ?fans) GROUP BY ?movie`
   - `SELECT (AVG(?age) as ?avgAge) WHERE { ?p :age ?age }`
   - Verify Acero/DataFusion aggregation kernels used

5. **Performance Validation** (Critical):
   - Triple scan: > 10 M triples/sec
   - 1-pattern query: > 10 M triples/sec
   - 2-pattern join: > 1 M triples/sec (was 5K, need 200x)
   - 3-pattern join: > 500K triples/sec (was 575, need 870x)
   - Aggregation: > 100 M triples/sec

6. **Scaling Tests**:
   - 1K triples: < 100ms for complex query
   - 10K triples: < 1 sec for complex query
   - 100K triples: < 10 sec for complex query
   - 1M triples: < 60 sec for complex query
   - Verify O(n log n) scaling, not O(n²)

#### 13.5.3 Streaming State Backend Tests

**File**: `tests/sabot/state/`

**Test Cases**:

1. **Key-Value State**:
   - Insert 1M key-value pairs
   - Point lookups (Get): > 500K ops/sec
   - Range scans: > 10 M rows/sec
   - Hot key cache hit rate: > 95%

2. **Window State**:
   - Tumbling window (1000 windows, 1000 keys per window)
   - Session window (variable size windows)
   - Range scan for window expiration
   - Verify memory usage constant (streaming)

3. **Checkpoint/Restore**:
   - Create checkpoint with 1M keys
   - Checkpoint time: < 100ms
   - Restore time: < 200ms
   - Verify exactly-once semantics (atomic commit)

4. **Concurrent Access**:
   - 10 operators writing to different key ranges
   - 10 operators reading from different key ranges
   - Verify thread safety (no corruption)
   - Verify no deadlocks

5. **Performance Validation**:
   - State write: > 200K ops/sec
   - State read: > 500K ops/sec
   - Checkpoint: < 100ms
   - Restore: < 200ms

#### 13.5.4 End-to-End Pipeline Tests

**File**: `tests/sabot/e2e/`

**Test Cases**:

1. **Kafka → Graph Storage → Cypher Query**:
   - Ingest 100K graph events from Kafka
   - Store as nodes/edges in MarbleDB
   - Run Cypher pattern match query
   - Verify results correct
   - Measure end-to-end latency (< 100ms P99)

2. **Kafka → RDF Storage → SPARQL Query**:
   - Ingest 100K RDF triples from Kafka
   - Store in 3-index structure (SPO, POS, OSP)
   - Run SPARQL multi-pattern query
   - Verify 100x speedup vs current implementation
   - Measure end-to-end latency (< 500ms P99)

3. **Stream Join with State Backend**:
   - Stream-stream join with 1M active keys
   - Window aggregation with state
   - Checkpoint every 10 seconds
   - Verify exactly-once semantics
   - Measure throughput (> 10K events/sec)

4. **Materialized View Refresh**:
   - Base table with 1M rows
   - Materialized view with aggregation
   - Incremental refresh with 100K updates
   - Refresh time: < 1 sec
   - Query materialized view: > 10 M rows/sec

---

## 14. Migration Path

### 17.1 Backward Compatibility

**Principle**: Existing code must continue working

**Approach**:
1. **Keep RocksDB API**: Don't remove, it's for testing
2. **Keep existing APIs**: `ArrowSSTableReader` remains
3. **New namespace**: `marble::arrow` for new APIs
4. **Gradual adoption**: Users opt-in to new APIs

### 17.2 Phased Rollout

**Phase 1**: Internal only
- New APIs available but not documented
- Team validates with internal tools
- Performance testing

**Phase 2**: Beta
- Document new APIs
- Examples and tutorials
- Early adopters provide feedback

**Phase 3**: GA
- Promote Arrow-first APIs as primary
- RocksDB API marked as "legacy" (but supported)
- Comprehensive docs and examples

### 17.3 Migration Guide

**File**: `MarbleDB/docs/MIGRATION_TO_ARROW_API.md` (NEW)

**Contents**:

1. **Why migrate?**
   - Better performance (streaming, zero-copy)
   - Ecosystem integration (Acero, DataFusion, DuckDB)
   - Future-proof

2. **Before and After**:
   ```cpp
   // BEFORE: RocksDB API
   DB* db;
   DB::Open(options, path, &db);
   std::string value;
   db->Get(ReadOptions(), "key", &value);

   // AFTER: Arrow API
   DB* db;
   DB::Open(options, path, &db);
   auto reader = marble::arrow::MarbleRecordBatchReader::Open(
       db, "table", {"column"}, {predicate}).ValueOrDie();
   while (true) {
       std::shared_ptr<RecordBatch> batch;
       reader->ReadNext(&batch);
       if (!batch) break;
       ProcessBatch(batch);
   }
   ```

3. **Common patterns**:
   - Point lookup → Scan with filter
   - Range scan → RecordBatchReader
   - Joins → Use Acero/DataFusion

4. **Performance tips**:
   - Use predicates for pushdown
   - Use column projection
   - Stream results (don't materialize)

### 17.4 Deprecation Timeline

**No deprecation planned**

- RocksDB API is for testing/benchmarking
- Will remain supported indefinitely
- Arrow APIs are **additional**, not replacement

---

## 15. Open Questions

### 17.1 Technical Decisions

1. **Batch Size Strategy**:
   - Fixed (e.g., 32K rows always)?
   - Adaptive (tune based on consumer)?
   - Configurable?
   - **Recommendation**: Start fixed, add adaptive later

2. **Memtable Integration**:
   - Include memtable in RecordBatchReader?
   - Or only flushed SSTables?
   - **Recommendation**: Include memtable for consistency

3. **Transaction Isolation**:
   - Snapshot isolation for scans?
   - Or read latest version?
   - **Recommendation**: Snapshot (consistent point-in-time)

4. **Statistics Persistence**:
   - Store in SSTable footer?
   - Separate stats file?
   - **Recommendation**: SSTable footer (already done)

5. **Compression**:
   - Arrow IPC supports compression
   - Which codec? (LZ4, ZSTD, Snappy)
   - **Recommendation**: ZSTD (best ratio, good speed)

### 17.2 Ecosystem Integration

1. **Substrait Support**:
   - Should MarbleDB understand Substrait plans?
   - Or leave that to Acero/DataFusion?
   - **Recommendation**: Leave to compute engines

2. **Arrow Flight**:
   - Should MarbleDB be a Flight server?
   - For distributed queries?
   - **Recommendation**: Yes, separate feature (Phase 9)

3. **Parquet Compatibility**:
   - Should SSTables be readable as Parquet?
   - Or stay Arrow IPC?
   - **Recommendation**: Stay Arrow IPC (different use case)

### 17.3 Performance Tuning

1. **Bloom Filter Parameters**:
   - False positive rate: 1%? 0.1%?
   - Bits per key: 10? 20?
   - **Recommendation**: Adaptive based on workload

2. **Zone Map Granularity**:
   - Block size: 8192 rows? 16384?
   - Trade-off: overhead vs precision
   - **Recommendation**: 8192 (current), make configurable

3. **Sparse Index Density**:
   - Index every N keys: 100? 1000?
   - **Recommendation**: 100 (current), adaptive based on key size

---

## 16. Success Metrics

### 16.1 Functional Metrics

**Must Have** (blocking GA):
- ✅ RecordBatchReader interface working
- ✅ LSM merge with deduplication
- ✅ Predicate pushdown (bloom + zone map)
- ✅ Acero integration (scan, join, aggregate)
- ✅ PyArrow integration
- ✅ **Sabot integration (graph, RDF, streaming state)**
- ✅ All tests passing (including Sabot integration tests)

**Nice to Have** (post-GA):
- FFI for Rust/DataFusion
- Memory-mapped reads
- Adaptive batch sizing
- Arrow Flight server

### 16.2 Performance Metrics

**Targets** (vs current RocksDB API):

| Metric | Current | Target | Status |
|--------|---------|--------|--------|
| Full scan throughput | 3.52 M rows/sec | 5-10 M rows/sec | 🎯 |
| Selective scan (1%) | N/A | 100 M rows/sec | 🎯 |
| Memory usage (10M rows) | ~5 GB | ~10 MB | 🎯 |
| Hash join (100K x 10K) | N/A | 50-100 M rows/sec | 🎯 |
| Aggregation (1M rows) | N/A | 100-200 M rows/sec | 🎯 |

### 16.3 Adoption Metrics

**Internal Adoption** (Primary):
- **Sabot using MarbleDB as primary storage (CRITICAL)**
- **Sabot graph queries working (Cypher)**
- **Sabot RDF queries working (SPARQL with 100x speedup)**
- **Sabot streaming state working (checkpoint/restore)**
- MarbleDB benchmarks using Arrow APIs
- Internal tools migrated

**External Adoption** (post-GA):
- GitHub stars/forks
- PyPI downloads
- Rust crate downloads
- Questions on forums/Slack

### 16.4 Sabot Integration Success (Primary Use Case)

**Critical**: All metrics must be met for Sabot production deployment.

#### 16.4.1 Graph Storage Success

| Metric | Target | Status |
|--------|--------|--------|
| Node scan throughput | > 10 M nodes/sec | 🎯 |
| Edge scan throughput | > 10 M edges/sec | 🎯 |
| Cypher 2-hop pattern | > 1 M matches/sec | 🎯 |
| Cypher 3-hop pattern | > 500K matches/sec | 🎯 |
| Property filter (bloom) | > 5 M nodes/sec | 🎯 |
| Aggregation | > 100 M rows/sec | 🎯 |

**Must Have**:
- ✅ Cypher queries compile to Acero/DataFusion plans
- ✅ Property projection working (column pruning)
- ✅ Bloom filter integration (node/edge ID filters)
- ✅ All graph tests passing (Section 13.5.1)

#### 16.4.2 RDF/SPARQL Success

| Metric | Current | Target | Status |
|--------|---------|--------|--------|
| Triple scan | 147K/sec | > 10 M/sec | 🎯 68x improvement |
| 1-pattern query | 40K/sec | > 10 M/sec | 🎯 250x improvement |
| 2-pattern join | **5K/sec** ❌ | > 1 M/sec | 🎯 **200x improvement** |
| 3-pattern join | **575/sec** ❌ | > 500K/sec | 🎯 **870x improvement** |
| Aggregation | 10K/sec | > 100 M/sec | 🎯 10,000x improvement |

**Must Have**:
- ✅ SPARQL queries use hash joins (not nested loops)
- ✅ 3-index strategy working (SPO, POS, OSP)
- ✅ Optimal index selection (query planner)
- ✅ O(n log n) scaling confirmed (not O(n²))
- ✅ All RDF tests passing (Section 13.5.2)
- ✅ **100x speedup validated on 130K triple dataset**

#### 16.4.3 Streaming State Success

| Metric | Current | Target | Status |
|--------|---------|--------|--------|
| State write | 150K ops/sec | > 200K ops/sec | 🎯 |
| State read | 450K ops/sec | > 500K ops/sec | 🎯 |
| State scan | 3 M rows/sec | > 10 M rows/sec | 🎯 |
| Checkpoint | 500 ms | < 100 ms | 🎯 |
| Restore | 1000 ms | < 200 ms | 🎯 |

**Must Have**:
- ✅ Hot key cache integrated (P99 < 10ms)
- ✅ Incremental checkpointing working
- ✅ Exactly-once semantics validated
- ✅ All state tests passing (Section 13.5.3)

#### 16.4.4 End-to-End Pipeline Success

**Must Have**:
- ✅ Kafka → Graph → Cypher pipeline working
- ✅ Kafka → RDF → SPARQL pipeline working
- ✅ Stream join with state backend working
- ✅ Materialized view refresh working
- ✅ All E2E tests passing (Section 13.5.4)

**Deployment Readiness**:
- ✅ Sabot can replace current storage with MarbleDB
- ✅ No performance regressions (all improvements validated)
- ✅ Production-scale datasets tested (1M nodes, 1M triples)
- ✅ Documentation complete (integration guide)

---

## 17. Conclusion

### 17.1 Vision Recap

**MarbleDB should be the ideal Arrow-native storage engine**, seamlessly integrating with the entire Arrow ecosystem:

- ✅ **Zero-copy access** (Arrow IPC format, memory-mapped reads)
- ✅ **Streaming execution** (RecordBatchReader, constant memory)
- ✅ **Predicate pushdown** (bloom filters, zone maps, statistics)
- ✅ **Universal compatibility** (Acero, DataFusion, DuckDB, Polars, PyArrow)
- ✅ **Best-in-class performance** (columnar + LSM + optimizations)

### 17.2 Why This Matters

**Current State**: MarbleDB is a great storage engine, but isolated
**Future State**: MarbleDB is the **premier Arrow-native storage backend for Sabot**

**Impact on Sabot** (Primary Use Case):
1. **Graph Performance**: 5-10x faster Cypher queries (pattern matching, aggregations)
2. **RDF Performance**: 100-870x faster SPARQL queries (fixes O(n²) bug)
3. **State Backend**: Fast checkpointing (<100ms), exactly-once semantics
4. **Unified Storage**: Single engine for graphs, RDF, and streaming state

**Impact on Arrow Ecosystem** (Secondary):
1. **Performance**: 2-10x faster than current API (streaming + zero-copy)
2. **Ecosystem**: Works with every major query engine (Acero, DataFusion, DuckDB)
3. **Adoption**: Becomes the go-to for Arrow-native OLTP+OLAP workloads
4. **Differentiation**: Only LSM-based Arrow-native storage (unique position)

### 17.3 Next Steps

1. **Review this document with Sabot team** (primary stakeholder)
   - Validate Sabot requirements (Section 4 and 11.5)
   - Confirm performance targets (graph, RDF, streaming state)
   - Prioritize critical features (SPARQL O(n²) fix is top priority)

2. **Approve technical approach**
   - Standard Arrow interfaces (RecordBatchReader, Dataset)
   - Integration with Acero/DataFusion for Sabot queries
   - LSM-aware iterator with predicate pushdown

3. **Commit to implementation plan** (8 stages)
   - Stage 1-5: Core Arrow API implementation
   - Stage 6: **Sabot integration (CRITICAL)**
   - Stage 8: **Sabot production hardening**

4. **Start Stage 1** (RecordBatchReader)
   - Prioritize Sabot workloads in all testing
   - Test with Sabot graph storage from day 1

---

**End of Document**

---

## Appendix A: Related Work

### Arrow Ecosystem Projects

- **Apache Arrow Acero**: C++ query engine (columnar, vectorized)
- **Apache DataFusion**: Rust query engine (used by InfluxDB, Ballista)
- **DuckDB**: OLAP database with Arrow integration
- **Polars**: DataFrame library (Python/Rust) built on Arrow
- **Velox**: Meta's query engine (used by Presto)
- **Lance**: ML-focused columnar format (Arrow-based)

### Arrow-Native Storage

- **Parquet**: Read-only, no indexes, no OLTP
- **Delta Lake**: Lakehouse format on Parquet (metadata-heavy)
- **Iceberg**: Similar to Delta Lake
- **Lance**: Vector embeddings + Arrow (different use case)
- **MarbleDB**: **ONLY Arrow-native LSM with OLTP support** ← Unique!

### LSM Storage Engines

- **RocksDB**: Row-oriented, no Arrow integration
- **LevelDB**: Row-oriented, older
- **Cassandra**: LSM-based, row-oriented
- **ScyllaDB**: LSM-based, row-oriented
- **MarbleDB**: **ONLY LSM with Arrow-native storage** ← Unique!

**Conclusion**: MarbleDB fills a unique niche: Arrow-native + LSM + OLTP support.

## Appendix B: References

1. **Arrow Format Specification**: https://arrow.apache.org/docs/format/Columnar.html
2. **Arrow C Data Interface**: https://arrow.apache.org/docs/format/CDataInterface.html
3. **Arrow IPC Format**: https://arrow.apache.org/docs/format/Columnar.html#ipc-file-format
4. **Acero Documentation**: https://arrow.apache.org/docs/cpp/streaming_execution.html
5. **DataFusion Architecture**: https://arrow.apache.org/datafusion/
6. **DuckDB Arrow Integration**: https://duckdb.org/docs/api/python/arrow.html
7. **LSM Tree Paper (O'Neil et al.)**: http://www.cs.umb.edu/~poneil/lsmtree.pdf
8. **RocksDB Wiki**: https://github.com/facebook/rocksdb/wiki

---

**Document Version**: 1.0
**Last Updated**: November 7, 2025
**Author**: MarbleDB Team
**Status**: Draft for Review
