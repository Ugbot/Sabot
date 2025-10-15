# MarbleDB Requirements for SabotQL and SabotSQL

**Date:** October 12, 2025
**Status:** Specification Document
**Purpose:** Define what SabotQL (SPARQL) and SabotSQL (Streaming SQL) need from MarbleDB

---

## Executive Summary

We are transitioning from Tonbo (Rust embedded DB) to MarbleDB (C++ columnar LSM-tree) as the unified storage backend for:
- **SabotQL:** SPARQL triple store with 3 permuted indexes
- **SabotSQL:** Streaming SQL with stateful operators and Kafka integration

**Key Benefits:**
1. **10-100x performance improvement** via LSM-tree range scans (vs current O(n) linear scan)
2. **Unified storage layer** - one system instead of Tonbo + RocksDB
3. **RAFT replication** built-in for distributed deployment
4. **Arrow-native** columnar storage with zero-copy operations
5. **Modern C++ async** with Boost.Asio + C++20 coroutines + io_uring

**Current Blockers:**
- `CreateColumnFamily()` returns NotImplemented → can't initialize stores
- `InsertBatch()` returns NotImplemented → can't insert data
- `NewIterator()` returns NotImplemented → forced to use O(n) linear scan
- `ScanTable()` returns NotImplemented → can't query data

**Timeline:**
- **P0 (2-3 weeks):** Basic APIs → unblock SabotQL testing
- **P1 (2-3 weeks):** Range scans → 10-100x performance win
- **P2 (3-4 weeks):** RAFT integration → distributed deployment
- **Total:** 7-10 weeks aggressive, 3-6 months realistic

---

## 1. Async Architecture (Modern C++ Stack)

### Components

**Boost.Asio** - Cross-platform async I/O framework
- Already integrated via NuRaft dependency
- Event loop for non-blocking operations
- Cross-platform abstractions

**Backend-Specific I/O:**
- **Linux:** io_uring (zero-copy, kernel-level async, best performance)
- **macOS:** kqueue (BSD-style event notification)
- **Windows:** IOCP (I/O completion ports)

**C++20 Coroutines** - Clean async syntax
- Zero-overhead async/await via compiler transformation
- Natural control flow (no callback hell)
- Stackless coroutines (efficient memory usage)

**Thread Pool** - CPU-bound operations
- Compaction (heavy I/O + CPU)
- SSTable flush/merge
- Bloom filter construction
- Isolates slow operations from latency-sensitive paths

### Architecture Diagram

```
┌─────────────────────────────────────────────┐
│  SabotQL / SabotSQL (Cython async)          │
│  - async def scan_pattern(...)              │
│  - async def insert_batch(...)              │
└────────────────────┬────────────────────────┘
                     │ Cython → C++ callback bridge
                     ▼
┌─────────────────────────────────────────────┐
│  MarbleDB C++ API (C++20 Coroutines)        │
│  - co_await AsyncInsertBatch()              │
│  - co_await AsyncScanIndex()                │
│  - co_await AsyncNewIterator()              │
└────────────────────┬────────────────────────┘
                     │
            ┌────────┴─────────┐
            ▼                  ▼
┌────────────────────┐  ┌──────────────────┐
│  Asio Event Loop   │  │  Thread Pool     │
│  - io_uring (Linux)│  │  - Compaction    │
│  - kqueue (macOS)  │  │  - SSTable flush │
│  - IOCP (Windows)  │  │  - Bloom filters │
│  - Network I/O     │  │  - Heavy CPU ops │
└────────────────────┘  └──────────────────┘
```

### Code Example

```cpp
// MarbleDB async API
Task<arrow::Result<std::shared_ptr<arrow::Table>>>
TripleStoreImpl::AsyncScanPattern(const TriplePattern& pattern) {
    // All operations are async via coroutines
    auto index = SelectIndex(pattern);
    auto range = BuildKeyRange(pattern);

    // Async iterator creation
    auto it = co_await db_->AsyncNewIterator(ReadOptions(), range);

    // Async iteration
    arrow::Int64Builder col1, col2, col3;
    while (co_await it->Valid()) {
        auto key = co_await it->Key();
        auto value = co_await it->Value();

        // Extract columns from key/value
        col1.Append(ExtractCol1(key));
        col2.Append(ExtractCol2(key));
        col3.Append(ExtractCol3(key));

        co_await it->Next();
    }

    // Build result table
    auto schema = GetIndexSchema(index);
    ARROW_ASSIGN_OR_RAISE(auto c1, col1.Finish());
    ARROW_ASSIGN_OR_RAISE(auto c2, col2.Finish());
    ARROW_ASSIGN_OR_RAISE(auto c3, col3.Finish());

    co_return arrow::Table::Make(schema, {c1, c2, c3});
}
```

**Integration with Cython:**

```cpp
// C++ callback-based wrapper for Cython
void MarbleDB::AsyncScanTable(
    const std::string& table_name,
    std::function<void(arrow::Result<std::shared_ptr<arrow::Table>>)> callback) {

    // Launch coroutine
    auto task = AsyncScanTableInternal(table_name);

    // Post to event loop, callback when done
    asio::co_spawn(io_context_, std::move(task),
        [callback](std::exception_ptr eptr, auto result) {
            if (eptr) {
                callback(arrow::Status::UnknownError("Exception"));
            } else {
                callback(result);
            }
        });
}
```

```python
# Cython async/await integration
async def scan_table(table_name: str) -> ca.Table:
    future = asyncio.Future()

    def callback(result):
        if result.ok():
            future.set_result(result.ValueOrDie())
        else:
            future.set_exception(Exception(result.status().ToString()))

    db.AsyncScanTable(table_name.encode(), callback)
    return await future
```

---

## 2. Column Families Required

### SabotQL (SPARQL Triple Store)

**Purpose:** RDF triple storage with 3 permuted indexes for efficient query patterns

#### SPO Index (Subject-Predicate-Object)
```cpp
ColumnFamilyDescriptor spo_cf("SPO", {
    .schema = arrow::schema({
        arrow::field("subject", arrow::int64()),
        arrow::field("predicate", arrow::int64()),
        arrow::field("object", arrow::int64())
    }),
    .enable_bloom_filter = true,
    .enable_sparse_index = true,
    .index_granularity = 8192,
    .is_raft_replicated = false  // Optional: true for distributed SPARQL
});
```
- **Usage:** Subject-bound queries `(Alice, ?p, ?o)`
- **Access Pattern:** Range scan on subject prefix
- **Size:** 24 bytes per triple (3 × int64)
- **Performance:** O(log n + k) where k = matches

#### POS Index (Predicate-Object-Subject)
```cpp
ColumnFamilyDescriptor pos_cf("POS", {
    .schema = arrow::schema({
        arrow::field("predicate", arrow::int64()),
        arrow::field("object", arrow::int64()),
        arrow::field("subject", arrow::int64())
    }),
    .enable_bloom_filter = true,
    .enable_sparse_index = true,
    .is_raft_replicated = false
});
```
- **Usage:** Predicate-bound queries `(?s, knows, ?o)`
- **Access Pattern:** Range scan on predicate prefix
- **Example Query:** Find all "knows" relationships

#### OSP Index (Object-Subject-Predicate)
```cpp
ColumnFamilyDescriptor osp_cf("OSP", {
    .schema = arrow::schema({
        arrow::field("object", arrow::int64()),
        arrow::field("subject", arrow::int64()),
        arrow::field("predicate", arrow::int64())
    }),
    .enable_bloom_filter = true,
    .enable_sparse_index = true,
    .is_raft_replicated = false
});
```
- **Usage:** Object-bound queries `(?s, ?p, Bob)`
- **Access Pattern:** Range scan on object prefix
- **Example Query:** Find all triples referencing "Bob"

#### Vocabulary Table
```cpp
ColumnFamilyDescriptor vocab_cf("vocabulary", {
    .schema = arrow::schema({
        arrow::field("id", arrow::int64()),
        arrow::field("lexical", arrow::utf8()),
        arrow::field("kind", arrow::uint8()),      // IRI, Literal, BlankNode
        arrow::field("language", arrow::utf8()),   // e.g., "en"
        arrow::field("datatype", arrow::utf8())    // e.g., "xsd:integer"
    }),
    .enable_bloom_filter = true,
    .is_raft_replicated = false  // Optional: true for distributed
});
```
- **Usage:** Bidirectional mapping between terms and ValueIds
- **Access Pattern:** Point lookups by ID or term hash
- **Size:** Variable (lexical strings can be large)
- **Caching:** LRU cache for hot terms (10K entries)

### SabotSQL (Streaming SQL)

**Purpose:** Stateful streaming SQL with Kafka integration

#### Dimension Tables (RAFT Replicated)
```cpp
// Example: securities reference data
ColumnFamilyDescriptor securities_cf("securities", {
    .schema = arrow::schema({
        arrow::field("instrumentId", arrow::utf8()),      // Primary key
        arrow::field("NAME", arrow::utf8()),
        arrow::field("SECTOR", arrow::utf8()),
        arrow::field("EXCHANGE", arrow::utf8())
    }),
    .is_raft_replicated = true,    // Replicate to all nodes
    .is_broadcast = true,           // Cache in-memory on all nodes
    .enable_bloom_filter = true
});
```
- **Usage:** Stream-table joins for enrichment
- **Size:** Small (< 100M rows typical)
- **Access Pattern:** Point lookups by primary key
- **Replication:** RAFT ensures consistency across all agents
- **Broadcasting:** Cached in-memory on all nodes (no network overhead)

**Benefits:**
- No shuffle/repartitioning needed for joins
- Instant lookups (in-memory)
- Automatic replication via RAFT
- Updates propagate to all nodes

#### Connector State (RAFT Replicated)
```cpp
ColumnFamilyDescriptor connector_offsets_cf("connector_offsets", {
    .schema = arrow::schema({
        arrow::field("connector_id", arrow::utf8()),
        arrow::field("partition", arrow::int32()),
        arrow::field("offset", arrow::int64()),
        arrow::field("timestamp", arrow::timestamp(TimeUnit::MILLI))
    }),
    .is_raft_replicated = true,    // Critical for exactly-once
    .enable_sparse_index = true
});
```
- **Usage:** Kafka offset tracking for fault tolerance
- **Size:** Small (one row per partition)
- **Access Pattern:** Point lookups and updates by (connector_id, partition)
- **Replication:** RAFT guarantees exactly-once processing
- **Recovery:** New node reads committed offsets from RAFT group

**Exactly-Once Guarantee:**
1. Agent consumes batch from Kafka
2. Agent processes batch, updates state
3. Agent commits offset to MarbleDB RAFT
4. On crash: new agent reads last committed offset
5. Resumes from last checkpoint (no duplicates)

#### Window Aggregates (Local, Not Replicated)
```cpp
ColumnFamilyDescriptor window_aggs_cf("window_aggregates", {
    .schema = arrow::schema({
        arrow::field("key", arrow::utf8()),               // e.g., "AAPL"
        arrow::field("window_start", arrow::timestamp(TimeUnit::MILLI)),
        arrow::field("count", arrow::int64()),
        arrow::field("sum", arrow::float64()),
        arrow::field("min", arrow::float64()),
        arrow::field("max", arrow::float64())
    }),
    .is_raft_replicated = false,   // Local per agent
    .enable_sparse_index = true
});
```
- **Usage:** Tumbling/sliding window aggregations
- **Size:** Medium (one row per window per key)
- **Access Pattern:** Point lookups by (key, window_start), range scans
- **Partitioning:** Data partitioned by key across agents
- **No Replication:** Each agent owns its key partitions

**Window Lifecycle:**
1. First record arrives → create window state
2. More records → update aggregates
3. Watermark advances past window end → emit result, delete state

#### Join Buffers (Local, Not Replicated)
```cpp
ColumnFamilyDescriptor join_buffers_cf("join_buffers", {
    .schema = arrow::schema({
        arrow::field("key", arrow::utf8()),
        arrow::field("ts", arrow::timestamp(TimeUnit::MILLI)),
        arrow::field("data", arrow::binary())    // Serialized row
    }),
    .is_raft_replicated = false,
    .enable_sparse_index = true,
    .enable_ttl = true,                 // Auto-cleanup old data
    .ttl_seconds = 3600                 // 1 hour retention
});
```
- **Usage:** Stream-stream joins (buffer left/right streams)
- **Size:** Variable (depends on join window size)
- **Access Pattern:** Range scans by (key, ts) for join matching
- **TTL:** Automatic cleanup of expired data
- **No Replication:** Local buffers per agent

---

## 3. Core API Requirements

### Critical APIs (Currently NotImplemented)

#### P0 - Blocking SabotQL Testing

**CreateColumnFamily**
```cpp
Status CreateColumnFamily(const ColumnFamilyDescriptor& descriptor,
                          ColumnFamilyHandle** handle);
```
- **Current:** Returns `Status::NotImplemented`
- **Needed:** Actually create column family with Arrow schema
- **Implementation:**
  - Register CF in internal map
  - Associate Arrow schema with CF
  - Create memtable for CF
  - Return handle for future operations
- **Estimate:** 2-4 hours
- **Impact:** Unblocks store initialization

**InsertBatch**
```cpp
Status InsertBatch(const std::string& table_name,
                   const std::shared_ptr<arrow::RecordBatch>& batch);
```
- **Current:** Returns `Status::NotImplemented`
- **Needed:** Bulk insert Arrow batches
- **Implementation:**
  - Look up column family by name
  - Convert Arrow batch to internal format
  - Write to memtable
  - Trigger SSTable flush if memtable full
- **Estimate:** 4-8 hours
- **Impact:** Can insert triples and dimension tables

**ScanTable**
```cpp
Status ScanTable(const std::string& table_name,
                std::unique_ptr<QueryResult>* result);
```
- **Current:** Returns `Status::NotImplemented`
- **Needed:** Full table scan returning Arrow Table
- **Implementation:**
  - Linear scan of memtable + all SSTables
  - Merge results
  - Return as QueryResult with Arrow batches
- **Estimate:** 2-4 hours
- **Impact:** Can query data (even if slow)

#### P1 - 10-100x Performance Win

**NewIterator (KEY API)**
```cpp
Status NewIterator(const ReadOptions& options,
                   const KeyRange& range,
                   std::unique_ptr<Iterator>* iterator);
```
- **Current:** Returns `Status::NotImplemented`
- **Needed:** Range scan with (start_key, end_key) bounds
- **Implementation:**
  - Create merge iterator across memtable + SSTables
  - Seek to start_key using sparse index
  - Iterate until end_key
  - Support forward and reverse iteration
- **Estimate:** 8-16 hours
- **Impact:** **10-100x speedup** for selective queries

**Iterator Interface:**
```cpp
class Iterator {
public:
    virtual bool Valid() const = 0;
    virtual void Next() = 0;
    virtual void Prev() = 0;          // Reverse iteration
    virtual Key key() const = 0;
    virtual std::string value() const = 0;
    virtual Status status() const = 0;

    // Async variants
    virtual Task<bool> AsyncValid() const = 0;
    virtual Task<void> AsyncNext() = 0;
    virtual Task<Key> AsyncKey() const = 0;
    virtual Task<std::string> AsyncValue() const = 0;
};
```

**Range Scan Example:**
```cpp
// Find all triples with subject = Alice (ValueId = 42)
KeyRange range;
range.start_key = EncodeKey(42, 0, 0);      // (Alice, *, *)
range.end_key = EncodeKey(42, UINT64_MAX, UINT64_MAX);
range.include_start = true;
range.include_end = false;

std::unique_ptr<Iterator> it;
co_await db->AsyncNewIterator(ReadOptions(), range, &it);

while (co_await it->AsyncValid()) {
    auto key = co_await it->AsyncKey();
    auto value = co_await it->AsyncValue();
    // Process triple
    co_await it->AsyncNext();
}
```

**Performance:**
- **Without NewIterator:** O(n) - scan all 10M triples, filter in memory
- **With NewIterator:** O(log n + k) - sparse index seek + scan k matches
- **Speedup:** 10-100x on selective queries (k << n)

#### Additional APIs

**MultiGet**
```cpp
Status MultiGet(const ReadOptions& options,
                const std::vector<Key>& keys,
                std::vector<std::shared_ptr<Record>>* records);
```
- **Usage:** Batch point lookups (faster than individual Gets)
- **Example:** Vocabulary lookups for multiple ValueIds
- **Optimization:** Single lock acquisition, batch I/O

**DeleteRange**
```cpp
Status DeleteRange(const WriteOptions& options,
                   const Key& begin_key,
                   const Key& end_key);
```
- **Usage:** Efficient bulk deletion
- **Example:** Delete closed window state
- **Implementation:** Tombstones for range (no per-key deletes)

**CompactRange**
```cpp
Status CompactRange(const KeyRange& range);
```
- **Usage:** Trigger compaction for specific range
- **Example:** Compact old window data
- **Implementation:** Merge SSTables in range, remove tombstones

---

## 4. Tonbo Feature Parity

### Features We're Replacing

**Arrow/Parquet Columnar Storage**
- Tonbo: Native Arrow/Parquet LSM tree
- MarbleDB: Need columnar block storage with Arrow
- **Status:** Planned, not yet implemented

**Zero-Copy Operations**
- Tonbo: Zero-copy deserialization via Arrow
- MarbleDB: Arrow-native interface (RecordBatch in/out)
- **Status:** Partial (InsertBatch accepts RecordBatch)

**Pushdown Operations**
- Tonbo: Filter, limit, projection pushdown
- MarbleDB: Need predicate pushdown to SSTable level
- **Status:** Planned (bloom filters, sparse index)

**Range Scans**
- Tonbo: `scan((Bound::Included(&start), Bound::Excluded(&end)))`
- MarbleDB: `NewIterator(ReadOptions, KeyRange, &iterator)`
- **Status:** NotImplemented (P1 priority)

**Reverse Scan**
- Tonbo: Descending order scans (newest-first)
- MarbleDB: `ReadOptions.reverse_order = true`
- **Status:** Designed, not implemented

**Transactions**
- Tonbo: Optimistic transactions with `commit()`
- MarbleDB: `DBTransaction` class exists but not implemented
- **Status:** Planned (P2)

**Compaction**
- Tonbo: Leveled compaction strategy
- MarbleDB: `CompactRange()` exists but not implemented
- **Status:** Partial (automatic compaction may work)

**Schema Flexibility**
- Tonbo: Runtime-defined schemas via Arrow
- MarbleDB: Column families with Arrow schemas
- **Status:** Designed (ColumnFamilyDescriptor.schema)

**Fully Async API**
- Tonbo: Rust async with tokio/async-std
- MarbleDB: C++ with Boost.Asio + C++20 coroutines + io_uring
- **Status:** Planned (P1)

### Key Differences

**Language:**
- Tonbo: Rust (safe, memory-efficient, slow compile times)
- MarbleDB: C++ (fast, flexible, requires careful memory management)

**Async Model:**
- Tonbo: Rust async/await (tokio executor)
- MarbleDB: C++20 coroutines (io_uring/kqueue/IOCP)
- **Advantage:** io_uring more mature than tokio-uring

**Integration:**
- Tonbo: Rust FFI → Cython (some overhead)
- MarbleDB: C++ → Cython (direct, zero overhead)

**Maturity:**
- Tonbo: v0.3.2, labeled "unstable"
- MarbleDB: Custom, full control over implementation

---

## 5. RAFT Replication Specification

### Tables Needing Replication

**is_raft_replicated = true:**

1. **Dimension Tables** (securities, products, reference data)
   - **Why:** Broadcast to all agents for local lookups
   - **Size:** Small (< 100M rows)
   - **Updates:** Infrequent
   - **Benefit:** No shuffle needed for stream-table joins

2. **Connector State** (Kafka offsets)
   - **Why:** Exactly-once processing guarantee
   - **Size:** Tiny (one row per partition)
   - **Updates:** Frequent (after each batch)
   - **Benefit:** Fault tolerance - new nodes read committed offsets

3. **Optional: Triple Store Indexes** (SPO, POS, OSP)
   - **Why:** Distributed SPARQL queries
   - **Size:** Large (10M-1B triples)
   - **Updates:** Infrequent (bulk loads)
   - **Benefit:** Query distribution across cluster

### Tables NOT Needing Replication

**is_raft_replicated = false:**

1. **Window Aggregates**
   - **Why:** Partitioned by key, local to each agent
   - **Size:** Medium (thousands of windows)
   - **Updates:** Continuous
   - **Benefit:** No replication overhead

2. **Join Buffers**
   - **Why:** Partitioned by key, local to each agent
   - **Size:** Variable
   - **Updates:** Continuous
   - **Benefit:** No replication overhead

3. **Temporary Tables**
   - **Why:** Ephemeral, agent-specific
   - **Benefit:** No network cost

### Raft Operation Types

```cpp
enum class RaftOperationType {
    kWalEntry,       // Replicate write operations
    kTableCreate,    // Schema changes
    kTableDrop,
    kIndexCreate,
    kIndexDrop,
    kSnapshot,       // Full state checkpoint
    kConfigChange,   // Add/remove nodes
    kCustom
};

struct RaftOperation {
    RaftOperationType type;
    std::string table_name;
    std::string data;          // Serialized payload
    uint64_t timestamp;
};
```

### State Machine Implementation

```cpp
class MarbleDBStateMachine : public RaftStateMachine {
public:
    MarbleDBStateMachine(std::shared_ptr<MarbleDB> db) : db_(db) {}

    // Apply committed operation to local state
    Status ApplyOperation(const RaftOperation& operation) override {
        switch (operation.type) {
            case RaftOperationType::kWalEntry: {
                // Deserialize and apply write
                auto batch = DeserializeArrowBatch(operation.data);
                return db_->InsertBatch(operation.table_name, batch);
            }

            case RaftOperationType::kTableCreate: {
                // Create column family
                auto descriptor = DeserializeDescriptor(operation.data);
                ColumnFamilyHandle* handle;
                return db_->CreateColumnFamily(descriptor, &handle);
            }

            case RaftOperationType::kSnapshot: {
                // Full checkpoint
                return db_->CreateCheckpoint(operation.data);
            }

            default:
                return Status::InvalidArgument("Unknown operation type");
        }
    }

    // Create snapshot of current state
    Status CreateSnapshot(uint64_t log_index) override {
        std::string checkpoint_path = "/tmp/checkpoint_" + std::to_string(log_index);
        return db_->CreateCheckpoint(checkpoint_path);
    }

    // Restore state from snapshot
    Status RestoreFromSnapshot(uint64_t log_index) override {
        std::string checkpoint_path = "/tmp/checkpoint_" + std::to_string(log_index);
        return db_->RestoreFromCheckpoint(checkpoint_path);
    }

    uint64_t GetLastAppliedIndex() const override {
        return last_applied_index_;
    }

private:
    std::shared_ptr<MarbleDB> db_;
    std::atomic<uint64_t> last_applied_index_{0};
};
```

### Replication Flow

**Write Path (Replicated Table):**
```
1. Client: db->InsertBatch("securities", batch)
2. MarbleDB: Check if table is_raft_replicated
3. If replicated:
   a. Serialize batch → RaftOperation
   b. raft_server->ProposeOperation(operation)
   c. RAFT consensus (replicate to majority)
   d. RAFT commits operation
   e. State machine applies to local DB
   f. Return success to client
4. If not replicated:
   a. Write directly to local DB
   b. Return success immediately
```

**Read Path (Replicated Table):**
```
1. Client: db->ScanTable("securities", &result)
2. MarbleDB: Read from LOCAL replica (no network)
3. Return Arrow Table immediately
```

**Recovery:**
```
1. New node joins RAFT group
2. RAFT identifies missing log entries
3. Leader sends snapshot to new node
4. New node calls RestoreFromSnapshot()
5. New node catches up with log replay
6. New node becomes active replica
```

### Configuration

```cpp
// Create RAFT-replicated column family
ColumnFamilyDescriptor cf("securities", {
    .schema = securities_schema,
    .is_raft_replicated = true,     // Enable RAFT
    .is_broadcast = true,            // Cache in-memory on all nodes
    .replication_factor = 3          // Replicate to 3 nodes
});

// Create local-only column family
ColumnFamilyDescriptor cf("window_aggregates", {
    .schema = window_schema,
    .is_raft_replicated = false      // No replication
});
```

---

## 6. Performance Requirements

### Current Performance (In-Memory Cache)

**Algorithm:** Linear O(n) scan with in-memory filtering

| Dataset Size | Query Latency | Throughput |
|--------------|---------------|------------|
| 1K triples | 0.5 ms | 2,000 q/s |
| 10K triples | 2 ms | 500 q/s |
| 100K triples | 20 ms | 50 q/s |
| 1M triples | 200 ms | 5 q/s |
| 10M triples | 2 sec | 0.5 q/s |

**Scaling:** Linear degradation - performance decreases proportionally with data size

**Example:** Query for `(Alice, ?p, ?o)` in 10M triple database
- Must scan all 10M triples
- Filter for subject = Alice
- Takes ~2 seconds even if only 10 matches

### Target Performance (LSM-Tree with Range Scans)

**Algorithm:** O(log n + k) sparse index seek + iterate k matches

| Dataset Size | Query Latency | Throughput | Speedup |
|--------------|---------------|------------|---------|
| 1K triples | 0.5 ms | 2,000 q/s | 1x (baseline) |
| 10K triples | 0.6 ms | 1,667 q/s | 3.3x |
| 100K triples | 0.8 ms | 1,250 q/s | **25x** |
| 1M triples | 1.2 ms | 833 q/s | **167x** |
| 10M triples | 2 ms | 500 q/s | **1000x** |

**Scaling:** Logarithmic - performance degrades slowly

**Example:** Query for `(Alice, ?p, ?o)` in 10M triple database
- Sparse index seek to subject = Alice: O(log n) ~10 lookups
- Scan matching triples: O(k) ~10 triples
- Total: ~2 ms (1000x faster than linear scan)

### Breakdown by Query Selectivity

**High Selectivity (k = 10 results):**
- Current: 2 seconds (scan all 10M)
- Target: 2 ms (seek + 10 reads)
- **Speedup: 1000x**

**Medium Selectivity (k = 1,000 results):**
- Current: 2 seconds
- Target: 5 ms (seek + 1K reads)
- **Speedup: 400x**

**Low Selectivity (k = 100,000 results):**
- Current: 2 seconds
- Target: 200 ms (seek + 100K reads)
- **Speedup: 10x**

### Comparison with Other Systems

**For 10M triple dataset, selective query (100 results):**

| System | Algorithm | Latency | Notes |
|--------|-----------|---------|-------|
| **QLever** | Compressed indexes | 1-10 ms | Highly optimized SPARQL engine |
| **Virtuoso** | Cost-based optimizer | 5-20 ms | Commercial RDF database |
| **RocksDB** | LSM-tree range scan | 2-5 ms | General-purpose KV store |
| **Tonbo** | Arrow/Parquet LSM | 3-8 ms | Columnar embedded DB |
| **SabotQL (current)** | Linear scan | 2000 ms | In-memory cache |
| **SabotQL (target)** | LSM range scan | 2-5 ms | **Competitive with best systems** |

### Benchmarking Plan

**Metrics to Track:**
1. Query latency (p50, p95, p99)
2. Throughput (queries/second)
3. Scaling with dataset size
4. Memory usage
5. Disk I/O patterns

**Benchmark Queries:**
1. Point lookup: `(Alice, knows, Bob)` → single triple
2. Subject scan: `(Alice, ?p, ?o)` → all Alice triples
3. Predicate scan: `(?s, knows, ?o)` → all "knows" relationships
4. Complex join: 2-3 triple patterns with joins

**Comparison Baselines:**
- RocksDB (general LSM-tree)
- Tonbo (columnar LSM-tree)
- QLever (if available)

---

## 7. Implementation Priorities

### Phase 1 (P0): Basic Functionality - 2-3 weeks

**Goal:** Unblock SabotQL testing

**Tasks:**

1. **CreateColumnFamily() - 2-4 hours**
   - Create in-memory registry: `map<string, ColumnFamilyHandle*>`
   - Associate Arrow schema with each CF
   - Create memtable for CF
   - Return handle
   - **Deliverable:** Can initialize SPO/POS/OSP/vocab column families

2. **InsertBatch() - 4-8 hours**
   - Look up CF by name
   - Convert Arrow RecordBatch to internal row format
   - Write to memtable
   - Check memtable size threshold
   - If full: flush to SSTable
   - **Deliverable:** Can insert triples and dimension tables

3. **Basic ScanTable() - 2-4 hours**
   - Linear scan of memtable
   - Linear scan of all SSTables
   - Merge results (handle duplicates/tombstones)
   - Return as Arrow Table via QueryResult
   - **Deliverable:** Can query data (slow but functional)

**Milestone:** SabotQL integration tests pass

### Phase 2 (P1): Performance - 2-3 weeks

**Goal:** 10-100x speedup via range scans

**Tasks:**

4. **NewIterator() + Range Scans - 8-16 hours**
   - Implement merge iterator across memtable + SSTables
   - Sparse index seeking:
     - Binary search index to find SSTable block
     - Load block from disk
     - Binary search within block (sorted keys)
   - Support KeyRange (start, end, include_start, include_end)
   - Handle forward and reverse iteration
   - **Deliverable:** O(log n + k) queries

5. **Predicate Pushdown - 4-8 hours**
   - Bloom filter per SSTable
   - Check bloom before loading SSTable
   - Block-level bloom filters
   - Skip blocks that definitely don't match
   - **Deliverable:** Skip irrelevant data blocks

6. **Async API with Coroutines - 8-12 hours**
   - Integrate Boost.Asio
   - Wrap I/O operations with async_read/async_write
   - C++20 coroutine wrappers:
     ```cpp
     Task<T> -> std::coroutine_handle<>
     co_await -> suspend/resume
     ```
   - Thread pool for CPU-bound operations
   - Callback-based API for Cython integration
   - **Deliverable:** Fully async MarbleDB API

**Milestone:** 10-100x speedup on SabotQL benchmarks

### Phase 3 (P2): Fault Tolerance - 3-4 weeks

**Goal:** Distributed deployment with RAFT

**Tasks:**

7. **RAFT State Machine Integration - 16-24 hours**
   - Implement `MarbleDBStateMachine`
   - `ApplyOperation()` for different operation types
   - Serialize/deserialize operations
   - Table-level replication flags
   - Write path: propose to RAFT if replicated
   - Read path: always local (no network)
   - **Deliverable:** Replicated dimension tables and connector state

8. **Checkpoint/Restore - 8-16 hours**
   - `CreateCheckpoint()`:
     - Snapshot all memtables to disk
     - Copy all SSTables to checkpoint dir
     - Save metadata (CF list, schemas, etc.)
   - `RestoreFromCheckpoint()`:
     - Load memtables from checkpoint
     - Register SSTables
     - Restore metadata
   - Integration with RAFT snapshots
   - **Deliverable:** Fast recovery for new nodes

**Milestone:** Multi-node SabotQL/SabotSQL deployment

### Summary Timeline

| Phase | Duration | Effort | Deliverable |
|-------|----------|--------|-------------|
| **P0: Basic APIs** | 2-3 weeks | 8-16 hours | SabotQL tests pass |
| **P1: Performance** | 2-3 weeks | 20-36 hours | 10-100x speedup |
| **P2: Fault Tolerance** | 3-4 weeks | 24-40 hours | Distributed deployment |
| **Total** | 7-10 weeks | 52-92 hours | Production-ready |

**Aggressive Timeline:** 7-10 weeks (full-time developer)
**Realistic Timeline:** 3-6 months (accounting for other priorities, testing, debugging)

---

## 8. Integration Examples

### SabotQL: Triple Store Initialization

```cpp
#include <marble/db.h>
#include <sabot_ql/storage/triple_store.h>

using namespace marble;
using namespace sabot_ql;

// Create MarbleDB instance
DBOptions options;
options.db_path = "/data/sabot/triples";
options.enable_sparse_index = true;
options.enable_bloom_filter = true;
options.index_granularity = 8192;

std::unique_ptr<MarbleDB> db;
auto status = MarbleDB::Open(options, nullptr, &db);

// Create SPO column family
ColumnFamilyDescriptor spo_cf("SPO", {
    .schema = arrow::schema({
        arrow::field("subject", arrow::int64()),
        arrow::field("predicate", arrow::int64()),
        arrow::field("object", arrow::int64())
    }),
    .enable_bloom_filter = true,
    .enable_sparse_index = true,
    .index_granularity = 8192,
    .is_raft_replicated = false  // Or true for distributed SPARQL
});

ColumnFamilyHandle* spo_handle;
status = db->CreateColumnFamily(spo_cf, &spo_handle);

// Similarly create POS and OSP
ColumnFamilyHandle* pos_handle;
ColumnFamilyHandle* osp_handle;
// ... (same pattern)

// Create vocabulary column family
ColumnFamilyDescriptor vocab_cf("vocabulary", {
    .schema = arrow::schema({
        arrow::field("id", arrow::int64()),
        arrow::field("lexical", arrow::utf8()),
        arrow::field("kind", arrow::uint8()),
        arrow::field("language", arrow::utf8()),
        arrow::field("datatype", arrow::utf8())
    }),
    .enable_bloom_filter = true
});

ColumnFamilyHandle* vocab_handle;
status = db->CreateColumnFamily(vocab_cf, &vocab_handle);

// Wrap in TripleStore
auto triple_store = TripleStore::Open("/data/sabot/triples", std::move(db));
```

### SabotQL: Inserting Triples

```cpp
// Build Arrow batch with triples
arrow::Int64Builder s_builder, p_builder, o_builder;

for (const auto& triple : triples) {
    s_builder.Append(triple.subject.getBits());
    p_builder.Append(triple.predicate.getBits());
    o_builder.Append(triple.object.getBits());
}

std::shared_ptr<arrow::Array> s_array, p_array, o_array;
s_builder.Finish(&s_array);
p_builder.Finish(&p_array);
o_builder.Finish(&o_array);

auto schema = arrow::schema({
    arrow::field("subject", arrow::int64()),
    arrow::field("predicate", arrow::int64()),
    arrow::field("object", arrow::int64())
});

auto batch = arrow::RecordBatch::Make(schema, triples.size(),
                                     {s_array, p_array, o_array});

// Insert into SPO index
status = db->InsertBatch("SPO", batch);

// Also insert into POS and OSP (reordered columns)
// ...
```

### SabotQL: Querying with Range Scan

```cpp
// Query: (Alice, ?p, ?o) where Alice has ValueId = 42
// Use SPO index for subject-bound query

KeyRange range;
range.start_key = EncodeTripleKey(42, 0, 0);              // (Alice, *, *)
range.end_key = EncodeTripleKey(42, UINT64_MAX, UINT64_MAX);
range.include_start = true;
range.include_end = false;

ReadOptions read_opts;
std::unique_ptr<Iterator> it;

co_await db->AsyncNewIterator(read_opts, range, &it);

arrow::Int64Builder p_builder, o_builder;

while (co_await it->AsyncValid()) {
    auto key = co_await it->AsyncKey();

    // Decode key: (subject, predicate, object)
    uint64_t s, p, o;
    DecodeTripleKey(key, &s, &p, &o);

    p_builder.Append(p);
    o_builder.Append(o);

    co_await it->AsyncNext();
}

// Build result table
std::shared_ptr<arrow::Array> p_array, o_array;
p_builder.Finish(&p_array);
o_builder.Finish(&o_array);

auto result_schema = arrow::schema({
    arrow::field("predicate", arrow::int64()),
    arrow::field("object", arrow::int64())
});

auto result_table = arrow::Table::Make(result_schema, {p_array, o_array});
```

### SabotSQL: Dimension Table Registration

```cpp
// Load securities dimension table (10M rows)
auto securities_table = LoadSecuritiesFromCSV("securities.csv");

// Create RAFT-replicated column family
ColumnFamilyDescriptor securities_cf("securities", {
    .schema = securities_table->schema(),
    .is_raft_replicated = true,    // Replicate to all nodes
    .is_broadcast = true,           // Cache in-memory on all nodes
    .enable_bloom_filter = true
});

ColumnFamilyHandle* securities_handle;
auto status = db->CreateColumnFamily(securities_cf, &securities_handle);

// Insert dimension table (via RAFT consensus)
for (int64_t offset = 0; offset < securities_table->num_rows(); offset += 10000) {
    auto batch = securities_table->Slice(offset, 10000);

    // This goes through RAFT: propose → consensus → apply to all nodes
    status = db->InsertBatch("securities", batch);

    if (!status.ok()) {
        std::cerr << "Insert failed: " << status.ToString() << std::endl;
        break;
    }
}

std::cout << "Inserted " << securities_table->num_rows()
          << " securities (replicated to all nodes)" << std::endl;
```

### SabotSQL: Kafka Offset Checkpointing

```cpp
// Create connector state table (RAFT replicated)
ColumnFamilyDescriptor offsets_cf("connector_offsets", {
    .schema = arrow::schema({
        arrow::field("connector_id", arrow::utf8()),
        arrow::field("partition", arrow::int32()),
        arrow::field("offset", arrow::int64()),
        arrow::field("timestamp", arrow::timestamp(TimeUnit::MILLI))
    }),
    .is_raft_replicated = true,    // Critical for exactly-once
    .enable_sparse_index = true
});

ColumnFamilyHandle* offsets_handle;
db->CreateColumnFamily(offsets_cf, &offsets_handle);

// Kafka consumer loop
while (running) {
    // Consume batch from Kafka
    auto records = kafka_consumer->poll(Duration::milliseconds(1000));

    // Process records
    auto result_batch = ProcessRecords(records);

    // Update state (window aggregates)
    UpdateWindowAggregates(result_batch);

    // Commit offsets to MarbleDB (via RAFT)
    for (const auto& [partition, offset] : records.offsets()) {
        arrow::StringBuilder connector_builder;
        arrow::Int32Builder partition_builder;
        arrow::Int64Builder offset_builder;
        arrow::TimestampBuilder ts_builder(arrow::timestamp(TimeUnit::MILLI),
                                           arrow::default_memory_pool());

        connector_builder.Append("kafka-trades");
        partition_builder.Append(partition);
        offset_builder.Append(offset);
        ts_builder.Append(CurrentTimeMillis());

        std::shared_ptr<arrow::Array> c_array, p_array, o_array, t_array;
        connector_builder.Finish(&c_array);
        partition_builder.Finish(&p_array);
        offset_builder.Finish(&o_array);
        ts_builder.Finish(&t_array);

        auto schema = arrow::schema({
            arrow::field("connector_id", arrow::utf8()),
            arrow::field("partition", arrow::int32()),
            arrow::field("offset", arrow::int64()),
            arrow::field("timestamp", arrow::timestamp(TimeUnit::MILLI))
        });

        auto batch = arrow::RecordBatch::Make(schema, 1,
                                              {c_array, p_array, o_array, t_array});

        // This commits via RAFT (replicated to all nodes)
        auto status = db->InsertBatch("connector_offsets", batch);

        if (!status.ok()) {
            std::cerr << "Failed to commit offset: " << status.ToString() << std::endl;
            // Rollback and retry
            break;
        }
    }

    // Offsets are now committed (exactly-once guaranteed)
}

// On recovery after crash:
std::unique_ptr<QueryResult> result;
db->ScanTable("connector_offsets", &result);

// Read last committed offsets for each partition
while (result->HasNext()) {
    std::shared_ptr<arrow::RecordBatch> batch;
    result->Next(&batch);

    auto connector_array = std::static_pointer_cast<arrow::StringArray>(batch->column(0));
    auto partition_array = std::static_pointer_cast<arrow::Int32Array>(batch->column(1));
    auto offset_array = std::static_pointer_cast<arrow::Int64Array>(batch->column(2));

    for (int64_t i = 0; i < batch->num_rows(); i++) {
        std::string connector_id = connector_array->GetString(i);
        int32_t partition = partition_array->Value(i);
        int64_t offset = offset_array->Value(i);

        // Resume Kafka consumer from last committed offset
        kafka_consumer->seek(partition, offset);
    }
}
```

### SabotSQL: Window Aggregation State

```cpp
// Create window aggregates table (local, NOT replicated)
ColumnFamilyDescriptor window_cf("window_aggregates", {
    .schema = arrow::schema({
        arrow::field("key", arrow::utf8()),               // e.g., "AAPL"
        arrow::field("window_start", arrow::timestamp(TimeUnit::MILLI)),
        arrow::field("count", arrow::int64()),
        arrow::field("sum", arrow::float64()),
        arrow::field("min", arrow::float64()),
        arrow::field("max", arrow::float64())
    }),
    .is_raft_replicated = false,   // Local per agent (partitioned by key)
    .enable_sparse_index = true
});

ColumnFamilyHandle* window_handle;
db->CreateColumnFamily(window_cf, &window_handle);

// Process incoming trades
for (const auto& trade : trades) {
    // Compute window key
    int64_t window_start = (trade.timestamp / 60000) * 60000;  // 1-minute tumbling
    std::string state_key = trade.symbol + ":" + std::to_string(window_start);

    // Read current window state
    Key lookup_key = EncodeWindowKey(trade.symbol, window_start);
    std::shared_ptr<Record> current_state;

    auto status = db->Get(ReadOptions(), lookup_key, &current_state);

    // Update aggregates
    int64_t count = 1;
    double sum = trade.price;
    double min = trade.price;
    double max = trade.price;

    if (status.ok()) {
        // State exists - update
        count += current_state->GetInt64("count");
        sum += current_state->GetDouble("sum");
        min = std::min(min, current_state->GetDouble("min"));
        max = std::max(max, current_state->GetDouble("max"));
    }

    // Write updated state
    arrow::StringBuilder key_builder;
    arrow::TimestampBuilder ts_builder(arrow::timestamp(TimeUnit::MILLI),
                                       arrow::default_memory_pool());
    arrow::Int64Builder count_builder;
    arrow::DoubleBuilder sum_builder, min_builder, max_builder;

    key_builder.Append(trade.symbol);
    ts_builder.Append(window_start);
    count_builder.Append(count);
    sum_builder.Append(sum);
    min_builder.Append(min);
    max_builder.Append(max);

    // ... (finish arrays and create batch)

    status = db->InsertBatch("window_aggregates", batch);
}

// Check watermark - emit closed windows
int64_t watermark = GetCurrentWatermark();

// Range scan for windows that ended before watermark
KeyRange closed_windows;
closed_windows.start_key = EncodeWindowKey("", 0);
closed_windows.end_key = EncodeWindowKey("", watermark - 60000);  // Window size

std::unique_ptr<Iterator> it;
db->NewIterator(ReadOptions(), closed_windows, &it);

while (it->Valid()) {
    // Emit window result
    auto key = it->key();
    auto value = it->value();

    std::string symbol;
    int64_t window_start;
    DecodeWindowKey(key, &symbol, &window_start);

    // Parse aggregates from value
    auto state = ParseWindowState(value);

    std::cout << "Window closed: " << symbol
              << " [" << window_start << "]: "
              << "count=" << state.count
              << " avg=" << (state.sum / state.count) << std::endl;

    // Delete closed window state
    db->Delete(WriteOptions(), key);

    it->Next();
}
```

---

## 9. Next Steps

### Immediate (This Week)

1. **Review Requirements** with team
   - Validate column family schemas
   - Confirm async architecture (Asio + coroutines)
   - Agree on priorities (P0 → P1 → P2)

2. **Set Up Development Environment**
   - Boost.Asio dependency (already via NuRaft)
   - C++20 compiler (GCC 10+, Clang 11+, MSVC 2019+)
   - io_uring library for Linux
   - Arrow C++ library

3. **Create Stub Implementations**
   - Empty methods returning success
   - Allows SabotQL/SabotSQL integration to proceed
   - Gradually fill in functionality

### P0 Implementation (Weeks 1-2)

1. **CreateColumnFamily()**
   - In-memory CF registry
   - Schema validation
   - Memtable creation

2. **InsertBatch()**
   - Arrow → internal format conversion
   - Memtable write
   - SSTable flush trigger

3. **ScanTable()**
   - Linear scan implementation
   - Arrow Table result

4. **Testing**
   - SabotQL integration tests
   - Basic queries working
   - Verify correctness

### P1 Implementation (Weeks 3-4)

1. **NewIterator()**
   - Merge iterator
   - Sparse index seeking
   - Range scan support

2. **Async API**
   - Boost.Asio integration
   - C++20 coroutine wrappers
   - Cython callback bridge

3. **Performance Testing**
   - Benchmark suite
   - Compare vs RocksDB, Tonbo
   - Verify 10-100x speedup

### P2 Implementation (Weeks 5-8)

1. **RAFT Integration**
   - MarbleDBStateMachine
   - Table replication flags
   - Snapshot/restore

2. **Distributed Testing**
   - Multi-node deployment
   - Failure recovery
   - Consistency verification

3. **Production Readiness**
   - Monitoring
   - Error handling
   - Documentation

---

## 10. Open Questions

1. **Async API Details**
   - Should we use std::future or custom Task<T>?
   - How to handle cancellation?
   - Error propagation strategy?

2. **Memory Management**
   - How much to cache in-memory?
   - LRU eviction policy for hot data?
   - Memory limits per column family?

3. **Compaction Strategy**
   - Leveled (current) or tiered?
   - Compaction triggers?
   - Background thread count?

4. **Schema Evolution**
   - Can we add columns to existing CFs?
   - How to handle schema incompatibilities?
   - Migration strategy?

5. **Testing Strategy**
   - Unit tests per component?
   - Integration tests with SabotQL/SabotSQL?
   - Performance regression tests?

6. **Monitoring**
   - What metrics to expose?
   - Integration with Prometheus?
   - Logging strategy?

---

## 11. Success Criteria

**P0 Success:**
- ✅ SabotQL can create triple stores
- ✅ SabotQL can insert triples
- ✅ SabotQL integration tests pass
- ✅ Basic queries return correct results

**P1 Success:**
- ✅ 10-100x speedup on selective queries
- ✅ Performance comparable to RocksDB/Tonbo
- ✅ Fully async API working
- ✅ Integration with Cython async/await

**P2 Success:**
- ✅ Multi-node SabotQL deployment
- ✅ Dimension tables replicated via RAFT
- ✅ Kafka offset checkpointing works
- ✅ Fault tolerance verified

**Production Readiness:**
- ✅ Comprehensive test coverage
- ✅ Performance benchmarks published
- ✅ Documentation complete
- ✅ Monitoring integrated
- ✅ Used in production for 1+ month

---

## Appendix A: Key Encoding Schemes

### Triple Keys (SPO Index)

```
Key: [subject: 8 bytes][predicate: 8 bytes][object: 8 bytes]
Value: empty (data is in key)

Example:
  Triple: (Alice, knows, Bob) where Alice=42, knows=100, Bob=99
  Key: 0x0000002A000000640000063 (24 bytes)
  Value: empty
```

### Vocabulary Keys

```
Key: [id: 8 bytes]
Value: [lexical: string][kind: 1 byte][language: string][datatype: string]

Example:
  Term: "Alice" (IRI)
  Key: 0x000000000000002A (8 bytes)
  Value: "http://example.org/Alice" || 0x00 || "" || ""
```

### Window State Keys

```
Key: [symbol: string][window_start: 8 bytes]
Value: [count: 8 bytes][sum: 8 bytes][min: 8 bytes][max: 8 bytes]

Example:
  Symbol: "AAPL", Window: 2025-01-01 10:00:00
  Key: "AAPL" || 0x0000018D3B123000
  Value: 0x0000000000000096 || ... (count=150, sum=22500.0, etc.)
```

---

## Appendix B: Glossary

**LSM-Tree:** Log-Structured Merge Tree - write-optimized data structure with sorted SSTables

**SSTable:** Sorted String Table - immutable on-disk file with sorted key-value pairs

**Memtable:** In-memory write buffer, flushed to SSTable when full

**Sparse Index:** Index with entry for every Nth row (e.g., 8192 rows) instead of every row

**Bloom Filter:** Probabilistic data structure for fast negative lookups (definitely not present)

**Range Scan:** Query with start and end key bounds, returns all keys in range

**RAFT:** Consensus algorithm for distributed state machine replication

**Arrow:** Columnar in-memory data format for analytics

**Parquet:** Columnar on-disk file format (Arrow's persistent counterpart)

**io_uring:** Linux kernel async I/O interface (zero-copy, high performance)

**Coroutine:** Stackless function that can suspend and resume execution

**Asio:** Boost library for cross-platform async I/O and networking

---

**End of Requirements Document**
