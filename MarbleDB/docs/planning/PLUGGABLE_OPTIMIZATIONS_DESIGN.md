# MarbleDB Pluggable Optimization Architecture

**Author:** Claude (with Ben Gamble)
**Date:** 2025-11-04
**Status:** Design Phase
**Version:** 1.0

---

## Table of Contents

1. [Problem Statement](#problem-statement)
2. [Research Findings](#research-findings)
3. [Proposed Architecture](#proposed-architecture)
4. [API Design](#api-design)
5. [Implementation Strategy](#implementation-strategy)
6. [File Structure](#file-structure)
7. [Migration Path](#migration-path)
8. [Success Criteria](#success-criteria)
9. [Performance Expectations](#performance-expectations)

---

## 1. Problem Statement

### Current State

MarbleDB currently has **hardcoded optimizations** that apply globally to all tables regardless of their schema or workload:

```cpp
// Global settings in DBOptions (db.h lines 61-67)
bool enable_hot_key_cache = true;        // Applied to ALL tables
bool enable_negative_cache = true;       // Applied to ALL tables
bool enable_bloom_filter = true;         // Applied to ALL SSTables
```

### Issues

1. **Inefficient Resource Usage**
   - Time-series data pays bloom filter overhead despite only doing range scans
   - Analytical workloads waste memory on hot key caches (uniform access patterns)
   - Document search tables duplicate work (inverted index + bloom filter)

2. **Hardcoded for Specific Schemas**
   - Bloom filter code in `api.cpp` lines 797-808 is hardcoded for RDF triples
   - Expects 3 int64 columns (subject, predicate, object)
   - Doesn't work for generic key-value or other schemas

3. **Lack of Flexibility**
   - Can't configure optimizations per-table
   - Can't add new optimization strategies without modifying core code
   - No way to compose multiple strategies (e.g., bloom + cache + skipping index)

### Use Cases with Different Needs

| Use Case | Schema | Access Pattern | Optimal Optimizations |
|----------|--------|----------------|----------------------|
| **RDF Triple Store** | (S, P, O) × 3 permutations | Point lookups, joins | Bloom filters, predicate caching |
| **OLTP (Key-Value)** | (key, value) | Skewed point lookups | Hot/negative cache, bloom filters |
| **Time-Series** | (timestamp, metric_id, value) | Range scans | Skipping indexes, TTL |
| **Property Graph** | Vertices + Edges | Traversals, pattern matching | Adjacency cache, index selection |
| **Document Search** | (doc_id, text, metadata) | Full-text search | Inverted index (already has it) |

### Goal

Create a **pluggable optimization system** where each table can have its own optimizations based on:
- Schema type (detected automatically)
- Workload characteristics (hints from user)
- Enabled capabilities (from TableCapabilities)

---

## 2. Research Findings

### Existing Infrastructure

MarbleDB already has excellent foundation for pluggable optimizations:

#### A. TableCapabilities Struct
**File:** `include/marble/table_capabilities.h`

Already supports feature flags for:
- MVCC settings
- Temporal/bitemporal settings
- Full-text search
- TTL management
- **Hot key cache settings** (exists but not connected!)
- **Negative cache settings** (exists but not connected!)

```cpp
struct TableCapabilities {
    // MVCC
    bool enable_mvcc = false;
    MVCCSettings mvcc_settings;

    // Temporal
    bool enable_temporal = false;
    TemporalSettings temporal_settings;

    // Full-text search
    bool enable_full_text_search = false;
    FullTextSearchSettings fts_settings;

    // TTL
    bool enable_ttl = false;
    TTLSettings ttl_settings;

    // Hot key cache (NOT INTEGRATED!)
    bool enable_hot_key_cache = false;
    HotKeyCacheSettings hot_key_settings;

    // Negative cache (NOT INTEGRATED!)
    bool enable_negative_cache = false;
    NegativeCacheSettings negative_cache_settings;
};
```

**Finding:** Infrastructure exists, just needs to be connected to optimization system!

#### B. ColumnFamilyOptions with Capabilities
**File:** `include/marble/column_family.h` lines 32-64

```cpp
struct ColumnFamilyOptions {
    std::shared_ptr<arrow::Schema> schema;

    // Compaction, block settings, bloom filter settings...

    // KEY FINDING: Already has capabilities!
    TableCapabilities capabilities;  // Line 61
};
```

#### C. Strategy Pattern Usage
**Files:** `include/marble/compaction.h`, `include/marble/merge_operator.h`

MarbleDB already uses strategy pattern for:
- Compaction strategies (LeveledCompaction, SizeTieredCompaction)
- Merge operators (StringAppendOperator, Int64AddOperator)
- Wait strategies (BusySpinWaitStrategy, BlockingWaitStrategy)

**Finding:** Team is familiar with strategy pattern, fits existing codebase style.

#### D. Metadata Serialization
**File:** `include/marble/column_family.h` lines 207-218

```cpp
struct ColumnFamilyMetadata {
    uint32_t id;
    std::string name;
    std::string schema_json;      // Arrow schema serialized
    std::string options_json;      // Includes capabilities!
};
```

**Finding:** Serialization infrastructure already exists for persisting optimization config.

### Current Schema Types in MarbleDB

From codebase analysis:

1. **RDF Triple Schemas** (SabotQL)
   - 3 int64 columns: subject, predicate, object
   - 3 permuted indexes: SPO, POS, OSP
   - Bloom filters enabled, sparse index enabled
   - Use case: SPARQL queries, graph reasoning

2. **Classic Key-Value Schemas**
   - Flexible Arrow schemas
   - Use cases: OLTP, session stores, state backends

3. **Property Graph Schemas**
   - Vertex tables + Edge tables
   - Use cases: Cypher queries, graph analytics

4. **Time-Series Schemas**
   - Timestamp column + value columns
   - Use cases: Metrics, monitoring, IoT

5. **Document/Search Schemas**
   - Text columns with inverted indexes
   - Use cases: Log analysis, knowledge bases

---

## 3. Proposed Architecture

### Design Principles

1. **Strategy Pattern** for optimization implementations
2. **Factory Pattern** for instantiation and auto-configuration
3. **Pipeline Pattern** for composing multiple strategies
4. **Incremental Migration** - new system runs alongside old code initially

### Component Diagram

```
┌──────────────────────────────────────────────────────────┐
│                   ColumnFamilyOptions                     │
│  ┌────────────────────────────────────────────────────┐  │
│  │         OptimizationConfig                         │  │
│  │  - auto_configure: bool                            │  │
│  │  - enabled_strategies: vector<string>              │  │
│  │  - strategy_configs: map<string, config>           │  │
│  │  - workload_hints: WorkloadHints                   │  │
│  └────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────┘
                            ↓
          ┌─────────────────────────────────┐
          │   OptimizationFactory           │
          │  - CreateFromCapabilities()     │
          │  - DetectSchemaType()            │
          │  - Auto-configure strategies     │
          └─────────────────────────────────┘
                            ↓
          ┌─────────────────────────────────┐
          │   OptimizationPipeline          │
          │  - strategies_: vector          │
          │  - OnRead(), OnWrite()          │
          │  - OnCompaction(), OnFlush()    │
          └─────────────────────────────────┘
                            ↓
       ┌──────────────────────────────────────────┐
       │         OptimizationStrategy             │
       │  (abstract base class)                   │
       │  - OnRead(key, ReadContext*)             │
       │  - OnWrite(key, WriteContext*)           │
       │  - OnCompaction(CompactionContext*)      │
       │  - OnFlush(FlushContext*)                │
       │  - Serialize(), Deserialize()            │
       └──────────────────────────────────────────┘
                       ↑
       ┌───────────────┼───────────────┬──────────────┐
       │               │               │              │
┌──────────────┐ ┌─────────────┐ ┌──────────┐ ┌──────────────┐
│BloomFilter   │ │Cache        │ │Skipping  │ │TripleStore   │
│Strategy      │ │Strategy     │ │Index     │ │Strategy      │
│              │ │             │ │Strategy  │ │              │
│- Block bloom │ │- Hot cache  │ │- Min/max │ │- Predicate   │
│- Table bloom │ │- Negative   │ │- Time    │ │  caching     │
│              │ │  cache      │ │  buckets │ │- Join opts   │
└──────────────┘ └─────────────┘ └──────────┘ └──────────────┘
```

### Lifecycle Flow

```
1. CreateColumnFamily(options)
   ↓
2. Factory::CreateFromCapabilities(options.capabilities, schema, hints)
   ↓
3. Detect schema type (RDF triple? Key-value? Time-series?)
   ↓
4. Select strategies based on:
   - Schema type
   - Enabled capabilities
   - Workload hints
   ↓
5. Create OptimizationPipeline with selected strategies
   ↓
6. Attach pipeline to ColumnFamilyHandle
   ↓
7. On operations (Get/Put/Scan/Compact):
   - Call pipeline->OnRead/OnWrite/OnCompaction/OnFlush
   - Each strategy applies its optimization
   - Strategies can short-circuit (e.g., bloom filter says "not found")
```

---

## 4. API Design

### 4.1 Base OptimizationStrategy Interface

```cpp
// include/marble/optimization_strategy.h

namespace marble {

/**
 * @brief Context passed to OnRead() hook
 */
struct ReadContext {
    const Key& key;
    const ReadOptions& options;

    // Optimization hints
    bool is_point_lookup = true;       // vs range scan
    bool skip_bloom_filter = false;     // Strategy can set this
    bool skip_cache = false;            // Strategy can set this
    bool definitely_not_found = false;  // Short-circuit flag
};

/**
 * @brief Context passed to OnWrite() hook
 */
struct WriteContext {
    const Key& key;
    const Record& record;
    const WriteOptions& options;

    // Metadata for optimizations
    uint64_t batch_id = 0;
    uint32_t row_offset = 0;
};

/**
 * @brief Base class for pluggable optimizations
 */
class OptimizationStrategy {
public:
    virtual ~OptimizationStrategy() = default;

    /**
     * @brief Called when table is created
     */
    virtual Status OnTableCreate(const TableCapabilities& caps) = 0;

    /**
     * @brief Called before a read operation
     *
     * Strategies can:
     * - Check bloom filters (set definitely_not_found = true to short-circuit)
     * - Lookup caches (populate result if found)
     * - Update access statistics
     *
     * @return OK if read can proceed, NotFound to short-circuit
     */
    virtual Status OnRead(ReadContext* ctx) = 0;

    /**
     * @brief Called after a successful read
     *
     * Strategies can:
     * - Update caches
     * - Record access patterns
     */
    virtual void OnReadComplete(const Key& key, const Record& record) = 0;

    /**
     * @brief Called before a write operation
     *
     * Strategies can:
     * - Update bloom filters
     * - Invalidate caches
     */
    virtual Status OnWrite(WriteContext* ctx) = 0;

    /**
     * @brief Called during compaction
     *
     * Strategies can:
     * - Merge bloom filters
     * - Update skipping indexes
     */
    virtual Status OnCompaction(CompactionContext* ctx) = 0;

    /**
     * @brief Called during memtable flush
     */
    virtual Status OnFlush(FlushContext* ctx) = 0;

    /**
     * @brief Get memory usage of this strategy
     */
    virtual size_t MemoryUsage() const = 0;

    /**
     * @brief Clear all cached data
     */
    virtual void Clear() = 0;

    /**
     * @brief Serialize strategy state for persistence
     */
    virtual std::vector<uint8_t> Serialize() const = 0;

    /**
     * @brief Deserialize strategy state
     */
    virtual Status Deserialize(const std::vector<uint8_t>& data) = 0;

    /**
     * @brief Get strategy name for identification
     */
    virtual std::string Name() const = 0;
};

/**
 * @brief Pipeline for composing multiple optimization strategies
 */
class OptimizationPipeline {
public:
    OptimizationPipeline() = default;

    /**
     * @brief Add a strategy to the pipeline
     *
     * Strategies are executed in order added
     */
    void AddStrategy(std::unique_ptr<OptimizationStrategy> strategy);

    /**
     * @brief Remove a strategy by name
     */
    void RemoveStrategy(const std::string& name);

    /**
     * @brief Get strategy by name
     */
    OptimizationStrategy* GetStrategy(const std::string& name);

    /**
     * @brief Apply all strategies to a read operation
     *
     * Strategies are called in order. If any strategy returns NotFound,
     * the pipeline short-circuits and returns NotFound.
     */
    Status OnRead(ReadContext* ctx);

    /**
     * @brief Notify all strategies of successful read
     */
    void OnReadComplete(const Key& key, const Record& record);

    /**
     * @brief Apply all strategies to a write operation
     */
    Status OnWrite(WriteContext* ctx);

    /**
     * @brief Apply all strategies to compaction
     */
    Status OnCompaction(CompactionContext* ctx);

    /**
     * @brief Apply all strategies to flush
     */
    Status OnFlush(FlushContext* ctx);

    /**
     * @brief Get total memory usage of all strategies
     */
    size_t MemoryUsage() const;

    /**
     * @brief Clear all strategies
     */
    void Clear();

    /**
     * @brief Serialize all strategies
     */
    std::vector<uint8_t> Serialize() const;

    /**
     * @brief Deserialize and restore strategies
     */
    Status Deserialize(const std::vector<uint8_t>& data);

private:
    std::vector<std::unique_ptr<OptimizationStrategy>> strategies_;
    std::unordered_map<std::string, OptimizationStrategy*> strategy_map_;
};

} // namespace marble
```

### 4.2 OptimizationFactory

```cpp
// include/marble/optimization_factory.h

namespace marble {

/**
 * @brief Detected schema type for auto-configuration
 */
enum class SchemaType {
    kUnknown,
    kRDFTriple,        // 3 int64 columns (S, P, O)
    kKeyValue,         // 2 columns (key, value)
    kTimeSeries,       // Timestamp column + values
    kPropertyGraph,    // Vertex/edge schema
    kDocument          // Text columns
};

/**
 * @brief Workload hints for optimization selection
 */
struct WorkloadHints {
    enum class AccessPattern {
        kPointLookups,      // Mostly Get() operations
        kRangeScans,        // Mostly range iterators
        kMixed,             // Both point and range
        kFullScans          // Mostly full table scans
    };

    enum class UpdatePattern {
        kWriteHeavy,        // Mostly writes
        kReadHeavy,         // Mostly reads
        kBalanced           // Mixed read/write
    };

    AccessPattern access = AccessPattern::kMixed;
    UpdatePattern update = UpdatePattern::kBalanced;

    // Skewness: 0.0 = uniform, 1.0 = highly skewed (Zipfian)
    double key_skewness = 0.5;

    // Selectivity: 0.0 = very selective queries, 1.0 = full scans
    double query_selectivity = 0.1;
};

/**
 * @brief Factory for creating optimization strategies
 */
class OptimizationFactory {
public:
    /**
     * @brief Create optimization pipeline from capabilities and schema
     *
     * Automatically selects appropriate strategies based on:
     * - Detected schema type
     * - Enabled capabilities
     * - Workload hints
     *
     * @param caps TableCapabilities (MVCC, temporal, TTL, etc.)
     * @param schema Arrow schema for detecting type
     * @param hints Workload characteristics for tuning
     * @return Configured OptimizationPipeline
     */
    static std::unique_ptr<OptimizationPipeline>
    CreateFromCapabilities(
        const TableCapabilities& caps,
        const std::shared_ptr<arrow::Schema>& schema,
        const WorkloadHints& hints = WorkloadHints()
    );

    /**
     * @brief Detect schema type from Arrow schema
     *
     * Heuristics:
     * - 3 int64 columns → kRDFTriple
     * - 2 columns (any types) → kKeyValue
     * - Timestamp column present → kTimeSeries
     * - Complex nested schema → kDocument or kPropertyGraph
     */
    static SchemaType DetectSchemaType(const std::shared_ptr<arrow::Schema>& schema);

    /**
     * @brief Create specific optimization strategy
     */
    static std::unique_ptr<OptimizationStrategy>
    CreateBloomFilterStrategy(const BloomFilterStrategy::Config& config);

    static std::unique_ptr<OptimizationStrategy>
    CreateCacheStrategy(const CacheStrategy::Config& config);

    static std::unique_ptr<OptimizationStrategy>
    CreateSkippingIndexStrategy(const SkippingIndexStrategy::Config& config);

    static std::unique_ptr<OptimizationStrategy>
    CreateTripleStoreStrategy(const TripleStoreStrategy::Config& config);

private:
    // Auto-configuration logic
    static void ConfigureForRDFTriple(
        OptimizationPipeline* pipeline,
        const TableCapabilities& caps,
        const WorkloadHints& hints
    );

    static void ConfigureForKeyValue(
        OptimizationPipeline* pipeline,
        const TableCapabilities& caps,
        const WorkloadHints& hints
    );

    static void ConfigureForTimeSeries(
        OptimizationPipeline* pipeline,
        const TableCapabilities& caps,
        const WorkloadHints& hints
    );
};

} // namespace marble
```

### 4.3 Strategy Configurations

```cpp
// include/marble/optimizations/bloom_filter_strategy.h

namespace marble {

/**
 * @brief Bloom filter optimization for point lookups
 *
 * Suitable for:
 * - RDF triple stores (exact match lookups)
 * - Key-value stores (point queries)
 * - Property graphs (vertex/edge lookups)
 *
 * Not suitable for:
 * - Time-series (range queries dominate)
 * - Full scans (no point lookups)
 */
class BloomFilterStrategy : public OptimizationStrategy {
public:
    struct Config {
        bool enable_block_level = true;   // Bloom per block
        bool enable_table_level = false;  // Bloom for entire SSTable
        size_t bits_per_key = 10;         // Space/accuracy tradeoff
        double false_positive_rate = 0.01; // 1% FP rate
    };

    explicit BloomFilterStrategy(const Config& config);

    // OptimizationStrategy interface
    Status OnTableCreate(const TableCapabilities& caps) override;
    Status OnRead(ReadContext* ctx) override;
    void OnReadComplete(const Key& key, const Record& record) override;
    Status OnWrite(WriteContext* ctx) override;
    Status OnCompaction(CompactionContext* ctx) override;
    Status OnFlush(FlushContext* ctx) override;

    size_t MemoryUsage() const override;
    void Clear() override;
    std::vector<uint8_t> Serialize() const override;
    Status Deserialize(const std::vector<uint8_t>& data) override;
    std::string Name() const override { return "bloom_filter"; }

private:
    Config config_;
    std::vector<std::shared_ptr<BloomFilter>> block_blooms_;
    std::shared_ptr<BloomFilter> table_bloom_;

    // Statistics
    std::atomic<uint64_t> bloom_checks_{0};
    std::atomic<uint64_t> bloom_hits_{0};
    std::atomic<uint64_t> false_positives_{0};
};

} // namespace marble
```

```cpp
// include/marble/optimizations/cache_strategy.h

namespace marble {

/**
 * @brief Hot/negative cache optimization for skewed workloads
 *
 * Suitable for:
 * - OLTP workloads (skewed access patterns)
 * - Session stores (hot user data)
 * - RDF lookups (frequent predicates)
 *
 * Not suitable for:
 * - Uniform access patterns
 * - Sequential scans
 */
class CacheStrategy : public OptimizationStrategy {
public:
    struct Config {
        bool enable_hot_cache = true;
        bool enable_negative_cache = true;

        // Hot cache settings
        size_t hot_cache_size_mb = 64;
        uint32_t promotion_threshold = 3;  // Accesses to promote

        // Negative cache settings
        size_t negative_cache_entries = 10000;
    };

    explicit CacheStrategy(const Config& config);

    // OptimizationStrategy interface
    Status OnTableCreate(const TableCapabilities& caps) override;
    Status OnRead(ReadContext* ctx) override;
    void OnReadComplete(const Key& key, const Record& record) override;
    Status OnWrite(WriteContext* ctx) override;
    Status OnCompaction(CompactionContext* ctx) override;
    Status OnFlush(FlushContext* ctx) override;

    size_t MemoryUsage() const override;
    void Clear() override;
    std::vector<uint8_t> Serialize() const override;
    Status Deserialize(const std::vector<uint8_t>& data) override;
    std::string Name() const override { return "cache"; }

private:
    Config config_;
    std::unique_ptr<HotKeyCache> hot_cache_;
    std::unique_ptr<NegativeCache> negative_cache_;

    // Access tracking for adaptive promotion
    std::unique_ptr<AccessTracker> access_tracker_;
};

} // namespace marble
```

```cpp
// include/marble/optimizations/skipping_index_strategy.h

namespace marble {

/**
 * @brief Skipping index optimization for analytical queries
 *
 * Suitable for:
 * - Time-series data (range queries, aggregations)
 * - Analytical workloads (OLAP scans)
 * - Log data (time-based queries)
 *
 * Not suitable for:
 * - Small point queries
 * - Random access patterns
 */
class SkippingIndexStrategy : public OptimizationStrategy {
public:
    enum class IndexType {
        kGeneric,        // Min/max/count for all columns
        kTimeSeries,     // Time bucket statistics
        kGranular        // Multi-level granularity
    };

    struct Config {
        IndexType type = IndexType::kGeneric;
        size_t block_size_rows = 8192;
        size_t granule_size_rows = 1024;
        std::vector<std::string> indexed_columns;  // Empty = all columns
    };

    explicit SkippingIndexStrategy(const Config& config);

    // OptimizationStrategy interface
    Status OnTableCreate(const TableCapabilities& caps) override;
    Status OnRead(ReadContext* ctx) override;
    void OnReadComplete(const Key& key, const Record& record) override;
    Status OnWrite(WriteContext* ctx) override;
    Status OnCompaction(CompactionContext* ctx) override;
    Status OnFlush(FlushContext* ctx) override;

    size_t MemoryUsage() const override;
    void Clear() override;
    std::vector<uint8_t> Serialize() const override;
    Status Deserialize(const std::vector<uint8_t>& data) override;
    std::string Name() const override { return "skipping_index"; }

private:
    Config config_;
    std::unique_ptr<SkippingIndex> index_;

    // Statistics
    std::atomic<uint64_t> blocks_scanned_{0};
    std::atomic<uint64_t> blocks_skipped_{0};
};

} // namespace marble
```

```cpp
// include/marble/optimizations/triple_store_strategy.h

namespace marble {

/**
 * @brief RDF triple store specific optimizations
 *
 * Suitable for:
 * - RDF triple stores with permuted indexes (SPO, POS, OSP)
 * - SPARQL query workloads
 * - Knowledge graphs
 */
class TripleStoreStrategy : public OptimizationStrategy {
public:
    enum class IndexOrder {
        kSPO,  // Subject-Predicate-Object
        kPOS,  // Predicate-Object-Subject
        kOSP   // Object-Subject-Predicate
    };

    struct Config {
        IndexOrder order;

        // Predicate-aware caching
        bool cache_predicates = true;
        size_t predicate_cache_size = 1000;

        // Separate bloom per predicate
        bool bloom_per_predicate = false;

        // Join optimization
        bool enable_join_hints = true;
    };

    explicit TripleStoreStrategy(const Config& config);

    // OptimizationStrategy interface
    Status OnTableCreate(const TableCapabilities& caps) override;
    Status OnRead(ReadContext* ctx) override;
    void OnReadComplete(const Key& key, const Record& record) override;
    Status OnWrite(WriteContext* ctx) override;
    Status OnCompaction(CompactionContext* ctx) override;
    Status OnFlush(FlushContext* ctx) override;

    size_t MemoryUsage() const override;
    void Clear() override;
    std::vector<uint8_t> Serialize() const override;
    Status Deserialize(const std::vector<uint8_t>& data) override;
    std::string Name() const override { return "triple_store"; }

private:
    Config config_;

    // Predicate-aware bloom filters
    std::unordered_map<int64_t, std::shared_ptr<BloomFilter>> predicate_blooms_;

    // Frequent predicate cache
    std::unordered_map<int64_t, std::vector<std::pair<int64_t, int64_t>>> predicate_cache_;

    // Join selectivity statistics
    struct JoinStats {
        int64_t predicate;
        double selectivity;  // 0.0 = very selective, 1.0 = returns everything
    };
    std::unordered_map<int64_t, JoinStats> join_stats_;
};

} // namespace marble
```

### 4.4 Integration with ColumnFamilyOptions

```cpp
// Extend ColumnFamilyOptions in include/marble/column_family.h

struct ColumnFamilyOptions {
    // ... existing fields ...
    std::shared_ptr<arrow::Schema> schema;
    TableCapabilities capabilities;

    // NEW: Optimization configuration
    struct OptimizationConfig {
        /**
         * @brief Auto-configure from capabilities and schema?
         *
         * If true, OptimizationFactory will:
         * 1. Detect schema type
         * 2. Select appropriate strategies
         * 3. Configure based on workload hints
         *
         * If false, use explicit enabled_strategies list.
         */
        bool auto_configure = true;

        /**
         * @brief Explicitly enabled strategies (overrides auto)
         *
         * Example: {"bloom_filter", "cache", "skipping_index"}
         */
        std::vector<std::string> enabled_strategies;

        /**
         * @brief Configuration for specific strategies
         *
         * Example:
         * strategy_configs["bloom_filter"] = "{\"bits_per_key\": 10}"
         * strategy_configs["cache"] = "{\"hot_cache_size_mb\": 128}"
         */
        std::unordered_map<std::string, std::string> strategy_configs;

        /**
         * @brief Workload hints for auto-configuration
         */
        WorkloadHints workload_hints;
    } optimization_config;
};
```

---

## 5. Implementation Strategy

### Incremental Migration Approach

We will **NOT** do a big-bang rewrite. Instead:

1. **Build new system alongside old code**
   - New OptimizationStrategy framework coexists with existing optimizations
   - Both systems run in parallel initially
   - Can A/B test performance

2. **Per-table opt-in**
   - Tables can opt into new system via `optimization_config.auto_configure = true`
   - Existing tables continue using old global settings
   - Gradual migration table-by-table

3. **Feature parity first**
   - Implement BloomFilterStrategy that matches existing bloom filter behavior
   - Implement CacheStrategy that matches existing cache behavior
   - Verify identical performance before migration

4. **Deprecation timeline**
   - Phase 1: New system available, opt-in
   - Phase 2: New system default for new tables, old tables unchanged
   - Phase 3: Migrate all tables to new system
   - Phase 4: Remove old optimization code

### Dual Code Path Example

```cpp
// In Get() method (api.cpp)

Status Get(const ReadOptions& options, const Key& key, Record** record) {
    auto* cf_info = GetColumnFamily("default");

    // NEW SYSTEM (if enabled)
    if (cf_info->optimizations_) {
        ReadContext ctx{key, options};
        auto status = cf_info->optimizations_->OnRead(&ctx);
        if (status.IsNotFound()) {
            return Status::NotFound("Key not found (via optimization)");
        }
        // ... continue with read
    }

    // OLD SYSTEM (existing code, runs in parallel)
    if (cf_info->bloom_filter) {
        // ... existing bloom filter check
    }
    if (cf_info->hot_key_cache) {
        // ... existing cache check
    }

    // Actual read from LSM tree
    // ...
}
```

### Testing During Migration

```cpp
// Benchmark comparison
void BenchmarkOptimizations() {
    // Test 1: Old system only
    ColumnFamilyOptions old_opts;
    old_opts.enable_bloom_filter = true;
    // ... run benchmark, measure perf

    // Test 2: New system only
    ColumnFamilyOptions new_opts;
    new_opts.optimization_config.auto_configure = true;
    // ... run benchmark, measure perf

    // Compare results
    ASSERT_GE(new_perf, old_perf * 0.95);  // Within 5% is acceptable
}
```

---

## 6. File Structure

### New Files to Create

```
MarbleDB/
├── include/marble/
│   ├── optimization_strategy.h          ← Base interface + Pipeline
│   ├── optimization_factory.h           ← Factory + WorkloadHints
│   └── optimizations/                   ← NEW DIRECTORY
│       ├── bloom_filter_strategy.h
│       ├── cache_strategy.h
│       ├── skipping_index_strategy.h
│       └── triple_store_strategy.h
│
├── src/core/
│   ├── optimization_strategy.cpp        ← Base implementations
│   ├── optimization_factory.cpp         ← Factory logic
│   └── optimizations/                   ← NEW DIRECTORY
│       ├── bloom_filter_strategy.cpp
│       ├── cache_strategy.cpp
│       ├── skipping_index_strategy.cpp
│       └── triple_store_strategy.cpp
│
├── tests/unit/
│   ├── test_optimization_strategies.cpp ← Unit tests
│   └── test_optimization_factory.cpp    ← Factory tests
│
├── tests/integration/
│   └── test_cf_optimizations.cpp        ← Integration tests
│
├── benchmarks/
│   └── optimization_strategy_bench.cpp  ← Performance benchmarks
│
└── docs/
    ├── planning/
    │   ├── PLUGGABLE_OPTIMIZATIONS_DESIGN.md     ← THIS FILE
    │   └── OPTIMIZATION_REFACTOR_ROADMAP.md      ← Implementation plan
    ├── PLUGGABLE_OPTIMIZATIONS.md                ← User guide
    └── OPTIMIZATION_TUNING_GUIDE.md              ← Tuning guide
```

### Files to Modify

```
include/marble/column_family.h
  - Add OptimizationConfig to ColumnFamilyOptions
  - Add optimizations_ member to ColumnFamilyHandle

src/core/column_family.cpp
  - Initialize optimization pipeline on CreateColumnFamily()
  - Call optimization hooks in read/write paths

include/marble/db.h
  - Document migration from global settings to per-CF config

src/core/api.cpp
  - Integrate optimization pipeline into Get()/Put()/Delete()
  - Pass ReadContext/WriteContext to strategies

src/core/sstable.cpp
  - Serialize optimization metadata with SSTables
  - Call OnFlush() during memtable flush

src/core/lsm_storage.cpp
  - Call OnCompaction() during compaction

CMakeLists.txt
  - Add new source files
  - Update target dependencies
```

---

## 7. Migration Path

### Phase 0: Documentation (CURRENT)
- Create design docs
- Get review and approval
- Update PROJECT_MAP.md

### Phase 1: Base Infrastructure (Days 2-3)
**Goal:** Build foundation without breaking existing code

**Tasks:**
1. Create `optimization_strategy.h|.cpp` with base interfaces
2. Create `optimization_factory.h|.cpp` with factory skeleton
3. Add `OptimizationConfig` to `ColumnFamilyOptions`
4. Add `optimizations_` member to `ColumnFamilyHandle`
5. Unit tests for base framework

**Success Criteria:**
- All existing tests still pass
- New interfaces compile
- No performance regression

### Phase 2: Strategy Implementations (Days 4-6)
**Goal:** Implement all 4 core strategies

**Tasks:**
1. Implement `BloomFilterStrategy`
   - Reuse existing `BloomFilter` class
   - Match existing bloom filter behavior
   - Unit tests

2. Implement `CacheStrategy`
   - Reuse existing `HotKeyCache` and `NegativeCache`
   - Match existing cache behavior
   - Unit tests

3. Implement `SkippingIndexStrategy`
   - Reuse existing `SkippingIndex`
   - Support time-series optimizations
   - Unit tests

4. Implement `TripleStoreStrategy`
   - RDF-specific optimizations
   - Predicate-aware bloom filters
   - Unit tests

**Success Criteria:**
- Each strategy passes unit tests
- Memory usage within budget
- Serialization works correctly

### Phase 3: Auto-Configuration (Days 7-8)
**Goal:** Smart defaults for common schemas

**Tasks:**
1. Implement schema type detection
2. Implement `CreateFromCapabilities()`
3. Auto-config logic for each schema type:
   - RDF triple → BloomFilter + TripleStore
   - Key-value + skewed → Cache + BloomFilter
   - Time-series → SkippingIndex
4. Integration tests with auto-config

**Success Criteria:**
- Correct strategy selection for known schemas
- Workload hints affect strategy configuration
- Manual override works

### Phase 4: Integration (Days 9-11)
**Goal:** Connect optimizations to read/write paths

**Tasks:**
1. Modify `Get()` to call `OnRead()` hook
2. Modify `Put()` to call `OnWrite()` hook
3. Modify `Flush()` to call `OnFlush()` hook
4. Modify `Compact()` to call `OnCompaction()` hook
5. Run both old and new systems in parallel
6. Log performance metrics for comparison

**Success Criteria:**
- Dual code paths work correctly
- No crashes or data corruption
- Performance parity with old system

### Phase 5: Validation (Days 12-13)
**Goal:** Prove new system is better

**Tasks:**
1. Run all existing tests with new system enabled
2. Run benchmarks comparing old vs new
3. Validate auto-configuration on real workloads
4. Memory profiling
5. Performance regression testing

**Success Criteria:**
- All tests pass
- Performance ≥ old system (or within 5%)
- Memory usage reasonable
- Auto-config makes good choices

### Phase 6: Migration (Days 14+)
**Goal:** Migrate all tables to new system

**Tasks:**
1. Create migration guide
2. Migrate critical tables first (with monitoring)
3. Gather performance data
4. Migrate remaining tables
5. Mark old optimization code as deprecated
6. Remove old code (future milestone)

**Success Criteria:**
- All tables using new system
- No performance regressions
- Users understand how to configure optimizations

### Rollback Plan

If issues arise during migration:

1. **Per-table rollback**
   - Set `optimization_config.auto_configure = false`
   - Table reverts to old global settings

2. **Global rollback**
   - Add feature flag in `DBOptions`
   - `bool use_legacy_optimizations = false;`
   - Can disable new system entirely

3. **Data safety**
   - Optimization metadata stored in SSTable footer
   - If new system fails, can still read data with old system
   - No data loss possible

---

## 8. Success Criteria

### Functional Requirements

✅ **Feature Parity**
- All existing optimizations available as strategies
- No loss of functionality

✅ **Flexibility**
- Can enable/disable optimizations per-table
- Can configure strategy parameters
- Can add new strategies without modifying core code

✅ **Auto-Configuration**
- Detects schema type correctly (RDF, key-value, time-series)
- Selects appropriate strategies automatically
- Respects workload hints

✅ **Backward Compatibility**
- Existing tables continue to work
- Old global settings still respected (during transition)
- Migration path is clear

### Performance Requirements

✅ **No Regressions**
- Performance ≥ old system on all workloads
- Acceptable: Within 5% for edge cases
- Target: 5-50% improvement on optimized workloads

✅ **Memory Efficiency**
- Pay only for enabled optimizations
- Memory usage proportional to workload needs
- No global overhead for unused features

✅ **Scalability**
- Optimization overhead O(1) per operation
- Pipeline execution < 1μs per operation
- Memory overhead < 10% of data size

### Code Quality Requirements

✅ **Maintainability**
- Clear separation of concerns
- Easy to add new strategies
- Well-documented interfaces

✅ **Testability**
- Each strategy independently testable
- Integration tests for realistic workloads
- Performance benchmarks for validation

✅ **Documentation**
- Architecture guide
- API documentation
- Tuning guide
- Migration guide

---

## 9. Performance Expectations

### Expected Improvements

Based on workload characteristics:

#### RDF Triple Store
**Current:** Global bloom filter on 3-column triples
**New:** TripleStoreStrategy + BloomFilterStrategy

**Expected Improvements:**
- **Predicate-selective queries:** 2-5x faster
  - Per-predicate bloom filters reduce false positives
  - Predicate caching eliminates repeated joins

- **Join-heavy queries:** 3-10x faster
  - Join selectivity hints enable better query planning
  - Frequent predicate patterns cached

- **Memory:** 20-30% reduction
  - Only cache hot predicates, not all triples
  - Bloom filters sized per-predicate workload

**Validation:** Run SPARQL benchmark suite

#### OLTP Key-Value
**Current:** Global hot key cache + negative cache
**New:** CacheStrategy with adaptive promotion

**Expected Improvements:**
- **Hot key access:** 10-50x faster
  - Adaptive promotion tracks real access patterns
  - LRU eviction keeps hottest keys

- **Negative queries:** 100-1000x faster
  - Negative cache prevents repeated LSM tree probes
  - Bloom filters eliminate disk I/O

- **Memory:** 40-60% reduction
  - Cache only truly hot keys (not warmup noise)
  - Promotion threshold filters cold keys

**Validation:** Run YCSB benchmark with Zipfian distribution

#### Time-Series Analytics
**Current:** No skipping index, scans entire dataset
**New:** SkippingIndexStrategy with time buckets

**Expected Improvements:**
- **Range queries:** 100-1000x faster
  - Min/max statistics skip irrelevant blocks
  - Time bucket pruning eliminates entire SSTables

- **Aggregations:** 50-500x faster
  - Count/sum pre-computed per block
  - No need to read actual data for many queries

- **Memory:** Minimal overhead (< 1% of data size)
  - Statistics stored with SSTable metadata
  - No in-memory caching needed

**Validation:** Run ClickBench queries on time-series data

### Measurement Plan

For each workload:

1. **Baseline Measurement (Old System)**
   ```bash
   ./benchmark --workload=rdf_triple --old-system
   # Record: throughput, latency (p50/p99), memory usage
   ```

2. **New System Measurement**
   ```bash
   ./benchmark --workload=rdf_triple --new-system
   # Record: throughput, latency (p50/p99), memory usage
   ```

3. **Comparison**
   ```
   Improvement = (new_throughput - old_throughput) / old_throughput
   ```

4. **Acceptance Criteria**
   - Improvement ≥ 0% (no regression)
   - Target: Improvement ≥ 50% for optimized workloads
   - Memory overhead ≤ 20%

---

## Appendix A: Auto-Configuration Logic

### Schema Type Detection

```cpp
SchemaType DetectSchemaType(const arrow::Schema& schema) {
    // RDF Triple: 3 int64 columns
    if (schema->num_fields() == 3 &&
        schema->field(0)->type()->id() == arrow::Type::INT64 &&
        schema->field(1)->type()->id() == arrow::Type::INT64 &&
        schema->field(2)->type()->id() == arrow::Type::INT64) {

        // Check column names
        auto name0 = schema->field(0)->name();
        auto name1 = schema->field(1)->name();
        auto name2 = schema->field(2)->name();

        if ((name0 == "subject" && name1 == "predicate" && name2 == "object") ||
            (name0 == "predicate" && name1 == "object" && name2 == "subject") ||
            (name0 == "object" && name1 == "subject" && name2 == "predicate")) {
            return SchemaType::kRDFTriple;
        }
    }

    // Key-Value: 2 columns
    if (schema->num_fields() == 2) {
        return SchemaType::kKeyValue;
    }

    // Time-Series: Has timestamp column
    for (int i = 0; i < schema->num_fields(); ++i) {
        auto type = schema->field(i)->type()->id();
        if (type == arrow::Type::TIMESTAMP) {
            return SchemaType::kTimeSeries;
        }
    }

    // Property Graph: Complex nested schema
    if (schema->num_fields() > 3) {
        for (int i = 0; i < schema->num_fields(); ++i) {
            auto type = schema->field(i)->type()->id();
            if (type == arrow::Type::LIST || type == arrow::Type::STRUCT) {
                return SchemaType::kPropertyGraph;
            }
        }
    }

    return SchemaType::kUnknown;
}
```

### Strategy Selection Logic

```cpp
void ConfigureForRDFTriple(OptimizationPipeline* pipeline,
                          const TableCapabilities& caps,
                          const WorkloadHints& hints) {
    // Always add bloom filter for RDF (exact match queries)
    BloomFilterStrategy::Config bloom_config;
    bloom_config.enable_block_level = true;
    bloom_config.bits_per_key = 10;
    pipeline->AddStrategy(CreateBloomFilterStrategy(bloom_config));

    // Add RDF-specific optimizations
    TripleStoreStrategy::Config triple_config;
    triple_config.cache_predicates = true;
    triple_config.bloom_per_predicate = (hints.key_skewness > 0.7);
    pipeline->AddStrategy(CreateTripleStoreStrategy(triple_config));

    // Add cache if workload is skewed
    if (hints.key_skewness > 0.6) {
        CacheStrategy::Config cache_config;
        cache_config.enable_hot_cache = true;
        cache_config.hot_cache_size_mb = 64;
        pipeline->AddStrategy(CreateCacheStrategy(cache_config));
    }
}

void ConfigureForTimeSeries(OptimizationPipeline* pipeline,
                           const TableCapabilities& caps,
                           const WorkloadHints& hints) {
    // Always add skipping index for time-series
    SkippingIndexStrategy::Config skip_config;
    skip_config.type = SkippingIndexStrategy::IndexType::kTimeSeries;
    skip_config.block_size_rows = 8192;
    pipeline->AddStrategy(CreateSkippingIndexStrategy(skip_config));

    // No bloom filter (range queries don't benefit)
    // No cache (sequential access patterns)
}
```

---

## Appendix B: Usage Examples

### Example 1: RDF Triple Store (Manual Config)

```cpp
// Create SPO index
ColumnFamilyOptions spo_options;
spo_options.schema = arrow::schema({
    arrow::field("subject", arrow::int64()),
    arrow::field("predicate", arrow::int64()),
    arrow::field("object", arrow::int64())
});

// Manual configuration
spo_options.optimization_config.auto_configure = false;
spo_options.optimization_config.enabled_strategies = {
    "bloom_filter",
    "triple_store",
    "cache"
};

// Configure bloom filter
spo_options.optimization_config.strategy_configs["bloom_filter"] = R"({
    "enable_block_level": true,
    "bits_per_key": 10
})";

// Configure triple store
spo_options.optimization_config.strategy_configs["triple_store"] = R"({
    "order": "SPO",
    "cache_predicates": true,
    "bloom_per_predicate": false
})";

// Create column family
auto status = db->CreateColumnFamily(spo_options, "SPO", &spo_handle);
```

### Example 2: Time-Series (Auto-Config)

```cpp
// Create time-series table
ColumnFamilyOptions ts_options;
ts_options.schema = arrow::schema({
    arrow::field("timestamp", arrow::timestamp(arrow::TimeUnit::MILLI)),
    arrow::field("metric_id", arrow::int64()),
    arrow::field("value", arrow::float64())
});

// Auto-configure (will detect time-series schema)
ts_options.optimization_config.auto_configure = true;

// Provide workload hints
ts_options.optimization_config.workload_hints.access =
    WorkloadHints::AccessPattern::kRangeScans;
ts_options.optimization_config.workload_hints.query_selectivity = 0.01;  // Very selective

// Enable TTL
ts_options.capabilities.enable_ttl = true;
ts_options.capabilities.ttl_settings.ttl_duration_ms = 86400000;  // 24h

// Create column family (will auto-select SkippingIndexStrategy)
auto status = db->CreateColumnFamily(ts_options, "metrics", &metrics_handle);
```

### Example 3: OLTP Key-Value (Auto-Config with Hints)

```cpp
// Create OLTP table
ColumnFamilyOptions kv_options;
kv_options.schema = arrow::schema({
    arrow::field("key", arrow::uint64()),
    arrow::field("value", arrow::binary())
});

// Auto-configure with workload hints
kv_options.optimization_config.auto_configure = true;
kv_options.optimization_config.workload_hints.access =
    WorkloadHints::AccessPattern::kPointLookups;
kv_options.optimization_config.workload_hints.key_skewness = 0.9;  // Highly skewed!

// Enable MVCC
kv_options.capabilities.enable_mvcc = true;

// Create column family (will auto-select CacheStrategy + BloomFilterStrategy)
auto status = db->CreateColumnFamily(kv_options, "sessions", &sessions_handle);
```

---

**END OF DESIGN DOCUMENT**

This design provides a comprehensive blueprint for implementing pluggable optimizations in MarbleDB. The next step is to create the implementation roadmap (OPTIMIZATION_REFACTOR_ROADMAP.md) with detailed phase breakdowns and timelines.
