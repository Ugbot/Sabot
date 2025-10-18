# MarbleDB Flexible Table Capabilities

**Design Document for Per-Table/Column-Family Feature Configuration**

**Date:** October 18, 2025
**Status:** Design Phase
**Goal:** Enable different tables/column families to have different capabilities (MVCC, temporal, search, etc.)

---

## Executive Summary

MarbleDB currently has a monolithic architecture where all features apply to all tables. This design proposes a **flexible capability system** where each table or column family can opt-in to specific features based on their use case.

**Key Principle:** Not every table needs every feature. Let users choose trade-offs per table.

**Example Use Cases:**
- **Transactions table**: MVCC enabled, no temporal, no search
- **Audit log table**: Bitemporal enabled, MVCC disabled, no search
- **Documentation table**: Full-text search enabled, no MVCC, no temporal
- **Hot cache table**: All features disabled for maximum throughput

---

## Current State

### Existing ColumnFamilyOptions (from `include/marble/column_family.h`)

```cpp
struct ColumnFamilyOptions {
    std::shared_ptr<arrow::Schema> schema;
    int primary_key_index = 0;
    std::shared_ptr<MergeOperator> merge_operator;

    // Compaction settings
    size_t target_file_size = 64 * 1024 * 1024;
    size_t max_bytes_for_level_base = 256 * 1024 * 1024;
    int max_levels = 7;

    // Bloom filter settings
    bool enable_bloom_filter = true;
    size_t bloom_filter_bits_per_key = 10;

    // Block settings
    size_t block_size = 8192;
    bool enable_sparse_index = true;
    size_t index_granularity = 8192;

    // Write options
    bool enable_wal = true;
    size_t wal_buffer_size = 4 * 1024;
};
```

**Limitations:**
- ❌ No MVCC configuration
- ❌ No temporal configuration
- ❌ No full-text search configuration
- ❌ No TTL configuration
- ❌ No zero-copy RecordRef configuration

---

## Proposed Design

### 1. TableCapabilities Struct

Add a new `TableCapabilities` struct that encapsulates opt-in features:

```cpp
namespace marble {

/**
 * @brief Feature capabilities that can be enabled per table/column family
 *
 * Each capability has performance and storage trade-offs. Enable only what you need.
 */
struct TableCapabilities {
    // ===== MVCC (Multi-Version Concurrency Control) =====
    /**
     * Enable MVCC for snapshot isolation transactions
     *
     * Benefits:
     * - Snapshot isolation (repeatable reads)
     * - Non-blocking reads during writes
     * - Point-in-time queries
     *
     * Costs:
     * - Write amplification: +30-50% (versioned keys)
     * - Storage overhead: +20-40% (version metadata)
     * - Read latency: +5-10 μs (version filtering)
     */
    bool enable_mvcc = false;

    // MVCC-specific settings (only used if enable_mvcc = true)
    struct MVCCSettings {
        // Version garbage collection policy
        enum class GCPolicy {
            kKeepAllVersions,      // No GC (for audit)
            kKeepRecentVersions,   // Keep N recent versions
            kKeepVersionsUntil     // Keep versions until timestamp
        };
        GCPolicy gc_policy = GCPolicy::kKeepRecentVersions;

        size_t max_versions_per_key = 10;  // For kKeepRecentVersions
        uint64_t gc_timestamp_ms = 0;      // For kKeepVersionsUntil

        // Snapshot retention policy
        size_t max_active_snapshots = 100;
        uint64_t snapshot_ttl_ms = 3600000;  // 1 hour default
    } mvcc_settings;

    // ===== Temporal/Bitemporal Features =====
    /**
     * Enable temporal versioning (system time and/or valid time)
     *
     * Benefits:
     * - Time travel queries (AS OF timestamp)
     * - Historical reconstruction (see data as it was)
     * - Regulatory compliance (audit trail)
     * - Bitemporal queries (system + valid time)
     *
     * Costs:
     * - Storage overhead: +40-100% (full history)
     * - Write amplification: +20-30% (metadata)
     * - Query complexity: +10-50ms (temporal reconstruction)
     */
    enum class TemporalModel {
        kNone = 0,        // No temporal features
        kSystemTime,      // Only system time (when data was inserted)
        kValidTime,       // Only valid time (when data is/was valid)
        kBitemporal       // Both system and valid time
    };
    TemporalModel temporal_model = TemporalModel::kNone;

    // Temporal-specific settings
    struct TemporalSettings {
        // Snapshot retention policy
        size_t max_snapshots = 1000;
        uint64_t snapshot_retention_ms = 86400000;  // 24 hours

        // Valid time range for bitemporal
        uint64_t min_valid_time_ms = 0;
        uint64_t max_valid_time_ms = UINT64_MAX;

        // Automatic snapshot creation
        bool auto_create_snapshots = true;
        uint64_t snapshot_interval_ms = 3600000;  // 1 hour
    } temporal_settings;

    // ===== Full-Text Search =====
    /**
     * Enable Lucene-style inverted index for full-text search
     *
     * Benefits:
     * - Fast text search (1-15ms for 1M docs)
     * - Phrase queries, wildcards, fuzzy matching
     * - Relevance ranking (TF-IDF, BM25)
     *
     * Costs:
     * - Index overhead: +30-80% storage (inverted index)
     * - Write amplification: +20-40% (index updates)
     * - Memory usage: +10-20% (in-memory dictionary)
     */
    bool enable_full_text_search = false;

    // Search-specific settings
    struct SearchSettings {
        // Columns to index for full-text search
        std::vector<std::string> indexed_columns;

        // Analyzer configuration
        enum class Analyzer {
            kStandard,      // Tokenize by whitespace + lowercase
            kSimple,        // Only lowercase
            kWhitespace,    // Only tokenize by whitespace
            kKeyword        // No tokenization (exact match)
        };
        Analyzer analyzer = Analyzer::kStandard;

        // Indexing options
        bool enable_term_positions = true;   // For phrase queries
        bool enable_term_offsets = false;    // For highlighting
        size_t max_terms_per_doc = 10000;    // Limit for protection

        // In-memory dictionary size
        size_t dictionary_cache_mb = 64;
    } search_settings;

    // ===== Time To Live (TTL) =====
    /**
     * Enable automatic expiration of old data
     *
     * Benefits:
     * - Automatic cleanup (no manual purging)
     * - Bounded storage growth
     * - Compliance with data retention policies
     *
     * Costs:
     * - Background overhead: +1-3% CPU (cleanup)
     * - Write overhead: +5-10% (timestamp tracking)
     */
    bool enable_ttl = false;

    // TTL-specific settings
    struct TTLSettings {
        // Column containing timestamp for TTL calculation
        std::string timestamp_column;

        // TTL duration
        uint64_t ttl_duration_ms = 86400000;  // 24 hours default

        // Cleanup policy
        enum class CleanupPolicy {
            kLazy,          // Clean during compaction only
            kBackground,    // Periodic background cleanup
            kImmediate      // Clean immediately on expiration (expensive)
        };
        CleanupPolicy cleanup_policy = CleanupPolicy::kBackground;

        // Background cleanup interval (for kBackground policy)
        uint64_t cleanup_interval_ms = 1800000;  // 30 minutes
    } ttl_settings;

    // ===== Zero-Copy RecordRef =====
    /**
     * Enable zero-copy record access (borrow from Arrow batches)
     *
     * Benefits:
     * - 10-100x less memory (no materialization)
     * - 5-10x faster scans (no deserialization)
     * - Better cache efficiency
     *
     * Costs:
     * - Lifetime complexity (must keep batch alive)
     * - API change (std::string_view vs std::string)
     */
    bool enable_zero_copy_reads = true;  // Enabled by default (low cost)

    // ===== Hot Key Cache =====
    /**
     * Enable LRU cache for frequently accessed keys
     *
     * Benefits:
     * - 5-10 μs hot key lookups (80% of queries)
     * - Reduced I/O for skewed workloads
     *
     * Costs:
     * - Memory: Configurable cache size
     * - Cache management: +1-2% CPU
     */
    bool enable_hot_key_cache = true;  // Enabled by default (low cost)

    struct HotKeyCacheSettings {
        size_t cache_size_mb = 64;
        uint32_t promotion_threshold = 3;  // Promote after N accesses
    } hot_key_cache_settings;

    // ===== Negative Cache =====
    /**
     * Enable cache for non-existent keys
     *
     * Benefits:
     * - Fast negative lookups (key doesn't exist)
     * - Protection against missing key attacks
     *
     * Costs:
     * - Memory: ~1MB for 10,000 entries
     */
    bool enable_negative_cache = true;  // Enabled by default (low cost)

    struct NegativeCacheSettings {
        size_t max_entries = 10000;
    } negative_cache_settings;

    // ===== Merge Operators =====
    /**
     * Enable custom merge operators for atomic updates
     *
     * Already supported via ColumnFamilyOptions.merge_operator
     * No additional cost if not used
     */
    // (Handled by existing ColumnFamilyOptions.merge_operator)
};

} // namespace marble
```

---

### 2. Updated ColumnFamilyOptions

Extend `ColumnFamilyOptions` to include `TableCapabilities`:

```cpp
struct ColumnFamilyOptions {
    // ===== Existing fields =====
    std::shared_ptr<arrow::Schema> schema;
    int primary_key_index = 0;
    std::shared_ptr<MergeOperator> merge_operator;

    size_t target_file_size = 64 * 1024 * 1024;
    size_t max_bytes_for_level_base = 256 * 1024 * 1024;
    int max_levels = 7;

    bool enable_bloom_filter = true;
    size_t bloom_filter_bits_per_key = 10;

    size_t block_size = 8192;
    bool enable_sparse_index = true;
    size_t index_granularity = 8192;

    bool enable_wal = true;
    size_t wal_buffer_size = 4 * 1024;

    // ===== NEW: Capability configuration =====
    TableCapabilities capabilities;

    ColumnFamilyOptions() = default;
};
```

---

### 3. Usage Examples

#### Example 1: High-Throughput Transactional Table (MVCC Only)

```cpp
#include <marble/db.h>
#include <marble/column_family.h>

// Create a transactional table with MVCC for snapshot isolation
marble::ColumnFamilyOptions txn_options;
txn_options.schema = txn_schema;

// Enable MVCC for snapshot isolation
txn_options.capabilities.enable_mvcc = true;
txn_options.capabilities.mvcc_settings.gc_policy =
    marble::TableCapabilities::MVCCSettings::GCPolicy::kKeepRecentVersions;
txn_options.capabilities.mvcc_settings.max_versions_per_key = 5;

// Keep other features disabled for performance
txn_options.capabilities.temporal_model = marble::TableCapabilities::TemporalModel::kNone;
txn_options.capabilities.enable_full_text_search = false;
txn_options.capabilities.enable_ttl = false;

marble::ColumnFamilyHandle* txn_cf;
db->CreateColumnFamily(marble::ColumnFamilyDescriptor("transactions", txn_options), &txn_cf);

// Use MVCC transactions
marble::Snapshot snapshot;
db->CreateSnapshot(&snapshot);  // Snapshot isolation

marble::ReadOptions read_opts;
read_opts.snapshot = &snapshot;
db->Get(read_opts, txn_cf, key, &record);  // Consistent read at snapshot
```

**Trade-offs:**
- ✅ Snapshot isolation for consistent reads
- ✅ Non-blocking reads (no write locks)
- ❌ +30% write amplification (versioned keys)
- ❌ +20% storage overhead (version metadata)

---

#### Example 2: Audit Log Table (Bitemporal, No MVCC)

```cpp
// Create an audit log table with bitemporal versioning
marble::ColumnFamilyOptions audit_options;
audit_options.schema = audit_schema;

// Enable bitemporal for full audit trail
audit_options.capabilities.temporal_model =
    marble::TableCapabilities::TemporalModel::kBitemporal;
audit_options.capabilities.temporal_settings.max_snapshots = 10000;
audit_options.capabilities.temporal_settings.snapshot_retention_ms = 31536000000;  // 1 year

// Disable MVCC (temporal provides versioning)
audit_options.capabilities.enable_mvcc = false;

// Keep data forever (no TTL)
audit_options.capabilities.enable_ttl = false;

marble::ColumnFamilyHandle* audit_cf;
db->CreateColumnFamily(marble::ColumnFamilyDescriptor("audit_log", audit_options), &audit_cf);

// Insert with valid time
marble::TemporalMetadata metadata;
metadata.valid_from = start_timestamp;
metadata.valid_to = end_timestamp;
db->TemporalInsert(audit_cf, batch, metadata);

// Query historical data
marble::TemporalQuerySpec query;
query.snapshot_id = snapshot_2024_01_01;  // System time: Jan 1, 2024
query.valid_time_start = valid_2023_12_01;  // Valid time: Dec 1, 2023
query.valid_time_end = valid_2023_12_31;

std::unique_ptr<marble::QueryResult> result;
db->TemporalScan(audit_cf, query, scan_spec, &result);
```

**Trade-offs:**
- ✅ Complete audit trail (system + valid time)
- ✅ Regulatory compliance (immutable history)
- ❌ +80% storage overhead (full history)
- ❌ +10-50ms query latency (temporal reconstruction)

---

#### Example 3: Documentation Table (Full-Text Search Only)

```cpp
// Create a documentation table with full-text search
marble::ColumnFamilyOptions docs_options;
docs_options.schema = docs_schema;

// Enable full-text search
docs_options.capabilities.enable_full_text_search = true;
docs_options.capabilities.search_settings.indexed_columns = {"title", "body", "tags"};
docs_options.capabilities.search_settings.analyzer =
    marble::TableCapabilities::SearchSettings::Analyzer::kStandard;
docs_options.capabilities.search_settings.enable_term_positions = true;  // For phrase queries

// Disable MVCC and temporal (not needed for docs)
docs_options.capabilities.enable_mvcc = false;
docs_options.capabilities.temporal_model = marble::TableCapabilities::TemporalModel::kNone;

// Enable TTL for old docs
docs_options.capabilities.enable_ttl = true;
docs_options.capabilities.ttl_settings.timestamp_column = "created_at";
docs_options.capabilities.ttl_settings.ttl_duration_ms = 15552000000;  // 180 days
docs_options.capabilities.ttl_settings.cleanup_policy =
    marble::TableCapabilities::TTLSettings::CleanupPolicy::kBackground;

marble::ColumnFamilyHandle* docs_cf;
db->CreateColumnFamily(marble::ColumnFamilyDescriptor("documentation", docs_options), &docs_cf);

// Full-text search query
marble::SearchQuery search_query;
search_query.query_string = "Apache Arrow columnar format";
search_query.fields = {"title", "body"};
search_query.max_results = 100;

std::unique_ptr<marble::SearchResult> results;
db->Search(docs_cf, search_query, &results);
```

**Trade-offs:**
- ✅ Fast full-text search (1-15ms for 1M docs)
- ✅ Phrase queries, wildcards, fuzzy matching
- ❌ +50% storage overhead (inverted index)
- ❌ +30% write amplification (index updates)

---

#### Example 4: High-Throughput Cache Table (All Features Disabled)

```cpp
// Create a high-throughput cache table with minimal overhead
marble::ColumnFamilyOptions cache_options;
cache_options.schema = cache_schema;

// Disable all optional features for maximum throughput
cache_options.capabilities.enable_mvcc = false;
cache_options.capabilities.temporal_model = marble::TableCapabilities::TemporalModel::kNone;
cache_options.capabilities.enable_full_text_search = false;
cache_options.capabilities.enable_ttl = false;

// Keep hot key cache enabled (low cost, high benefit)
cache_options.capabilities.enable_hot_key_cache = true;
cache_options.capabilities.hot_key_cache_settings.cache_size_mb = 256;

marble::ColumnFamilyHandle* cache_cf;
db->CreateColumnFamily(marble::ColumnFamilyDescriptor("cache", cache_options), &cache_cf);

// Maximum write throughput (no versioning overhead)
db->Put(options, cache_cf, record);  // ~7M rows/sec
```

**Trade-offs:**
- ✅ Maximum write throughput (7M rows/sec)
- ✅ Minimum storage overhead
- ✅ Lowest latency (5-10 μs hot keys)
- ❌ No ACID transactions
- ❌ No time travel
- ❌ No search

---

#### Example 5: Mixed-Use Table (MVCC + TTL + Hot Key Cache)

```cpp
// Create a session table with MVCC for consistency, TTL for cleanup, and caching
marble::ColumnFamilyOptions session_options;
session_options.schema = session_schema;

// Enable MVCC for snapshot isolation
session_options.capabilities.enable_mvcc = true;
session_options.capabilities.mvcc_settings.max_versions_per_key = 3;

// Enable TTL for automatic session expiration
session_options.capabilities.enable_ttl = true;
session_options.capabilities.ttl_settings.timestamp_column = "last_access";
session_options.capabilities.ttl_settings.ttl_duration_ms = 3600000;  // 1 hour
session_options.capabilities.ttl_settings.cleanup_policy =
    marble::TableCapabilities::TTLSettings::CleanupPolicy::kBackground;

// Enable hot key cache (sessions are hot)
session_options.capabilities.enable_hot_key_cache = true;
session_options.capabilities.hot_key_cache_settings.cache_size_mb = 128;

// Disable temporal and search (not needed)
session_options.capabilities.temporal_model = marble::TableCapabilities::TemporalModel::kNone;
session_options.capabilities.enable_full_text_search = false;

marble::ColumnFamilyHandle* session_cf;
db->CreateColumnFamily(marble::ColumnFamilyDescriptor("sessions", session_options), &session_cf);
```

**Trade-offs:**
- ✅ MVCC snapshot isolation
- ✅ Automatic session cleanup (TTL)
- ✅ Fast hot key lookups (5-10 μs)
- ❌ +30% write amplification (MVCC)
- ❌ +2% CPU overhead (TTL background cleanup)

---

## Performance Impact Matrix

| Capability | Write Throughput | Read Latency | Storage Overhead | Memory Overhead | CPU Overhead |
|------------|------------------|--------------|------------------|-----------------|--------------|
| **MVCC** | -30% | +5-10 μs | +20-40% | +5-10% | +2-5% |
| **Temporal (System)** | -20% | +10-30ms | +40-60% | +10-20% | +5-10% |
| **Temporal (Bitemporal)** | -30% | +20-50ms | +80-100% | +15-25% | +10-15% |
| **Full-Text Search** | -20% | +1-15ms | +30-80% | +10-20% | +5-10% |
| **TTL** | -5% | 0 | 0 | +1-2% | +1-3% |
| **Zero-Copy Reads** | 0 | -50% | 0 | -90% | 0 |
| **Hot Key Cache** | 0 | -50% (hit) | 0 | +Cache Size | +1-2% |
| **Negative Cache** | 0 | -30% (miss) | 0 | +1-2 MB | +1% |

**Baseline:** No optional features enabled
**Read Latency:** Point lookups (negative = improvement)
**Write Throughput:** Arrow batch inserts (negative = degradation)

---

## Recommended Configurations by Use Case

### 1. OLTP Transactions (Banking, E-commerce)

```cpp
capabilities.enable_mvcc = true;  // Snapshot isolation
capabilities.enable_hot_key_cache = true;  // Fast reads
capabilities.enable_negative_cache = true;  // Handle missing keys
capabilities.enable_ttl = false;  // Keep data indefinitely
capabilities.temporal_model = TemporalModel::kNone;  // Use MVCC instead
capabilities.enable_full_text_search = false;  // Not needed
```

**Performance:** 5-10 μs reads (hot), 4-5M writes/sec

---

### 2. Audit Logs (Compliance, Regulatory)

```cpp
capabilities.enable_mvcc = false;  // Temporal provides versioning
capabilities.temporal_model = TemporalModel::kBitemporal;  // Full history
capabilities.temporal_settings.snapshot_retention_ms = 31536000000;  // 1 year
capabilities.enable_ttl = false;  // Keep forever
capabilities.enable_full_text_search = false;  // Add if needed for log search
```

**Performance:** 3-4M writes/sec, 10-50ms temporal queries

---

### 3. Document Search (Wikis, Knowledge Base)

```cpp
capabilities.enable_mvcc = false;  // Not needed
capabilities.temporal_model = TemporalModel::kNone;  // Not needed
capabilities.enable_full_text_search = true;  // Core feature
capabilities.search_settings.indexed_columns = {"title", "body", "tags"};
capabilities.enable_ttl = true;  // Clean old docs
capabilities.ttl_settings.ttl_duration_ms = 15552000000;  // 180 days
```

**Performance:** 3-4M writes/sec, 1-15ms search queries

---

### 4. Time-Series Metrics (IoT, Monitoring)

```cpp
capabilities.enable_mvcc = false;  // Not needed
capabilities.temporal_model = TemporalModel::kNone;  // Use TTL instead
capabilities.enable_ttl = true;  // Auto-expire old metrics
capabilities.ttl_settings.ttl_duration_ms = 604800000;  // 7 days
capabilities.enable_hot_key_cache = true;  // Recent metrics are hot
capabilities.enable_full_text_search = false;  // Not needed
```

**Performance:** 6-7M writes/sec, 5-10 μs reads (recent data)

---

### 5. Session Store (Web Apps, APIs)

```cpp
capabilities.enable_mvcc = true;  // Consistent session reads
capabilities.enable_ttl = true;  // Auto-expire inactive sessions
capabilities.ttl_settings.ttl_duration_ms = 3600000;  // 1 hour
capabilities.enable_hot_key_cache = true;  // Sessions are hot
capabilities.enable_negative_cache = true;  // Handle invalid session IDs
capabilities.temporal_model = TemporalModel::kNone;  // Not needed
```

**Performance:** 4-5M writes/sec, 5-10 μs reads (active sessions)

---

### 6. High-Throughput Cache (Materialized Views, Aggregates)

```cpp
capabilities.enable_mvcc = false;  // Not needed (overwrite semantics)
capabilities.temporal_model = TemporalModel::kNone;  // Not needed
capabilities.enable_ttl = false;  // Manual invalidation
capabilities.enable_hot_key_cache = true;  // Cache is hot
capabilities.enable_zero_copy_reads = true;  // Fast scans
capabilities.enable_full_text_search = false;  // Not needed
```

**Performance:** 7M writes/sec, 5-10 μs reads (hot keys), 20-50M rows/sec scans

---

## Implementation Plan

### Phase 1: Core Infrastructure (1 week)

**Goal:** Add `TableCapabilities` struct and integrate with `ColumnFamilyOptions`

**Tasks:**
1. Create `include/marble/table_capabilities.h` with complete struct definition
2. Update `include/marble/column_family.h` to include `TableCapabilities`
3. Update `src/core/column_family.cpp` to store and retrieve capabilities
4. Add capability serialization/deserialization for manifest
5. Write unit tests for capability storage

**Deliverables:**
- ✅ `TableCapabilities` struct with all fields
- ✅ ColumnFamilyOptions integration
- ✅ Manifest persistence
- ✅ Unit tests

---

### Phase 2: MVCC Integration (2 weeks)

**Goal:** Make MVCC opt-in via `capabilities.enable_mvcc`

**Tasks:**
1. Implement `MVCCManager` class (`src/core/mvcc.cpp`)
2. Integrate with LSM storage (timestamped keys)
3. Add version garbage collection
4. Update read/write paths to check `capabilities.enable_mvcc`
5. Add MVCC-specific tests

**Deliverables:**
- ✅ MVCCManager implementation
- ✅ Snapshot isolation working
- ✅ Version GC working
- ✅ 30% write overhead measured

---

### Phase 3: Temporal Integration (2 weeks)

**Goal:** Make temporal features opt-in via `capabilities.temporal_model`

**Tasks:**
1. Fix FIXMEs in `src/core/temporal_reconstruction.cpp`
2. Implement proper version chain building
3. Add temporal query optimizer
4. Update write path to check `capabilities.temporal_model`
5. Add bitemporal-specific tests

**Deliverables:**
- ✅ Production-ready temporal reconstruction
- ✅ AS OF and VALID TIME queries working
- ✅ 80% storage overhead measured for bitemporal

---

### Phase 4: Full-Text Search Integration (3 weeks)

**Goal:** Make search opt-in via `capabilities.enable_full_text_search`

**Tasks:**
1. Implement `SearchIndexManager` class
2. Build inverted index (posting lists)
3. Implement search query parser
4. Add relevance ranking (TF-IDF or BM25)
5. Update write path to index text columns
6. Add search-specific tests

**Deliverables:**
- ✅ Lucene-style inverted index
- ✅ Search query engine
- ✅ 1-15ms search queries for 1M docs

---

### Phase 5: TTL Integration (1 week)

**Goal:** Make TTL opt-in via `capabilities.enable_ttl`

**Tasks:**
1. Already implemented in `docs/ADVANCED_FEATURES.md`
2. Integrate TTLManager with capabilities
3. Update background cleanup to check `capabilities.enable_ttl`
4. Add TTL-specific tests

**Deliverables:**
- ✅ TTL opt-in working
- ✅ Background cleanup respecting capabilities

---

### Phase 6: Documentation and Examples (1 week)

**Goal:** Comprehensive documentation and working examples

**Tasks:**
1. Update all existing docs to mention capabilities
2. Create capability migration guide
3. Write 5 example applications (one per use case)
4. Performance benchmark suite for each capability
5. Add troubleshooting guide

**Deliverables:**
- ✅ Updated documentation
- ✅ 5 working examples
- ✅ Benchmark suite
- ✅ Migration guide

---

## Total Effort: 10 weeks

---

## API Changes

### Backward Compatibility

**Existing code continues to work:**
```cpp
// Old code (no capabilities specified)
marble::ColumnFamilyOptions options;
options.schema = schema;
db->CreateColumnFamily(marble::ColumnFamilyDescriptor("test", options), &handle);

// Behavior: All capabilities default to false (except hot_key_cache, negative_cache, zero_copy)
// No breaking changes
```

**New code (explicit capabilities):**
```cpp
// New code (explicit capabilities)
marble::ColumnFamilyOptions options;
options.schema = schema;
options.capabilities.enable_mvcc = true;  // Opt-in to MVCC
options.capabilities.temporal_model = marble::TableCapabilities::TemporalModel::kBitemporal;
db->CreateColumnFamily(marble::ColumnFamilyDescriptor("test", options), &handle);
```

---

### Runtime Capability Checks

**Read path:**
```cpp
Status MarbleDB::Get(const ReadOptions& options,
                     ColumnFamilyHandle* cf,
                     const Key& key,
                     std::shared_ptr<Record>* record) {
    const auto& caps = cf->GetCapabilities();

    if (caps.enable_mvcc && options.snapshot != nullptr) {
        // MVCC path: filter by snapshot timestamp
        return MVCCGet(options, cf, key, record);
    } else if (caps.temporal_model != TableCapabilities::TemporalModel::kNone) {
        // Temporal path: reconstruct from version chain
        return TemporalGet(options, cf, key, record);
    } else {
        // Fast path: direct LSM lookup
        return FastGet(options, cf, key, record);
    }
}
```

**Write path:**
```cpp
Status MarbleDB::Put(const WriteOptions& options,
                     ColumnFamilyHandle* cf,
                     std::shared_ptr<Record> record) {
    const auto& caps = cf->GetCapabilities();

    if (caps.enable_mvcc) {
        // MVCC path: write with timestamp
        return MVCCPut(options, cf, record);
    }

    if (caps.temporal_model != TableCapabilities::TemporalModel::kNone) {
        // Temporal path: write with system/valid time metadata
        return TemporalPut(options, cf, record);
    }

    if (caps.enable_full_text_search) {
        // Index text columns
        search_index_manager_->Index(cf, record);
    }

    if (caps.enable_ttl) {
        // Add TTL metadata
        ttl_manager_->TrackRecord(cf, record);
    }

    // Standard LSM write
    return LSMPut(options, cf, record);
}
```

---

## Testing Strategy

### Unit Tests

```cpp
// Test capability configuration
TEST(TableCapabilitiesTest, MVCCEnabled) {
    marble::ColumnFamilyOptions options;
    options.capabilities.enable_mvcc = true;

    // Create CF and verify MVCC works
    ColumnFamilyHandle* cf;
    db->CreateColumnFamily(marble::ColumnFamilyDescriptor("test", options), &cf);

    // MVCC should work
    marble::Snapshot snapshot;
    db->CreateSnapshot(&snapshot);
    // ... test snapshot isolation
}

TEST(TableCapabilitiesTest, MVCCDisabled) {
    marble::ColumnFamilyOptions options;
    options.capabilities.enable_mvcc = false;  // Explicitly disabled

    ColumnFamilyHandle* cf;
    db->CreateColumnFamily(marble::ColumnFamilyDescriptor("test", options), &cf);

    // MVCC operations should not work or be no-ops
    marble::Snapshot snapshot;
    auto status = db->CreateSnapshot(&snapshot);
    EXPECT_FALSE(status.ok());  // Should fail or be no-op
}
```

### Integration Tests

```cpp
// Test mixed capabilities across CFs
TEST(IntegrationTest, MixedCapabilitiesAcrossCFs) {
    // CF1: MVCC only
    marble::ColumnFamilyOptions mvcc_opts;
    mvcc_opts.capabilities.enable_mvcc = true;
    ColumnFamilyHandle* mvcc_cf;
    db->CreateColumnFamily(marble::ColumnFamilyDescriptor("mvcc", mvcc_opts), &mvcc_cf);

    // CF2: Temporal only
    marble::ColumnFamilyOptions temporal_opts;
    temporal_opts.capabilities.temporal_model = marble::TableCapabilities::TemporalModel::kBitemporal;
    ColumnFamilyHandle* temporal_cf;
    db->CreateColumnFamily(marble::ColumnFamilyDescriptor("temporal", temporal_opts), &temporal_cf);

    // CF3: Search only
    marble::ColumnFamilyOptions search_opts;
    search_opts.capabilities.enable_full_text_search = true;
    search_opts.capabilities.search_settings.indexed_columns = {"text"};
    ColumnFamilyHandle* search_cf;
    db->CreateColumnFamily(marble::ColumnFamilyDescriptor("search", search_opts), &search_cf);

    // Verify each CF has independent capabilities
    // ... test MVCC on mvcc_cf only
    // ... test temporal on temporal_cf only
    // ... test search on search_cf only
}
```

### Performance Tests

```cpp
// Benchmark capability overhead
BENCHMARK(BM_WriteWithoutMVCC) {
    marble::ColumnFamilyOptions options;
    options.capabilities.enable_mvcc = false;
    // Measure write throughput
}

BENCHMARK(BM_WriteWithMVCC) {
    marble::ColumnFamilyOptions options;
    options.capabilities.enable_mvcc = true;
    // Measure write throughput
    // Expected: 30% slower than BM_WriteWithoutMVCC
}
```

---

## Migration Guide

### For Existing MarbleDB Users

**Current code:**
```cpp
marble::ColumnFamilyOptions options;
options.schema = schema;
db->CreateColumnFamily(marble::ColumnFamilyDescriptor("test", options), &cf);
```

**Migrated code (no behavior change):**
```cpp
marble::ColumnFamilyOptions options;
options.schema = schema;
// Capabilities default to safe values (all disabled except caches)
// No code change needed!
db->CreateColumnFamily(marble::ColumnFamilyDescriptor("test", options), &cf);
```

### To Opt-In to New Features

**Enable MVCC:**
```cpp
options.capabilities.enable_mvcc = true;
options.capabilities.mvcc_settings.max_versions_per_key = 10;
```

**Enable Bitemporal:**
```cpp
options.capabilities.temporal_model = marble::TableCapabilities::TemporalModel::kBitemporal;
```

**Enable Search:**
```cpp
options.capabilities.enable_full_text_search = true;
options.capabilities.search_settings.indexed_columns = {"title", "body"};
```

---

## Open Questions

1. **Should capabilities be mutable after table creation?**
   - Pro: Allows schema evolution (add MVCC later)
   - Con: Complex to implement (requires data migration)
   - **Recommendation:** Immutable for MVP, add mutation in Phase 7

2. **Should there be capability presets (e.g., "transactional", "analytical", "search")?**
   - Pro: Easier for users to get started
   - Con: Reduces flexibility
   - **Recommendation:** Add presets as helper functions

3. **How to handle conflicting capabilities (e.g., MVCC + Temporal)?**
   - MVCC provides versioning via timestamps
   - Temporal provides versioning via system/valid time
   - Both enabled = storage overhead +50-70%
   - **Recommendation:** Allow both, document trade-offs, warn at creation time

4. **Should capabilities affect compaction strategy?**
   - MVCC: Keep more versions → slower compaction
   - Temporal: Keep all history → disable aggressive compaction
   - TTL: Aggressive compaction for expired data
   - **Recommendation:** Auto-adjust compaction based on capabilities

---

## Summary

This design enables **flexible per-table/column-family capabilities** where different tables can opt-in to different features:

✅ **MVCC** - Snapshot isolation transactions
✅ **Temporal** - Time travel and bitemporal queries
✅ **Full-Text Search** - Lucene-style inverted index
✅ **TTL** - Automatic data expiration
✅ **Zero-Copy** - High-performance reads
✅ **Hot Key Cache** - Low-latency point lookups

**Key Benefits:**
1. **Flexibility:** Each table chooses its own features
2. **Performance:** Only pay for what you use
3. **Backward Compatibility:** Existing code continues to work
4. **Clear Trade-offs:** Documented performance impact for each capability

**Next Steps:**
1. Review this design document
2. Approve Phase 1 implementation (core infrastructure)
3. Begin implementing `TableCapabilities` struct
