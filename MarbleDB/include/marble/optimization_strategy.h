/**
 * Pluggable Optimization Strategy System for MarbleDB
 *
 * Enables per-table, workload-specific optimizations through a strategy pattern.
 * Supports RDF triple stores, OLTP key-value, time-series analytics, and property graphs.
 */

#pragma once

#include <memory>
#include <string>
#include <vector>
#include <unordered_map>
#include <cstdint>
#include <arrow/api.h>
#include "marble/status.h"

namespace marble {

// Forward declarations
class Key;
class Record;
struct TableCapabilities;
struct ColumnPredicate;  // Defined in api.h

//==============================================================================
// Context Structures - Passed to optimization strategies at various lifecycle points
//==============================================================================

/**
 * ReadContext - Context for read operations
 *
 * Strategies can inspect the read request and set flags to short-circuit
 * expensive operations (e.g., skip bloom filter if cache hit, skip disk
 * read if definitely not found).
 */
struct ReadContext {
    const Key& key;
    bool is_point_lookup = true;      // true for Get(), false for Scan()
    bool is_range_scan = false;       // true for range queries

    // Optimization flags (set by strategies)
    bool skip_bloom_filter = false;   // Skip bloom filter check
    bool skip_cache = false;          // Skip cache lookup
    bool definitely_not_found = false; // Short-circuit: key definitely doesn't exist
    bool definitely_found = false;     // Short-circuit: value in cache, skip disk

    // Performance hints (set by calling code)
    bool expect_hit = false;          // Caller expects key to exist (hot path)
    bool batch_context = false;       // Part of a batch operation

    // Schema context (for TripleStore strategy)
    std::string predicate_value;      // For RDF queries (e.g., "rdf:type")
    bool is_triple_lookup = false;    // Is this an RDF triple lookup?

    // NEW: Predicate pushdown support (for 100-1000x performance improvements)
    std::vector<ColumnPredicate> predicates;  // Column predicates for filtering
    bool has_predicates = false;              // Are predicates provided?
    bool skip_sstable = false;                // Short-circuit: skip entire SSTable (zone map fail)
};

/**
 * WriteContext - Context for write operations
 *
 * Strategies can inspect writes to update auxiliary data structures
 * (bloom filters, caches, statistics).
 */
struct WriteContext {
    const Key& key;
    const std::shared_ptr<arrow::RecordBatch>& batch;
    size_t row_offset;                // Row within batch

    // Write type
    enum WriteType { INSERT, UPDATE, DELETE };
    WriteType write_type = INSERT;

    // Optimization hints
    bool is_hot_key = false;          // Caller marks this as frequently accessed
    bool batch_context = false;       // Part of a batch write

    // Schema context
    bool is_triple_write = false;     // RDF triple write
    std::string predicate_value;      // For RDF writes
};

/**
 * CompactionContext - Context for compaction operations
 *
 * Strategies can rebuild auxiliary structures during compaction
 * (e.g., rebuild bloom filters, update statistics).
 */
struct CompactionContext {
    size_t level;                     // LSM level being compacted
    size_t num_input_files;           // Number of SSTables being merged
    size_t estimated_output_size;     // Estimated output size in bytes

    // Input batches (strategies can scan these to rebuild structures)
    std::vector<std::shared_ptr<arrow::RecordBatch>> input_batches;

    // Optimization flags
    bool rebuild_bloom_filters = false;  // Set by BloomFilterStrategy
    bool rebuild_statistics = false;     // Set by SkippingIndexStrategy
    bool merge_caches = false;           // Set by CacheStrategy
};

/**
 * FlushContext - Context for memtable flush operations
 *
 * Strategies can serialize auxiliary structures during flush
 * (e.g., write bloom filters to SSTable metadata).
 */
struct FlushContext {
    const std::shared_ptr<arrow::RecordBatch>& memtable_batch;
    size_t num_records;

    // Metadata to attach to SSTable (strategies populate this)
    std::unordered_map<std::string, std::vector<uint8_t>> metadata;

    // Optimization flags
    bool include_bloom_filter = false;
    bool include_statistics = false;
    bool include_cache_hints = false;
};

//==============================================================================
// OptimizationStrategy - Abstract base class
//==============================================================================

/**
 * Base class for all optimization strategies.
 *
 * Subclasses implement workload-specific optimizations:
 * - BloomFilterStrategy: Probabilistic membership testing
 * - CacheStrategy: Hot key and negative caching
 * - SkippingIndexStrategy: Min/max statistics for range queries
 * - TripleStoreStrategy: RDF-specific optimizations
 *
 * Lifecycle:
 * 1. OnTableCreate() - Called when table is created/opened
 * 2. OnRead() - Called before each read (can short-circuit)
 * 3. OnReadComplete() - Called after successful read (update caches)
 * 4. OnWrite() - Called for each write (update structures)
 * 5. OnCompaction() - Called during compaction (rebuild structures)
 * 6. OnFlush() - Called during flush (serialize structures)
 */
class OptimizationStrategy {
public:
    virtual ~OptimizationStrategy() = default;

    //==========================================================================
    // Lifecycle hooks
    //==========================================================================

    /**
     * Called when table is created or opened.
     * Strategy can inspect schema and capabilities to initialize.
     */
    virtual Status OnTableCreate(const TableCapabilities& caps) = 0;

    /**
     * Called before read operation.
     * Strategy can set flags in ReadContext to optimize the read.
     *
     * Return Status::NotFound() to short-circuit the read entirely.
     */
    virtual Status OnRead(ReadContext* ctx) = 0;

    /**
     * Called after successful read.
     * Strategy can update caches, access counts, etc.
     */
    virtual void OnReadComplete(const Key& key, const Record& record) = 0;

    /**
     * Called before write operation.
     * Strategy can update auxiliary structures (bloom filters, caches, stats).
     */
    virtual Status OnWrite(WriteContext* ctx) = 0;

    /**
     * Called during compaction.
     * Strategy can rebuild auxiliary structures from input batches.
     */
    virtual Status OnCompaction(CompactionContext* ctx) = 0;

    /**
     * Called during memtable flush.
     * Strategy can serialize auxiliary structures to SSTable metadata.
     */
    virtual Status OnFlush(FlushContext* ctx) = 0;

    //==========================================================================
    // Management
    //==========================================================================

    /**
     * Get current memory usage of auxiliary structures.
     */
    virtual size_t MemoryUsage() const = 0;

    /**
     * Clear all auxiliary structures (useful for testing).
     */
    virtual void Clear() = 0;

    /**
     * Serialize auxiliary structures for persistence.
     */
    virtual std::vector<uint8_t> Serialize() const = 0;

    /**
     * Deserialize auxiliary structures from persisted data.
     */
    virtual Status Deserialize(const std::vector<uint8_t>& data) = 0;

    /**
     * Get strategy name (for logging, debugging).
     */
    virtual std::string Name() const = 0;

    /**
     * Get human-readable stats (for monitoring).
     */
    virtual std::string GetStats() const { return ""; }
};

//==============================================================================
// OptimizationPipeline - Composes multiple strategies
//==============================================================================

/**
 * Composes multiple OptimizationStrategy instances into a pipeline.
 *
 * Calls strategies in order, with short-circuit logic:
 * - OnRead: Stops if any strategy returns Status::NotFound()
 * - OnWrite/OnCompaction/OnFlush: Calls all strategies
 *
 * Example:
 *   OptimizationPipeline pipeline;
 *   pipeline.AddStrategy(std::make_unique<BloomFilterStrategy>());
 *   pipeline.AddStrategy(std::make_unique<CacheStrategy>());
 *   pipeline.OnRead(&ctx);  // Calls both strategies
 */
class OptimizationPipeline {
public:
    OptimizationPipeline() = default;
    ~OptimizationPipeline() = default;

    // Disable copy (strategies are unique_ptr)
    OptimizationPipeline(const OptimizationPipeline&) = delete;
    OptimizationPipeline& operator=(const OptimizationPipeline&) = delete;

    // Enable move
    OptimizationPipeline(OptimizationPipeline&&) = default;
    OptimizationPipeline& operator=(OptimizationPipeline&&) = default;

    //==========================================================================
    // Strategy management
    //==========================================================================

    /**
     * Add a strategy to the end of the pipeline.
     */
    void AddStrategy(std::unique_ptr<OptimizationStrategy> strategy);

    /**
     * Remove a strategy by name.
     */
    void RemoveStrategy(const std::string& name);

    /**
     * Get strategy by name (returns nullptr if not found).
     */
    OptimizationStrategy* GetStrategy(const std::string& name);

    /**
     * Clear all strategies.
     */
    void ClearStrategies();

    /**
     * Get number of strategies in pipeline.
     */
    size_t NumStrategies() const { return strategies_.size(); }

    //==========================================================================
    // Lifecycle hooks (delegate to all strategies)
    //==========================================================================

    Status OnTableCreate(const TableCapabilities& caps);

    /**
     * Call all strategies' OnRead().
     * Short-circuits if any strategy returns Status::NotFound().
     */
    Status OnRead(ReadContext* ctx);

    void OnReadComplete(const Key& key, const Record& record);

    Status OnWrite(WriteContext* ctx);

    Status OnCompaction(CompactionContext* ctx);

    Status OnFlush(FlushContext* ctx);

    //==========================================================================
    // Management (aggregate across all strategies)
    //==========================================================================

    size_t MemoryUsage() const;

    void Clear();

    std::vector<uint8_t> Serialize() const;

    Status Deserialize(const std::vector<uint8_t>& data);

    /**
     * Get stats from all strategies (formatted as JSON-like string).
     */
    std::string GetStats() const;

private:
    std::vector<std::unique_ptr<OptimizationStrategy>> strategies_;
    std::unordered_map<std::string, size_t> strategy_index_;  // name -> index
};

//==============================================================================
// Workload Hints - Manual tuning for auto-configuration
//==============================================================================

/**
 * Hints about workload characteristics for auto-configuration.
 *
 * Used by OptimizationFactory to select appropriate strategies.
 * All fields are optional; factory will use heuristics if not provided.
 */
struct WorkloadHints {
    // Access patterns
    enum AccessPattern {
        UNKNOWN = 0,
        POINT_LOOKUP,     // Get() queries (e.g., key-value stores)
        RANGE_SCAN,       // Scan() queries (e.g., time-series analytics)
        MIXED             // Both point and range (e.g., OLTP + analytics)
    };
    AccessPattern access_pattern = UNKNOWN;

    // Data distribution
    bool has_hot_keys = false;        // Skewed access (e.g., OLTP sessions)
    bool is_time_series = false;      // Sorted by timestamp
    bool is_rdf_triple = false;       // RDF triple store

    // Selectivity hints
    double expected_hit_rate = 0.5;   // Expected cache hit rate (0.0-1.0)
    double range_selectivity = 0.1;   // Expected % of data scanned per query

    // Performance requirements
    bool optimize_for_write_throughput = false;
    bool optimize_for_read_latency = false;
    bool optimize_for_memory = false;

    // Manual strategy selection (overrides auto-detection)
    bool force_bloom_filter = false;
    bool force_cache = false;
    bool force_skipping_index = false;
    bool force_triple_store = false;
};

}  // namespace marble
