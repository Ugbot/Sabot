/************************************************************************
MarbleDB Column Families
Inspired by RocksDB's column family design

Enables multiple independent datasets within a single database instance,
each with its own schema, compaction settings, and merge operators.
**************************************************************************/

#pragma once

#include <marble/status.h>
#include <marble/record.h>
#include <marble/merge_operator.h>
#include <marble/table_capabilities.h>
#include <marble/optimization_strategy.h>
#include <marble/table_schema.h>
#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <memory>
#include <string>
#include <vector>
#include <unordered_map>

namespace marble {

// Forward declarations
class LSMSSTable;
class MemTable;
class OptimizationPipeline;

/**
 * @brief Optimization configuration for a column family
 *
 * Controls pluggable optimization strategies (bloom filters, caches, etc.)
 * for this table. Can be auto-configured from schema or manually specified.
 */
struct OptimizationConfig {
    // Enable pluggable optimization system (default: false for backward compatibility)
    bool enable_pluggable_optimizations = false;

    // Auto-configure from schema (default: true)
    bool auto_configure = true;

    // Workload hints for auto-configuration
    WorkloadHints workload_hints;

    // Optimization pipeline (created by factory or manually)
    std::unique_ptr<OptimizationPipeline> pipeline;

    // Legacy bloom filter settings (used when pluggable optimizations disabled)
    bool legacy_bloom_filter_enabled = true;

    OptimizationConfig() = default;

    // Copy constructor - copies config but not pipeline (pipeline must be recreated)
    OptimizationConfig(const OptimizationConfig& other)
        : enable_pluggable_optimizations(other.enable_pluggable_optimizations),
          auto_configure(other.auto_configure),
          workload_hints(other.workload_hints),
          pipeline(nullptr),  // Pipeline not copied, must be recreated
          legacy_bloom_filter_enabled(other.legacy_bloom_filter_enabled) {}

    // Copy assignment - copies config but not pipeline
    OptimizationConfig& operator=(const OptimizationConfig& other) {
        if (this != &other) {
            enable_pluggable_optimizations = other.enable_pluggable_optimizations;
            auto_configure = other.auto_configure;
            workload_hints = other.workload_hints;
            pipeline = nullptr;  // Pipeline not copied, must be recreated
            legacy_bloom_filter_enabled = other.legacy_bloom_filter_enabled;
        }
        return *this;
    }

    // Move semantics
    OptimizationConfig(OptimizationConfig&& other) = default;
    OptimizationConfig& operator=(OptimizationConfig&& other) = default;
};

/**
 * @brief Column Family options
 *
 * Each CF can have its own compaction settings, merge operator, etc.
 */
struct ColumnFamilyOptions {
    // Arrow schema for this column family (legacy - use table_schema for full support)
    std::shared_ptr<arrow::Schema> schema;

    // Primary key column index (legacy - use table_schema for composite keys)
    int primary_key_index = 0;

    // Full table schema with composite key support (Cassandra/ClickHouse-style)
    // If set, this takes precedence over schema + primary_key_index
    std::shared_ptr<WideTableSchema> table_schema;
    
    // Merge operator (optional)
    std::shared_ptr<MergeOperator> merge_operator;
    
    // Compaction settings
    size_t target_file_size = 64 * 1024 * 1024;  // 64MB
    size_t max_bytes_for_level_base = 256 * 1024 * 1024;  // 256MB
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

    // Feature capabilities (MVCC, temporal, search, TTL, etc.)
    TableCapabilities capabilities;

    // Optimization configuration (pluggable strategies)
    OptimizationConfig optimization_config;

    ColumnFamilyOptions() = default;

    // Copy/move semantics - OptimizationConfig handles its own copying
    ColumnFamilyOptions(const ColumnFamilyOptions&) = default;
    ColumnFamilyOptions& operator=(const ColumnFamilyOptions&) = default;
    ColumnFamilyOptions(ColumnFamilyOptions&&) = default;
    ColumnFamilyOptions& operator=(ColumnFamilyOptions&&) = default;
};

/**
 * @brief Handle to a column family
 *
 * Opaque handle used to reference a specific column family in DB operations.
 * Similar to RocksDB's ColumnFamilyHandle.
 *
 * Wide-table API (Cassandra/ClickHouse-style):
 * - Put(RecordBatch) - Write full rows with schema validation
 * - Get(key, columns) - Read with column projection
 * - Scan(options) - Range scan with predicates and projection
 */
class ColumnFamilyHandle {
public:
    ColumnFamilyHandle(const std::string& name, uint32_t id)
        : name_(name), id_(id) {}

    const std::string& name() const { return name_; }
    uint32_t id() const { return id_; }

    // Get capabilities for this column family
    const TableCapabilities& GetCapabilities() const { return options_.capabilities; }
    TableCapabilities& MutableCapabilities() { return options_.capabilities; }

    // ========================================================================
    // Wide-Table API (Cassandra/ClickHouse-style)
    // ========================================================================

    /**
     * @brief Get the wide table schema (returns nullptr if using legacy API)
     */
    std::shared_ptr<WideTableSchema> GetWideTableSchema() const {
        return options_.table_schema;
    }

    /**
     * @brief Check if this column family has a wide table schema
     */
    bool HasWideTableSchema() const {
        return options_.table_schema != nullptr;
    }

    /**
     * @brief Write rows as Arrow RecordBatch (validates against schema)
     *
     * The RecordBatch must match the table schema. Rows are automatically
     * keyed using the composite key definition (partition + clustering columns).
     *
     * @param rows Arrow RecordBatch containing rows to write
     * @return Status OK on success, InvalidArgument if schema mismatch
     */
    Status Put(const std::shared_ptr<arrow::RecordBatch>& rows);

    /**
     * @brief Read a single row by composite key with column projection
     *
     * @param key Encoded composite key (from WideTableSchema::EncodeKey)
     * @param columns Column names to return (empty = all columns)
     * @param result Output RecordBatch containing the row(s)
     * @return Status OK on success, NotFound if key doesn't exist
     */
    Status Get(uint64_t key,
               const std::vector<std::string>& columns,
               std::shared_ptr<arrow::RecordBatch>* result);

    /**
     * @brief Scan range of keys with predicates and projection
     *
     * Supports:
     * - Key range filtering (start_key, end_key)
     * - Column projection (return only requested columns)
     * - Predicate pushdown (Arrow compute expressions)
     * - Limit and offset
     *
     * @param options Scan options (key range, columns, filter, limit)
     * @param results Output vector of RecordBatches
     * @return Status OK on success
     */
    Status Scan(const ScanOptions& options,
                std::vector<std::shared_ptr<arrow::RecordBatch>>* results);

    /**
     * @brief Delete a row by composite key
     *
     * @param key Encoded composite key
     * @return Status OK on success
     */
    Status Delete(uint64_t key);

    // ========================================================================
    // Internal access
    // ========================================================================

    MemTable* mutable_memtable() { return mutable_; }
    const std::vector<std::shared_ptr<LSMSSTable>>& immutable_memtables() const {
        return immutables_;
    }
    const std::vector<std::vector<std::shared_ptr<LSMSSTable>>>& levels() const {
        return levels_;
    }

private:
    friend class ColumnFamilySet;
    
    std::string name_;
    uint32_t id_;
    
    // LSM tree components for this CF
    MemTable* mutable_ = nullptr;
    std::vector<std::shared_ptr<LSMSSTable>> immutables_;
    std::vector<std::vector<std::shared_ptr<LSMSSTable>>> levels_;
    
    ColumnFamilyOptions options_;
};

/**
 * @brief Descriptor for creating a column family
 */
struct ColumnFamilyDescriptor {
    std::string name;
    ColumnFamilyOptions options;

    // Default constructor for Cython/SWIG bindings
    ColumnFamilyDescriptor() = default;

    ColumnFamilyDescriptor(const std::string& n, const ColumnFamilyOptions& opts)
        : name(n), options(opts) {}
};

/**
 * @brief Manages all column families in a database
 */
class ColumnFamilySet {
public:
    ColumnFamilySet();
    ~ColumnFamilySet();
    
    /**
     * @brief Create a new column family
     */
    Status CreateColumnFamily(const ColumnFamilyDescriptor& descriptor,
                             ColumnFamilyHandle** handle);
    
    /**
     * @brief Get column family by name
     */
    ColumnFamilyHandle* GetColumnFamily(const std::string& name);
    ColumnFamilyHandle* GetColumnFamily(uint32_t id);
    
    /**
     * @brief Drop a column family
     */
    Status DropColumnFamily(const std::string& name);
    Status DropColumnFamily(ColumnFamilyHandle* handle);
    
    /**
     * @brief List all column families
     */
    std::vector<std::string> ListColumnFamilies() const;
    
    /**
     * @brief Get default column family
     */
    ColumnFamilyHandle* DefaultColumnFamily();
    
    /**
     * @brief Number of column families
     */
    size_t size() const;

private:
    // Lock-free copy-on-write using atomic pointer
    // Column families are metadata (rare writes, frequent reads)
    struct ColumnFamilyData {
        std::unordered_map<std::string, ColumnFamilyHandle*> families;  // Raw pointers (owned elsewhere)
        std::unordered_map<uint32_t, ColumnFamilyHandle*> id_to_handle;
        ColumnFamilyHandle* default_cf = nullptr;
        std::vector<std::unique_ptr<ColumnFamilyHandle>> owned_handles;  // Actual ownership

        ColumnFamilyData() = default;

        // Deep copy for COW
        ColumnFamilyData(const ColumnFamilyData& other) {
            default_cf = other.default_cf;
            // Copy ownership
            for (const auto& handle_ptr : other.owned_handles) {
                auto new_handle = std::make_unique<ColumnFamilyHandle>(*handle_ptr);
                ColumnFamilyHandle* raw = new_handle.get();
                families[new_handle->name()] = raw;
                id_to_handle[new_handle->id()] = raw;
                if (new_handle->name() == "default") {
                    default_cf = raw;
                }
                owned_handles.push_back(std::move(new_handle));
            }
        }
    };

    // Atomic pointer for lock-free reads/writes
    std::atomic<ColumnFamilyData*> data_{nullptr};

    // Atomic ID generator
    std::atomic<uint32_t> next_id_{0};

    // Helper: Load current state (lock-free)
    ColumnFamilyData* load_data() const {
        return data_.load(std::memory_order_acquire);
    }

    // Helper: CAS update
    bool cas_update(ColumnFamilyData* expected, ColumnFamilyData* desired) {
        return data_.compare_exchange_strong(expected, desired,
                                             std::memory_order_acq_rel,
                                             std::memory_order_acquire);
    }
};

/**
 * @brief Column family metadata stored in manifest
 */
struct ColumnFamilyMetadata {
    uint32_t id;
    std::string name;
    std::string schema_json;  // Serialized Arrow schema
    std::string options_json;  // Serialized options (includes capabilities)

    // Serialize/deserialize
    std::string Serialize() const;
    static Status Deserialize(const std::string& data, ColumnFamilyMetadata* metadata);
};

// Helper functions for capability serialization
namespace CapabilitySerialization {
    /**
     * @brief Serialize TableCapabilities to JSON string
     */
    std::string SerializeCapabilities(const TableCapabilities& caps);

    /**
     * @brief Deserialize TableCapabilities from JSON string
     */
    Status DeserializeCapabilities(const std::string& json, TableCapabilities* caps);
}

} // namespace marble

