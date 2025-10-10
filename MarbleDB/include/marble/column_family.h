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
#include <arrow/api.h>
#include <memory>
#include <string>
#include <vector>
#include <unordered_map>

namespace marble {

// Forward declarations
class LSMSSTable;
class MemTable;

/**
 * @brief Column Family options
 * 
 * Each CF can have its own compaction settings, merge operator, etc.
 */
struct ColumnFamilyOptions {
    // Arrow schema for this column family
    std::shared_ptr<arrow::Schema> schema;
    
    // Primary key column index
    int primary_key_index = 0;
    
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
    
    ColumnFamilyOptions() = default;
};

/**
 * @brief Handle to a column family
 * 
 * Opaque handle used to reference a specific column family in DB operations.
 * Similar to RocksDB's ColumnFamilyHandle.
 */
class ColumnFamilyHandle {
public:
    ColumnFamilyHandle(const std::string& name, uint32_t id)
        : name_(name), id_(id) {}
    
    const std::string& name() const { return name_; }
    uint32_t id() const { return id_; }
    
    // Internal access
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
    size_t size() const { return families_.size(); }

private:
    std::unordered_map<std::string, std::unique_ptr<ColumnFamilyHandle>> families_;
    std::unordered_map<uint32_t, ColumnFamilyHandle*> id_to_handle_;
    ColumnFamilyHandle* default_cf_ = nullptr;
    uint32_t next_id_ = 0;
    
    std::mutex mutex_;
};

/**
 * @brief Column family metadata stored in manifest
 */
struct ColumnFamilyMetadata {
    uint32_t id;
    std::string name;
    std::string schema_json;  // Serialized Arrow schema
    std::string options_json;  // Serialized options
    
    // Serialize/deserialize
    std::string Serialize() const;
    static Status Deserialize(const std::string& data, ColumnFamilyMetadata* metadata);
};

} // namespace marble

