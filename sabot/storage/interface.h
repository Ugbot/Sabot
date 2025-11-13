#pragma once

/**
 * @file interface.h
 * @brief Sabot Storage Interface - Zero-cost abstraction for storage backends
 * 
 * This interface provides a stable, backend-agnostic API for Sabot's storage needs.
 * It decouples Sabot from specific storage implementations (MarbleDB, RocksDB, etc.)
 * and allows easy swapping of backends without changing Sabot's code.
 * 
 * Design principles:
 * - Zero-cost abstraction: All methods are inline where possible
 * - Minimal API: Only what Sabot needs, nothing more
 * - Clean separation: No MarbleDB-specific types leak through
 * - Future-proof: Easy to swap implementations
 */

#include <memory>
#include <string>
#include <vector>
#include <cstdint>
#include <functional>

// Forward declarations to avoid including Arrow headers
namespace arrow {
    class Schema;
    class RecordBatch;
    class Table;
}

namespace sabot {
namespace storage {

// Forward declarations
class StorageBackend;
class StateBackend;
class StoreBackend;

/**
 * @brief Status result for storage operations
 * 
 * Simple error code system (no exceptions per Sabot rules).
 */
enum class StatusCode {
    OK = 0,
    NotFound = 1,
    InvalidArgument = 2,
    IOError = 3,
    NotSupported = 4,
    InternalError = 5
};

struct Status {
    StatusCode code;
    std::string message;
    
    Status() : code(StatusCode::OK) {}
    Status(StatusCode c, const std::string& msg = "") : code(c), message(msg) {}
    
    bool ok() const { return code == StatusCode::OK; }
    bool IsNotFound() const { return code == StatusCode::NotFound; }
    
    static Status OK() { return Status(); }
    static Status NotFound(const std::string& msg = "") { return Status(StatusCode::NotFound, msg); }
    static Status InvalidArgument(const std::string& msg = "") { return Status(StatusCode::InvalidArgument, msg); }
    static Status IOError(const std::string& msg = "") { return Status(StatusCode::IOError, msg); }
    static Status NotSupported(const std::string& msg = "") { return Status(StatusCode::NotSupported, msg); }
    static Status InternalError(const std::string& msg = "") { return Status(StatusCode::InternalError, msg); }
};

/**
 * @brief Configuration for storage backends
 */
struct StorageConfig {
    std::string path;
    size_t memtable_size_mb = 64;
    bool enable_bloom_filter = true;
    bool enable_sparse_index = true;
    size_t index_granularity = 8192;
    
    StorageConfig() = default;
    StorageConfig(const std::string& p) : path(p) {}
};

/**
 * @brief State Backend Interface
 * 
 * Provides key-value operations for Flink-style state management.
 * Used for operator state, window state, join buffers, etc.
 */
class StateBackend {
public:
    virtual ~StateBackend() = default;
    
    // Lifecycle
    virtual Status Open(const StorageConfig& config) = 0;
    virtual Status Close() = 0;
    virtual Status Flush() = 0;
    
    // Point operations
    virtual Status Put(const std::string& key, const std::string& value) = 0;
    virtual Status Get(const std::string& key, std::string* value) = 0;
    virtual Status Delete(const std::string& key) = 0;
    virtual Status Exists(const std::string& key, bool* exists) = 0;
    
    // Batch operations
    virtual Status MultiGet(const std::vector<std::string>& keys,
                           std::vector<std::string>* values) = 0;
    
    // Range operations
    virtual Status DeleteRange(const std::string& start_key,
                              const std::string& end_key) = 0;
    
    // Scanning
    using ScanCallback = std::function<bool(const std::string& key, const std::string& value)>;
    virtual Status Scan(const std::string& start_key,
                      const std::string& end_key,
                      ScanCallback callback) = 0;
    
    // Checkpointing
    virtual Status CreateCheckpoint(std::string* checkpoint_path) = 0;
    virtual Status RestoreFromCheckpoint(const std::string& checkpoint_path) = 0;
};

/**
 * @brief Store Backend Interface
 * 
 * Provides Arrow-based table operations for columnar data storage.
 * Used for dimension tables, window aggregates, join buffers, etc.
 */
class StoreBackend {
public:
    virtual ~StoreBackend() = default;
    
    // Lifecycle
    virtual Status Open(const StorageConfig& config) = 0;
    virtual Status Close() = 0;
    virtual Status Flush() = 0;
    
    // Table management
    virtual Status CreateTable(const std::string& table_name,
                              const std::shared_ptr<arrow::Schema>& schema) = 0;
    virtual Status ListTables(std::vector<std::string>* table_names) = 0;
    
    // Batch operations
    virtual Status InsertBatch(const std::string& table_name,
                              const std::shared_ptr<arrow::RecordBatch>& batch) = 0;
    
    // Query operations
    virtual Status ScanTable(const std::string& table_name,
                            std::shared_ptr<arrow::Table>* table) = 0;
    
    // Helper: Scan table and return as batches (for easier Cython conversion)
    virtual Status ScanTableBatches(const std::string& table_name,
                                    std::vector<std::shared_ptr<arrow::RecordBatch>>* batches) = 0;
    
    // Helper: Get batch count for iteration (caches batches for GetBatchAt)
    virtual Status GetBatchCount(const std::string& table_name, size_t* count) = 0;
    
    // Helper: Get batch by index (requires GetBatchCount called first)
    virtual Status GetBatchAt(const std::string& table_name, size_t index,
                             std::shared_ptr<arrow::RecordBatch>* batch) = 0;
    
    // Helper: Clear cached batches to free memory
    virtual Status ClearBatchCache(const std::string& table_name) = 0;
    
    // Range operations
    virtual Status ScanRange(const std::string& table_name,
                            const std::string& start_key,
                            const std::string& end_key,
                            std::shared_ptr<arrow::Table>* table) = 0;
    
    virtual Status DeleteRange(const std::string& table_name,
                              const std::string& start_key,
                              const std::string& end_key) = 0;
    
    // Iterator for efficient streaming
    class Iterator {
    public:
        virtual ~Iterator() = default;
        virtual bool Valid() = 0;
        virtual void Next() = 0;
        virtual Status GetBatch(std::shared_ptr<arrow::RecordBatch>* batch) = 0;
    };
    
    virtual Status NewIterator(const std::string& table_name,
                              const std::string& start_key,
                              const std::string& end_key,
                              std::unique_ptr<Iterator>* iterator) = 0;
};

/**
 * @brief Factory functions for creating backends
 */
std::unique_ptr<StateBackend> CreateStateBackend(const std::string& backend_type);
std::unique_ptr<StoreBackend> CreateStoreBackend(const std::string& backend_type);

} // namespace storage
} // namespace sabot

