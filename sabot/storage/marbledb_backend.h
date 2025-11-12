#pragma once

/**
 * @file marbledb_backend.h
 * @brief MarbleDB implementation of Sabot Storage Interface
 * 
 * This is the MarbleDB-specific implementation of the storage interface.
 * It wraps MarbleDB's API and adapts it to Sabot's needs.
 * 
 * If MarbleDB changes or we want to swap it out, only this file needs to change.
 */

#include "interface.h"
#include <memory>
#include <string>
#include <unordered_map>

// Forward declarations to avoid including MarbleDB headers here
namespace marble {
    class MarbleDB;
    class ColumnFamilyHandle;
}

namespace sabot {
namespace storage {

/**
 * @brief MarbleDB implementation of StateBackend
 */
class MarbleDBStateBackend : public StateBackend {
public:
    MarbleDBStateBackend();
    ~MarbleDBStateBackend() override;
    
    // Non-copyable, movable
    MarbleDBStateBackend(const MarbleDBStateBackend&) = delete;
    MarbleDBStateBackend& operator=(const MarbleDBStateBackend&) = delete;
    MarbleDBStateBackend(MarbleDBStateBackend&&) noexcept;
    MarbleDBStateBackend& operator=(MarbleDBStateBackend&&) noexcept;
    
    Status Open(const StorageConfig& config) override;
    Status Close() override;
    Status Flush() override;
    
    Status Put(const std::string& key, const std::string& value) override;
    Status Get(const std::string& key, std::string* value) override;
    Status Delete(const std::string& key) override;
    Status Exists(const std::string& key, bool* exists) override;
    
    Status MultiGet(const std::vector<std::string>& keys,
                   std::vector<std::string>* values) override;
    
    Status DeleteRange(const std::string& start_key,
                      const std::string& end_key) override;
    
    Status Scan(const std::string& start_key,
               const std::string& end_key,
               ScanCallback callback) override;
    
    Status CreateCheckpoint(std::string* checkpoint_path) override;
    Status RestoreFromCheckpoint(const std::string& checkpoint_path) override;

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

/**
 * @brief MarbleDB implementation of StoreBackend
 */
class MarbleDBStoreBackend : public StoreBackend {
public:
    MarbleDBStoreBackend();
    ~MarbleDBStoreBackend() override;
    
    // Non-copyable, movable
    MarbleDBStoreBackend(const MarbleDBStoreBackend&) = delete;
    MarbleDBStoreBackend& operator=(const MarbleDBStoreBackend&) = delete;
    MarbleDBStoreBackend(MarbleDBStoreBackend&&) noexcept;
    MarbleDBStoreBackend& operator=(MarbleDBStoreBackend&&) noexcept;
    
    Status Open(const StorageConfig& config) override;
    Status Close() override;
    Status Flush() override;
    
    Status CreateTable(const std::string& table_name,
                      const std::shared_ptr<arrow::Schema>& schema) override;
    Status ListTables(std::vector<std::string>* table_names) override;
    
    Status InsertBatch(const std::string& table_name,
                      const std::shared_ptr<arrow::RecordBatch>& batch) override;
    
    Status ScanTable(const std::string& table_name,
                    std::shared_ptr<arrow::Table>* table) override;
    
    Status ScanRange(const std::string& table_name,
                    const std::string& start_key,
                    const std::string& end_key,
                    std::shared_ptr<arrow::Table>* table) override;
    
    Status DeleteRange(const std::string& table_name,
                      const std::string& start_key,
                      const std::string& end_key) override;
    
    class IteratorImpl;
    Status NewIterator(const std::string& table_name,
                      const std::string& start_key,
                      const std::string& end_key,
                      std::unique_ptr<Iterator>* iterator) override;

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

} // namespace storage
} // namespace sabot

