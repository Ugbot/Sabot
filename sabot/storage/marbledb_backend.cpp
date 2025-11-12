/**
 * @file marbledb_backend.cpp
 * @brief MarbleDB implementation of Sabot Storage Interface
 * 
 * This file contains the actual implementation that bridges Sabot's interface
 * to MarbleDB's API. All MarbleDB-specific code is isolated here.
 */

#include "marbledb_backend.h"

#include <marble/api.h>
#include <marble/db.h>
#include <marble/lsm_storage.h>
#include <arrow/api.h>
#include <arrow/table.h>
#include <arrow/record_batch.h>
#include <ctime>

#include <memory>
#include <unordered_map>
#include <algorithm>
#include <cstring>

namespace sabot {
namespace storage {

//==============================================================================
// Helper: Hash string to uint64 for LSMTree
//==============================================================================
static uint64_t HashString(const std::string& str) {
    // Simple hash function - can be improved if needed
    uint64_t hash = 5381;
    for (char c : str) {
        hash = ((hash << 5) + hash) + c;
    }
    return hash;
}

//==============================================================================
// MarbleDBStateBackend Implementation
//==============================================================================

class MarbleDBStateBackend::Impl {
public:
    std::unique_ptr<marble::LSMTree> lsm_tree_;
    std::string db_path_;
    bool is_open_ = false;
    
    // Encode key+value for LSMTree storage
    // Format: [key_len(4 bytes)][key_bytes][value_bytes]
    static std::string EncodeKV(const std::string& key, const std::string& value) {
        uint32_t key_len = static_cast<uint32_t>(key.size());
        std::string encoded;
        encoded.reserve(4 + key_len + value.size());
        encoded.append(reinterpret_cast<const char*>(&key_len), 4);
        encoded.append(key);
        encoded.append(value);
        return encoded;
    }
    
    // Decode key+value from LSMTree storage
    static std::pair<std::string, std::string> DecodeKV(const std::string& data) {
        if (data.size() < 4) {
            return {"", ""};
        }
        uint32_t key_len;
        std::memcpy(&key_len, data.data(), 4);
        if (data.size() < 4 + key_len) {
            return {"", ""};
        }
        std::string key = data.substr(4, key_len);
        std::string value = data.substr(4 + key_len);
        return {key, value};
    }
};

MarbleDBStateBackend::MarbleDBStateBackend() : impl_(std::make_unique<Impl>()) {}

MarbleDBStateBackend::~MarbleDBStateBackend() {
    Close();
}

MarbleDBStateBackend::MarbleDBStateBackend(MarbleDBStateBackend&&) noexcept = default;
MarbleDBStateBackend& MarbleDBStateBackend::operator=(MarbleDBStateBackend&&) noexcept = default;

Status MarbleDBStateBackend::Open(const StorageConfig& config) {
    if (impl_->is_open_) {
        return Status::OK();
    }
    
    impl_->db_path_ = config.path;
    
    // Create LSM Tree
    impl_->lsm_tree_ = marble::CreateLSMTree();
    if (!impl_->lsm_tree_) {
        return Status::InternalError("Failed to create LSMTree");
    }
    
    // Configure LSM Tree
    marble::LSMTreeConfig lsm_config;
    lsm_config.data_directory = config.path + "/data";
    lsm_config.wal_directory = config.path + "/wal";
    lsm_config.temp_directory = config.path + "/temp";
    lsm_config.memtable_max_size_bytes = config.memtable_size_mb * 1024 * 1024;
    lsm_config.enable_bloom_filters = config.enable_bloom_filter;
    lsm_config.enable_mmap_flush = true;
    
    auto status = impl_->lsm_tree_->Init(lsm_config);
    if (!status.ok()) {
        return Status::InternalError("LSMTree Init failed: " + status.ToString());
    }
    
    impl_->is_open_ = true;
    return Status::OK();
}

Status MarbleDBStateBackend::Close() {
    if (!impl_->is_open_) {
        return Status::OK();
    }
    
    if (impl_->lsm_tree_) {
        auto status = impl_->lsm_tree_->Shutdown();
        impl_->lsm_tree_.reset();
        if (!status.ok()) {
            return Status::IOError("LSMTree Shutdown failed: " + status.ToString());
        }
    }
    
    impl_->is_open_ = false;
    return Status::OK();
}

Status MarbleDBStateBackend::Flush() {
    if (!impl_->is_open_ || !impl_->lsm_tree_) {
        return Status::InvalidArgument("Backend not open");
    }
    
    auto status = impl_->lsm_tree_->Flush();
    if (!status.ok()) {
        return Status::IOError("Flush failed: " + status.ToString());
    }
    return Status::OK();
}

Status MarbleDBStateBackend::Put(const std::string& key, const std::string& value) {
    if (!impl_->is_open_ || !impl_->lsm_tree_) {
        return Status::InvalidArgument("Backend not open");
    }
    
    uint64_t key_hash = HashString(key);
    std::string lsm_value = Impl::EncodeKV(key, value);
    
    auto status = impl_->lsm_tree_->Put(key_hash, lsm_value);
    if (!status.ok()) {
        return Status::IOError("Put failed: " + status.ToString());
    }
    return Status::OK();
}

Status MarbleDBStateBackend::Get(const std::string& key, std::string* value) {
    if (!impl_->is_open_ || !impl_->lsm_tree_) {
        return Status::InvalidArgument("Backend not open");
    }
    
    if (!value) {
        return Status::InvalidArgument("value pointer is null");
    }
    
    uint64_t key_hash = HashString(key);
    std::string lsm_value;
    
    auto status = impl_->lsm_tree_->Get(key_hash, &lsm_value);
    if (!status.ok()) {
        if (status.IsNotFound()) {
            return Status::NotFound("Key not found: " + key);
        }
        return Status::IOError("Get failed: " + status.ToString());
    }
    
    auto decoded = Impl::DecodeKV(lsm_value);
    *value = decoded.second;
    return Status::OK();
}

Status MarbleDBStateBackend::Delete(const std::string& key) {
    if (!impl_->is_open_ || !impl_->lsm_tree_) {
        return Status::InvalidArgument("Backend not open");
    }
    
    uint64_t key_hash = HashString(key);
    auto status = impl_->lsm_tree_->Delete(key_hash);
    if (!status.ok()) {
        return Status::IOError("Delete failed: " + status.ToString());
    }
    return Status::OK();
}

Status MarbleDBStateBackend::Exists(const std::string& key, bool* exists) {
    if (!impl_->is_open_ || !impl_->lsm_tree_) {
        return Status::InvalidArgument("Backend not open");
    }
    
    if (!exists) {
        return Status::InvalidArgument("exists pointer is null");
    }
    
    uint64_t key_hash = HashString(key);
    std::string lsm_value;
    auto status = impl_->lsm_tree_->Get(key_hash, &lsm_value);
    *exists = status.ok();
    return Status::OK();
}

Status MarbleDBStateBackend::MultiGet(const std::vector<std::string>& keys,
                                      std::vector<std::string>* values) {
    if (!impl_->is_open_ || !impl_->lsm_tree_) {
        return Status::InvalidArgument("Backend not open");
    }
    
    if (!values) {
        return Status::InvalidArgument("values pointer is null");
    }
    
    values->clear();
    values->reserve(keys.size());
    
    for (const auto& key : keys) {
        std::string value;
        auto status = Get(key, &value);
        if (status.ok()) {
            values->push_back(std::move(value));
        } else if (status.code == StatusCode::NotFound) {
            values->push_back(""); // Not found - empty string
        } else {
            return status; // Error
        }
    }
    
    return Status::OK();
}

Status MarbleDBStateBackend::DeleteRange(const std::string& start_key,
                                         const std::string& end_key) {
    if (!impl_->is_open_ || !impl_->lsm_tree_) {
        return Status::InvalidArgument("Backend not open");
    }
    
    uint64_t start_hash = HashString(start_key);
    uint64_t end_hash = HashString(end_key);
    
    std::vector<std::pair<uint64_t, std::string>> results;
    auto status = impl_->lsm_tree_->Scan(start_hash, end_hash, &results);
    if (!status.ok()) {
        return Status::IOError("Scan failed: " + status.ToString());
    }
    
    // Delete each key in range
    for (const auto& pair : results) {
        auto decoded = Impl::DecodeKV(pair.second);
        if (decoded.first >= start_key && decoded.first < end_key) {
            auto del_status = impl_->lsm_tree_->Delete(pair.first);
            if (!del_status.ok()) {
                // Continue deleting others, but log error
            }
        }
    }
    
    return Status::OK();
}

Status MarbleDBStateBackend::Scan(const std::string& start_key,
                                  const std::string& end_key,
                                  ScanCallback callback) {
    if (!impl_->is_open_ || !impl_->lsm_tree_) {
        return Status::InvalidArgument("Backend not open");
    }
    
    uint64_t start_hash = start_key.empty() ? 0 : HashString(start_key);
    uint64_t end_hash = end_key.empty() ? UINT64_MAX : HashString(end_key);
    
    std::vector<std::pair<uint64_t, std::string>> results;
    auto status = impl_->lsm_tree_->Scan(start_hash, end_hash, &results);
    if (!status.ok()) {
        return Status::IOError("Scan failed: " + status.ToString());
    }
    
    // Call callback for each result
    for (const auto& pair : results) {
        auto decoded = Impl::DecodeKV(pair.second);
        if (decoded.first >= start_key && (end_key.empty() || decoded.first < end_key)) {
            if (!callback(decoded.first, decoded.second)) {
                break; // Callback requested stop
            }
        }
    }
    
    return Status::OK();
}

Status MarbleDBStateBackend::CreateCheckpoint(std::string* checkpoint_path) {
    if (!impl_->is_open_ || !impl_->lsm_tree_) {
        return Status::InvalidArgument("Backend not open");
    }
    
    // Flush first
    auto flush_status = Flush();
    if (!flush_status.ok()) {
        return flush_status;
    }
    
    // Create checkpoint path
    *checkpoint_path = impl_->db_path_ + "/checkpoints/" + std::to_string(time(nullptr));
    
    // For now, checkpoint is just the data directory
    // TODO: Implement proper checkpointing if MarbleDB supports it
    return Status::OK();
}

Status MarbleDBStateBackend::RestoreFromCheckpoint(const std::string& checkpoint_path) {
    if (impl_->is_open_) {
        Close();
    }
    
    // For now, just reopen from the checkpoint path
    // TODO: Implement proper restore if MarbleDB supports it
    StorageConfig config;
    config.path = checkpoint_path;
    return Open(config);
}

//==============================================================================
// MarbleDBStoreBackend Implementation
//==============================================================================

class MarbleDBStoreBackend::Impl {
public:
    std::unique_ptr<marble::MarbleDB> db_;
    std::unordered_map<std::string, marble::ColumnFamilyHandle*> column_families_;
    std::string db_path_;
    bool is_open_ = false;
};

MarbleDBStoreBackend::MarbleDBStoreBackend() : impl_(std::make_unique<Impl>()) {}

MarbleDBStoreBackend::~MarbleDBStoreBackend() {
    Close();
}

MarbleDBStoreBackend::MarbleDBStoreBackend(MarbleDBStoreBackend&&) noexcept = default;
MarbleDBStoreBackend& MarbleDBStoreBackend::operator=(MarbleDBStoreBackend&&) noexcept = default;

Status MarbleDBStoreBackend::Open(const StorageConfig& config) {
    if (impl_->is_open_) {
        return Status::OK();
    }
    
    impl_->db_path_ = config.path;
    
    // Open MarbleDB using api.h function
    auto status = marble::OpenDatabase(config.path, &impl_->db_);
    if (!status.ok()) {
        return Status::InternalError("Failed to open MarbleDB: " + status.ToString());
    }
    
    impl_->is_open_ = true;
    return Status::OK();
}

Status MarbleDBStoreBackend::Close() {
    if (!impl_->is_open_) {
        return Status::OK();
    }
    
    if (impl_->db_) {
        marble::CloseDatabase(&impl_->db_);
        impl_->column_families_.clear();
    }
    
    impl_->is_open_ = false;
    return Status::OK();
}

Status MarbleDBStoreBackend::Flush() {
    if (!impl_->is_open_ || !impl_->db_) {
        return Status::InvalidArgument("Backend not open");
    }
    
    auto status = impl_->db_->Flush();
    if (!status.ok()) {
        return Status::IOError("Flush failed: " + status.ToString());
    }
    return Status::OK();
}

Status MarbleDBStoreBackend::CreateTable(const std::string& table_name,
                                         const std::shared_ptr<arrow::Schema>& schema) {
    if (!impl_->is_open_ || !impl_->db_) {
        return Status::InvalidArgument("Backend not open");
    }
    
    if (!schema) {
        return Status::InvalidArgument("Schema is null");
    }
    
    // Create column family descriptor
    marble::ColumnFamilyDescriptor desc;
    desc.name = table_name;
    desc.options.schema = schema;
    desc.options.enable_bloom_filter = true;
    desc.options.enable_sparse_index = true;
    desc.options.index_granularity = 8192;
    
    marble::ColumnFamilyHandle* handle = nullptr;
    auto status = impl_->db_->CreateColumnFamily(desc, &handle);
    if (!status.ok()) {
        return Status::IOError("CreateColumnFamily failed: " + status.ToString());
    }
    
    impl_->column_families_[table_name] = handle;
    return Status::OK();
}

Status MarbleDBStoreBackend::ListTables(std::vector<std::string>* table_names) {
    if (!impl_->is_open_ || !impl_->db_) {
        return Status::InvalidArgument("Backend not open");
    }
    
    if (!table_names) {
        return Status::InvalidArgument("table_names pointer is null");
    }
    
    *table_names = impl_->db_->ListColumnFamilies();
    return Status::OK();
}

Status MarbleDBStoreBackend::InsertBatch(const std::string& table_name,
                                        const std::shared_ptr<arrow::RecordBatch>& batch) {
    if (!impl_->is_open_ || !impl_->db_) {
        return Status::InvalidArgument("Backend not open");
    }
    
    if (!batch) {
        return Status::InvalidArgument("Batch is null");
    }
    
    auto status = impl_->db_->InsertBatch(table_name, batch);
    if (!status.ok()) {
        return Status::IOError("InsertBatch failed: " + status.ToString());
    }
    return Status::OK();
}

Status MarbleDBStoreBackend::ScanTable(const std::string& table_name,
                                       std::shared_ptr<arrow::Table>* table) {
    if (!impl_->is_open_ || !impl_->db_) {
        return Status::InvalidArgument("Backend not open");
    }
    
    if (!table) {
        return Status::InvalidArgument("table pointer is null");
    }
    
    std::unique_ptr<marble::QueryResult> result;
    auto status = impl_->db_->ScanTable(table_name, &result);
    if (!status.ok()) {
        return Status::IOError("ScanTable failed: " + status.ToString());
    }
    
    // Collect batches from QueryResult
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    std::shared_ptr<arrow::RecordBatch> batch;
    
    while (result->HasNext()) {
        auto next_status = result->Next(&batch);
        if (next_status.ok() && batch) {
            batches.push_back(batch);
        } else {
            break;
        }
    }
    
    if (batches.empty()) {
        // Empty table - try to get schema from QueryResult
        // If schema() returns nullptr, create empty table with empty schema
        auto schema = result->schema();
        if (schema) {
            *table = arrow::Table::Make(schema, {});
        } else {
            *table = arrow::Table::Make(arrow::schema({}), {});
        }
    } else {
        // Create table from batches
        auto schema = batches[0]->schema();
        auto table_result = arrow::Table::FromRecordBatches(schema, batches);
        if (!table_result.ok()) {
            return Status::IOError("Table::FromRecordBatches failed: " + table_result.status().ToString());
        }
        *table = table_result.ValueOrDie();
    }
    
    return Status::OK();
}

Status MarbleDBStoreBackend::ScanRange(const std::string& table_name,
                                      const std::string& start_key,
                                      const std::string& end_key,
                                      std::shared_ptr<arrow::Table>* table) {
    // For now, use iterator-based approach
    // TODO: Optimize if MarbleDB has better range scan API
    std::unique_ptr<Iterator> iterator;
    auto status = NewIterator(table_name, start_key, end_key, &iterator);
    if (!status.ok()) {
        return status;
    }
    
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    std::shared_ptr<arrow::RecordBatch> batch;
    
    while (iterator->Valid()) {
        auto get_status = iterator->GetBatch(&batch);
        if (get_status.ok() && batch) {
            batches.push_back(batch);
        }
        iterator->Next();
    }
    
    if (batches.empty()) {
        *table = arrow::Table::Make(arrow::schema({}), {});
    } else {
        auto schema = batches[0]->schema();
        auto table_result = arrow::Table::FromRecordBatches(schema, batches);
        if (!table_result.ok()) {
            return Status::IOError("Table::FromRecordBatches failed");
        }
        *table = table_result.ValueOrDie();
    }
    
    return Status::OK();
}

Status MarbleDBStoreBackend::DeleteRange(const std::string& table_name,
                                         const std::string& start_key,
                                         const std::string& end_key) {
    if (!impl_->is_open_ || !impl_->db_) {
        return Status::InvalidArgument("Backend not open");
    }
    
    marble::WriteOptions write_opts;
    marble::Key begin_key, end_key_cpp;
    begin_key.set_string(start_key);
    end_key_cpp.set_string(end_key);
    
    auto it = impl_->column_families_.find(table_name);
    if (it != impl_->column_families_.end()) {
        auto status = impl_->db_->DeleteRange(write_opts, it->second, begin_key, end_key_cpp);
        if (!status.ok()) {
            return Status::IOError("DeleteRange failed: " + status.ToString());
        }
    } else {
        auto status = impl_->db_->DeleteRange(write_opts, begin_key, end_key_cpp);
        if (!status.ok()) {
            return Status::IOError("DeleteRange failed: " + status.ToString());
        }
    }
    
    return Status::OK();
}

// Iterator implementation
class MarbleDBStoreBackend::IteratorImpl : public StoreBackend::Iterator {
public:
    std::unique_ptr<marble::Iterator> iterator_;
    
    IteratorImpl(std::unique_ptr<marble::Iterator> iter) : iterator_(std::move(iter)) {}
    
    bool Valid() override {
        return iterator_ && iterator_->Valid();
    }
    
    void Next() override {
        if (iterator_) {
            iterator_->Next();
        }
    }
    
    Status GetBatch(std::shared_ptr<arrow::RecordBatch>* batch) override {
        if (!iterator_ || !iterator_->Valid()) {
            return Status::InvalidArgument("Iterator not valid");
        }
        
        // TODO: Convert Record to RecordBatch
        // For now, return error
        return Status::NotSupported("Iterator GetBatch not yet implemented");
    }
};

Status MarbleDBStoreBackend::NewIterator(const std::string& table_name,
                                         const std::string& start_key,
                                         const std::string& end_key,
                                         std::unique_ptr<Iterator>* iterator) {
    if (!impl_->is_open_ || !impl_->db_) {
        return Status::InvalidArgument("Backend not open");
    }
    
    marble::ReadOptions read_opts;
    marble::KeyRange range;
    
    if (!start_key.empty()) {
        range.start_key = std::make_shared<marble::Key>();
        range.start_key->set_string(start_key);
        range.include_start = true;
    }
    
    if (!end_key.empty()) {
        range.end_key = std::make_shared<marble::Key>();
        range.end_key->set_string(end_key);
        range.include_end = false;
    }
    
    std::unique_ptr<marble::Iterator> marble_iter;
    auto status = impl_->db_->NewIterator(table_name, read_opts, range, &marble_iter);
    if (!status.ok()) {
        return Status::IOError("NewIterator failed: " + status.ToString());
    }
    
    *iterator = std::make_unique<IteratorImpl>(std::move(marble_iter));
    return Status::OK();
}

} // namespace storage
} // namespace sabot

