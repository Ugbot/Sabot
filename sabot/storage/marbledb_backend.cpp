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
    std::unique_ptr<marble::MarbleDB> db_;
    marble::ColumnFamilyHandle* state_cf_ = nullptr;
    std::string db_path_;
    bool is_open_ = false;
    const std::string STATE_CF_NAME = "__sabot_state__";
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
    
    // Open MarbleDB (same as StoreBackend)
    auto marble_status = marble::OpenDatabase(config.path, &impl_->db_);
    if (!marble_status.ok()) {
        return Status::InternalError("Failed to open MarbleDB: " + marble_status.ToString());
    }
    
    // Create column family for state storage with schema {key: string, value: binary}
    auto schema = arrow::schema({
        arrow::field("key", arrow::utf8()),
        arrow::field("value", arrow::binary())
    });
    
    marble::ColumnFamilyOptions cf_options;
    cf_options.schema = schema;
    cf_options.enable_bloom_filter = config.enable_bloom_filter;
    cf_options.enable_sparse_index = config.enable_sparse_index;
    cf_options.index_granularity = config.index_granularity;
    
    marble::ColumnFamilyDescriptor cf_desc(impl_->STATE_CF_NAME, cf_options);
    
    marble_status = impl_->db_->CreateColumnFamily(cf_desc, &impl_->state_cf_);
    if (!marble_status.ok()) {
        return Status::InternalError("Failed to create state column family: " + marble_status.ToString());
    }
    
    impl_->is_open_ = true;
    return Status::OK();
}

Status MarbleDBStateBackend::Close() {
    if (!impl_->is_open_) {
        return Status::OK();
    }
    
    if (impl_->db_) {
        auto status = impl_->db_->Flush();
        impl_->db_.reset();
        if (!status.ok()) {
            return Status::IOError("Flush failed: " + status.ToString());
        }
    }
    
    impl_->state_cf_ = nullptr;
    impl_->is_open_ = false;
    return Status::OK();
}

Status MarbleDBStateBackend::Flush() {
    if (!impl_->is_open_ || !impl_->db_) {
        return Status::InvalidArgument("Backend not open");
    }
    
    auto status = impl_->db_->Flush();
    if (!status.ok()) {
        return Status::IOError("Flush failed: " + status.ToString());
    }
    return Status::OK();
}

Status MarbleDBStateBackend::Put(const std::string& key, const std::string& value) {
    if (!impl_->is_open_ || !impl_->db_) {
        return Status::InvalidArgument("Backend not open");
    }
    
    // Create Arrow RecordBatch with {key, value}
    arrow::StringBuilder key_builder;
    arrow::BinaryBuilder value_builder;
    
    auto append_status = key_builder.Append(key);
    if (!append_status.ok()) {
        return Status::InvalidArgument("Failed to append key: " + append_status.ToString());
    }
    
    append_status = value_builder.Append(value);
    if (!append_status.ok()) {
        return Status::InvalidArgument("Failed to append value: " + append_status.ToString());
    }
    
    std::shared_ptr<arrow::Array> key_array, value_array;
    auto finish_status = key_builder.Finish(&key_array);
    if (!finish_status.ok()) {
        return Status::InvalidArgument("Failed to finish key array: " + finish_status.ToString());
    }
    
    finish_status = value_builder.Finish(&value_array);
    if (!finish_status.ok()) {
        return Status::InvalidArgument("Failed to finish value array: " + finish_status.ToString());
    }
    
    auto schema = arrow::schema({
        arrow::field("key", arrow::utf8()),
        arrow::field("value", arrow::binary())
    });
    
    auto batch = arrow::RecordBatch::Make(schema, 1, {key_array, value_array});
    
    auto status = impl_->db_->InsertBatch(impl_->STATE_CF_NAME, batch);
    if (!status.ok()) {
        return Status::IOError("InsertBatch failed: " + status.ToString());
    }
    return Status::OK();
}

Status MarbleDBStateBackend::Get(const std::string& key, std::string* value) {
    if (!impl_->is_open_ || !impl_->db_) {
        return Status::InvalidArgument("Backend not open");
    }
    
    if (!value) {
        return Status::InvalidArgument("value pointer is null");
    }
    
    // Scan table for the specific key
    std::unique_ptr<marble::QueryResult> result;
    auto status = impl_->db_->ScanTable(impl_->STATE_CF_NAME, &result);
    if (!status.ok()) {
        return Status::IOError("ScanTable failed: " + status.ToString());
    }
    
    // Iterate through results to find matching key
    std::shared_ptr<arrow::RecordBatch> batch;
    while (result->HasNext()) {
        auto next_status = result->Next(&batch);
        if (next_status.ok() && batch && batch->num_rows() > 0) {
            // Get key column
            auto key_array = std::static_pointer_cast<arrow::StringArray>(batch->column(0));
            auto value_array = std::static_pointer_cast<arrow::BinaryArray>(batch->column(1));
            
            // Search for matching key
            for (int64_t i = 0; i < batch->num_rows(); i++) {
                if (key_array->GetString(i) == key) {
                    *value = value_array->GetString(i);
                    return Status::OK();
                }
            }
        }
    }
    
    return Status::NotFound("Key not found: " + key);
}

Status MarbleDBStateBackend::Delete(const std::string& key) {
    if (!impl_->is_open_ || !impl_->db_) {
        return Status::InvalidArgument("Backend not open");
    }
    
    // For now, use Put with empty value to mark as deleted
    // TODO: Implement proper deletion via DeleteRange or tombstone
    return Put(key, "");
}

Status MarbleDBStateBackend::Exists(const std::string& key, bool* exists) {
    if (!impl_->is_open_ || !impl_->db_) {
        return Status::InvalidArgument("Backend not open");
    }
    
    if (!exists) {
        return Status::InvalidArgument("exists pointer is null");
    }
    
    std::string value;
    auto status = Get(key, &value);
    *exists = status.ok();
    return Status::OK();
}

Status MarbleDBStateBackend::MultiGet(const std::vector<std::string>& keys,
                                      std::vector<std::string>* values) {
    if (!impl_->is_open_ || !impl_->db_) {
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
    if (!impl_->is_open_ || !impl_->db_) {
        return Status::InvalidArgument("Backend not open");
    }
    
    // TODO: Implement using MarbleDB::DeleteRange when available
    // For now, scan and delete individually
    std::unique_ptr<marble::QueryResult> result;
    auto status = impl_->db_->ScanTable(impl_->STATE_CF_NAME, &result);
    if (!status.ok()) {
        return Status::IOError("ScanTable failed: " + status.ToString());
    }
    
    // Collect keys in range, then delete
    std::vector<std::string> keys_to_delete;
    std::shared_ptr<arrow::RecordBatch> batch;
    while (result->HasNext()) {
        auto next_status = result->Next(&batch);
        if (next_status.ok() && batch && batch->num_rows() > 0) {
            auto key_array = std::static_pointer_cast<arrow::StringArray>(batch->column(0));
            for (int64_t i = 0; i < batch->num_rows(); i++) {
                std::string key = key_array->GetString(i);
                if (key >= start_key && key < end_key) {
                    keys_to_delete.push_back(key);
                }
            }
        }
    }
    
    // Delete each key
    for (const auto& key : keys_to_delete) {
        Delete(key);
    }
    
    return Status::OK();
}

Status MarbleDBStateBackend::Scan(const std::string& start_key,
                                  const std::string& end_key,
                                  ScanCallback callback) {
    if (!impl_->is_open_ || !impl_->db_) {
        return Status::InvalidArgument("Backend not open");
    }
    
    // Scan table and filter keys in range
    std::unique_ptr<marble::QueryResult> result;
    auto status = impl_->db_->ScanTable(impl_->STATE_CF_NAME, &result);
    if (!status.ok()) {
        return Status::IOError("ScanTable failed: " + status.ToString());
    }
    
    std::shared_ptr<arrow::RecordBatch> batch;
    while (result->HasNext()) {
        auto next_status = result->Next(&batch);
        if (next_status.ok() && batch && batch->num_rows() > 0) {
            auto key_array = std::static_pointer_cast<arrow::StringArray>(batch->column(0));
            auto value_array = std::static_pointer_cast<arrow::BinaryArray>(batch->column(1));
            
            for (int64_t i = 0; i < batch->num_rows(); i++) {
                std::string key = key_array->GetString(i);
                if (key >= start_key && (end_key.empty() || key < end_key)) {
                    std::string value = value_array->GetString(i);
                    if (!callback(key, value)) {
                        return Status::OK(); // Callback requested stop
                    }
                }
            }
        }
    }
    
    return Status::OK();
}

Status MarbleDBStateBackend::CreateCheckpoint(std::string* checkpoint_path) {
    if (!impl_->is_open_ || !impl_->db_) {
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
    std::unordered_map<std::string, std::vector<std::shared_ptr<arrow::RecordBatch>>> cached_batches_;
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
    marble::ColumnFamilyOptions options;
    options.schema = schema;
    options.enable_bloom_filter = true;
    options.enable_sparse_index = true;
    options.index_granularity = 8192;
    
    marble::ColumnFamilyDescriptor desc(table_name, options);
    
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
            *table = arrow::Table::Make(schema, std::vector<std::shared_ptr<arrow::ChunkedArray>>{});
        } else {
            *table = arrow::Table::Make(arrow::schema({}), std::vector<std::shared_ptr<arrow::ChunkedArray>>{});
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
        *table = arrow::Table::Make(arrow::schema({}), std::vector<std::shared_ptr<arrow::ChunkedArray>>{});
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
    
    // TODO: Implement DeleteRange with proper Key creation
    // This requires KeyFactory or schema to create Key objects from strings
    // For now, return NotSupported
    return Status::NotSupported("DeleteRange with string keys not yet implemented - need KeyFactory");
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
    
    // KeyRange constructor: KeyRange(start, start_inclusive, end, end_inclusive)
    // Key is abstract, need to use a concrete implementation
    // For string keys, we'll need to create them via the schema or use a helper
    // For now, use KeyRange::All() or KeyRange::StartAt/EndAt
    marble::KeyRange range = marble::KeyRange::All();
    
    // TODO: Implement proper Key creation from strings
    // This requires access to the table's KeyFactory or schema
    // For now, use All() which scans everything
    
    std::unique_ptr<marble::Iterator> marble_iter;
    auto status = impl_->db_->NewIterator(table_name, read_opts, range, &marble_iter);
    if (!status.ok()) {
        return Status::IOError("NewIterator failed: " + status.ToString());
    }
    
    *iterator = std::make_unique<IteratorImpl>(std::move(marble_iter));
    return Status::OK();
}

Status MarbleDBStoreBackend::ScanTableBatches(const std::string& table_name,
                                              std::vector<std::shared_ptr<arrow::RecordBatch>>* batches) {
    if (!impl_->is_open_ || !impl_->db_) {
        return Status::InvalidArgument("Backend not open");
    }
    
    if (!batches) {
        return Status::InvalidArgument("batches pointer is null");
    }
    
    // Use the same logic as ScanTable but return batches directly
    std::unique_ptr<marble::QueryResult> result;
    auto status = impl_->db_->ScanTable(table_name, &result);
    if (!status.ok()) {
        return Status::IOError("ScanTable failed: " + status.ToString());
    }
    
    // Collect batches from QueryResult
    std::shared_ptr<arrow::RecordBatch> batch;
    while (result->HasNext()) {
        auto next_status = result->Next(&batch);
        if (next_status.ok() && batch) {
            batches->push_back(batch);
        } else {
            break;
        }
    }
    
    return Status::OK();
}

Status MarbleDBStoreBackend::GetBatchCount(const std::string& table_name, size_t* count) {
    if (!impl_->is_open_ || !impl_->db_) {
        return Status::InvalidArgument("Backend not open");
    }
    
    if (!count) {
        return Status::InvalidArgument("count pointer is null");
    }
    
    // Get batches and count them
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    auto status = ScanTableBatches(table_name, &batches);
    if (!status.ok()) {
        return status;
    }
    
    *count = batches.size();
    
    // Store batches in impl for GetBatchAt
    // TODO: This is a hack - should use cached QueryResult instead
    impl_->cached_batches_[table_name] = std::move(batches);
    
    return Status::OK();
}

Status MarbleDBStoreBackend::GetBatchAt(const std::string& table_name, size_t index,
                                        std::shared_ptr<arrow::RecordBatch>* batch) {
    if (!impl_->is_open_ || !impl_->db_) {
        return Status::InvalidArgument("Backend not open");
    }
    
    if (!batch) {
        return Status::InvalidArgument("batch pointer is null");
    }
    
    // Get from cached batches
    auto it = impl_->cached_batches_.find(table_name);
    if (it == impl_->cached_batches_.end()) {
        return Status::InvalidArgument("No cached batches - call GetBatchCount first");
    }
    
    if (index >= it->second.size()) {
        return Status::InvalidArgument("Index out of range");
    }
    
    *batch = it->second[index];
    return Status::OK();
}

Status MarbleDBStoreBackend::ClearBatchCache(const std::string& table_name) {
    impl_->cached_batches_.erase(table_name);
    return Status::OK();
}

} // namespace storage
} // namespace sabot

