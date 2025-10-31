#include "marble/async.h"
#include "marble/db.h"
#include "marble/query.h"
#include <future>
#include <thread>
#include <iostream>

namespace marble {

//==============================================================================
// AsyncMarbleDB Implementation
//==============================================================================

AsyncMarbleDB::AsyncMarbleDB()
    : worker_thread_(std::make_unique<std::thread>()) {
}

AsyncMarbleDB::~AsyncMarbleDB() {
    if (worker_thread_ && worker_thread_->joinable()) {
        worker_thread_->join();
    }
}

AsyncResult<Status> AsyncMarbleDB::Open(const std::string& path) {
    auto future = std::async(std::launch::async, [this, path]() {
        DBOptions options;
        options.db_path = path;
        options.create_if_missing = true;

        db_ = std::make_unique<MarbleDB>();
        std::unique_ptr<MarbleDB> db_ptr;
        Status status = MarbleDB::Open(options, nullptr, &db_ptr);
        if (status.ok()) {
            db_ = std::move(db_ptr);
        }
        return status;
    });

    return AsyncResult<Status>(std::move(future));
}

AsyncResult<Status> AsyncMarbleDB::Close() {
    auto future = std::async(std::launch::async, [this]() {
        if (!db_) {
            return Status::OK();
        }
        Status status = db_->Close();
        db_.reset();
        return status;
    });

    return AsyncResult<Status>(std::move(future));
}

AsyncResult<Status> AsyncMarbleDB::CreateColumnFamily(const std::string& name,
                                                     const std::shared_ptr<arrow::Schema>& schema) {
    auto future = std::async(std::launch::async, [this, name, schema]() {
        if (!db_) {
            return Status::NotFound("Database not open");
        }

        ColumnFamilyOptions options;
        options.schema = schema;
        options.enable_bloom_filter = true;
        options.enable_sparse_index = true;

        ColumnFamilyDescriptor descriptor(name, options);
        ColumnFamilyHandle* handle = nullptr;

        return db_->CreateColumnFamily(descriptor, &handle);
    });

    return AsyncResult<Status>(std::move(future));
}

AsyncResult<Status> AsyncMarbleDB::InsertBatch(const std::string& table_name,
                                              const std::shared_ptr<arrow::RecordBatch>& batch) {
    auto future = std::async(std::launch::async, [this, table_name, batch]() {
        if (!db_) {
            return Status::NotFound("Database not open");
        }
        return db_->InsertBatch(table_name, batch);
    });

    return AsyncResult<Status>(std::move(future));
}

AsyncResult<Status> AsyncMarbleDB::ScanTable(const std::string& table_name,
                                            std::unique_ptr<QueryResult>* result) {
    auto future = std::async(std::launch::async, [this, table_name, result]() {
        if (!db_) {
            return Status::NotFound("Database not open");
        }
        return db_->ScanTable(table_name, result);
    });

    return AsyncResult<Status>(std::move(future));
}

AsyncResult<std::unique_ptr<Iterator>> AsyncMarbleDB::NewIterator(const KeyRange& range) {
    auto future = std::async(std::launch::async, [this, range]() {
        if (!db_) {
            return std::unique_ptr<Iterator>(nullptr);
        }

        std::unique_ptr<Iterator> iterator;
        ReadOptions options;
        Status status = db_->NewIterator(options, range, &iterator);
        if (!status.ok()) {
            std::cerr << "Iterator creation failed: " << status.ToString() << std::endl;
            return std::unique_ptr<Iterator>(nullptr);
        }
        return iterator;
    });

    return AsyncResult<std::unique_ptr<Iterator>>(std::move(future));
}

std::vector<std::string> AsyncMarbleDB::ListColumnFamilies() const {
    if (!db_) return {};
    return db_->ListColumnFamilies();
}

//==============================================================================
// AsyncIterator Implementation
//==============================================================================

AsyncIterator::AsyncIterator(std::unique_ptr<Iterator> iterator)
    : iterator_(std::move(iterator)) {
}

AsyncIterator::~AsyncIterator() = default;

AsyncResult<bool> AsyncIterator::Valid() {
    auto future = std::async(std::launch::async, [this]() {
        if (!iterator_) return false;
        return iterator_->Valid();
    });
    return AsyncResult<bool>(std::move(future));
}

AsyncResult<void> AsyncIterator::Next() {
    auto future = std::async(std::launch::async, [this]() {
        if (iterator_) {
            iterator_->Next();
        }
    });
    return AsyncResult<void>(std::move(future));
}

AsyncResult<std::shared_ptr<Key>> AsyncIterator::Key() {
    auto future = std::async(std::launch::async, [this]() {
        if (!iterator_ || !iterator_->Valid()) return nullptr;
        return iterator_->key();
    });
    return AsyncResult<std::shared_ptr<Key>>(std::move(future));
}

AsyncResult<std::shared_ptr<Record>> AsyncIterator::Value() {
    auto future = std::async(std::launch::async, [this]() {
        if (!iterator_ || !iterator_->Valid()) return nullptr;
        return iterator_->value();
    });
    return AsyncResult<std::shared_ptr<Record>>(std::move(future));
}

AsyncResult<Status> AsyncIterator::Status() {
    auto future = std::async(std::launch::async, [this]() {
        if (!iterator_) return Status::NotFound("Iterator not initialized");
        return iterator_->status();
    });
    return AsyncResult<Status>(std::move(future));
}

//==============================================================================
// AsyncQueryResult Implementation
//==============================================================================

class AsyncTableQueryResult : public AsyncQueryResult {
public:
    AsyncTableQueryResult(std::unique_ptr<TableQueryResult> result)
        : result_(std::move(result)) {}

    AsyncResult<Status> NextBatch(std::shared_ptr<arrow::RecordBatch>* batch) override {
        auto future = std::async(std::launch::async, [this, batch]() {
            // For table results, return the entire table as one batch
            // In production, this would stream multiple batches
            if (consumed_) {
                return Status::OK();  // End of stream
            }

            consumed_ = true;

            std::shared_ptr<arrow::Table> table;
            Status status = result_->GetTable(&table);
            if (status.ok() && table && table->num_rows() > 0) {
                // Convert first chunk to RecordBatch
                auto batches = table->ToRecordBatches();
                if (!batches.ok()) {
                    return Status::InternalError("Failed to convert table to batches");
                }
                *batch = (*batches)[0];
            }
            return status;
        });

        return AsyncResult<Status>(std::move(future));
    }

    AsyncResult<bool> HasMore() override {
        auto future = std::async(std::launch::async, [this]() {
            return !consumed_;
        });
        return AsyncResult<bool>(std::move(future));
    }

    std::shared_ptr<arrow::Schema> Schema() const override {
        std::shared_ptr<arrow::Table> table;
        if (result_->GetTable(&table).ok() && table) {
            return table->schema();
        }
        return nullptr;
    }

private:
    std::unique_ptr<TableQueryResult> result_;
    bool consumed_ = false;
};

std::unique_ptr<AsyncQueryResult> MakeAsyncQueryResult(
    std::unique_ptr<QueryResult> sync_result) {

    // Check if it's a table result
    auto* table_result = dynamic_cast<TableQueryResult*>(sync_result.get());
    if (table_result) {
        auto async_result = std::make_unique<AsyncTableQueryResult>(
            std::unique_ptr<TableQueryResult>(table_result));
        sync_result.release();  // Ownership transferred
        return async_result;
    }

    // For other result types, return nullptr for now
    return nullptr;
}

} // namespace marble
