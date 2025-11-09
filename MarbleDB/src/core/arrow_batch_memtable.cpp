#include "marble/arrow_batch_memtable.h"
#include <arrow/compute/api.h>
#include <arrow/util/key_value_metadata.h>

namespace marble {

//==============================================================================
// ArrowBatchMemTable Implementation
//==============================================================================

ArrowBatchMemTable::ArrowBatchMemTable(
    std::shared_ptr<arrow::Schema> schema,
    const Config& config)
    : schema_(std::move(schema))
    , config_(config) {
}

Status ArrowBatchMemTable::PutBatch(const std::shared_ptr<arrow::RecordBatch>& batch) {
    if (!batch) {
        return Status::InvalidArgument("Batch is null");
    }

#ifdef NDEBUG
    // In release builds, skip schema check for performance (assume caller validates)
    // Schema is validated once when memtable is created
#else
    // In debug builds, validate schema on every call
    if (!batch->schema()->Equals(schema_)) {
        return Status::InvalidArgument("Batch schema does not match memtable schema");
    }
#endif

    std::lock_guard<std::mutex> lock(mutex_);

    // Fast batch size estimation using Arrow's API
    // This is faster than walking all buffers manually
    size_t batch_bytes = 0;
    auto result = arrow::ipc::GetRecordBatchSize(*batch);
    if (result.ok()) {
        batch_bytes = *result;
    } else {
        // Fallback: rough estimate based on num_rows and columns
        batch_bytes = batch->num_rows() * batch->num_columns() * 8;  // Rough estimate
    }

    // Store batch
    size_t batch_idx = batches_.size();
    batches_.push_back(batch);

    // Update statistics
    total_rows_ += batch->num_rows();
    total_bytes_ += batch_bytes;

    // Build row index if enabled
    if (config_.build_row_index) {
        auto status = BuildRowIndex(batch, batch_idx);
        if (!status.ok()) {
            return status;
        }
    }

    return Status::OK();
}

Status ArrowBatchMemTable::Get(const Key& key, std::shared_ptr<Record>* record) const {
    std::lock_guard<std::mutex> lock(mutex_);

    // Use row index for point lookup
    size_t key_hash = key.Hash();
    auto it = row_index_.find(key_hash);
    if (it == row_index_.end()) {
        return Status::NotFound("Key not found in memtable");
    }

    const auto& location = it->second;
    if (location.batch_idx >= batches_.size()) {
        return Status::InternalError("Invalid batch index in row_index");
    }

    auto batch = batches_[location.batch_idx];
    if (location.row_offset >= batch->num_rows()) {
        return Status::InternalError("Invalid row offset in row_index");
    }

    // Create a SimpleRecord from the row
    // Note: SimpleRecord wraps a RecordBatch + row offset
    *record = std::make_shared<SimpleRecord>(
        std::make_shared<Int64Key>(key_hash),
        batch,
        location.row_offset
    );

    return Status::OK();
}

Status ArrowBatchMemTable::GetBatches(
    std::vector<std::shared_ptr<arrow::RecordBatch>>* batches) const {

    std::lock_guard<std::mutex> lock(mutex_);

    // Zero-copy: return shared pointers to our batches
    *batches = batches_;

    return Status::OK();
}

Status ArrowBatchMemTable::ScanBatches(
    uint64_t start_key,
    uint64_t end_key,
    std::vector<std::shared_ptr<arrow::RecordBatch>>* batches) const {

    std::lock_guard<std::mutex> lock(mutex_);

    // For now, return all batches (filtering can be done by caller)
    // TODO: Implement per-batch key range filtering using batch metadata
    *batches = batches_;

    return Status::OK();
}

bool ArrowBatchMemTable::ShouldFlush() const {
    std::lock_guard<std::mutex> lock(mutex_);

    return total_bytes_ >= config_.max_bytes ||
           batches_.size() >= config_.max_batches;
}

void ArrowBatchMemTable::Clear() {
    std::lock_guard<std::mutex> lock(mutex_);

    batches_.clear();
    row_index_.clear();
    total_rows_ = 0;
    total_bytes_ = 0;
}

Status ArrowBatchMemTable::BuildRowIndex(
    const std::shared_ptr<arrow::RecordBatch>& batch,
    size_t batch_idx) {

    if (batch->num_columns() == 0 || batch->num_rows() == 0) {
        return Status::OK();  // Nothing to index
    }

    // Extract keys from first column
    auto key_column = batch->column(0);

    for (int64_t row_idx = 0; row_idx < batch->num_rows(); ++row_idx) {
        size_t key_hash = 0;
        auto status = ExtractKeyHash(batch, row_idx, &key_hash);
        if (!status.ok()) {
            continue;  // Skip rows with extraction errors
        }

        // Add to row index
        row_index_[key_hash] = RowLocation{batch_idx, row_idx};
    }

    return Status::OK();
}

Status ArrowBatchMemTable::ExtractKeyHash(
    const std::shared_ptr<arrow::RecordBatch>& batch,
    int64_t row_idx,
    size_t* key_hash) const {

    if (batch->num_columns() == 0) {
        return Status::InvalidArgument("Batch has no columns");
    }

    auto key_column = batch->column(0);

    // Extract key based on type
    switch (key_column->type()->id()) {
        case arrow::Type::INT64: {
            auto arr = std::static_pointer_cast<arrow::Int64Array>(key_column);
            if (arr->IsNull(row_idx)) {
                return Status::InvalidArgument("Null key");
            }
            *key_hash = std::hash<int64_t>{}(arr->Value(row_idx));
            break;
        }
        case arrow::Type::UINT64: {
            auto arr = std::static_pointer_cast<arrow::UInt64Array>(key_column);
            if (arr->IsNull(row_idx)) {
                return Status::InvalidArgument("Null key");
            }
            *key_hash = std::hash<uint64_t>{}(arr->Value(row_idx));
            break;
        }
        case arrow::Type::INT32: {
            auto arr = std::static_pointer_cast<arrow::Int32Array>(key_column);
            if (arr->IsNull(row_idx)) {
                return Status::InvalidArgument("Null key");
            }
            *key_hash = std::hash<int32_t>{}(arr->Value(row_idx));
            break;
        }
        case arrow::Type::STRING: {
            auto arr = std::static_pointer_cast<arrow::StringArray>(key_column);
            if (arr->IsNull(row_idx)) {
                return Status::InvalidArgument("Null key");
            }
            *key_hash = std::hash<std::string>{}(arr->GetString(row_idx));
            break;
        }
        default:
            return Status::InvalidArgument("Unsupported key type");
    }

    return Status::OK();
}

//==============================================================================
// ImmutableArrowBatchMemTable Implementation
//==============================================================================

ImmutableArrowBatchMemTable::ImmutableArrowBatchMemTable(ArrowBatchMemTable&& source)
    : schema_(source.schema_)
    , batches_(std::move(source.batches_))
    , row_index_(std::move(source.row_index_))
    , total_rows_(source.total_rows_)
    , total_bytes_(source.total_bytes_) {
}

Status ImmutableArrowBatchMemTable::Get(
    const Key& key,
    std::shared_ptr<Record>* record) const {

    size_t key_hash = key.Hash();
    auto it = row_index_.find(key_hash);
    if (it == row_index_.end()) {
        return Status::NotFound("Key not found in immutable memtable");
    }

    const auto& location = it->second;
    if (location.batch_idx >= batches_.size()) {
        return Status::InternalError("Invalid batch index");
    }

    auto batch = batches_[location.batch_idx];
    if (location.row_offset >= batch->num_rows()) {
        return Status::InternalError("Invalid row offset");
    }

    *record = std::make_shared<SimpleRecord>(
        std::make_shared<Int64Key>(key_hash),
        batch,
        location.row_offset
    );

    return Status::OK();
}

Status ImmutableArrowBatchMemTable::GetBatches(
    std::vector<std::shared_ptr<arrow::RecordBatch>>* batches) const {

    *batches = batches_;
    return Status::OK();
}

Status ImmutableArrowBatchMemTable::ScanBatches(
    uint64_t start_key,
    uint64_t end_key,
    std::vector<std::shared_ptr<arrow::RecordBatch>>* batches) const {

    // For now, return all batches
    *batches = batches_;
    return Status::OK();
}

Status ImmutableArrowBatchMemTable::ToRecordBatch(
    std::shared_ptr<arrow::RecordBatch>* combined) const {

    if (batches_.empty()) {
        return Status::InvalidArgument("No batches to concatenate");
    }

    if (batches_.size() == 1) {
        // Single batch - no concatenation needed
        *combined = batches_[0];
        return Status::OK();
    }

    // Concatenate multiple batches
    auto result = arrow::ConcatenateRecordBatches(batches_);
    if (!result.ok()) {
        return Status::InternalError("Failed to concatenate batches: " +
                                    result.status().ToString());
    }

    *combined = result.ValueOrDie();
    return Status::OK();
}

//==============================================================================
// Factory Function
//==============================================================================

std::unique_ptr<ArrowBatchMemTable> CreateArrowBatchMemTable(
    std::shared_ptr<arrow::Schema> schema,
    const ArrowBatchMemTable::Config& config) {

    return std::make_unique<ArrowBatchMemTable>(std::move(schema), config);
}

} // namespace marble
