#include "marble/sstable_arrow.h"
#include <arrow/builder.h>
#include <arrow/compute/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/util/compression.h>
#include <nlohmann/json.hpp>
#include <algorithm>
#include <numeric>

namespace marble {

//==============================================================================
// ColumnStatistics Implementation
//==============================================================================

bool ColumnStatistics::CanSatisfyPredicate(const ColumnPredicate& predicate) const {
    if (predicate.column_name != column_name) {
        return true;  // Not our column, can't eliminate
    }

    if (!min_value || !max_value) {
        return true;  // No stats available, assume it can satisfy
    }

    // For now, implement basic range checks
    // This could be extended with more sophisticated predicate evaluation
    switch (predicate.type) {
        case PredicateType::kLessThan: {
            // If max_value >= predicate.value, some rows might satisfy
            // For now, be conservative and allow the scan
            return true;
        }
        case PredicateType::kGreaterThan: {
            // For now, be conservative and allow the scan
            return true;
        }
        case PredicateType::kEqual: {
            // For now, be conservative and allow the scan
            return true;
        }
        case PredicateType::kBetween: {
            // For now, be conservative and allow the scan
            return true;
        }
        default:
            // For other predicate types, assume they can be satisfied
            break;
    }

    return true;
}

//==============================================================================
// ArrowPredicateEvaluator Implementation
//==============================================================================

ArrowPredicateEvaluator::ArrowPredicateEvaluator(const std::vector<ColumnPredicate>& predicates)
    : predicates_(predicates) {
    // Pre-compile predicates for performance
    for (const auto& pred : predicates_) {
        CompiledPredicate compiled;
        compiled.column_name = pred.column_name;
        compiled.value = pred.value;

        switch (pred.type) {
            case PredicateType::kEqual:
                compiled.op = arrow::compute::CompareOperator::EQUAL;
                break;
            case PredicateType::kNotEqual:
                compiled.op = arrow::compute::CompareOperator::NOT_EQUAL;
                break;
            case PredicateType::kLessThan:
                compiled.op = arrow::compute::CompareOperator::LESS;
                break;
            case PredicateType::kLessThanOrEqual:
                compiled.op = arrow::compute::CompareOperator::LESS_EQUAL;
                break;
            case PredicateType::kGreaterThan:
                compiled.op = arrow::compute::CompareOperator::GREATER;
                break;
            case PredicateType::kGreaterThanOrEqual:
                compiled.op = arrow::compute::CompareOperator::GREATER_EQUAL;
                break;
            default:
                // For complex predicates, we'll handle them differently
                continue;
        }

        compiled_predicates_.push_back(compiled);
    }
}

Status ArrowPredicateEvaluator::EvaluateBatch(
    const std::shared_ptr<arrow::RecordBatch>& batch,
    std::shared_ptr<arrow::BooleanArray>* mask) const {

    if (!HasPredicates()) {
        // No predicates, all rows match
        arrow::BooleanBuilder builder;
        ARROW_RETURN_NOT_OK(builder.AppendValues(std::vector<bool>(batch->num_rows(), true)));
        ARROW_ASSIGN_OR_RAISE(auto array, builder.Finish());
        *mask = std::static_pointer_cast<arrow::BooleanArray>(array);
        return Status::OK();
    }

    // Start with all rows matching
    std::vector<bool> result_mask(batch->num_rows(), true);

    for (const auto& compiled : compiled_predicates_) {
        auto column = batch->GetColumnByName(compiled.column_name);
        if (!column) {
            return Status::InvalidArgument("Column not found: " + compiled.column_name);
        }

        // For now, skip Arrow compute evaluations and use simple filtering
        // TODO: Re-enable once Arrow compute API is properly integrated
        // Just keep all rows for now
    }

    // Handle complex predicates (BETWEEN, IN, LIKE, etc.)
    // For now, skip complex predicates - TODO: Re-implement with correct Arrow API

    // Build the boolean mask array
    arrow::BooleanBuilder builder;
    ARROW_RETURN_NOT_OK(builder.AppendValues(result_mask));
    ARROW_ASSIGN_OR_RAISE(auto array, builder.Finish());
    *mask = std::static_pointer_cast<arrow::BooleanArray>(array);
    return Status::OK();
}

Status ArrowPredicateEvaluator::FilterBatch(
    const std::shared_ptr<arrow::RecordBatch>& batch,
    std::shared_ptr<arrow::RecordBatch>* filtered_batch) const {

    std::shared_ptr<arrow::BooleanArray> mask;
    ARROW_RETURN_NOT_OK(EvaluateBatch(batch, &mask));

    // Filter the batch using the mask
    // TODO: Re-enable Arrow compute::Filter when API is updated
    // For now, just return the original batch
    *filtered_batch = batch;

    return Status::OK();
}

//==============================================================================
// ArrowColumnProjector Implementation
//==============================================================================

ArrowColumnProjector::ArrowColumnProjector(const std::vector<std::string>& columns)
    : projected_columns_(columns) {
    needs_projection_ = !columns.empty();
}

Status ArrowColumnProjector::ProjectBatch(
    const std::shared_ptr<arrow::RecordBatch>& batch,
    std::shared_ptr<arrow::RecordBatch>* projected_batch) const {

    if (!NeedsProjection()) {
        *projected_batch = batch;
        return Status::OK();
    }

    std::vector<int> indices;
    ARROW_RETURN_NOT_OK(GetProjectionIndices(batch->schema(), &indices));

    // Select the columns
    std::vector<std::shared_ptr<arrow::Array>> columns;
    columns.reserve(indices.size());

    for (int idx : indices) {
        columns.push_back(batch->column(idx));
    }

    auto projected_schema = GetProjectedSchema(batch->schema());
    *projected_batch = arrow::RecordBatch::Make(projected_schema, batch->num_rows(), columns);

    return Status::OK();
}

std::shared_ptr<arrow::Schema> ArrowColumnProjector::GetProjectedSchema(
    const std::shared_ptr<arrow::Schema>& input_schema) const {

    if (!NeedsProjection()) {
        return input_schema;
    }

    std::vector<std::shared_ptr<arrow::Field>> fields;
    for (const auto& col_name : projected_columns_) {
        auto field = input_schema->GetFieldByName(col_name);
        if (field) {
            fields.push_back(field);
        }
    }

    return arrow::schema(fields);
}

bool ArrowColumnProjector::NeedsProjection() const {
    return needs_projection_;
}

Status ArrowColumnProjector::GetProjectionIndices(
    const std::shared_ptr<arrow::Schema>& schema,
    std::vector<int>* indices) const {

    indices->clear();

    if (!NeedsProjection()) {
        // Return all indices
        indices->resize(schema->num_fields());
        std::iota(indices->begin(), indices->end(), 0);
        return Status::OK();
    }

    for (const auto& col_name : projected_columns_) {
        auto field_index = schema->GetFieldIndex(col_name);
        if (field_index == -1) {
            return Status::InvalidArgument("Column not found: " + col_name);
        }
        indices->push_back(field_index);
    }

    return Status::OK();
}

//==============================================================================
// ArrowRecordBatchIterator Implementation
//==============================================================================

class ArrowRecordBatchIteratorImpl : public ArrowRecordBatchIterator {
public:
    ArrowRecordBatchIteratorImpl(
        std::shared_ptr<arrow::Schema> schema,
        std::vector<std::shared_ptr<arrow::RecordBatch>> batches,
        std::unique_ptr<ArrowPredicateEvaluator> evaluator,
        std::unique_ptr<ArrowColumnProjector> projector)
        : schema_(std::move(schema))
        , batches_(std::move(batches))
        , evaluator_(std::move(evaluator))
        , projector_(std::move(projector))
        , current_batch_(0) {}

    bool HasNext() const override {
        return current_batch_ < batches_.size();
    }

    Status Next(std::shared_ptr<arrow::RecordBatch>* batch) override {
        if (!HasNext()) {
            return Status::InvalidArgument("No more batches");
        }

        auto current_batch = batches_[current_batch_++];

        // Apply predicate filtering if needed
        std::shared_ptr<arrow::RecordBatch> filtered_batch;
        if (evaluator_ && evaluator_->HasPredicates()) {
            ARROW_RETURN_NOT_OK(evaluator_->FilterBatch(current_batch, &filtered_batch));
        } else {
            filtered_batch = current_batch;
        }

        // Apply column projection if needed
        if (projector_ && projector_->NeedsProjection()) {
            ARROW_RETURN_NOT_OK(projector_->ProjectBatch(filtered_batch, batch));
        } else {
            *batch = filtered_batch;
        }

        return Status::OK();
    }

    std::shared_ptr<arrow::Schema> schema() const override {
        if (projector_ && projector_->NeedsProjection()) {
            return projector_->GetProjectedSchema(schema_);
        }
        return schema_;
    }

    int64_t EstimatedRemainingRows() const override {
        int64_t remaining = 0;
        for (size_t i = current_batch_; i < batches_.size(); ++i) {
            remaining += batches_[i]->num_rows();
        }
        return remaining;
    }

    Status SkipRows(int64_t num_rows) override {
        // Simple implementation - skip entire batches until we reach the target
        int64_t skipped = 0;
        while (skipped < num_rows && HasNext()) {
            auto batch = batches_[current_batch_];
            if (skipped + batch->num_rows() <= num_rows) {
                skipped += batch->num_rows();
                current_batch_++;
            } else {
                // Need to partially skip this batch - this would require
                // modifying the batch, which is complex. For now, just move forward.
                break;
            }
        }
        return Status::OK();
    }

private:
    std::shared_ptr<arrow::Schema> schema_;
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches_;
    std::unique_ptr<ArrowPredicateEvaluator> evaluator_;
    std::unique_ptr<ArrowColumnProjector> projector_;
    size_t current_batch_;
};

//==============================================================================
// ArrowSSTable Implementation
//==============================================================================

class ArrowSSTableImpl : public ArrowSSTable {
public:
    ArrowSSTableImpl(const std::string& filepath,
                     const SSTableMetadata& metadata,
                     std::shared_ptr<arrow::Schema> schema,
                     std::vector<std::shared_ptr<arrow::RecordBatch>> batches,
                     std::shared_ptr<FileSystem> fs);

    ~ArrowSSTableImpl() override = default;

    // ArrowSSTable interface
    const SSTableMetadata& GetMetadata() const override { return metadata_; }
    std::shared_ptr<arrow::Schema> GetArrowSchema() const override { return schema_; }

    Status ScanWithPushdown(
        const std::vector<std::string>& projection_columns,
        const std::vector<ColumnPredicate>& predicates,
        int64_t batch_size,
        std::unique_ptr<ArrowRecordBatchIterator>* iterator) override;

    Status CountRows(
        const std::vector<ColumnPredicate>& predicates,
        int64_t* count) override;

    Status GetColumnStats(
        const std::string& column_name,
        ColumnStatistics* stats) override;

    Status CanSatisfyPredicates(
        const std::vector<ColumnPredicate>& predicates,
        bool* can_satisfy) override;

    std::string GetFilePath() const override { return filepath_; }
    uint64_t GetFileSize() const override { return metadata_.file_size; }
    size_t GetMemoryUsage() const override;
    Status Validate() const override;

private:
    std::string filepath_;
    SSTableMetadata metadata_;
    std::shared_ptr<arrow::Schema> schema_;
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches_;
    std::shared_ptr<FileSystem> fs_;

    // Cached column statistics
    std::unordered_map<std::string, ColumnStatistics> column_stats_cache_;
};

ArrowSSTableImpl::ArrowSSTableImpl(const std::string& filepath,
                                   const SSTableMetadata& metadata,
                                   std::shared_ptr<arrow::Schema> schema,
                                   std::vector<std::shared_ptr<arrow::RecordBatch>> batches,
                                   std::shared_ptr<FileSystem> fs)
    : filepath_(filepath),
      metadata_(metadata),
      schema_(std::move(schema)),
      batches_(std::move(batches)),
      fs_(std::move(fs)) {
}

Status ArrowSSTableImpl::ScanWithPushdown(
    const std::vector<std::string>& projection_columns,
    const std::vector<ColumnPredicate>& predicates,
    int64_t batch_size,
    std::unique_ptr<ArrowRecordBatchIterator>* iterator) {

    // Create evaluator and projector
    auto evaluator = CreateArrowPredicateEvaluator(predicates);
    auto projector = CreateArrowColumnProjector(projection_columns);

    // Create iterator
    *iterator = std::make_unique<ArrowRecordBatchIteratorImpl>(
        schema_, batches_, std::move(evaluator), std::move(projector));

    return Status::OK();
}

Status ArrowSSTableImpl::CountRows(
    const std::vector<ColumnPredicate>& predicates,
    int64_t* count) {

    *count = 0;

    // Quick check using column statistics
    bool can_satisfy = true;
    ARROW_RETURN_NOT_OK(CanSatisfyPredicates(predicates, &can_satisfy));
    if (!can_satisfy) {
        return Status::OK();  // No rows can satisfy predicates
    }

    // If no predicates or simple case, return total count
    if (predicates.empty()) {
        for (const auto& batch : batches_) {
            *count += batch->num_rows();
        }
        return Status::OK();
    }

    // Otherwise, need to scan and count
    auto evaluator = CreateArrowPredicateEvaluator(predicates);
    for (const auto& batch : batches_) {
        std::shared_ptr<arrow::BooleanArray> mask;
        ARROW_RETURN_NOT_OK(evaluator->EvaluateBatch(batch, &mask));

        for (int64_t i = 0; i < mask->length(); ++i) {
            if (mask->Value(i)) {
                (*count)++;
            }
        }
    }

    return Status::OK();
}

Status ArrowSSTableImpl::GetColumnStats(
    const std::string& column_name,
    ColumnStatistics* stats) {

    // Check cache first
    auto it = column_stats_cache_.find(column_name);
    if (it != column_stats_cache_.end()) {
        *stats = it->second;
        return Status::OK();
    }

    // Build statistics from all batches
    std::vector<std::shared_ptr<arrow::Array>> chunks;

    for (const auto& batch : batches_) {
        auto column = batch->GetColumnByName(column_name);
        if (column) {
            // Arrays don't have chunks - just add the array itself
            chunks.push_back(column);
        }
    }

    if (chunks.empty()) {
        return Status::InvalidArgument("Column not found: " + column_name);
    }

    auto chunked_array = std::make_shared<arrow::ChunkedArray>(chunks);
    ARROW_RETURN_NOT_OK(BuildColumnStatistics(chunked_array, column_name, stats));

    // Cache the result
    column_stats_cache_[column_name] = *stats;

    return Status::OK();
}

Status ArrowSSTableImpl::CanSatisfyPredicates(
    const std::vector<ColumnPredicate>& predicates,
    bool* can_satisfy) {

    *can_satisfy = true;

    for (const auto& pred : predicates) {
        ColumnStatistics stats(pred.column_name);
        ARROW_RETURN_NOT_OK(GetColumnStats(pred.column_name, &stats));

        if (!stats.CanSatisfyPredicate(pred)) {
            *can_satisfy = false;
            return Status::OK();
        }
    }

    return Status::OK();
}

size_t ArrowSSTableImpl::GetMemoryUsage() const {
    size_t usage = sizeof(*this);
    usage += filepath_.capacity();
    usage += schema_->num_fields() * sizeof(std::shared_ptr<arrow::Field>);

    for (const auto& batch : batches_) {
        usage += batch->num_rows() * batch->num_columns() * sizeof(void*);  // Rough estimate
    }

    return usage;
}

Status ArrowSSTableImpl::Validate() const {
    // Basic validation
    if (batches_.empty()) {
        return Status::InvalidArgument("SSTable has no data batches");
    }

    for (const auto& batch : batches_) {
        if (!batch) {
            return Status::InvalidArgument("Null batch found");
        }
        if (batch->schema()->num_fields() != schema_->num_fields()) {
            return Status::InvalidArgument("Batch schema mismatch");
        }
    }

    return Status::OK();
}

//==============================================================================
// Utility Functions
//==============================================================================

Status BuildColumnStatistics(
    const std::shared_ptr<arrow::ChunkedArray>& column,
    const std::string& column_name,
    ColumnStatistics* stats) {

    stats->column_name = column_name;
    stats->distinct_count = -1;  // Not computed for now

    if (column->length() == 0) {
        return Status::OK();
    }

    // Compute basic statistics
    // TODO: Re-enable Arrow compute statistics when API is updated
    // For now, provide placeholder statistics
    stats->null_count = 0;
    stats->has_nulls = false;
    stats->min_value = nullptr;
    stats->max_value = nullptr;
    stats->mean = 0.0;
    stats->std_dev = 0.0;
    stats->min_length = 0;
    stats->max_length = 0;
    stats->avg_length = 0.0;

    return Status::OK();
}

//==============================================================================
// Factory Functions
//==============================================================================

std::unique_ptr<ArrowSSTableManager> CreateArrowSSTableManager(
    std::shared_ptr<FileSystem> fs) {
    // TODO: Implement ArrowSSTableManagerImpl
    return nullptr;
}

std::unique_ptr<ArrowPredicateEvaluator> CreateArrowPredicateEvaluator(
    const std::vector<ColumnPredicate>& predicates) {
    return std::make_unique<ArrowPredicateEvaluator>(predicates);
}

std::unique_ptr<ArrowColumnProjector> CreateArrowColumnProjector(
    const std::vector<std::string>& columns) {
    return std::make_unique<ArrowColumnProjector>(columns);
}

} // namespace marble
