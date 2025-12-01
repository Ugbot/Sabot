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
// Helper: Compare Arrow Scalars
//==============================================================================

// Returns: -1 if a < b, 0 if a == b, 1 if a > b
static int CompareScalars(const std::shared_ptr<arrow::Scalar>& a,
                          const std::shared_ptr<arrow::Scalar>& b) {
    if (!a || !b || !a->is_valid || !b->is_valid) {
        // Handle nulls: non-null > null
        if (a && a->is_valid && (!b || !b->is_valid)) return 1;
        if (b && b->is_valid && (!a || !a->is_valid)) return -1;
        return 0;
    }

    // Type must match
    if (a->type->id() != b->type->id()) {
        return 0;  // Cannot compare different types
    }

    switch (a->type->id()) {
        case arrow::Type::INT8: {
            auto va = std::static_pointer_cast<arrow::Int8Scalar>(a)->value;
            auto vb = std::static_pointer_cast<arrow::Int8Scalar>(b)->value;
            return (va < vb) ? -1 : (va > vb) ? 1 : 0;
        }
        case arrow::Type::INT16: {
            auto va = std::static_pointer_cast<arrow::Int16Scalar>(a)->value;
            auto vb = std::static_pointer_cast<arrow::Int16Scalar>(b)->value;
            return (va < vb) ? -1 : (va > vb) ? 1 : 0;
        }
        case arrow::Type::INT32: {
            auto va = std::static_pointer_cast<arrow::Int32Scalar>(a)->value;
            auto vb = std::static_pointer_cast<arrow::Int32Scalar>(b)->value;
            return (va < vb) ? -1 : (va > vb) ? 1 : 0;
        }
        case arrow::Type::INT64: {
            auto va = std::static_pointer_cast<arrow::Int64Scalar>(a)->value;
            auto vb = std::static_pointer_cast<arrow::Int64Scalar>(b)->value;
            return (va < vb) ? -1 : (va > vb) ? 1 : 0;
        }
        case arrow::Type::UINT8: {
            auto va = std::static_pointer_cast<arrow::UInt8Scalar>(a)->value;
            auto vb = std::static_pointer_cast<arrow::UInt8Scalar>(b)->value;
            return (va < vb) ? -1 : (va > vb) ? 1 : 0;
        }
        case arrow::Type::UINT16: {
            auto va = std::static_pointer_cast<arrow::UInt16Scalar>(a)->value;
            auto vb = std::static_pointer_cast<arrow::UInt16Scalar>(b)->value;
            return (va < vb) ? -1 : (va > vb) ? 1 : 0;
        }
        case arrow::Type::UINT32: {
            auto va = std::static_pointer_cast<arrow::UInt32Scalar>(a)->value;
            auto vb = std::static_pointer_cast<arrow::UInt32Scalar>(b)->value;
            return (va < vb) ? -1 : (va > vb) ? 1 : 0;
        }
        case arrow::Type::UINT64: {
            auto va = std::static_pointer_cast<arrow::UInt64Scalar>(a)->value;
            auto vb = std::static_pointer_cast<arrow::UInt64Scalar>(b)->value;
            return (va < vb) ? -1 : (va > vb) ? 1 : 0;
        }
        case arrow::Type::FLOAT: {
            auto va = std::static_pointer_cast<arrow::FloatScalar>(a)->value;
            auto vb = std::static_pointer_cast<arrow::FloatScalar>(b)->value;
            return (va < vb) ? -1 : (va > vb) ? 1 : 0;
        }
        case arrow::Type::DOUBLE: {
            auto va = std::static_pointer_cast<arrow::DoubleScalar>(a)->value;
            auto vb = std::static_pointer_cast<arrow::DoubleScalar>(b)->value;
            return (va < vb) ? -1 : (va > vb) ? 1 : 0;
        }
        case arrow::Type::STRING: {
            auto va = std::static_pointer_cast<arrow::StringScalar>(a)->value->ToString();
            auto vb = std::static_pointer_cast<arrow::StringScalar>(b)->value->ToString();
            return va.compare(vb);
        }
        case arrow::Type::BINARY: {
            auto va = std::static_pointer_cast<arrow::BinaryScalar>(a)->value->ToString();
            auto vb = std::static_pointer_cast<arrow::BinaryScalar>(b)->value->ToString();
            return va.compare(vb);
        }
        case arrow::Type::TIMESTAMP: {
            auto va = std::static_pointer_cast<arrow::TimestampScalar>(a)->value;
            auto vb = std::static_pointer_cast<arrow::TimestampScalar>(b)->value;
            return (va < vb) ? -1 : (va > vb) ? 1 : 0;
        }
        case arrow::Type::DATE32: {
            auto va = std::static_pointer_cast<arrow::Date32Scalar>(a)->value;
            auto vb = std::static_pointer_cast<arrow::Date32Scalar>(b)->value;
            return (va < vb) ? -1 : (va > vb) ? 1 : 0;
        }
        case arrow::Type::DATE64: {
            auto va = std::static_pointer_cast<arrow::Date64Scalar>(a)->value;
            auto vb = std::static_pointer_cast<arrow::Date64Scalar>(b)->value;
            return (va < vb) ? -1 : (va > vb) ? 1 : 0;
        }
        default:
            return 0;  // Unknown type - cannot compare
    }
}

//==============================================================================
// ColumnStatistics Implementation
//==============================================================================

bool ColumnStatistics::CanSatisfyPredicate(const ColumnPredicate& predicate) const {
    if (predicate.column_name != column_name) {
        return true;  // Not our column, can't eliminate
    }

    if (!min_value || !max_value || !predicate.value) {
        return true;  // No stats or no predicate value, assume it can satisfy
    }

    // Zone map pruning logic:
    // - Return false if we can PROVE no rows satisfy the predicate
    // - Return true (conservative) if some rows MIGHT satisfy
    switch (predicate.predicate_type) {
        case ColumnPredicate::PredicateType::kLessThan:
            // Query: column < X
            // Satisfiable if min_value < X (at least one row might be < X)
            return CompareScalars(min_value, predicate.value) < 0;

        case ColumnPredicate::PredicateType::kLessThanOrEqual:
            // Query: column <= X
            // Satisfiable if min_value <= X
            return CompareScalars(min_value, predicate.value) <= 0;

        case ColumnPredicate::PredicateType::kGreaterThan:
            // Query: column > X
            // Satisfiable if max_value > X (at least one row might be > X)
            return CompareScalars(max_value, predicate.value) > 0;

        case ColumnPredicate::PredicateType::kGreaterThanOrEqual:
            // Query: column >= X
            // Satisfiable if max_value >= X
            return CompareScalars(max_value, predicate.value) >= 0;

        case ColumnPredicate::PredicateType::kEqual:
            // Query: column == X
            // Satisfiable if min_value <= X <= max_value
            return CompareScalars(min_value, predicate.value) <= 0 &&
                   CompareScalars(predicate.value, max_value) <= 0;

        case ColumnPredicate::PredicateType::kNotEqual:
            // Query: column != X
            // Can only prune if ALL values equal X (min == max == X)
            // This is rare, so be conservative
            return !(CompareScalars(min_value, max_value) == 0 &&
                     CompareScalars(min_value, predicate.value) == 0);

        case ColumnPredicate::PredicateType::kLike:
        case ColumnPredicate::PredicateType::kIn:
            // Cannot evaluate pattern matching or IN lists with zone maps
            return true;
    }

    return true;  // Conservative default
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

        switch (pred.predicate_type) {
            case ColumnPredicate::PredicateType::kEqual:
                compiled.op = arrow::compute::CompareOperator::EQUAL;
                break;
            case ColumnPredicate::PredicateType::kNotEqual:
                compiled.op = arrow::compute::CompareOperator::NOT_EQUAL;
                break;
            case ColumnPredicate::PredicateType::kLessThan:
                compiled.op = arrow::compute::CompareOperator::LESS;
                break;
            case ColumnPredicate::PredicateType::kLessThanOrEqual:
                compiled.op = arrow::compute::CompareOperator::LESS_EQUAL;
                break;
            case ColumnPredicate::PredicateType::kGreaterThan:
                compiled.op = arrow::compute::CompareOperator::GREATER;
                break;
            case ColumnPredicate::PredicateType::kGreaterThanOrEqual:
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

    // Start with no combined mask (will be populated on first predicate)
    std::shared_ptr<arrow::BooleanArray> combined_mask;

    for (const auto& compiled : compiled_predicates_) {
        auto column = batch->GetColumnByName(compiled.column_name);
        if (!column) {
            // Column not found - skip this predicate (conservative)
            continue;
        }

        if (!compiled.value) {
            // No value to compare - skip this predicate
            continue;
        }

        // Use Arrow compute to compare column with scalar value
        // This is SIMD-optimized for performance
        // Map CompareOperator to function name
        std::string compare_func;
        switch (compiled.op) {
            case arrow::compute::CompareOperator::EQUAL:
                compare_func = "equal";
                break;
            case arrow::compute::CompareOperator::NOT_EQUAL:
                compare_func = "not_equal";
                break;
            case arrow::compute::CompareOperator::LESS:
                compare_func = "less";
                break;
            case arrow::compute::CompareOperator::LESS_EQUAL:
                compare_func = "less_equal";
                break;
            case arrow::compute::CompareOperator::GREATER:
                compare_func = "greater";
                break;
            case arrow::compute::CompareOperator::GREATER_EQUAL:
                compare_func = "greater_equal";
                break;
            default:
                continue;  // Unknown operator
        }

        auto compare_result = arrow::compute::CallFunction(
            compare_func,
            {column, compiled.value});

        if (!compare_result.ok()) {
            // Comparison failed - skip this predicate (conservative)
            continue;
        }

        auto predicate_mask = std::static_pointer_cast<arrow::BooleanArray>(
            compare_result.ValueOrDie().make_array());

        if (!combined_mask) {
            // First predicate - use its mask directly
            combined_mask = predicate_mask;
        } else {
            // AND with existing mask using Arrow compute
            auto and_result = arrow::compute::And(combined_mask, predicate_mask);
            if (and_result.ok()) {
                combined_mask = std::static_pointer_cast<arrow::BooleanArray>(
                    and_result.ValueOrDie().make_array());
            }
        }
    }

    if (!combined_mask) {
        // No predicates evaluated - all rows match
        arrow::BooleanBuilder builder;
        ARROW_RETURN_NOT_OK(builder.AppendValues(std::vector<bool>(batch->num_rows(), true)));
        ARROW_ASSIGN_OR_RAISE(auto array, builder.Finish());
        *mask = std::static_pointer_cast<arrow::BooleanArray>(array);
    } else {
        *mask = combined_mask;
    }

    return Status::OK();
}

Status ArrowPredicateEvaluator::FilterBatch(
    const std::shared_ptr<arrow::RecordBatch>& batch,
    std::shared_ptr<arrow::RecordBatch>* filtered_batch) const {

    if (!HasPredicates()) {
        // No predicates - return original batch
        *filtered_batch = batch;
        return Status::OK();
    }

    std::shared_ptr<arrow::BooleanArray> mask;
    ARROW_RETURN_NOT_OK(EvaluateBatch(batch, &mask));

    // Count true values in mask - if all true, skip filtering
    int64_t true_count = 0;
    for (int64_t i = 0; i < mask->length(); ++i) {
        if (mask->Value(i)) true_count++;
    }

    if (true_count == batch->num_rows()) {
        // All rows match - return original batch (skip filtering overhead)
        *filtered_batch = batch;
        return Status::OK();
    }

    if (true_count == 0) {
        // No rows match - return empty batch with same schema
        std::vector<std::shared_ptr<arrow::Array>> empty_arrays;
        for (int i = 0; i < batch->num_columns(); ++i) {
            // Create empty array of same type
            auto type = batch->column(i)->type();
            std::unique_ptr<arrow::ArrayBuilder> builder;
            auto builder_status = arrow::MakeBuilder(arrow::default_memory_pool(), type, &builder);
            if (builder_status.ok()) {
                auto finish_result = builder->Finish();
                if (finish_result.ok()) {
                    empty_arrays.push_back(finish_result.ValueOrDie());
                }
            }
        }
        *filtered_batch = arrow::RecordBatch::Make(batch->schema(), 0, empty_arrays);
        return Status::OK();
    }

    // Filter the batch using Arrow compute (SIMD-optimized)
    auto filter_result = arrow::compute::Filter(
        batch,
        mask,
        arrow::compute::FilterOptions::Defaults());

    if (!filter_result.ok()) {
        // Filter failed - return original batch (conservative)
        *filtered_batch = batch;
        return Status::OK();
    }

    *filtered_batch = filter_result.ValueOrDie().record_batch();
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
    stats->distinct_count = -1;  // Not computed for efficiency
    stats->null_count = 0;
    stats->has_nulls = false;
    stats->min_value = nullptr;
    stats->max_value = nullptr;
    stats->mean = 0.0;
    stats->std_dev = 0.0;
    stats->min_length = 0;
    stats->max_length = 0;
    stats->avg_length = 0;

    if (column->length() == 0) {
        return Status::OK();
    }

    // Compute null count
    stats->null_count = column->null_count();
    stats->has_nulls = (stats->null_count > 0);

    // Compute min/max using Arrow Compute
    auto min_result = arrow::compute::MinMax(column);
    if (min_result.ok()) {
        auto min_max_struct = min_result.ValueOrDie().scalar_as<arrow::StructScalar>();
        if (min_max_struct.is_valid) {
            // StructScalar contains {min, max} fields
            const auto& field_values = min_max_struct.value;
            if (field_values.size() >= 2) {
                stats->min_value = field_values[0];  // min
                stats->max_value = field_values[1];  // max
            }
        }
    }

    // Type-specific statistics
    auto type_id = column->type()->id();

    // Numeric types: compute mean and stddev
    if (arrow::is_floating(type_id) || arrow::is_integer(type_id)) {
        // Mean
        auto mean_result = arrow::compute::Mean(column);
        if (mean_result.ok()) {
            auto mean_scalar = mean_result.ValueOrDie().scalar();
            if (mean_scalar && mean_scalar->is_valid) {
                if (mean_scalar->type->id() == arrow::Type::DOUBLE) {
                    stats->mean = std::static_pointer_cast<arrow::DoubleScalar>(mean_scalar)->value;
                }
            }
        }

        // Standard deviation (variance then sqrt)
        auto variance_result = arrow::compute::Variance(column);
        if (variance_result.ok()) {
            auto var_scalar = variance_result.ValueOrDie().scalar();
            if (var_scalar && var_scalar->is_valid) {
                if (var_scalar->type->id() == arrow::Type::DOUBLE) {
                    double variance = std::static_pointer_cast<arrow::DoubleScalar>(var_scalar)->value;
                    stats->std_dev = std::sqrt(variance);
                }
            }
        }
    }

    // String/Binary types: compute length statistics
    if (type_id == arrow::Type::STRING || type_id == arrow::Type::LARGE_STRING ||
        type_id == arrow::Type::BINARY || type_id == arrow::Type::LARGE_BINARY) {

        int64_t total_length = 0;
        int64_t min_len = INT64_MAX;
        int64_t max_len = 0;
        int64_t valid_count = 0;

        for (int chunk_idx = 0; chunk_idx < column->num_chunks(); ++chunk_idx) {
            auto chunk = column->chunk(chunk_idx);

            // Compute binary length for this chunk using CallFunction
            auto length_result = arrow::compute::CallFunction("binary_length", {chunk});
            if (!length_result.ok()) continue;

            auto lengths = length_result.ValueOrDie().make_array();
            auto int32_lengths = std::static_pointer_cast<arrow::Int32Array>(lengths);

            for (int64_t i = 0; i < int32_lengths->length(); ++i) {
                if (!int32_lengths->IsNull(i)) {
                    int32_t len = int32_lengths->Value(i);
                    total_length += len;
                    min_len = std::min(min_len, static_cast<int64_t>(len));
                    max_len = std::max(max_len, static_cast<int64_t>(len));
                    valid_count++;
                }
            }
        }

        if (valid_count > 0) {
            stats->min_length = min_len;
            stats->max_length = max_len;
            stats->avg_length = total_length / valid_count;
        }
    }

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
