#include "sabot_sql/operators/sort.h"
#include <arrow/compute/api_vector.h>
#include <arrow/compute/ordering.h>

namespace sabot_sql {
namespace operators {

// ========== SortOperator ==========

SortOperator::SortOperator(
    std::shared_ptr<Operator> child,
    const std::vector<SortSpec>& sort_keys,
    int64_t limit)
    : child_(child)
    , sort_keys_(sort_keys)
    , limit_(limit)
    , result_computed_(false) {
}

arrow::Result<std::shared_ptr<arrow::Schema>> 
SortOperator::GetOutputSchema() const {
    return child_->GetOutputSchema();
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> 
SortOperator::GetNextBatch() {
    if (result_computed_) {
        return nullptr;
    }
    
    // Perform sort
    ARROW_ASSIGN_OR_RAISE(result_, PerformSort());
    result_computed_ = true;
    
    // Return first batch
    if (result_->num_rows() > 0) {
        ARROW_ASSIGN_OR_RAISE(auto batch, result_->CombineChunksToBatch());
        return batch;
    }
    
    return nullptr;
}

bool SortOperator::HasNextBatch() const {
    return !result_computed_;
}

std::string SortOperator::ToString() const {
    std::string keys;
    for (size_t i = 0; i < sort_keys_.size(); ++i) {
        if (i > 0) keys += ", ";
        keys += sort_keys_[i].column_name;
        keys += (sort_keys_[i].order == SortOrder::ASCENDING) ? " ASC" : " DESC";
    }
    
    std::string result = "Sort(" + keys + ")";
    if (limit_ > 0) {
        result += " LIMIT " + std::to_string(limit_);
    }
    return result;
}

size_t SortOperator::EstimateCardinality() const {
    if (limit_ > 0) {
        return std::min(static_cast<size_t>(limit_), child_->EstimateCardinality());
    }
    return child_->EstimateCardinality();
}

arrow::Result<std::shared_ptr<arrow::Table>> 
SortOperator::GetAllResults() {
    if (!result_computed_) {
        ARROW_ASSIGN_OR_RAISE(result_, PerformSort());
        result_computed_ = true;
    }
    
    return result_;
}

arrow::Result<std::shared_ptr<arrow::Table>>
SortOperator::PerformSort() {
    // Get all input data
    ARROW_ASSIGN_OR_RAISE(auto input_table, child_->GetAllResults());
    
    if (input_table->num_rows() == 0) {
        return input_table;
    }
    
    if (sort_keys_.empty()) {
        return input_table;
    }
    
    auto sort_key = sort_keys_[0];
    auto column = input_table->GetColumnByName(sort_key.column_name);
    
    if (!column) {
        return arrow::Status::Invalid("Sort column not found: " + sort_key.column_name);
    }
    
    // Get sort order (using Arrow's enum)
    ::arrow::compute::SortOrder order = (sort_key.order == operators::SortOrder::ASCENDING)
        ? ::arrow::compute::SortOrder::Ascending
        : ::arrow::compute::SortOrder::Descending;
    
    // Get sort indices
    ARROW_ASSIGN_OR_RAISE(
        auto indices,
        arrow::compute::SortIndices(*column->chunk(0), order)
    );
    
    // Apply indices to each column of the table
    std::vector<std::shared_ptr<arrow::ChunkedArray>> sorted_columns;
    
    for (int i = 0; i < input_table->num_columns(); ++i) {
        auto col = input_table->column(i);
        ARROW_ASSIGN_OR_RAISE(
            auto taken_datum,
            arrow::compute::Take(col->chunk(0), indices)
        );
        auto taken_array = taken_datum.make_array();
        sorted_columns.push_back(std::make_shared<arrow::ChunkedArray>(taken_array));
    }
    
    // Build sorted table
    auto sorted_table = arrow::Table::Make(input_table->schema(), sorted_columns);
    
    // Apply limit if specified
    if (limit_ > 0 && sorted_table->num_rows() > limit_) {
        sorted_table = sorted_table->Slice(0, limit_);
    }
    
    return sorted_table;
}

// ========== LimitOperator ==========

LimitOperator::LimitOperator(std::shared_ptr<Operator> child, int64_t limit)
    : child_(child)
    , limit_(limit)
    , rows_returned_(0) {
}

arrow::Result<std::shared_ptr<arrow::Schema>> 
LimitOperator::GetOutputSchema() const {
    return child_->GetOutputSchema();
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> 
LimitOperator::GetNextBatch() {
    if (rows_returned_ >= limit_) {
        return nullptr;
    }
    
    ARROW_ASSIGN_OR_RAISE(auto batch, child_->GetNextBatch());
    
    if (!batch || batch->num_rows() == 0) {
        return batch;
    }
    
    // Check if we need to truncate
    int64_t rows_needed = limit_ - rows_returned_;
    
    if (batch->num_rows() <= rows_needed) {
        rows_returned_ += batch->num_rows();
        return batch;
    } else {
        // Truncate batch
        auto truncated = batch->Slice(0, rows_needed);
        rows_returned_ += rows_needed;
        return truncated;
    }
}

bool LimitOperator::HasNextBatch() const {
    return rows_returned_ < limit_ && child_->HasNextBatch();
}

std::string LimitOperator::ToString() const {
    return "Limit(" + std::to_string(limit_) + ")";
}

size_t LimitOperator::EstimateCardinality() const {
    return std::min(static_cast<size_t>(limit_), child_->EstimateCardinality());
}

arrow::Result<std::shared_ptr<arrow::Table>> 
LimitOperator::GetAllResults() {
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    
    while (HasNextBatch()) {
        ARROW_ASSIGN_OR_RAISE(auto batch, GetNextBatch());
        if (batch && batch->num_rows() > 0) {
            batches.push_back(batch);
        }
    }
    
    if (batches.empty()) {
        ARROW_ASSIGN_OR_RAISE(auto schema, GetOutputSchema());
        std::vector<std::shared_ptr<arrow::Array>> empty_arrays;
        return arrow::Table::Make(schema, empty_arrays);
    }
    
    return arrow::Table::FromRecordBatches(batches);
}

} // namespace operators
} // namespace sabot_sql

