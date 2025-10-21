#include "sabot_sql/operators/filter.h"
#include "sabot_sql/sql/string_operations.h"

namespace sabot_sql {
namespace operators {

FilterOperator::FilterOperator(
    std::shared_ptr<Operator> child,
    const std::string& column_name,
    const std::string& op,
    const std::string& value)
    : child_(child)
    , column_name_(column_name)
    , op_(op)
    , value_(value) {
}

arrow::Result<std::shared_ptr<arrow::Schema>> 
FilterOperator::GetOutputSchema() const {
    return child_->GetOutputSchema();
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> 
FilterOperator::GetNextBatch() {
    // Get next batch from child
    ARROW_ASSIGN_OR_RAISE(auto batch, child_->GetNextBatch());
    
    if (!batch || batch->num_rows() == 0) {
        return batch;
    }
    
    // Apply filter
    return ApplyFilter(batch);
}

bool FilterOperator::HasNextBatch() const {
    return child_->HasNextBatch();
}

std::string FilterOperator::ToString() const {
    return "Filter(" + column_name_ + " " + op_ + " '" + value_ + "')";
}

size_t FilterOperator::EstimateCardinality() const {
    // Estimate ~50% selectivity by default
    return child_->EstimateCardinality() / 2;
}

arrow::Result<std::shared_ptr<arrow::Table>> 
FilterOperator::GetAllResults() {
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    
    while (HasNextBatch()) {
        ARROW_ASSIGN_OR_RAISE(auto batch, GetNextBatch());
        if (batch && batch->num_rows() > 0) {
            batches.push_back(batch);
        }
    }
    
    if (batches.empty()) {
        // Return empty table with schema
        ARROW_ASSIGN_OR_RAISE(auto schema, GetOutputSchema());
        std::vector<std::shared_ptr<arrow::Array>> empty_arrays;
        return arrow::Table::Make(schema, empty_arrays);
    }
    
    return arrow::Table::FromRecordBatches(batches);
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>>
FilterOperator::ApplyFilter(const std::shared_ptr<arrow::RecordBatch>& batch) {
    // Find column
    auto schema = batch->schema();
    int col_idx = schema->GetFieldIndex(column_name_);
    
    if (col_idx < 0) {
        return arrow::Status::Invalid("Column not found: " + column_name_);
    }
    
    auto column = batch->column(col_idx);
    
    // Create predicate mask
    ARROW_ASSIGN_OR_RAISE(auto mask, CreatePredicateMask(column));
    
    // Apply filter using Arrow compute
    ARROW_ASSIGN_OR_RAISE(
        auto filtered_datum,
        arrow::compute::CallFunction("filter", {batch, mask})
    );
    
    return filtered_datum.record_batch();
}

arrow::Result<std::shared_ptr<arrow::BooleanArray>>
FilterOperator::CreatePredicateMask(const std::shared_ptr<arrow::Array>& column) {
    auto scalar = arrow::MakeScalar(value_);
    
    // Choose operation based on op_
    if (op_ == "=" || op_ == "==") {
        ARROW_ASSIGN_OR_RAISE(
            auto result,
            arrow::compute::CallFunction("equal", {column, scalar})
        );
        return std::static_pointer_cast<arrow::BooleanArray>(result.make_array());
        
    } else if (op_ == "!=" || op_ == "<>") {
        // For strings, use our optimized version
        if (column->type()->id() == arrow::Type::STRING) {
            return sql::StringOperations::NotEqual(column, value_);
        } else {
            ARROW_ASSIGN_OR_RAISE(
                auto result,
                arrow::compute::CallFunction("not_equal", {column, scalar})
            );
            return std::static_pointer_cast<arrow::BooleanArray>(result.make_array());
        }
        
    } else if (op_ == ">") {
        ARROW_ASSIGN_OR_RAISE(
            auto result,
            arrow::compute::CallFunction("greater", {column, scalar})
        );
        return std::static_pointer_cast<arrow::BooleanArray>(result.make_array());
        
    } else if (op_ == "<") {
        ARROW_ASSIGN_OR_RAISE(
            auto result,
            arrow::compute::CallFunction("less", {column, scalar})
        );
        return std::static_pointer_cast<arrow::BooleanArray>(result.make_array());
        
    } else if (op_ == ">=") {
        ARROW_ASSIGN_OR_RAISE(
            auto result,
            arrow::compute::CallFunction("greater_equal", {column, scalar})
        );
        return std::static_pointer_cast<arrow::BooleanArray>(result.make_array());
        
    } else if (op_ == "<=") {
        ARROW_ASSIGN_OR_RAISE(
            auto result,
            arrow::compute::CallFunction("less_equal", {column, scalar})
        );
        return std::static_pointer_cast<arrow::BooleanArray>(result.make_array());
        
    } else if (op_ == "LIKE") {
        return sql::StringOperations::MatchLike(column, value_);
        
    } else if (op_ == "~" || op_ == "REGEXP") {
        return sql::StringOperations::MatchRegex(column, value_);
        
    } else {
        return arrow::Status::NotImplemented("Operator not supported: " + op_);
    }
}

} // namespace operators
} // namespace sabot_sql

