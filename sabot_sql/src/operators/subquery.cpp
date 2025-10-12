#include "sabot_sql/operators/subquery.h"
#include <arrow/compute/api.h>

namespace sabot_sql {

SubqueryOperator::SubqueryOperator(
    std::shared_ptr<operators::Operator> outer,
    std::shared_ptr<operators::Operator> subquery,
    SubqueryType type)
    : operators::Operator()
    , subquery_(std::move(subquery))
    , type_(type)
    , result_cached_(false) {
}

arrow::Result<std::shared_ptr<arrow::Schema>> 
SubqueryOperator::GetOutputSchema() const {
    // Output schema depends on outer query
    return subquery_->GetOutputSchema();
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> 
SubqueryOperator::GetNextBatch() {
    switch (type_) {
        case SubqueryType::SCALAR:
            return ExecuteScalarSubquery();
        
        case SubqueryType::EXISTS:
            return ExecuteExistsSubquery();
        
        case SubqueryType::IN:
            return ExecuteInSubquery();
        
        case SubqueryType::CORRELATED:
            return ExecuteCorrelatedSubquery();
        
        case SubqueryType::UNCORRELATED:
            return ExecuteUncorrelatedSubquery();
        
        default:
            return arrow::Status::NotImplemented(
                "Subquery type not implemented");
    }
}

std::string SubqueryOperator::ToString() const {
    std::string type_str;
    switch (type_) {
        case SubqueryType::SCALAR: type_str = "SCALAR"; break;
        case SubqueryType::EXISTS: type_str = "EXISTS"; break;
        case SubqueryType::IN: type_str = "IN"; break;
        case SubqueryType::CORRELATED: type_str = "CORRELATED"; break;
        case SubqueryType::UNCORRELATED: type_str = "UNCORRELATED"; break;
    }
    
    return "Subquery(" + type_str + ")";
}

size_t SubqueryOperator::EstimateCardinality() const {
    // Cardinality is same as outer query
    return subquery_->EstimateCardinality();
}

arrow::Result<std::shared_ptr<arrow::Table>> 
SubqueryOperator::GetAllResults() {
    return subquery_->GetAllResults();
}

void SubqueryOperator::SetCorrelationPredicate(
    std::function<arrow::Result<std::shared_ptr<arrow::Table>>(
        const arrow::RecordBatch&)> predicate) {
    correlation_predicate_ = predicate;
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> 
SubqueryOperator::ExecuteScalarSubquery() {
    // Execute subquery and extract scalar value
    if (!result_cached_) {
        ARROW_ASSIGN_OR_RAISE(cached_result_, subquery_->GetAllResults());
        result_cached_ = true;
    }
    
    // Get scalar value from result
    ARROW_ASSIGN_OR_RAISE(auto scalar, SubqueryResultConverter::TableToScalar(cached_result_));
    
    // Get next batch from outer query
    ARROW_ASSIGN_OR_RAISE(auto outer_batch, subquery_->GetNextBatch());
    if (!outer_batch) {
        return nullptr;
    }
    
    // Add scalar value as new column to outer batch
    // TODO: Implement proper scalar column addition
    
    stats_.rows_processed += outer_batch->num_rows();
    stats_.batches_processed++;
    
    return outer_batch;
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> 
SubqueryOperator::ExecuteExistsSubquery() {
    // Execute subquery once
    if (!result_cached_) {
        ARROW_ASSIGN_OR_RAISE(cached_result_, subquery_->GetAllResults());
        result_cached_ = true;
    }
    
    // Check if subquery returned any rows
    ARROW_ASSIGN_OR_RAISE(bool exists, SubqueryResultConverter::TableToExists(cached_result_));
    
    // Get next batch from outer query
    ARROW_ASSIGN_OR_RAISE(auto outer_batch, subquery_->GetNextBatch());
    if (!outer_batch) {
        return nullptr;
    }
    
    // Filter outer batch based on EXISTS result
    if (exists) {
        stats_.rows_processed += outer_batch->num_rows();
        stats_.batches_processed++;
        return outer_batch;
    } else {
        // Return empty batch with same schema
        return arrow::RecordBatch::MakeEmpty(outer_batch->schema());
    }
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> 
SubqueryOperator::ExecuteInSubquery() {
    // Execute subquery once to get set of values
    if (!result_cached_) {
        ARROW_ASSIGN_OR_RAISE(cached_result_, subquery_->GetAllResults());
        result_cached_ = true;
    }
    
    // Get next batch from outer query
    ARROW_ASSIGN_OR_RAISE(auto outer_batch, subquery_->GetNextBatch());
    if (!outer_batch) {
        return nullptr;
    }
    
    // TODO: Implement proper IN filtering using subquery results
    // For now, just return outer batch
    
    stats_.rows_processed += outer_batch->num_rows();
    stats_.batches_processed++;
    
    return outer_batch;
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> 
SubqueryOperator::ExecuteCorrelatedSubquery() {
    // Get next batch from outer query
    ARROW_ASSIGN_OR_RAISE(auto outer_batch, subquery_->GetNextBatch());
    if (!outer_batch) {
        return nullptr;
    }
    
    // For each row in outer batch, execute subquery with correlation
    if (correlation_predicate_) {
        ARROW_ASSIGN_OR_RAISE(auto correlated_result, correlation_predicate_(*outer_batch));
        // TODO: Join correlated results with outer batch
    }
    
    stats_.rows_processed += outer_batch->num_rows();
    stats_.batches_processed++;
    
    return outer_batch;
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> 
SubqueryOperator::ExecuteUncorrelatedSubquery() {
    // Execute subquery once and cache result
    if (!result_cached_) {
        ARROW_ASSIGN_OR_RAISE(cached_result_, subquery_->GetAllResults());
        result_cached_ = true;
    }
    
    // Get next batch from outer query
    ARROW_ASSIGN_OR_RAISE(auto outer_batch, subquery_->GetNextBatch());
    if (!outer_batch) {
        return nullptr;
    }
    
    // Outer batch passes through
    // The subquery result is used by parent operator (e.g., JOIN)
    
    stats_.rows_processed += outer_batch->num_rows();
    stats_.batches_processed++;
    
    return outer_batch;
}

// SubqueryResultConverter implementation
arrow::Result<std::shared_ptr<arrow::Array>> 
SubqueryResultConverter::TableToSet(
    const std::shared_ptr<arrow::Table>& table,
    const std::string& column_name) {
    
    if (!table || table->num_rows() == 0) {
        return arrow::Status::Invalid("Empty table");
    }
    
    auto column = table->GetColumnByName(column_name);
    if (!column) {
        return arrow::Status::Invalid("Column not found: " + column_name);
    }
    
    // Combine chunks and return as single array
    return column->Flatten(arrow::default_memory_pool());
}

arrow::Result<bool> 
SubqueryResultConverter::TableToExists(
    const std::shared_ptr<arrow::Table>& table) {
    
    return table && table->num_rows() > 0;
}

arrow::Result<std::shared_ptr<arrow::Scalar>> 
SubqueryResultConverter::TableToScalar(
    const std::shared_ptr<arrow::Table>& table) {
    
    if (!table || table->num_rows() == 0 || table->num_columns() == 0) {
        return arrow::Status::Invalid("Empty table for scalar subquery");
    }
    
    if (table->num_rows() > 1) {
        return arrow::Status::Invalid(
            "Scalar subquery returned more than one row");
    }
    
    // Get first column, first row
    auto column = table->column(0);
    auto chunk = column->chunk(0);
    
    return chunk->GetScalar(0);
}

} // namespace sabot_sql

