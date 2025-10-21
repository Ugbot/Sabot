#include "sabot_sql/operators/projection.h"

namespace sabot_sql {
namespace operators {

ProjectionOperator::ProjectionOperator(
    std::shared_ptr<Operator> child,
    const std::vector<std::string>& column_names)
    : child_(child)
    , column_names_(column_names) {
    
    // Initialize will be called when first batch is available
}

arrow::Status ProjectionOperator::Initialize() {
    if (output_schema_) {
        return arrow::Status::OK();  // Already initialized
    }
    
    // Get child schema
    ARROW_ASSIGN_OR_RAISE(auto child_schema, child_->GetOutputSchema());
    
    // Build column indices and output schema
    std::vector<std::shared_ptr<arrow::Field>> output_fields;
    
    for (const auto& col_name : column_names_) {
        int idx = child_schema->GetFieldIndex(col_name);
        if (idx < 0) {
            return arrow::Status::Invalid("Column not found: " + col_name);
        }
        
        column_indices_.push_back(idx);
        output_fields.push_back(child_schema->field(idx));
    }
    
    output_schema_ = arrow::schema(output_fields);
    
    return arrow::Status::OK();
}

arrow::Result<std::shared_ptr<arrow::Schema>> 
ProjectionOperator::GetOutputSchema() const {
    if (output_schema_) {
        return output_schema_;
    }
    
    // Need to initialize first
    return arrow::Status::Invalid("Projection not initialized yet");
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> 
ProjectionOperator::GetNextBatch() {
    // Initialize if needed
    if (!output_schema_) {
        ARROW_RETURN_NOT_OK(const_cast<ProjectionOperator*>(this)->Initialize());
    }
    
    // Get next batch from child
    ARROW_ASSIGN_OR_RAISE(auto batch, child_->GetNextBatch());
    
    if (!batch || batch->num_rows() == 0) {
        return batch;
    }
    
    // Project columns
    return ProjectBatch(batch);
}

bool ProjectionOperator::HasNextBatch() const {
    return child_->HasNextBatch();
}

std::string ProjectionOperator::ToString() const {
    std::string cols;
    for (size_t i = 0; i < column_names_.size(); ++i) {
        if (i > 0) cols += ", ";
        cols += column_names_[i];
    }
    return "Project(" + cols + ")";
}

size_t ProjectionOperator::EstimateCardinality() const {
    return child_->EstimateCardinality();
}

arrow::Result<std::shared_ptr<arrow::Table>> 
ProjectionOperator::GetAllResults() {
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

arrow::Result<std::shared_ptr<arrow::RecordBatch>>
ProjectionOperator::ProjectBatch(const std::shared_ptr<arrow::RecordBatch>& batch) {
    // Extract selected columns
    std::vector<std::shared_ptr<arrow::Array>> selected_columns;
    
    for (int idx : column_indices_) {
        selected_columns.push_back(batch->column(idx));
    }
    
    // Create new batch with selected columns
    return arrow::RecordBatch::Make(output_schema_, batch->num_rows(), selected_columns);
}

} // namespace operators
} // namespace sabot_sql

