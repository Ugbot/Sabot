#include <sabot_ql/operators/bind.h>
#include <arrow/compute/api.h>
#include <arrow/compute/exec.h>
#include <arrow/compute/expression.h>
#include <sstream>
#include <chrono>

namespace sabot_ql {

BindOperator::BindOperator(
    std::shared_ptr<Operator> input,
    arrow::compute::Expression expression,
    std::string alias,
    std::string expression_str)
    : UnaryOperator(std::move(input)),
      expression_(std::move(expression)),
      alias_(std::move(alias)),
      expression_str_(std::move(expression_str)) {}

arrow::Result<std::shared_ptr<arrow::Schema>> BindOperator::GetOutputSchema() const {
    if (output_schema_) {
        return output_schema_;
    }

    // Get input schema
    ARROW_ASSIGN_OR_RAISE(auto input_schema, input_->GetOutputSchema());

    // Infer output type from expression
    // For now, we'll use int64 as default type
    // In a full implementation, we'd use Arrow's type inference on the expression
    auto new_field = arrow::field(alias_, arrow::int64());

    // Create output schema = input schema + new column
    std::vector<std::shared_ptr<arrow::Field>> fields;
    for (const auto& field : input_schema->fields()) {
        fields.push_back(field);
    }
    fields.push_back(new_field);

    output_schema_ = arrow::schema(fields);
    return output_schema_;
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> BindOperator::GetNextBatch() {
    auto start = std::chrono::high_resolution_clock::now();

    // Get next batch from input
    ARROW_ASSIGN_OR_RAISE(auto batch, input_->GetNextBatch());
    if (!batch) {
        return nullptr;
    }

    // Convert RecordBatch to ExecBatch for Arrow compute
    arrow::compute::ExecBatch exec_batch(*batch);

    // Execute expression on batch
    // Arrow's ExecuteScalarExpression evaluates expression on entire batch at once
    // This is vectorized and SIMD-optimized
    ARROW_ASSIGN_OR_RAISE(
        auto result_datum,
        arrow::compute::ExecuteScalarExpression(expression_, exec_batch)
    );

    // Convert result to array
    std::shared_ptr<arrow::Array> result_array;
    if (result_datum.is_scalar()) {
        // If result is scalar, broadcast to match batch length
        ARROW_ASSIGN_OR_RAISE(
            result_array,
            arrow::MakeArrayFromScalar(*result_datum.scalar(), batch->num_rows())
        );
    } else {
        result_array = result_datum.make_array();
    }

    // Append new column to batch
    std::vector<std::shared_ptr<arrow::Array>> columns;
    for (int i = 0; i < batch->num_columns(); ++i) {
        columns.push_back(batch->column(i));
    }
    columns.push_back(result_array);

    // Create new batch with appended column
    ARROW_ASSIGN_OR_RAISE(auto output_schema, GetOutputSchema());
    auto output_batch = arrow::RecordBatch::Make(
        output_schema,
        batch->num_rows(),
        columns
    );

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    stats_.execution_time_ms += duration.count() / 1000.0;
    stats_.rows_processed += batch->num_rows();
    stats_.batches_processed++;

    return output_batch;
}

std::string BindOperator::ToString() const {
    std::ostringstream oss;
    oss << "Bind(";
    if (!expression_str_.empty()) {
        oss << expression_str_;
    } else {
        oss << "<expression>";
    }
    oss << " AS " << alias_ << ")";

    // Add estimated cardinality
    oss << " [est. " << EstimateCardinality() << " rows]";

    return oss.str();
}

size_t BindOperator::EstimateCardinality() const {
    // BIND doesn't change cardinality - same as input
    return input_->EstimateCardinality();
}

} // namespace sabot_ql
