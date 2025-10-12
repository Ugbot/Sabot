#include <sabot_ql/operators/aggregate.h>
#include <sstream>
#include <chrono>

namespace sabot_ql {

// GroupByOperator implementation
arrow::Result<std::shared_ptr<arrow::Schema>> GroupByOperator::GetOutputSchema() const {
    std::vector<std::shared_ptr<arrow::Field>> fields;

    // Add group key fields
    ARROW_ASSIGN_OR_RAISE(auto input_schema, input_->GetOutputSchema());
    for (const auto& key : group_keys_) {
        int idx = input_schema->GetFieldIndex(key);
        if (idx < 0) {
            return arrow::Status::Invalid("Group key not found: " + key);
        }
        fields.push_back(input_schema->field(idx));
    }

    // Add aggregate output fields
    for (const auto& agg : aggregates_) {
        // Determine output type based on aggregate function
        std::shared_ptr<arrow::DataType> output_type;

        switch (agg.function) {
            case AggregateFunction::Count:
                output_type = arrow::int64();
                break;

            case AggregateFunction::Sum:
            case AggregateFunction::Avg:
            case AggregateFunction::Min:
            case AggregateFunction::Max:
                // Use type of input column
                {
                    int idx = input_schema->GetFieldIndex(agg.input_column);
                    if (idx >= 0) {
                        output_type = input_schema->field(idx)->type();
                    } else {
                        output_type = arrow::float64();  // Default
                    }
                }
                break;

            case AggregateFunction::GroupConcat:
            case AggregateFunction::Sample:
                output_type = arrow::utf8();
                break;
        }

        fields.push_back(arrow::field(agg.output_column, output_type));
    }

    return arrow::schema(fields);
}

std::string GroupByOperator::BuildGroupKey(
    const std::shared_ptr<arrow::RecordBatch>& batch,
    int64_t row_idx,
    const std::vector<int>& key_indices) const {

    std::ostringstream oss;
    for (size_t i = 0; i < key_indices.size(); ++i) {
        if (i > 0) oss << "|";

        auto array = batch->column(key_indices[i]);
        if (array->IsNull(row_idx)) {
            oss << "NULL";
        } else {
            auto scalar_result = array->GetScalar(row_idx);
            if (scalar_result.ok()) {
                oss << scalar_result.ValueOrDie()->ToString();
            }
        }
    }
    return oss.str();
}

arrow::Status GroupByOperator::ComputeAggregates() {
    if (computed_) {
        return arrow::Status::OK();
    }

    auto start = std::chrono::high_resolution_clock::now();

    // Materialize input
    ARROW_ASSIGN_OR_RAISE(input_table_, input_->GetAllResults());

    // Resolve group key indices
    std::vector<int> key_indices;
    for (const auto& key : group_keys_) {
        int idx = input_table_->schema()->GetFieldIndex(key);
        if (idx < 0) {
            return arrow::Status::Invalid("Group key not found: " + key);
        }
        key_indices.push_back(idx);
    }

    // Build groups - iterate through table batches
    ARROW_ASSIGN_OR_RAISE(auto batches, arrow::TableBatchReader(*input_table_).ToRecordBatches());

    int64_t global_row_offset = 0;
    for (const auto& batch : batches) {
        for (int64_t i = 0; i < batch->num_rows(); ++i) {
            std::string group_key = BuildGroupKey(batch, i, key_indices);
            groups_[group_key].push_back(global_row_offset + i);
        }
        global_row_offset += batch->num_rows();
    }

    // Compute aggregates for each group
    std::vector<std::shared_ptr<arrow::Array>> output_columns;

    // Build group key columns
    for (size_t key_idx = 0; key_idx < group_keys_.size(); ++key_idx) {
        auto key_col_idx = key_indices[key_idx];
        auto key_field = input_table_->schema()->field(key_col_idx);

        // Create builder for this key column
        std::unique_ptr<arrow::ArrayBuilder> builder;
        ARROW_RETURN_NOT_OK(arrow::MakeBuilder(
            arrow::default_memory_pool(),
            key_field->type(),
            &builder
        ));

        // Extract first value from each group
        for (const auto& [group_key, row_indices] : groups_) {
            if (row_indices.empty()) continue;

            int64_t first_row = row_indices[0];
            auto column = input_table_->column(key_col_idx);

            // Find chunk and row within chunk
            int64_t cumulative_rows = 0;
            for (int chunk_idx = 0; chunk_idx < column->num_chunks(); ++chunk_idx) {
                auto chunk = column->chunk(chunk_idx);
                if (first_row < cumulative_rows + chunk->length()) {
                    int64_t row_in_chunk = first_row - cumulative_rows;
                    auto scalar_result = chunk->GetScalar(row_in_chunk);
                    if (scalar_result.ok()) {
                        ARROW_RETURN_NOT_OK(builder->AppendScalar(*scalar_result.ValueOrDie()));
                    }
                    break;
                }
                cumulative_rows += chunk->length();
            }
        }

        ARROW_ASSIGN_OR_RAISE(auto key_array, builder->Finish());
        output_columns.push_back(key_array);
    }

    // Compute aggregate columns
    for (const auto& agg : aggregates_) {
        int input_col_idx = input_table_->schema()->GetFieldIndex(agg.input_column);
        if (input_col_idx < 0) {
            return arrow::Status::Invalid("Aggregate column not found: " + agg.input_column);
        }

        auto input_column = input_table_->column(input_col_idx);

        // Create builder for aggregate output
        std::unique_ptr<arrow::ArrayBuilder> builder;

        switch (agg.function) {
            case AggregateFunction::Count:
                {
                    arrow::Int64Builder count_builder;

                    for (const auto& [group_key, row_indices] : groups_) {
                        ARROW_RETURN_NOT_OK(count_builder.Append(row_indices.size()));
                    }

                    ARROW_ASSIGN_OR_RAISE(auto count_array, count_builder.Finish());
                    output_columns.push_back(count_array);
                }
                break;

            case AggregateFunction::Sum:
            case AggregateFunction::Avg:
            case AggregateFunction::Min:
            case AggregateFunction::Max:
                {
                    // Use Arrow compute kernels for each group
                    // TODO: Implement using Arrow compute::GroupBy when available
                    // For now, placeholder
                    return arrow::Status::NotImplemented("Aggregate function not yet implemented");
                }
                break;

            case AggregateFunction::GroupConcat:
            case AggregateFunction::Sample:
                return arrow::Status::NotImplemented("Aggregate function not yet implemented");
        }
    }

    // Create result table
    ARROW_ASSIGN_OR_RAISE(auto output_schema, GetOutputSchema());
    result_table_ = arrow::Table::Make(output_schema, output_columns);

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    stats_.execution_time_ms = duration.count() / 1000.0;

    computed_ = true;
    return arrow::Status::OK();
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> GroupByOperator::GetNextBatch() {
    if (exhausted_) {
        return nullptr;
    }

    if (!computed_) {
        ARROW_RETURN_NOT_OK(ComputeAggregates());
    }

    // Return result as single batch
    if (result_table_) {
        ARROW_ASSIGN_OR_RAISE(
            auto combined_batch,
            result_table_->CombineChunksToBatch()
        );
        exhausted_ = true;
        return combined_batch;
    }

    return nullptr;
}

bool GroupByOperator::HasNextBatch() const {
    return !exhausted_;
}

std::string GroupByOperator::ToString() const {
    std::ostringstream oss;
    oss << "GroupBy(";

    // Group keys
    oss << "keys=[";
    for (size_t i = 0; i < group_keys_.size(); ++i) {
        if (i > 0) oss << ", ";
        oss << group_keys_[i];
    }
    oss << "], ";

    // Aggregates
    oss << "aggs=[";
    for (size_t i = 0; i < aggregates_.size(); ++i) {
        if (i > 0) oss << ", ";

        const auto& agg = aggregates_[i];
        switch (agg.function) {
            case AggregateFunction::Count:
                oss << "COUNT";
                break;
            case AggregateFunction::Sum:
                oss << "SUM";
                break;
            case AggregateFunction::Avg:
                oss << "AVG";
                break;
            case AggregateFunction::Min:
                oss << "MIN";
                break;
            case AggregateFunction::Max:
                oss << "MAX";
                break;
            case AggregateFunction::GroupConcat:
                oss << "GROUP_CONCAT";
                break;
            case AggregateFunction::Sample:
                oss << "SAMPLE";
                break;
        }

        oss << "(" << agg.input_column << ")";
    }
    oss << "]";

    oss << ")\n";
    oss << "  └─ " << input_->ToString();

    return oss.str();
}

size_t GroupByOperator::EstimateCardinality() const {
    // Estimate: 10% of input rows become groups (conservative)
    return input_->EstimateCardinality() / 10;
}

// AggregateOperator implementation
arrow::Result<std::shared_ptr<arrow::Schema>> AggregateOperator::GetOutputSchema() const {
    std::vector<std::shared_ptr<arrow::Field>> fields;

    for (const auto& agg : aggregates_) {
        std::shared_ptr<arrow::DataType> output_type;

        switch (agg.function) {
            case AggregateFunction::Count:
                output_type = arrow::int64();
                break;

            case AggregateFunction::Sum:
            case AggregateFunction::Avg:
            case AggregateFunction::Min:
            case AggregateFunction::Max:
                output_type = arrow::float64();  // Default
                break;

            case AggregateFunction::GroupConcat:
            case AggregateFunction::Sample:
                output_type = arrow::utf8();
                break;
        }

        fields.push_back(arrow::field(agg.output_column, output_type));
    }

    return arrow::schema(fields);
}

arrow::Status AggregateOperator::ComputeAggregates() {
    if (computed_) {
        return arrow::Status::OK();
    }

    // Materialize input
    ARROW_ASSIGN_OR_RAISE(input_table_, input_->GetAllResults());

    // Compute aggregates using Arrow compute kernels
    std::vector<std::shared_ptr<arrow::Array>> output_columns;

    for (const auto& agg : aggregates_) {
        int input_col_idx = input_table_->schema()->GetFieldIndex(agg.input_column);
        if (input_col_idx < 0) {
            return arrow::Status::Invalid("Aggregate column not found: " + agg.input_column);
        }

        auto input_column = input_table_->column(input_col_idx);

        // Combine chunks into single array manually (Arrow 22.0 doesn't have CombineChunks)
        std::shared_ptr<arrow::Array> combined;
        if (input_column->num_chunks() == 1) {
            combined = input_column->chunk(0);
        } else {
            // Use arrow::Concatenate to combine chunks
            arrow::ArrayVector chunks;
            for (int i = 0; i < input_column->num_chunks(); ++i) {
                chunks.push_back(input_column->chunk(i));
            }
            ARROW_ASSIGN_OR_RAISE(combined, arrow::Concatenate(chunks));
        }

        std::shared_ptr<arrow::Scalar> result_scalar;

        switch (agg.function) {
            case AggregateFunction::Count: {
                ARROW_ASSIGN_OR_RAISE(
                    result_scalar,
                    aggregate_helpers::ComputeCount(combined)
                );
                break;
            }

            case AggregateFunction::Sum: {
                ARROW_ASSIGN_OR_RAISE(
                    result_scalar,
                    aggregate_helpers::ComputeSum(combined)
                );
                break;
            }

            case AggregateFunction::Avg: {
                ARROW_ASSIGN_OR_RAISE(
                    result_scalar,
                    aggregate_helpers::ComputeAvg(combined)
                );
                break;
            }

            case AggregateFunction::Min: {
                ARROW_ASSIGN_OR_RAISE(
                    result_scalar,
                    aggregate_helpers::ComputeMin(combined)
                );
                break;
            }

            case AggregateFunction::Max: {
                ARROW_ASSIGN_OR_RAISE(
                    result_scalar,
                    aggregate_helpers::ComputeMax(combined)
                );
                break;
            }

            case AggregateFunction::GroupConcat: {
                ARROW_ASSIGN_OR_RAISE(
                    result_scalar,
                    aggregate_helpers::ComputeGroupConcat(combined, agg.separator)
                );
                break;
            }

            case AggregateFunction::Sample: {
                ARROW_ASSIGN_OR_RAISE(
                    result_scalar,
                    aggregate_helpers::ComputeSample(combined)
                );
                break;
            }
        }

        // Convert scalar to array of length 1
        ARROW_ASSIGN_OR_RAISE(auto result_array, arrow::MakeArrayFromScalar(*result_scalar, 1));
        output_columns.push_back(result_array);
    }

    // Create result batch
    ARROW_ASSIGN_OR_RAISE(auto output_schema, GetOutputSchema());
    result_batch_ = arrow::RecordBatch::Make(output_schema, 1, output_columns);

    computed_ = true;
    return arrow::Status::OK();
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> AggregateOperator::GetNextBatch() {
    if (exhausted_) {
        return nullptr;
    }

    if (!computed_) {
        ARROW_RETURN_NOT_OK(ComputeAggregates());
    }

    exhausted_ = true;
    return result_batch_;
}

bool AggregateOperator::HasNextBatch() const {
    return !exhausted_;
}

std::string AggregateOperator::ToString() const {
    std::ostringstream oss;
    oss << "Aggregate(";

    for (size_t i = 0; i < aggregates_.size(); ++i) {
        if (i > 0) oss << ", ";

        const auto& agg = aggregates_[i];
        switch (agg.function) {
            case AggregateFunction::Count:
                oss << "COUNT";
                break;
            case AggregateFunction::Sum:
                oss << "SUM";
                break;
            case AggregateFunction::Avg:
                oss << "AVG";
                break;
            case AggregateFunction::Min:
                oss << "MIN";
                break;
            case AggregateFunction::Max:
                oss << "MAX";
                break;
            case AggregateFunction::GroupConcat:
                oss << "GROUP_CONCAT";
                break;
            case AggregateFunction::Sample:
                oss << "SAMPLE";
                break;
        }

        oss << "(" << agg.input_column << ")";
    }

    oss << ")\n";
    oss << "  └─ " << input_->ToString();

    return oss.str();
}

// Aggregate helper functions
namespace aggregate_helpers {

arrow::Result<std::shared_ptr<arrow::Scalar>> ComputeCount(
    const std::shared_ptr<arrow::Array>& array,
    bool count_nulls) {

    int64_t count = count_nulls ? array->length() : (array->length() - array->null_count());
    return arrow::MakeScalar(arrow::int64(), count);
}

arrow::Result<std::shared_ptr<arrow::Scalar>> ComputeSum(
    const std::shared_ptr<arrow::Array>& array) {

    arrow::compute::ExecContext ctx;
    ARROW_ASSIGN_OR_RAISE(
        auto result,
        arrow::compute::Sum(array, arrow::compute::ScalarAggregateOptions::Defaults(), &ctx)
    );

    return result.scalar();
}

arrow::Result<std::shared_ptr<arrow::Scalar>> ComputeAvg(
    const std::shared_ptr<arrow::Array>& array) {

    arrow::compute::ExecContext ctx;
    ARROW_ASSIGN_OR_RAISE(
        auto result,
        arrow::compute::Mean(array, arrow::compute::ScalarAggregateOptions::Defaults(), &ctx)
    );

    return result.scalar();
}

arrow::Result<std::shared_ptr<arrow::Scalar>> ComputeMin(
    const std::shared_ptr<arrow::Array>& array) {

    arrow::compute::ExecContext ctx;
    ARROW_ASSIGN_OR_RAISE(
        auto result,
        arrow::compute::MinMax(array, arrow::compute::ScalarAggregateOptions::Defaults(), &ctx)
    );

    auto min_max_scalar = std::static_pointer_cast<arrow::StructScalar>(result.scalar());
    return min_max_scalar->field("min");
}

arrow::Result<std::shared_ptr<arrow::Scalar>> ComputeMax(
    const std::shared_ptr<arrow::Array>& array) {

    arrow::compute::ExecContext ctx;
    ARROW_ASSIGN_OR_RAISE(
        auto result,
        arrow::compute::MinMax(array, arrow::compute::ScalarAggregateOptions::Defaults(), &ctx)
    );

    auto min_max_scalar = std::static_pointer_cast<arrow::StructScalar>(result.scalar());
    return min_max_scalar->field("max");
}

arrow::Result<std::shared_ptr<arrow::Scalar>> ComputeGroupConcat(
    const std::shared_ptr<arrow::Array>& array,
    const std::string& separator) {

    // Convert array to strings and concatenate
    std::ostringstream oss;
    bool first = true;

    for (int64_t i = 0; i < array->length(); ++i) {
        if (array->IsNull(i)) {
            continue;
        }

        if (!first) {
            oss << separator;
        }
        first = false;

        auto scalar_result = array->GetScalar(i);
        if (scalar_result.ok()) {
            oss << scalar_result.ValueOrDie()->ToString();
        }
    }

    return arrow::MakeScalar(arrow::utf8(), oss.str());
}

arrow::Result<std::shared_ptr<arrow::Scalar>> ComputeSample(
    const std::shared_ptr<arrow::Array>& array) {

    // Return first non-null value
    for (int64_t i = 0; i < array->length(); ++i) {
        if (!array->IsNull(i)) {
            return array->GetScalar(i);
        }
    }

    // Return null if all values are null
    return arrow::MakeNullScalar(array->type());
}

} // namespace aggregate_helpers

} // namespace sabot_ql
