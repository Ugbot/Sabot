#include <sabot_ql/operators/union.h>
#include <arrow/compute/api.h>
#include <arrow/util/key_value_metadata.h>
#include <sstream>
#include <unordered_set>

namespace sabot_ql {

namespace cp = arrow::compute;

UnionOperator::UnionOperator(std::vector<std::shared_ptr<Operator>> inputs,
                             bool deduplicate)
    : inputs_(std::move(inputs)), deduplicate_(deduplicate) {

    if (inputs_.empty()) {
        throw std::invalid_argument("UnionOperator requires at least one input");
    }
}

arrow::Result<std::shared_ptr<arrow::Schema>> UnionOperator::GetOutputSchema() const {
    // Return unified schema
    return const_cast<UnionOperator*>(this)->UnifySchemas();
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> UnionOperator::GetNextBatch() {
    // On first call, execute union
    if (!IsUnionExecuted()) {
        ARROW_RETURN_NOT_OK(ExecuteUnion());
    }

    // Check if we have more data
    if (current_batch_ >= union_table_->num_rows()) {
        return nullptr;  // No more data
    }

    // Calculate batch bounds
    int64_t offset = current_batch_;
    int64_t length = std::min(batch_size_,
                              static_cast<size_t>(union_table_->num_rows() - current_batch_));

    // Slice the table to get a batch
    auto batch_slice = union_table_->Slice(offset, length);

    // Convert to RecordBatch
    ARROW_ASSIGN_OR_RAISE(auto batch, batch_slice->CombineChunksToBatch());

    current_batch_ += length;

    // Update stats
    stats_.rows_processed += batch->num_rows();
    stats_.batches_processed += 1;

    return batch;
}

arrow::Status UnionOperator::ExecuteUnion() {
    auto start_time = std::chrono::high_resolution_clock::now();

    // 1. Unify schemas
    ARROW_ASSIGN_OR_RAISE(auto unified_schema, UnifySchemas());

    // 2. Collect all batches from all inputs
    std::vector<std::shared_ptr<arrow::RecordBatch>> all_batches;

    for (auto& input : inputs_) {
        // Get output schema for this input
        ARROW_ASSIGN_OR_RAISE(auto input_schema, input->GetOutputSchema());

        // Collect all batches from this input
        while (input->HasNextBatch()) {
            ARROW_ASSIGN_OR_RAISE(auto batch, input->GetNextBatch());
            if (batch) {
                // Pad batch to unified schema if needed
                if (!input_schema->Equals(unified_schema)) {
                    ARROW_ASSIGN_OR_RAISE(batch, PadBatch(batch, unified_schema));
                }
                all_batches.push_back(batch);
            }
        }
    }

    if (all_batches.empty()) {
        // Empty union - create empty table
        std::vector<std::shared_ptr<arrow::ChunkedArray>> empty_columns;
        union_table_ = arrow::Table::Make(unified_schema, empty_columns);
        return arrow::Status::OK();
    }

    // 3. Combine batches into table
    ARROW_ASSIGN_OR_RAISE(union_table_, arrow::Table::FromRecordBatches(all_batches));

    // 4. Deduplicate if needed
    if (deduplicate_) {
        ARROW_ASSIGN_OR_RAISE(union_table_, DeduplicateRows(union_table_));
    }

    // Update stats
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        end_time - start_time);

    stats_.execution_time_ms = duration.count();
    stats_.bytes_processed = union_table_->num_rows() * union_table_->num_columns() * 8;  // Rough estimate

    return arrow::Status::OK();
}

arrow::Result<std::shared_ptr<arrow::Schema>> UnionOperator::UnifySchemas() {
    // Collect all schemas
    std::vector<std::shared_ptr<arrow::Schema>> schemas;
    for (const auto& input : inputs_) {
        ARROW_ASSIGN_OR_RAISE(auto schema, input->GetOutputSchema());
        schemas.push_back(schema);
    }

    // Start with first schema
    auto unified_schema = schemas[0];

    // For each additional schema, add any missing columns
    for (size_t i = 1; i < schemas.size(); ++i) {
        auto schema = schemas[i];

        // Check for columns in schema that aren't in unified_schema
        for (int j = 0; j < schema->num_fields(); ++j) {
            auto field = schema->field(j);
            if (unified_schema->GetFieldIndex(field->name()) == -1) {
                // Add missing field
                unified_schema = unified_schema->AddField(
                    unified_schema->num_fields(), field).ValueOrDie();
            }
        }
    }

    // Also add columns from unified_schema that aren't in each input schema
    // (This ensures all schemas have all columns)
    for (size_t i = 0; i < schemas.size(); ++i) {
        auto schema = schemas[i];

        for (int j = 0; j < unified_schema->num_fields(); ++j) {
            auto field = unified_schema->field(j);
            if (schema->GetFieldIndex(field->name()) == -1) {
                // This column will be padded with nulls in PadBatch()
            }
        }
    }

    return unified_schema;
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> UnionOperator::PadBatch(
    const std::shared_ptr<arrow::RecordBatch>& batch,
    const std::shared_ptr<arrow::Schema>& target_schema) {

    // If schemas already match, no padding needed
    if (batch->schema()->Equals(target_schema)) {
        return batch;
    }

    // Build new batch with padded columns
    std::vector<std::shared_ptr<arrow::Array>> arrays;

    for (int i = 0; i < target_schema->num_fields(); ++i) {
        auto field = target_schema->field(i);
        auto col_idx = batch->schema()->GetFieldIndex(field->name());

        if (col_idx != -1) {
            // Column exists in batch - use it
            arrays.push_back(batch->column(col_idx));
        } else {
            // Column missing - create null array
            ARROW_ASSIGN_OR_RAISE(
                auto null_array,
                arrow::MakeArrayOfNull(field->type(), batch->num_rows())
            );
            arrays.push_back(null_array);
        }
    }

    return arrow::RecordBatch::Make(target_schema, batch->num_rows(), arrays);
}

arrow::Result<std::shared_ptr<arrow::Table>> UnionOperator::DeduplicateRows(
    const std::shared_ptr<arrow::Table>& table) {

    // Use Arrow's unique function on all columns
    // Strategy: Concatenate all columns into a struct, then use unique

    // 1. Build struct array from all columns
    std::vector<std::shared_ptr<arrow::Array>> columns;
    std::vector<std::shared_ptr<arrow::Field>> fields;

    for (int i = 0; i < table->num_columns(); ++i) {
        auto column = table->column(i);
        // Combine chunks manually (Arrow 22.0 doesn't have CombineChunks)
        std::shared_ptr<arrow::Array> combined;
        if (column->num_chunks() == 1) {
            combined = column->chunk(0);
        } else {
            arrow::ArrayVector chunks;
            for (int j = 0; j < column->num_chunks(); ++j) {
                chunks.push_back(column->chunk(j));
            }
            ARROW_ASSIGN_OR_RAISE(combined, arrow::Concatenate(chunks));
        }
        columns.push_back(combined);
        fields.push_back(table->schema()->field(i));
    }

    // Create struct array
    auto struct_type = arrow::struct_(fields);
    ARROW_ASSIGN_OR_RAISE(
        auto struct_array,
        arrow::StructArray::Make(columns, fields)
    );

    // 2. Find unique row indices using hash-based approach
    // (Arrow doesn't have a built-in unique for structs, so we do it manually)

    std::unordered_set<std::string> seen_rows;
    std::vector<int64_t> unique_indices;

    for (int64_t i = 0; i < struct_array->length(); ++i) {
        // Create hash key from row
        std::ostringstream key;
        for (size_t col = 0; col < columns.size(); ++col) {
            if (columns[col]->IsNull(i)) {
                key << "NULL,";
            } else {
                // Use string representation as key
                // TODO: More efficient hashing
                key << columns[col]->ToString() << ",";
            }
        }

        auto key_str = key.str();
        if (seen_rows.find(key_str) == seen_rows.end()) {
            seen_rows.insert(key_str);
            unique_indices.push_back(i);
        }
    }

    // 3. Use Take to select unique rows
    auto indices_builder = arrow::Int64Builder();
    ARROW_RETURN_NOT_OK(indices_builder.AppendValues(unique_indices));
    ARROW_ASSIGN_OR_RAISE(auto indices_array, indices_builder.Finish());

    ARROW_ASSIGN_OR_RAISE(
        auto unique_datum,
        cp::Take(table, indices_array)
    );

    return unique_datum.table();
}

std::string UnionOperator::ToString() const {
    std::ostringstream oss;
    if (deduplicate_) {
        oss << "UNION (deduplicate)";
    } else {
        oss << "UNION ALL (no dedup)";
    }
    oss << " [" << inputs_.size() << " inputs]";
    return oss.str();
}

size_t UnionOperator::EstimateCardinality() const {
    // Sum cardinalities of all inputs
    size_t total = 0;
    for (const auto& input : inputs_) {
        total += input->EstimateCardinality();
    }

    // If deduplicating, assume some overlap (reduce by ~20%)
    if (deduplicate_) {
        total = static_cast<size_t>(total * 0.8);
    }

    return total;
}

} // namespace sabot_ql
