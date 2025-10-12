#include <sabot_ql/operators/sort.h>
#include <arrow/compute/api.h>
#include <sstream>

namespace sabot_ql {

namespace cp = arrow::compute;

SortOperator::SortOperator(std::shared_ptr<Operator> input,
                           std::vector<SortKey> sort_keys)
    : UnaryOperator(std::move(input)), sort_keys_(std::move(sort_keys)) {

    if (sort_keys_.empty()) {
        throw std::invalid_argument("SortOperator requires at least one sort key");
    }
}

arrow::Result<std::shared_ptr<arrow::Schema>> SortOperator::GetOutputSchema() const {
    // Output schema is same as input schema
    return input_->GetOutputSchema();
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> SortOperator::GetNextBatch() {
    // On first call, sort all data
    if (!IsSorted()) {
        ARROW_RETURN_NOT_OK(SortAllData());
    }

    // Return next batch from sorted table
    if (current_batch_ >= sorted_table_->num_rows()) {
        return nullptr;  // No more data
    }

    // Calculate batch bounds
    int64_t offset = current_batch_;
    int64_t length = std::min(batch_size_,
                              static_cast<size_t>(sorted_table_->num_rows() - current_batch_));

    // Slice the table to get a batch
    auto batch_slice = sorted_table_->Slice(offset, length);

    // Convert to RecordBatch
    ARROW_ASSIGN_OR_RAISE(auto batch, batch_slice->CombineChunksToBatch());

    current_batch_ += length;

    // Update stats
    stats_.rows_processed += batch->num_rows();
    stats_.batches_processed += 1;

    return batch;
}

arrow::Status SortOperator::SortAllData() {
    auto start_time = std::chrono::high_resolution_clock::now();

    // 1. Collect all input batches into a table
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;

    while (input_->HasNextBatch()) {
        ARROW_ASSIGN_OR_RAISE(auto batch, input_->GetNextBatch());
        if (batch) {
            batches.push_back(batch);
        }
    }

    if (batches.empty()) {
        // Empty input - create empty table
        ARROW_ASSIGN_OR_RAISE(auto schema, input_->GetOutputSchema());
        // Explicitly create empty vectors for columns
        std::vector<std::shared_ptr<arrow::ChunkedArray>> empty_columns;
        sorted_table_ = arrow::Table::Make(schema, empty_columns);
        return arrow::Status::OK();
    }

    // Combine batches into table
    ARROW_ASSIGN_OR_RAISE(auto input_table, arrow::Table::FromRecordBatches(batches));

    // 2. Build Arrow SortOptions
    std::vector<cp::SortKey> arrow_sort_keys;

    for (const auto& key : sort_keys_) {
        // Find column index
        auto schema = input_table->schema();
        int col_idx = schema->GetFieldIndex(key.column_name);

        if (col_idx == -1) {
            return arrow::Status::Invalid("Sort column not found: " + key.column_name);
        }

        // Convert our SortDirection to Arrow's SortOrder
        cp::SortOrder order = (key.direction == SortDirection::Ascending)
                                  ? cp::SortOrder::Ascending
                                  : cp::SortOrder::Descending;

        arrow_sort_keys.emplace_back(col_idx, order);
    }

    // 3. Get sort indices
    ARROW_ASSIGN_OR_RAISE(
        auto indices,
        cp::SortIndices(input_table, cp::SortOptions(arrow_sort_keys))
    );

    // 4. Use Take to reorder the table
    ARROW_ASSIGN_OR_RAISE(
        auto sorted_datum,
        cp::Take(input_table, indices)
    );

    sorted_table_ = sorted_datum.table();

    // Update stats
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        end_time - start_time);

    stats_.execution_time_ms = duration.count();
    stats_.bytes_processed = sorted_table_->num_rows() * sorted_table_->num_columns() * 8;  // Rough estimate

    return arrow::Status::OK();
}

std::string SortOperator::ToString() const {
    std::ostringstream oss;
    oss << "Sort(";

    for (size_t i = 0; i < sort_keys_.size(); ++i) {
        if (i > 0) oss << ", ";
        oss << sort_keys_[i].ToString();
    }

    oss << ")";
    return oss.str();
}

size_t SortOperator::EstimateCardinality() const {
    // Sorting doesn't change cardinality
    return input_->EstimateCardinality();
}

} // namespace sabot_ql
