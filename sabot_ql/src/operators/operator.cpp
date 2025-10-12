#include <sabot_ql/operators/operator.h>
#include <sabot_ql/storage/triple_store.h>
#include <arrow/compute/api.h>
#include <sstream>
#include <chrono>

namespace sabot_ql {

std::string OperatorStats::ToString() const {
    std::ostringstream oss;
    oss << "OperatorStats{"
        << "rows=" << rows_processed
        << ", batches=" << batches_processed
        << ", bytes=" << bytes_processed
        << ", time=" << execution_time_ms << "ms"
        << "}";
    return oss.str();
}

arrow::Result<std::shared_ptr<arrow::Table>> Operator::GetAllResults() {
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;

    while (HasNextBatch()) {
        ARROW_ASSIGN_OR_RAISE(auto batch, GetNextBatch());
        if (batch) {
            batches.push_back(batch);
        }
    }

    if (batches.empty()) {
        // Return empty table with correct schema
        ARROW_ASSIGN_OR_RAISE(auto schema, GetOutputSchema());
        std::vector<std::shared_ptr<arrow::Array>> empty_arrays;
        return arrow::Table::Make(schema, empty_arrays);
    }

    return arrow::Table::FromRecordBatches(batches);
}

// TripleScanOperator implementation
TripleScanOperator::TripleScanOperator(
    std::shared_ptr<TripleStore> store,
    std::shared_ptr<Vocabulary> vocab,
    const TriplePattern& pattern)
    : store_(std::move(store)),
      vocab_(std::move(vocab)),
      pattern_(pattern) {}

arrow::Result<std::shared_ptr<arrow::Schema>> TripleScanOperator::GetOutputSchema() const {
    // Output schema depends on which variables are unbound
    std::vector<std::shared_ptr<arrow::Field>> fields;

    if (!pattern_.subject.has_value()) {
        fields.push_back(arrow::field("subject", arrow::int64()));
    }
    if (!pattern_.predicate.has_value()) {
        fields.push_back(arrow::field("predicate", arrow::int64()));
    }
    if (!pattern_.object.has_value()) {
        fields.push_back(arrow::field("object", arrow::int64()));
    }

    // If all are bound (no variables), return empty schema (point query)
    if (fields.empty()) {
        fields.push_back(arrow::field("exists", arrow::boolean()));
    }

    return arrow::schema(fields);
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> TripleScanOperator::GetNextBatch() {
    if (exhausted_) {
        return nullptr;
    }

    // On first call, execute the scan
    if (!results_) {
        auto start = std::chrono::high_resolution_clock::now();

        ARROW_ASSIGN_OR_RAISE(results_, store_->ScanPattern(pattern_));

        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
        stats_.execution_time_ms = duration.count() / 1000.0;

        current_batch_ = 0;
    }

    // Convert table to batches
    ARROW_ASSIGN_OR_RAISE(auto batches, arrow::TableBatchReader(*results_).ToRecordBatches());

    if (current_batch_ < batches.size()) {
        auto batch = batches[current_batch_];
        stats_.rows_processed += batch->num_rows();
        stats_.batches_processed++;
        current_batch_++;
        return batch;
    }

    exhausted_ = true;
    return nullptr;
}

bool TripleScanOperator::HasNextBatch() const {
    return !exhausted_;
}

std::string TripleScanOperator::ToString() const {
    std::ostringstream oss;
    oss << "TripleScan(";

    if (pattern_.subject.has_value()) {
        oss << "S=" << pattern_.subject.value();
    } else {
        oss << "?s";
    }

    oss << ", ";

    if (pattern_.predicate.has_value()) {
        oss << "P=" << pattern_.predicate.value();
    } else {
        oss << "?p";
    }

    oss << ", ";

    if (pattern_.object.has_value()) {
        oss << "O=" << pattern_.object.value();
    } else {
        oss << "?o";
    }

    oss << ")";

    // Add estimated cardinality
    auto card_result = EstimateCardinality();
    oss << " [est. " << card_result << " rows]";

    return oss.str();
}

size_t TripleScanOperator::EstimateCardinality() const {
    auto result = store_->EstimateCardinality(pattern_);
    if (!result.ok()) {
        return 0;
    }
    return result.ValueOrDie();
}

// FilterOperator implementation
FilterOperator::FilterOperator(
    std::shared_ptr<Operator> input,
    PredicateFn predicate,
    const std::string& predicate_description)
    : UnaryOperator(std::move(input)),
      predicate_(std::move(predicate)),
      predicate_description_(predicate_description) {}

arrow::Result<std::shared_ptr<arrow::Schema>> FilterOperator::GetOutputSchema() const {
    // Filter doesn't change schema
    return input_->GetOutputSchema();
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> FilterOperator::GetNextBatch() {
    auto start = std::chrono::high_resolution_clock::now();

    while (input_->HasNextBatch()) {
        ARROW_ASSIGN_OR_RAISE(auto batch, input_->GetNextBatch());
        if (!batch) {
            continue;
        }

        // Apply predicate to get boolean mask
        ARROW_ASSIGN_OR_RAISE(auto mask, predicate_(batch));

        // Use Arrow compute kernel to filter
        arrow::compute::ExecContext ctx;
        ARROW_ASSIGN_OR_RAISE(
            auto filtered_datum,
            arrow::compute::Filter(batch, mask, arrow::compute::FilterOptions::Defaults(), &ctx)
        );

        auto filtered_batch = filtered_datum.record_batch();

        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
        stats_.execution_time_ms += duration.count() / 1000.0;

        stats_.rows_processed += batch->num_rows();
        stats_.batches_processed++;
        stats_.bytes_processed += batch->num_rows() * batch->schema()->num_fields() * sizeof(int64_t);

        // Only return non-empty batches
        if (filtered_batch->num_rows() > 0) {
            return filtered_batch;
        }

        start = std::chrono::high_resolution_clock::now();
    }

    return nullptr;
}

std::string FilterOperator::ToString() const {
    std::ostringstream oss;
    oss << "Filter(" << predicate_description_ << ")\n";
    oss << "  └─ " << input_->ToString();
    return oss.str();
}

size_t FilterOperator::EstimateCardinality() const {
    // Apply selectivity estimate to input cardinality
    return static_cast<size_t>(input_->EstimateCardinality() * selectivity_);
}

// ProjectOperator implementation
ProjectOperator::ProjectOperator(
    std::shared_ptr<Operator> input,
    const std::vector<std::string>& column_names)
    : UnaryOperator(std::move(input)),
      column_names_(column_names) {

    // Resolve column names to indices
    auto schema_result = input_->GetOutputSchema();
    if (!schema_result.ok()) {
        throw std::runtime_error("Failed to get input schema");
    }
    auto schema = schema_result.ValueOrDie();

    for (const auto& name : column_names) {
        int idx = schema->GetFieldIndex(name);
        if (idx < 0) {
            throw std::invalid_argument("Column not found: " + name);
        }
        column_indices_.push_back(idx);
    }
}

ProjectOperator::ProjectOperator(
    std::shared_ptr<Operator> input,
    const std::vector<int>& column_indices)
    : UnaryOperator(std::move(input)),
      column_indices_(column_indices) {

    // Validate indices
    auto schema_result = input_->GetOutputSchema();
    if (!schema_result.ok()) {
        throw std::runtime_error("Failed to get input schema");
    }
    auto schema = schema_result.ValueOrDie();

    for (int idx : column_indices) {
        if (idx < 0 || idx >= schema->num_fields()) {
            throw std::invalid_argument("Invalid column index: " + std::to_string(idx));
        }
        column_names_.push_back(schema->field(idx)->name());
    }
}

arrow::Result<std::shared_ptr<arrow::Schema>> ProjectOperator::GetOutputSchema() const {
    ARROW_ASSIGN_OR_RAISE(auto input_schema, input_->GetOutputSchema());

    std::vector<std::shared_ptr<arrow::Field>> fields;
    for (int idx : column_indices_) {
        fields.push_back(input_schema->field(idx));
    }

    return arrow::schema(fields);
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> ProjectOperator::GetNextBatch() {
    ARROW_ASSIGN_OR_RAISE(auto batch, input_->GetNextBatch());
    if (!batch) {
        return nullptr;
    }

    // Select columns
    std::vector<std::shared_ptr<arrow::Array>> columns;
    for (int idx : column_indices_) {
        columns.push_back(batch->column(idx));
    }

    ARROW_ASSIGN_OR_RAISE(auto output_schema, GetOutputSchema());

    stats_.rows_processed += batch->num_rows();
    stats_.batches_processed++;

    return arrow::RecordBatch::Make(output_schema, batch->num_rows(), columns);
}

std::string ProjectOperator::ToString() const {
    std::ostringstream oss;
    oss << "Project(";
    for (size_t i = 0; i < column_names_.size(); ++i) {
        if (i > 0) oss << ", ";
        oss << column_names_[i];
    }
    oss << ")\n";
    oss << "  └─ " << input_->ToString();
    return oss.str();
}

size_t ProjectOperator::EstimateCardinality() const {
    // Projection doesn't change cardinality
    return input_->EstimateCardinality();
}

// LimitOperator implementation
LimitOperator::LimitOperator(std::shared_ptr<Operator> input, size_t limit)
    : UnaryOperator(std::move(input)),
      limit_(limit) {}

arrow::Result<std::shared_ptr<arrow::Schema>> LimitOperator::GetOutputSchema() const {
    return input_->GetOutputSchema();
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> LimitOperator::GetNextBatch() {
    if (rows_returned_ >= limit_) {
        return nullptr;
    }

    ARROW_ASSIGN_OR_RAISE(auto batch, input_->GetNextBatch());
    if (!batch) {
        return nullptr;
    }

    size_t rows_needed = limit_ - rows_returned_;

    if (static_cast<size_t>(batch->num_rows()) <= rows_needed) {
        // Return entire batch
        rows_returned_ += batch->num_rows();
        stats_.rows_processed += batch->num_rows();
        stats_.batches_processed++;
        return batch;
    } else {
        // Return partial batch
        auto sliced = batch->Slice(0, rows_needed);
        rows_returned_ += rows_needed;
        stats_.rows_processed += rows_needed;
        stats_.batches_processed++;
        return sliced;
    }
}

bool LimitOperator::HasNextBatch() const {
    return rows_returned_ < limit_ && input_->HasNextBatch();
}

std::string LimitOperator::ToString() const {
    std::ostringstream oss;
    oss << "Limit(" << limit_ << ")\n";
    oss << "  └─ " << input_->ToString();
    return oss.str();
}

size_t LimitOperator::EstimateCardinality() const {
    return std::min(limit_, input_->EstimateCardinality());
}

// DistinctOperator implementation
DistinctOperator::DistinctOperator(std::shared_ptr<Operator> input)
    : UnaryOperator(std::move(input)) {}

arrow::Result<std::shared_ptr<arrow::Schema>> DistinctOperator::GetOutputSchema() const {
    return input_->GetOutputSchema();
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> DistinctOperator::GetNextBatch() {
    while (input_->HasNextBatch()) {
        ARROW_ASSIGN_OR_RAISE(auto batch, input_->GetNextBatch());
        if (!batch) {
            continue;
        }

        // Build unique batch
        std::vector<size_t> unique_indices;

        for (int64_t i = 0; i < batch->num_rows(); ++i) {
            // Create row hash
            std::ostringstream row_hash;
            for (int col = 0; col < batch->num_columns(); ++col) {
                auto array = batch->column(col);
                // Simple hash: convert to string representation
                // TODO: Use proper hash function for better performance
                if (array->IsNull(i)) {
                    row_hash << "NULL;";
                } else {
                    auto scalar = array->GetScalar(i);
                    if (scalar.ok()) {
                        row_hash << scalar.ValueOrDie()->ToString() << ";";
                    }
                }
            }

            std::string hash = row_hash.str();
            if (seen_rows_.find(hash) == seen_rows_.end()) {
                seen_rows_.insert(hash);
                unique_indices.push_back(i);
            }
        }

        if (!unique_indices.empty()) {
            // Build unique batch by taking rows at unique_indices
            // Use Arrow compute kernel
            arrow::Int64Builder index_builder;
            ARROW_RETURN_NOT_OK(index_builder.AppendValues(
                reinterpret_cast<const int64_t*>(unique_indices.data()),
                unique_indices.size()
            ));

            ARROW_ASSIGN_OR_RAISE(auto indices, index_builder.Finish());

            arrow::compute::ExecContext ctx;
            ARROW_ASSIGN_OR_RAISE(
                auto taken_datum,
                arrow::compute::Take(batch, indices, arrow::compute::TakeOptions::Defaults(), &ctx)
            );

            stats_.rows_processed += batch->num_rows();
            stats_.batches_processed++;

            return taken_datum.record_batch();
        }
    }

    return nullptr;
}

std::string DistinctOperator::ToString() const {
    std::ostringstream oss;
    oss << "Distinct()\n";
    oss << "  └─ " << input_->ToString();
    return oss.str();
}

size_t DistinctOperator::EstimateCardinality() const {
    // Assume 50% reduction (conservative estimate)
    return input_->EstimateCardinality() / 2;
}

} // namespace sabot_ql
