#include <sabot_ql/execution/scan_operator.h>
#include <sabot_ql/storage/triple_store.h>
#include <sabot_ql/operators/metadata.h>
#include <sabot_ql/util/logging.h>
#include <arrow/api.h>
#include <sstream>
#include <chrono>
#include <iostream>
#include <algorithm>

namespace sabot_ql {

// ScanOperator: Physical operator that wraps TripleStore::ScanPattern()
// This is the basic building block for all SPARQL queries
//
// Example:
//   TriplePattern pattern{.subject = 5, .predicate = std::nullopt, .object = std::nullopt};
//   ScanOperator scan(store, pattern);
//   auto result = scan.GetAllResults();  // Returns Arrow table with all matches

ScanOperator::ScanOperator(
    std::shared_ptr<TripleStore> store,
    const TriplePattern& pattern,
    const std::string& description,
    const std::unordered_map<std::string, std::string>& var_bindings)
    : store_(std::move(store))
    , pattern_(pattern)
    , description_(description)
    , var_bindings_(var_bindings)
    , executed_(false) {

    // Validate pattern
    if (!store_) {
        throw std::invalid_argument("TripleStore cannot be null");
    }
}

arrow::Result<std::shared_ptr<arrow::Schema>> ScanOperator::GetOutputSchema() const {
    // Schema depends on which variables are unbound
    std::vector<std::shared_ptr<arrow::Field>> fields;

    // Helper to create field with metadata
    auto make_field = [this](const std::string& col_name) -> std::shared_ptr<arrow::Field> {
        auto field = arrow::field(col_name, arrow::int64());

        // Attach triple position metadata
        auto with_pos = metadata::AttachTriplePosition(field, col_name);
        if (with_pos.ok()) {
            field = *with_pos;
        }

        // Attach SPARQL variable metadata if available
        auto it = var_bindings_.find(col_name);
        if (it != var_bindings_.end()) {
            auto with_var = metadata::AttachSPARQLVariable(field, it->second);
            if (with_var.ok()) {
                field = *with_var;
            }
        }

        return field;
    };

    if (!pattern_.subject.has_value()) {
        fields.push_back(make_field("subject"));
    }
    if (!pattern_.predicate.has_value()) {
        fields.push_back(make_field("predicate"));
    }
    if (!pattern_.object.has_value()) {
        fields.push_back(make_field("object"));
    }

    if (fields.empty()) {
        // All bound - return boolean result (match or no match)
        fields.push_back(arrow::field("match", arrow::boolean()));
    }

    return arrow::schema(fields);
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> ScanOperator::GetNextBatch() {
    SABOT_LOG_SCAN("GetNextBatch: Starting (executed=" << executed_ << ")");
    if (executed_) {
        SABOT_LOG_SCAN("Already executed, returning nullptr");
        // Already returned all results
        return nullptr;
    }

    auto start_time = std::chrono::high_resolution_clock::now();

    // Execute scan
    SABOT_LOG_SCAN("Calling store_->ScanPattern()");
    ARROW_ASSIGN_OR_RAISE(auto result_table, store_->ScanPattern(pattern_));
    SABOT_LOG_SCAN("ScanPattern returned table with " << result_table->num_rows() << " rows, " << result_table->num_columns() << " cols");

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

    // Update stats
    stats_.rows_processed = result_table->num_rows();
    stats_.batches_processed = 1;
    stats_.execution_time_ms = duration.count();

    executed_ = true;

    // Convert table to single batch
    if (result_table->num_rows() == 0) {
        SABOT_LOG_SCAN("Empty result, creating empty batch");
        // Empty result
        ARROW_ASSIGN_OR_RAISE(auto schema, GetOutputSchema());
        SABOT_LOG_SCAN("Output schema has " << schema->num_fields() << " fields");
        std::vector<std::shared_ptr<arrow::Array>> empty_arrays;
        for (int i = 0; i < schema->num_fields(); ++i) {
            SABOT_LOG_SCAN("  Creating empty array for field " << i << ": " << schema->field(i)->name());
            ARROW_ASSIGN_OR_RAISE(auto empty_array, arrow::MakeArrayOfNull(schema->field(i)->type(), 0));
            empty_arrays.push_back(empty_array);
        }
        SABOT_LOG_SCAN("Creating empty batch with " << empty_arrays.size() << " arrays");
        return arrow::RecordBatch::Make(schema, 0, empty_arrays);
    }

    // Convert table to record batch
    ARROW_ASSIGN_OR_RAISE(auto combined_table, result_table->CombineChunks());

    // Extract arrays from table
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    for (int i = 0; i < combined_table->num_columns(); ++i) {
        arrays.push_back(combined_table->column(i)->chunk(0));
    }

    // Get the expected output schema (with semantic column names)
    ARROW_ASSIGN_OR_RAISE(auto expected_schema, GetOutputSchema());

    // Return batch with expected schema (renames col1/col2/col3 → subject/predicate/object)
    return arrow::RecordBatch::Make(expected_schema, combined_table->num_rows(), arrays);
}

bool ScanOperator::HasNextBatch() const {
    return !executed_;
}

std::string ScanOperator::ToString() const {
    if (!description_.empty()) {
        return description_;
    }

    // Generate description from pattern
    std::ostringstream oss;
    oss << "Scan(";

    if (pattern_.subject.has_value()) {
        oss << "S=" << pattern_.subject.value();
    } else {
        oss << "S=?";
    }
    oss << ", ";

    if (pattern_.predicate.has_value()) {
        oss << "P=" << pattern_.predicate.value();
    } else {
        oss << "P=?";
    }
    oss << ", ";

    if (pattern_.object.has_value()) {
        oss << "O=" << pattern_.object.value();
    } else {
        oss << "O=?";
    }
    oss << ")";

    return oss.str();
}

// Estimate cardinality using triple store statistics
size_t ScanOperator::EstimateCardinality() const {
    auto result = store_->EstimateCardinality(pattern_);
    if (result.ok()) {
        return result.ValueOrDie();
    }
    return 0;
}

OrderingProperty ScanOperator::GetOutputOrdering() const {
    // Determine which index the store will use for this pattern
    IndexType index = store_->SelectIndex(pattern_);

    OrderingProperty ordering;

    // Index scans return naturally sorted data based on the index type
    // We only include unbound (output) columns in the ordering

    std::vector<std::string> output_cols;
    if (!pattern_.subject.has_value()) output_cols.push_back("subject");
    if (!pattern_.predicate.has_value()) output_cols.push_back("predicate");
    if (!pattern_.object.has_value()) output_cols.push_back("object");

    if (output_cols.empty()) {
        // All bound - no output columns, no ordering
        return ordering;
    }

    // Define index orderings (full triple order)
    std::vector<std::string> index_order;
    switch (index) {
        case IndexType::SPO:
            index_order = {"subject", "predicate", "object"};
            break;
        case IndexType::POS:
            index_order = {"predicate", "object", "subject"};
            break;
        case IndexType::OSP:
            index_order = {"object", "subject", "predicate"};
            break;
    }

    // Build ordering from index order, keeping only output columns
    for (const auto& col : index_order) {
        if (std::find(output_cols.begin(), output_cols.end(), col) != output_cols.end()) {
            ordering.columns.push_back(col);
            ordering.directions.push_back(SortDirection::Ascending);
        }
    }

    return ordering;
}

} // namespace sabot_ql
