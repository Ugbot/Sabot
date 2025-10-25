#include <sabot_ql/operators/rename.h>
#include <sabot_ql/util/logging.h>
#include <arrow/api.h>
#include <sstream>
#include <iostream>

namespace sabot_ql {

RenameOperator::RenameOperator(
    std::shared_ptr<Operator> input,
    const std::unordered_map<std::string, std::string>& renamings)
    : UnaryOperator(std::move(input))
    , renamings_(renamings) {

    if (!input_) {
        throw std::invalid_argument("RenameOperator: input operator cannot be null");
    }
}

arrow::Result<std::shared_ptr<arrow::Schema>> RenameOperator::GetOutputSchema() const {
    if (!cached_output_schema_) {
        ARROW_ASSIGN_OR_RAISE(cached_output_schema_, BuildOutputSchema());
    }
    return cached_output_schema_;
}

arrow::Result<std::shared_ptr<arrow::Schema>> RenameOperator::BuildOutputSchema() const {
    // Get input schema
    ARROW_ASSIGN_OR_RAISE(auto input_schema, input_->GetOutputSchema());

    // Build new schema with renamed fields
    std::vector<std::shared_ptr<arrow::Field>> output_fields;
    output_fields.reserve(input_schema->num_fields());

    for (int i = 0; i < input_schema->num_fields(); ++i) {
        auto field = input_schema->field(i);
        auto old_name = field->name();

        // Check if this field should be renamed
        auto it = renamings_.find(old_name);
        if (it != renamings_.end()) {
            // Rename: create new field with same type and metadata
            auto new_name = it->second;
            output_fields.push_back(arrow::field(
                new_name,
                field->type(),
                field->nullable(),
                field->metadata()  // Preserve metadata (important!)
            ));
        } else {
            // Keep original field
            output_fields.push_back(field);
        }
    }

    return arrow::schema(output_fields);
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> RenameOperator::GetNextBatch() {
    SABOT_LOG_RENAME("GetNextBatch: Starting");
    // Get input batch
    SABOT_LOG_RENAME("Calling input_->GetNextBatch()");
    ARROW_ASSIGN_OR_RAISE(auto input_batch, input_->GetNextBatch());
    SABOT_LOG_RENAME("input_->GetNextBatch() returned");

    if (!input_batch) {
        SABOT_LOG_RENAME("input_batch is null, returning nullptr");
        // End of stream
        return nullptr;
    }
    SABOT_LOG_RENAME("input_batch has " << input_batch->num_rows() << " rows, " << input_batch->num_columns() << " cols");

    // Get output schema (with renamed columns)
    SABOT_LOG_RENAME("Calling GetOutputSchema()");
    ARROW_ASSIGN_OR_RAISE(auto output_schema, GetOutputSchema());
    SABOT_LOG_RENAME("GetOutputSchema() returned");

    // Zero-copy: just wrap same columns with new schema
    // No data is copied, only schema metadata changes
    SABOT_LOG_RENAME("Creating renamed batch");
    return arrow::RecordBatch::Make(
        output_schema,
        input_batch->num_rows(),
        input_batch->columns()
    );
}

std::string RenameOperator::ToString() const {
    std::ostringstream oss;
    oss << "Rename(";

    size_t count = 0;
    for (const auto& [old_name, new_name] : renamings_) {
        if (count > 0) oss << ", ";
        oss << old_name << " → " << new_name;
        count++;
    }

    oss << ")";
    return oss.str();
}

size_t RenameOperator::EstimateCardinality() const {
    // Rename doesn't change row count
    return input_->EstimateCardinality();
}

} // namespace sabot_ql
