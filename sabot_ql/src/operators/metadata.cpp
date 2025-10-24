#include <sabot_ql/operators/metadata.h>
#include <arrow/api.h>

namespace sabot_ql {
namespace metadata {

arrow::Result<std::shared_ptr<arrow::Field>> AttachSPARQLVariable(
    const std::shared_ptr<arrow::Field>& field,
    const std::string& var_name) {

    if (!field) {
        return arrow::Status::Invalid("AttachSPARQLVariable: field cannot be null");
    }

    // Get existing metadata or create new
    std::shared_ptr<arrow::KeyValueMetadata> metadata;
    if (field->metadata()) {
        metadata = field->metadata()->Copy();
    } else {
        metadata = std::make_shared<arrow::KeyValueMetadata>();
    }

    // Add or update SPARQL variable key
    ARROW_RETURN_NOT_OK(metadata->Set(KEY_SPARQL_VAR, var_name));

    // Create new field with updated metadata
    return arrow::field(
        field->name(),
        field->type(),
        field->nullable(),
        metadata
    );
}

std::optional<std::string> GetSPARQLVariable(
    const std::shared_ptr<arrow::Field>& field) {

    if (!field->metadata()) {
        return std::nullopt;
    }

    auto index = field->metadata()->FindKey(KEY_SPARQL_VAR);
    if (index == -1) {
        return std::nullopt;
    }

    return field->metadata()->value(index);
}

arrow::Result<std::shared_ptr<arrow::Field>> AttachTriplePosition(
    const std::shared_ptr<arrow::Field>& field,
    const std::string& position) {

    if (!field) {
        return arrow::Status::Invalid("AttachTriplePosition: field cannot be null");
    }

    // Get existing metadata or create new
    std::shared_ptr<arrow::KeyValueMetadata> metadata;
    if (field->metadata()) {
        metadata = field->metadata()->Copy();
    } else {
        metadata = std::make_shared<arrow::KeyValueMetadata>();
    }

    // Add or update triple position key
    ARROW_RETURN_NOT_OK(metadata->Set(KEY_TRIPLE_POS, position));

    // Create new field with updated metadata
    return arrow::field(
        field->name(),
        field->type(),
        field->nullable(),
        metadata
    );
}

std::optional<std::string> GetTriplePosition(
    const std::shared_ptr<arrow::Field>& field) {

    if (!field->metadata()) {
        return std::nullopt;
    }

    auto index = field->metadata()->FindKey(KEY_TRIPLE_POS);
    if (index == -1) {
        return std::nullopt;
    }

    return field->metadata()->value(index);
}

arrow::Result<std::shared_ptr<arrow::Schema>> AddSPARQLMetadata(
    const std::shared_ptr<arrow::Schema>& schema,
    const std::unordered_map<std::string, std::string>& var_bindings) {

    std::vector<std::shared_ptr<arrow::Field>> output_fields;
    output_fields.reserve(schema->num_fields());

    for (int i = 0; i < schema->num_fields(); ++i) {
        auto field = schema->field(i);
        auto field_name = field->name();

        // Check if this field has a SPARQL variable binding
        auto it = var_bindings.find(field_name);
        if (it != var_bindings.end()) {
            // Attach variable metadata
            ARROW_ASSIGN_OR_RAISE(
                auto field_with_metadata,
                AttachSPARQLVariable(field, it->second)
            );
            output_fields.push_back(field_with_metadata);
        } else {
            // Keep original field
            output_fields.push_back(field);
        }
    }

    return arrow::schema(output_fields);
}

std::unordered_map<std::string, std::string> ExtractSPARQLBindings(
    const std::shared_ptr<arrow::Schema>& schema) {

    std::unordered_map<std::string, std::string> bindings;

    for (int i = 0; i < schema->num_fields(); ++i) {
        auto field = schema->field(i);
        auto var_name = GetSPARQLVariable(field);
        if (var_name.has_value()) {
            bindings[field->name()] = *var_name;
        }
    }

    return bindings;
}

arrow::Result<std::shared_ptr<arrow::Field>> MergeMetadata(
    const std::shared_ptr<arrow::Field>& field1,
    const std::shared_ptr<arrow::Field>& field2) {

    // Start with field1's metadata
    std::shared_ptr<arrow::KeyValueMetadata> merged;
    if (field1->metadata()) {
        merged = field1->metadata()->Copy();
    } else {
        merged = std::make_shared<arrow::KeyValueMetadata>();
    }

    // Add field2's metadata (overwrites on conflict)
    if (field2->metadata()) {
        for (int64_t i = 0; i < field2->metadata()->size(); ++i) {
            auto key = field2->metadata()->key(i);
            auto value = field2->metadata()->value(i);
            ARROW_RETURN_NOT_OK(merged->Set(key, value));
        }
    }

    // Create new field with merged metadata
    return arrow::field(
        field1->name(),
        field1->type(),
        field1->nullable(),
        merged
    );
}

arrow::Result<std::shared_ptr<arrow::Field>> CopyMetadata(
    const std::shared_ptr<arrow::Field>& source_field,
    const std::shared_ptr<arrow::Field>& target_field) {

    // Copy metadata from source to target
    return arrow::field(
        target_field->name(),
        target_field->type(),
        target_field->nullable(),
        source_field->metadata()  // Use source's metadata
    );
}

} // namespace metadata
} // namespace sabot_ql
