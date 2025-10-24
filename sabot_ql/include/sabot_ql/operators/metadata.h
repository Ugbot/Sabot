#pragma once

#include <arrow/api.h>
#include <string>
#include <optional>
#include <unordered_map>

namespace sabot_ql {
namespace metadata {

/**
 * @brief Arrow Field Metadata Utilities
 *
 * Arrow supports attaching key-value metadata to fields and schemas.
 * We use this for:
 * - SPARQL variable tracking (sparql_var)
 * - Triple position tracking (triple_pos: subject/predicate/object)
 * - Provenance information (source, operator, etc.)
 *
 * Metadata flows through all Arrow operations automatically, enabling:
 * - Variable name propagation through operator chains
 * - Debugging and query provenance
 * - Optimization hints
 *
 * Example metadata:
 *   Field: "subject" (int64)
 *   Metadata: {"sparql_var": "s", "triple_pos": "subject"}
 */

// Metadata keys (constants for consistency)
// Using inline to avoid ODR violations across translation units
inline constexpr const char* KEY_SPARQL_VAR = "sparql_var";
inline constexpr const char* KEY_TRIPLE_POS = "triple_pos";
inline constexpr const char* KEY_SOURCE_OPERATOR = "source_op";

/**
 * @brief Attach SPARQL variable name to field metadata
 * @param field Original field
 * @param var_name SPARQL variable name (without '?' prefix)
 * @return New field with metadata attached
 */
arrow::Result<std::shared_ptr<arrow::Field>> AttachSPARQLVariable(
    const std::shared_ptr<arrow::Field>& field,
    const std::string& var_name
);

/**
 * @brief Get SPARQL variable name from field metadata
 * @param field Field to query
 * @return Variable name if present, nullopt otherwise
 */
std::optional<std::string> GetSPARQLVariable(
    const std::shared_ptr<arrow::Field>& field
);

/**
 * @brief Attach triple position to field metadata
 * @param field Original field
 * @param position Position string ("subject", "predicate", or "object")
 * @return New field with metadata attached
 */
arrow::Result<std::shared_ptr<arrow::Field>> AttachTriplePosition(
    const std::shared_ptr<arrow::Field>& field,
    const std::string& position
);

/**
 * @brief Get triple position from field metadata
 * @param field Field to query
 * @return Position if present, nullopt otherwise
 */
std::optional<std::string> GetTriplePosition(
    const std::shared_ptr<arrow::Field>& field
);

/**
 * @brief Add SPARQL metadata to entire schema
 * @param schema Original schema
 * @param var_bindings Map from column name to SPARQL variable name
 * @return New schema with metadata attached to fields
 *
 * Example:
 *   var_bindings = {{"subject", "s"}, {"object", "o"}}
 *   Result: Fields "subject" and "object" get sparql_var metadata
 */
arrow::Result<std::shared_ptr<arrow::Schema>> AddSPARQLMetadata(
    const std::shared_ptr<arrow::Schema>& schema,
    const std::unordered_map<std::string, std::string>& var_bindings
);

/**
 * @brief Extract SPARQL variable bindings from schema metadata
 * @param schema Schema to query
 * @return Map from column name to SPARQL variable name
 */
std::unordered_map<std::string, std::string> ExtractSPARQLBindings(
    const std::shared_ptr<arrow::Schema>& schema
);

/**
 * @brief Merge metadata from two fields
 * @param field1 First field
 * @param field2 Second field (takes precedence on conflicts)
 * @return New field with merged metadata
 *
 * Useful for joins where fields from different sources are combined
 */
arrow::Result<std::shared_ptr<arrow::Field>> MergeMetadata(
    const std::shared_ptr<arrow::Field>& field1,
    const std::shared_ptr<arrow::Field>& field2
);

/**
 * @brief Copy metadata from one field to another
 * @param source_field Field to copy metadata from
 * @param target_field Field to copy metadata to
 * @return New target field with source metadata
 */
arrow::Result<std::shared_ptr<arrow::Field>> CopyMetadata(
    const std::shared_ptr<arrow::Field>& source_field,
    const std::shared_ptr<arrow::Field>& target_field
);

} // namespace metadata
} // namespace sabot_ql
