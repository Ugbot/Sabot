#pragma once

#include <sabot_ql/operators/operator.h>
#include <arrow/api.h>
#include <memory>
#include <string>
#include <unordered_map>

namespace sabot_ql {

/**
 * @brief RenameOperator: Renames columns without copying data
 *
 * Arrow-native zero-copy column renaming. Changes only the schema,
 * not the actual data arrays.
 *
 * Design: Separation of concerns
 * - ProjectOperator: Selects which columns to output
 * - RenameOperator: Changes column names
 * - Together: Full projection control
 *
 * Use cases:
 * - SPARQL: Map storage columns (subject/predicate/object) to variables (s/p/o)
 * - SQL: Implement AS aliases
 * - Streaming: Standardize column names across sources
 *
 * Performance: O(1) - only creates new schema, no data copy
 *
 * Example:
 *   Input schema:  {subject: int64, predicate: int64, object: int64}
 *   Renamings:     {{"subject", "s"}, {"object", "o"}}
 *   Output schema: {s: int64, predicate: int64, o: int64}
 *
 * Arrow Integration:
 * - Uses arrow::Field and arrow::Schema for zero-copy
 * - Preserves field metadata (important for SPARQL variable tracking)
 * - Works with any Arrow data type
 */
class RenameOperator : public UnaryOperator {
public:
    /**
     * @brief Construct rename operator
     * @param input Input operator
     * @param renamings Map from old column names to new names
     *                  Columns not in map keep their original names
     */
    RenameOperator(
        std::shared_ptr<Operator> input,
        const std::unordered_map<std::string, std::string>& renamings
    );

    // Operator interface
    arrow::Result<std::shared_ptr<arrow::Schema>> GetOutputSchema() const override;
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> GetNextBatch() override;
    std::string ToString() const override;
    size_t EstimateCardinality() const override;

private:
    std::unordered_map<std::string, std::string> renamings_;

    // Build output schema with renamed columns (cached)
    mutable std::shared_ptr<arrow::Schema> cached_output_schema_;
    arrow::Result<std::shared_ptr<arrow::Schema>> BuildOutputSchema() const;
};

} // namespace sabot_ql
