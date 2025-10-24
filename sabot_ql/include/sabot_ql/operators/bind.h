#pragma once

#include <sabot_ql/operators/operator.h>
#include <arrow/compute/expression.h>
#include <arrow/compute/api.h>
#include <string>
#include <memory>

namespace sabot_ql {

/**
 * BIND Operator
 *
 * Implements SPARQL BIND clause by evaluating an expression and adding the
 * result as a new column to the input batches.
 *
 * Example SPARQL:
 *   SELECT ?name ?ageNextYear WHERE {
 *     ?person :name ?name .
 *     ?person :age ?age .
 *     BIND(?age + 1 AS ?ageNextYear)
 *   }
 *
 * Uses Arrow Expression system for evaluation:
 * - Leverages Arrow's SIMD-optimized compute kernels
 * - Zero-copy where possible
 * - Supports nested expressions
 * - 150+ built-in functions available
 *
 * Implementation:
 * 1. Input batch flows through
 * 2. Expression is evaluated using arrow::compute::ExecuteScalarExpression
 * 3. Result is appended as new column with alias name
 * 4. Output schema = input schema + new column
 */
class BindOperator : public UnaryOperator {
public:
    /**
     * Create BIND operator
     *
     * @param input Input operator providing data
     * @param expression Arrow Expression to evaluate
     * @param alias Name of new column to create
     * @param expression_str String representation for debugging/EXPLAIN
     */
    BindOperator(std::shared_ptr<Operator> input,
                 arrow::compute::Expression expression,
                 std::string alias,
                 std::string expression_str = "");

    arrow::Result<std::shared_ptr<arrow::Schema>> GetOutputSchema() const override;
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> GetNextBatch() override;
    std::string ToString() const override;
    size_t EstimateCardinality() const override;

private:
    arrow::compute::Expression expression_;
    std::string alias_;
    std::string expression_str_;  // For debugging/EXPLAIN
    mutable std::shared_ptr<arrow::Schema> output_schema_;
};

} // namespace sabot_ql
